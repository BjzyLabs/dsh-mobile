package com.clarklevis.dsh.shared

import com.clarklevis.dsh.shared.facade.SharedHistoryBootstrap
import com.clarklevis.dsh.shared.facade.SharedHistoryEffect
import com.clarklevis.dsh.shared.facade.SharedHistoryPatch
import com.clarklevis.dsh.shared.facade.SharedHistoryStore
import com.clarklevis.dsh.shared.facade.SharedMviEvent
import com.clarklevis.dsh.shared.facade.SharedMviEventObserver
import com.clarklevis.dsh.shared.protocol.GatewayEvent
import com.clarklevis.dsh.shared.protocol.SessionEvent
import com.clarklevis.dsh.shared.protocol.ToolDelta
import com.clarklevis.dsh.shared.protocol.wireJson
import com.clarklevis.dsh.shared.sync.HistorySyncConfiguration
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SharedHistoryStoreTest {
    @Test
    fun paginationStateAndNextCursorEffectShareOneTransaction() {
        val store = SharedHistoryStore()
        val received = mutableListOf<SharedMviEvent>()
        store.subscribe(SharedMviEventObserver(received::add))
        assertEquals(1, wireJson.decodeFromString<SharedHistoryBootstrap>(received.single().statePayloadJson!!).schema)

        assertTrue(store.start("s1", older = false, hasLocalEvents = false, earliestLocalSequence = null).accepted)
        assertEquals(SharedHistoryEffect("request-page", "s1", null), effects(received.last()).single())
        assertTrue(patch(received.last()).session?.isLoading == true)

        store.processingStarted("s1", rawEventCount = 2, hasMore = true)
        val page = listOf(event(10, "ten"), event(11, "eleven"))
        store.pageReceived(
            sessionId = "s1",
            eventsJson = wireJson.encodeToString(page),
            byteCount = 100,
            hasMore = true,
            nextBeforeSequence = 9,
            remoteActivityTimestamp = 20.0
        )
        val next = patch(received.last())
        assertEquals("replace", next.eventPatch?.kind)
        assertEquals(listOf(10, 11), next.eventPatch?.replacementEvents?.map { it.seq })
        assertEquals(SharedHistoryEffect("request-page", "s1", 9), effects(received.last()).single())

        store.pageReceived(
            sessionId = "s1",
            eventsJson = wireJson.encodeToString(listOf(event(8, "eight"), event(9, "nine"))),
            byteCount = 80,
            hasMore = false,
            nextBeforeSequence = null,
            remoteActivityTimestamp = 20.0
        )
        val completed = patch(received.last())
        assertEquals("completed", completed.outcome)
        assertFalse(completed.session!!.isLoading)
        assertEquals(20.0, completed.session.syncedActivityTimestamp)
        assertTrue(effects(received.last()).isEmpty())
    }

    @Test
    fun liveTailWinsHistoryDuplicatesAndAdvancesWatermark() {
        val store = SharedHistoryStore()
        val received = mutableListOf<SharedMviEvent>()
        store.subscribe(SharedMviEventObserver(received::add))
        store.start("s1", older = false, hasLocalEvents = false, earliestLocalSequence = null)
        store.pageReceived(
            "s1",
            wireJson.encodeToString(listOf(event(1, "history"))),
            byteCount = 10,
            hasMore = false,
            nextBeforeSequence = null,
            remoteActivityTimestamp = 1.0
        )

        store.liveEventReceived(wireJson.encodeToString(event(2, "live")))
        assertEquals("append", patch(received.last()).eventPatch?.kind)
        assertEquals(2.0, patch(received.last()).session?.syncedActivityTimestamp)

        store.start("s1", older = false, hasLocalEvents = true, earliestLocalSequence = 1)
        store.pageReceived(
            "s1",
            wireJson.encodeToString(listOf(event(2, "stale-history"), event(0, "older"))),
            byteCount = 10,
            hasMore = false,
            nextBeforeSequence = null,
            remoteActivityTimestamp = 2.0
        )
        val rebased = patch(received.last()).eventPatch!!.replacementEvents!!
        assertEquals(listOf(0, 1, 2), rebased.map { it.seq })
        assertEquals("live", rebased.last().event.text)
    }

    @Test
    fun duplicateAndOutOfOrderLiveEventsUseIndexedUpsert() {
        val store = SharedHistoryStore()
        val received = mutableListOf<SharedMviEvent>()
        store.subscribe(SharedMviEventObserver(received::add))

        store.liveEventReceived(wireJson.encodeToString(event(2, "two")))
        store.liveEventReceived(wireJson.encodeToString(event(1, "one")))
        var update = patch(received.last()).eventPatch!!
        assertEquals("upsert", update.kind)
        assertEquals(0, update.index)

        store.liveEventReceived(wireJson.encodeToString(event(2, "two-final")))
        update = patch(received.last()).eventPatch!!
        assertEquals("upsert", update.kind)
        assertEquals(1, update.index)
        assertEquals("two-final", update.record?.event?.text)
    }

    @Test
    fun malformedPageAndCursorLoopFailClosedWithoutReplacingEvents() {
        val store = SharedHistoryStore(HistorySyncConfiguration(pagesPerBatch = 3))
        val received = mutableListOf<SharedMviEvent>()
        store.subscribe(SharedMviEventObserver(received::add))
        store.liveEventReceived(wireJson.encodeToString(event(1, "local")))
        val beforeErrorCount = received.size

        val malformed = store.pageReceived(
            "s1", "not-json", 1, false, null, null
        )
        assertFalse(malformed.accepted)
        assertEquals(beforeErrorCount + 1, received.size)
        assertEquals("error", received.last().kind)

        store.start("s1", older = false, hasLocalEvents = true, earliestLocalSequence = 1)
        store.pageReceived(
            "s1", wireJson.encodeToString(listOf(event(1, "local"))), 1, true, 5, null
        )
        store.pageReceived(
            "s1", wireJson.encodeToString(emptyList<SessionEvent>()), 0, true, 5, null
        )
        val failed = patch(received.last())
        assertEquals("failed", failed.outcome)
        assertEquals("REPEATED_CURSOR", failed.failureCode)
        assertNull(failed.eventPatch)
    }

    // --- Streaming deltas share one turn-level `session.seq` ------------------

    @Test
    fun sameSequenceAssistantChunksAccumulateInsteadOfCollapsingToTheLastOne() {
        val store = SharedHistoryStore()
        val received = mutableListOf<SharedMviEvent>()
        store.subscribe(SharedMviEventObserver(received::add))

        // The first chunk of a step arrives at the turn's watermark and takes
        // the ordinary append path.
        assertTrue(store.liveEventReceived(wireJson.encodeToString(chunk(7, "Hel"))).accepted)
        assertEquals("append", patch(received.last()).eventPatch?.kind)

        // Everything after it shares that watermark. Two separate requirements
        // are pinned here:
        //   * the patch must be classified as a streaming delta, not the `upsert`
        //     that made the platform rebuild the whole projection once per token;
        //     and
        //   * the retained journal record must ACCUMULATE, because a same-sequence
        //     record that merely replaced its predecessor is exactly how the
        //     intermediate fragments were lost ("text appears one word at a
        //     time"). The patch record and the retained record differ on purpose.
        assertTrue(store.liveEventReceived(wireJson.encodeToString(chunk(7, "lo"))).accepted)
        var streamed = patch(received.last()).eventPatch!!
        assertEquals("stream", streamed.kind)
        // The patch carries the DELTA, because the conversation lane applies patch
        // records to a row it already holds. The retained journal record is the
        // accumulated one; the two are different contracts and both are asserted.
        assertEquals("lo", streamed.record?.event?.text)
        assertEquals("Hello", retained(store, "s1").single { it.seq == 7 }.event.text)

        assertTrue(store.liveEventReceived(wireJson.encodeToString(chunk(7, " there"))).accepted)
        streamed = patch(received.last()).eventPatch!!
        assertEquals("stream", streamed.kind)
        assertEquals(" there", streamed.record?.event?.text)
        assertEquals("Hello there", retained(store, "s1").single { it.seq == 7 }.event.text)

        assertTrue(store.liveEventReceived(wireJson.encodeToString(chunk(7, "!"))).accepted)

        // The authoritative finalization at the SAME durable seq must still be
        // accepted as the final block; dropping it would lose the finished text.
        assertTrue(store.liveEventReceived(wireJson.encodeToString(event(7, "Hello there!"))).accepted)
        val final = patch(received.last()).eventPatch!!
        assertEquals("upsert", final.kind)
        assertEquals("Hello there!", final.record?.event?.text)

        // Nothing may have been consumed by a whole-list replacement.
        assertNull(final.replacementEvents)
    }

    @Test
    fun sameSequenceReasoningAndToolCallDeltasTakeTheStreamPath() {
        val store = SharedHistoryStore()
        val received = mutableListOf<SharedMviEvent>()
        store.subscribe(SharedMviEventObserver(received::add))

        assertTrue(store.liveEventReceived(wireJson.encodeToString(reasoning(4, "thin"))).accepted)
        assertEquals("append", patch(received.last()).eventPatch?.kind)
        assertTrue(store.liveEventReceived(wireJson.encodeToString(reasoning(4, "king"))).accepted)
        var streamed = patch(received.last()).eventPatch!!
        assertEquals("stream", streamed.kind)
        assertEquals("king", streamed.record?.event?.text)
        assertEquals("thinking", retained(store, "s1").single { it.seq == 4 }.event.text)

        assertTrue(store.liveEventReceived(wireJson.encodeToString(toolCall(5, "{\"a\""))).accepted)
        assertEquals("append", patch(received.last()).eventPatch?.kind)
        assertTrue(store.liveEventReceived(wireJson.encodeToString(toolCall(5, ":1}"))).accepted)
        streamed = patch(received.last()).eventPatch!!
        assertEquals("stream", streamed.kind)
        assertEquals(":1}", streamed.record?.event?.tool?.argumentsDelta)
        assertEquals("{\"a\":1}", retained(store, "s1").single { it.seq == 5 }.event.tool?.argumentsDelta)
    }

    @Test
    fun nonDeltaChunksAndUnknownChunkTypesAreForwardedAsStreamsNotDropped() {
        val store = SharedHistoryStore()
        val received = mutableListOf<SharedMviEvent>()
        store.subscribe(SharedMviEventObserver(received::add))

        assertTrue(store.liveEventReceived(wireJson.encodeToString(chunk(9, "x"))).accepted)
        val before = received.size

        // `usage`/`finish`/`block-start`/`block-end` carry no display text but
        // do carry correlation and accounting, so they must be routed like any
        // other same-sequence chunk. An unknown chunk type must travel the same
        // way rather than being dropped to make a test pass.
        listOf("usage", "finish", "block-start", "block-end", "some-future-chunk-type").forEach { type ->
            assertTrue(
                store.liveEventReceived(wireJson.encodeToString(rawChunk(9, type))).accepted,
                "chunk type $type was rejected"
            )
            val forwarded = patch(received.last()).eventPatch!!
            assertEquals("stream", forwarded.kind, "chunk type $type was not forwarded as a stream")
            assertEquals(type, forwarded.record?.event?.chunkType)
        }
        assertTrue(received.size > before)
    }

    @Test
    fun outOfOrderGapFillStillTakesTheAuthoritativeUpsertPath() {
        val store = SharedHistoryStore()
        val received = mutableListOf<SharedMviEvent>()
        store.subscribe(SharedMviEventObserver(received::add))

        store.liveEventReceived(wireJson.encodeToString(chunk(5, "five")))
        // A chunk for an EARLIER watermark is a gap fill, not a delta: it inserts
        // a new position and must keep the rebaseline path.
        store.liveEventReceived(wireJson.encodeToString(chunk(2, "two")))
        val gapFill = patch(received.last()).eventPatch!!
        assertEquals("upsert", gapFill.kind)
        assertEquals(0, gapFill.index)
    }

    private fun patch(event: SharedMviEvent): SharedHistoryPatch =
        wireJson.decodeFromString(event.statePayloadJson!!)

    /**
     * The retained journal as the store would hand it to a new subscriber. A
     * fresh subscription bootstraps the current `eventsBySession`, which is how
     * the accumulated record is observable from the outside.
     */
    private fun retained(store: SharedHistoryStore, sessionId: String): List<SessionEvent> {
        val bootstrap = mutableListOf<SharedMviEvent>()
        store.subscribe(SharedMviEventObserver(bootstrap::add))
        return wireJson.decodeFromString<SharedHistoryBootstrap>(bootstrap.single().statePayloadJson!!)
            .eventsBySession[sessionId].orEmpty()
    }

    private fun effects(event: SharedMviEvent): List<SharedHistoryEffect> =
        wireJson.decodeFromString(event.effectsJson)

    private fun event(sequence: Int, text: String) = SessionEvent(
        sessionId = "s1",
        seq = sequence,
        time = sequence.toDouble(),
        event = GatewayEvent(type = "assistant/message", text = text)
    )

    private fun chunk(sequence: Int, text: String) = SessionEvent(
        sessionId = "s1",
        seq = sequence,
        time = sequence.toDouble(),
        event = GatewayEvent(
            type = "assistant/chunk",
            turn = 1,
            step = 1,
            chunkType = "text-delta",
            text = text
        )
    )

    private fun reasoning(sequence: Int, text: String) = SessionEvent(
        sessionId = "s1",
        seq = sequence,
        time = sequence.toDouble(),
        event = GatewayEvent(
            type = "assistant/chunk",
            turn = 1,
            step = 1,
            chunkType = "reasoning-delta",
            text = text
        )
    )

    private fun toolCall(sequence: Int, argumentsDelta: String) = SessionEvent(
        sessionId = "s1",
        seq = sequence,
        time = sequence.toDouble(),
        event = GatewayEvent(
            type = "assistant/chunk",
            turn = 1,
            step = 1,
            chunkType = "tool-call-delta",
            tool = ToolDelta(id = "call-1", name = "run_code", argumentsDelta = argumentsDelta)
        )
    )

    private fun rawChunk(sequence: Int, chunkType: String) = SessionEvent(
        sessionId = "s1",
        seq = sequence,
        time = sequence.toDouble(),
        event = GatewayEvent(type = "assistant/chunk", turn = 1, step = 1, chunkType = chunkType)
    )
}
