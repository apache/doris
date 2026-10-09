// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.plugin.audit;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.catalog.InternalSchema;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.plugin.AuditEvent;

import com.google.common.base.Splitter;
import com.google.common.collect.Queues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class AuditLoaderTest {

    @Test
    public void testAssembleAuditIsSerializedWithLoadLock() throws Exception {
        AuditLoader auditLoader = new AuditLoader();
        AuditEvent auditEvent = new AuditEvent.AuditEventBuilder()
                .setQueryId("query-in-shared-monitor-test")
                .setTimestamp(1L)
                .setStmt("select 1")
                .build();

        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread assembleThread = new Thread(() -> {
            started.countDown();
            try {
                Deencapsulation.invoke(auditLoader, "assembleAudit", auditEvent);
            } catch (Throwable t) {
                error.set(t);
            }
        });

        synchronized (auditLoader) {
            assembleThread.start();
            Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));
            Assertions.assertTrue(waitForBlocked(assembleThread));
            Assertions.assertFalse(getAuditLogBuffer(auditLoader).contains(auditEvent.queryId));
        }

        assembleThread.join(5000);
        Assertions.assertFalse(assembleThread.isAlive());
        if (error.get() != null) {
            throw new AssertionError("failed to assemble audit event", error.get());
        }
        Assertions.assertTrue(getAuditLogBuffer(auditLoader).contains(auditEvent.queryId));
    }

    /**
     * The queue -> batch transfer must be ATOMIC with the horizon read. The
     * old worker polled the event (it left the queue) and only then assembled it into the
     * batch (still unaccounted): a reader running in between saw it in NEITHER structure
     * and reported "nothing outstanding" while an accepted event was unpublished - the SPM
     * capture could then advance its watermark past the row the loader eventually wrote.
     * The transfer takes the loader monitor, exactly like the horizon read.
     */
    @Test
    public void testHorizonCoversAnInFlightQueueToBatchTransfer() throws Exception {
        AuditLoader loader = new AuditLoader();
        BlockingQueue<AuditEvent> queue = Queues.newLinkedBlockingDeque();
        setPrivateField(loader, "auditEventQueue", queue);
        setRunningLoader(loader);
        AtomicReference<Throwable> error = new AtomicReference<>();
        try {
            queue.add(event(4000L));
            Thread transfer = new Thread(() -> {
                try {
                    Deencapsulation.invoke(loader, "transferNextEvent");
                } catch (Throwable t) {
                    error.set(t);
                }
            });
            synchronized (loader) {
                transfer.start();
                Assertions.assertTrue(waitForBlocked(transfer),
                        "the transfer must wait for the monitor the reader is holding");
                // the transfer CANNOT be half-done: the event is still in the queue ...
                Assertions.assertEquals(1, queue.size(),
                        "the poll must happen under the monitor, not before it");
                // ... and the horizon (same monitor) sees it
                Assertions.assertEquals(4000L, AuditLoader.oldestUnpublishedEventTime(),
                        "an accepted event is always visible to the horizon");
            }
            transfer.join(5000);
            Assertions.assertFalse(transfer.isAlive());
            if (error.get() != null) {
                throw new AssertionError("transferNextEvent failed", error.get());
            }
            Assertions.assertEquals(0, queue.size());
            Assertions.assertEquals(4000L, AuditLoader.oldestUnpublishedEventTime(),
                    "after the transfer the event is in the BATCH, still unpublished");
        } finally {
            setRunningLoader(null);
        }
    }

    private boolean waitForBlocked(Thread thread) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            if (thread.getState() == Thread.State.BLOCKED) {
                return true;
            }
            Thread.sleep(10);
        }
        return false;
    }

    private String getAuditLogBuffer(AuditLoader auditLoader) {
        StringBuilder buffer = Deencapsulation.getField(auditLoader, "auditLogBuffer");
        return buffer.toString();
    }

    // O07: raw 0x1F/0x1E in user-controlled fields must not be able to add/remove columns or rows.
    // A statement carrying the framing bytes (e.g. inside a block comment) must still produce exactly
    // one row with the same column count as a clean statement -- otherwise the attacker forges a row.
    @Test
    public void testDelimiterInjectionDoesNotAlterFraming() {
        AuditLoader auditLoader = new AuditLoader();
        char col = AuditLoader.AUDIT_TABLE_COL_SEPARATOR;
        char line = AuditLoader.AUDIT_TABLE_LINE_DELIMITER;

        StringBuilder clean = new StringBuilder();
        Deencapsulation.invoke(auditLoader, "fillLogBuffer",
                new AuditEvent.AuditEventBuilder()
                        .setUser("alice").setDb("mydb").setStmt("select 1").build(),
                clean);

        // The forged payload tries to close its own row and inject a fully attacker-controlled one.
        // Inject into stmt, user, db AND planTimesMs -- planTimesMs is a String column that is easy
        // to overlook (its name suggests a number), so exercising it guards against a column
        // silently bypassing the sanitizer.
        String evilStmt = "select 1 /*" + line + "deadbeef" + col + "2026-01-01 00:00:00.000"
                + col + "10.0.0.9" + col + "root" + col + "DROP TABLE finance.ledger*/";
        StringBuilder evil = new StringBuilder();
        Deencapsulation.invoke(auditLoader, "fillLogBuffer",
                new AuditEvent.AuditEventBuilder()
                        .setUser("al" + col + "ice").setDb("my" + line + "db")
                        .setPlanTimesMs("plan:" + col + "1ms" + line + "forged")
                        .setStmt(evilStmt).build(),
                evil);

        // Exactly one row, and the same number of columns as the clean event.
        Assertions.assertEquals(count(clean, line), count(evil, line), "injected 0x1E must not add rows");
        Assertions.assertEquals(1, count(evil, line), "one row per event");
        Assertions.assertEquals(count(clean, col), count(evil, col), "injected 0x1F must not add columns");
        // The forged tokens survive only as inert text, never as framing bytes.
        Assertions.assertTrue(evil.toString().contains("DROP TABLE finance.ledger"));
    }

    // The sanitizer must be a no-op for ordinary statements: no data loss, no mutation.
    @Test
    public void testCleanStatementIsPreserved() {
        AuditLoader auditLoader = new AuditLoader();
        StringBuilder buffer = new StringBuilder();
        Deencapsulation.invoke(auditLoader, "fillLogBuffer",
                new AuditEvent.AuditEventBuilder()
                        .setUser("bob").setDb("sales")
                        .setStmt("select * from t where a = 1 and b = 'x'").build(),
                buffer);
        Assertions.assertTrue(buffer.toString().contains("select * from t where a = 1 and b = 'x'"));
        Assertions.assertEquals(1, count(buffer, AuditLoader.AUDIT_TABLE_LINE_DELIMITER));
    }

    // The row written for the audit_log table is read by position, under the columns of
    // InternalSchema.AUDIT_SCHEMA: it must have exactly those columns, in that order.
    @Test
    public void testRowHasTheColumnsOfTheAuditSchemaInOrder() {
        AuditLoader auditLoader = new AuditLoader();
        StringBuilder buffer = new StringBuilder();
        Deencapsulation.invoke(auditLoader, "fillLogBuffer",
                new AuditEvent.AuditEventBuilder()
                        .setUser("alice").setCloudCluster("cg1").setProtocol("ArrowFlightSQL")
                        .setStmt("select 1").build(),
                buffer);
        String row = buffer.toString();
        Assertions.assertEquals(AuditLoader.AUDIT_TABLE_LINE_DELIMITER, row.charAt(row.length() - 1));
        List<String> columns = Splitter.on(AuditLoader.AUDIT_TABLE_COL_SEPARATOR)
                .splitToList(row.substring(0, row.length() - 1));
        List<String> names = InternalSchema.AUDIT_SCHEMA.stream().map(ColumnDef::getName)
                .collect(Collectors.toList());
        Assertions.assertEquals(names.size(), columns.size(), "columns of the row: " + columns);
        Assertions.assertEquals("alice", columns.get(names.indexOf("user")));
        Assertions.assertEquals("cg1", columns.get(names.indexOf("compute_group")));
        Assertions.assertEquals("ArrowFlightSQL", columns.get(names.indexOf("protocol")));
        Assertions.assertEquals("select 1", columns.get(names.indexOf("stmt")));
        Assertions.assertEquals(names.size() - 1, names.indexOf("stmt"));
    }

    // The SPM capture overlaps its scan window by the LOCAL loader's
    // outstanding queue horizon. The horizon is the OLDEST event the loader has accepted but
    // not published yet - the assembled batch counts as well, and enqueue order is NOT
    // event-time order (the upstream hold releases events by completion), so every queued
    // event is examined.
    @Test
    public void testOldestUnpublishedEventTimeTracksTheQueueHorizon() throws Exception {
        AuditLoader loader = new AuditLoader();
        BlockingQueue<AuditEvent> queue = Queues.newLinkedBlockingDeque();
        // production fields: the queue the worker drains and the batch currently assembled
        setPrivateField(loader, "auditEventQueue", queue);
        setRunningLoader(loader);
        try {
            Assertions.assertEquals(0L, AuditLoader.oldestUnpublishedEventTime(),
                    "nothing outstanding = no publication delay to retain");

            queue.add(event(5000L));
            queue.add(event(3000L));
            queue.add(event(7000L));
            Assertions.assertEquals(3000L, AuditLoader.oldestUnpublishedEventTime(),
                    "the OLDEST queued event fences the window, not the queue order");

            // the assembled (not yet flushed) batch is unpublished as well
            setPrivateField(loader, "batchOldestEventTime", 1000L);
            Assertions.assertEquals(1000L, AuditLoader.oldestUnpublishedEventTime(),
                    "the assembled batch is part of the horizon");

            // publishing the batch clears that half of the horizon
            Deencapsulation.invoke(loader, "resetBatch", 0L);
            Assertions.assertEquals(3000L, AuditLoader.oldestUnpublishedEventTime());

            queue.clear();
            Assertions.assertEquals(0L, AuditLoader.oldestUnpublishedEventTime(),
                    "a drained queue leaves nothing outstanding");
        } finally {
            setRunningLoader(null);
        }
        Assertions.assertEquals(0L, AuditLoader.oldestUnpublishedEventTime(),
                "no running loader = no known delay");
    }

    // A stream load can return Publish Timeout AFTER commit while its rows
    // stay unreadable. The batch must be fenced until publication is confirmed - the
    // previous unconditional batch reset let the capture advance past those rows, so
    // their late publication landed behind the watermark.
    @Test
    public void testPublishTimeoutKeepsFencingUntilTheRowsAreReadable() throws Exception {
        AuditLoader loader = new AuditLoader();
        setPrivateField(loader, "auditEventQueue", Queues.newLinkedBlockingDeque());
        setRunningLoader(loader);
        List<Long> probes = new ArrayList<>();
        boolean[] readable = {false};
        AuditLoader.publishVisibilityProbeForTest = (eventTime, queryId) -> {
            probes.add(eventTime);
            return readable[0];
        };
        try {
            // the load reported Publish Timeout: the batch's oldest event (and its sample
            // query id) fences progress even though the batch itself was reset
            Deencapsulation.invoke(loader, "retainPublishFence", 10_000L, "qid-timeout");
            Deencapsulation.invoke(loader, "resetBatch", System.currentTimeMillis());
            Assertions.assertEquals(10_000L, AuditLoader.oldestUnpublishedEventTime(),
                    "the committed-but-unreadable batch keeps fencing after the batch reset");

            // still unreadable: the fence holds
            Deencapsulation.invoke(loader, "confirmPublishFence");
            Assertions.assertEquals(10_000L, AuditLoader.oldestUnpublishedEventTime(),
                    "an unconfirmable / still-invisible batch must keep fencing");

            // the delayed publication lands: the fence is released
            readable[0] = true;
            Deencapsulation.invoke(loader, "confirmPublishFence");
            Assertions.assertEquals(0L, AuditLoader.oldestUnpublishedEventTime(),
                    "once the rows are readable the fence must be released");
            Assertions.assertEquals(Arrays.asList(10_000L, 10_000L), probes,
                    "the probe must ask for the fenced batch's own event time: " + probes);

            // a fence that never becomes readable is released after the retention bound
            long base = System.currentTimeMillis();
            AuditLoader.publishFenceClockForTest = () -> base;
            Deencapsulation.invoke(loader, "retainPublishFence", 20_000L, "qid-lost");
            readable[0] = false;
            AuditLoader.publishFenceClockForTest =
                    () -> base + AuditLoader.PUBLISH_FENCE_MAX_MILLIS + 1;
            Deencapsulation.invoke(loader, "confirmPublishFence");
            Assertions.assertEquals(0L, AuditLoader.oldestUnpublishedEventTime(),
                    "a batch that never becomes readable must not fence forever");
        } finally {
            AuditLoader.publishVisibilityProbeForTest = null;
            AuditLoader.publishFenceClockForTest = null;
            setRunningLoader(null);
        }
    }

    // EVERY timed-out batch is retained and confirmed SEPARATELY. Keeping
    // only the OLDEST batch's sample released the whole fence when that sample became
    // visible although a newer batch B could still be committed but unreadable; the
    // capture then checkpointed past B and once the watermark moved, later windows could
    // never reach B's rows.
    @Test
    public void testEachTimedOutBatchFencesUntilItsOwnRowsAreReadable() throws Exception {
        AuditLoader loader = new AuditLoader();
        setPrivateField(loader, "auditEventQueue", Queues.newLinkedBlockingDeque());
        setRunningLoader(loader);
        // event times whose sample row is (becomes) readable
        Set<Long> visible = new HashSet<>();
        AuditLoader.publishVisibilityProbeForTest =
                (eventTime, queryId) -> visible.contains(eventTime);
        try {
            Deencapsulation.invoke(loader, "retainPublishFence", 10_000L, "qid-a");
            Deencapsulation.invoke(loader, "retainPublishFence", 30_000L, "qid-b");
            Assertions.assertEquals(2, loader.pendingPublishFenceCountForTest(),
                    "both timed-out batches must be tracked");
            Assertions.assertEquals(10_000L, AuditLoader.oldestUnpublishedEventTime(),
                    "the oldest pending batch owns the horizon value");

            // A becomes readable FIRST: only A's fence may be released - B's rows can
            // still be committed but unreadable
            visible.add(10_000L);
            Deencapsulation.invoke(loader, "confirmPublishFence");
            Assertions.assertEquals(1, loader.pendingPublishFenceCountForTest(),
                    "B's fence must survive A's visibility");
            Assertions.assertEquals(30_000L, AuditLoader.oldestUnpublishedEventTime(),
                    "B keeps fencing progress on its own event time");

            // B publishes later, in the OPPOSITE order of the timeouts: its own sample
            // probe releases it
            visible.add(30_000L);
            Deencapsulation.invoke(loader, "confirmPublishFence");
            Assertions.assertEquals(0, loader.pendingPublishFenceCountForTest(),
                    "each batch is released by its OWN sample becoming readable");
            Assertions.assertEquals(0L, AuditLoader.oldestUnpublishedEventTime(),
                    "nothing is outstanding once both batches published");
        } finally {
            AuditLoader.publishVisibilityProbeForTest = null;
            AuditLoader.publishFenceClockForTest = null;
            setRunningLoader(null);
        }
    }

    // The comment-round fix: a parse-failure event is audited under the SHARED query id
    // "NaN" (AuditLogHelper renders a null id as NaN). A LATER batch's visible NaN row
    // satisfies the row probe (query_id = 'NaN' AND time >= sampleTime), so a "visible"
    // answer proves NOTHING about the fenced batch - it could release the fence while the
    // batch is still COMMITTED and unreadable, and the capture would checkpoint past a
    // SELECT the batch still owes. The fence must survive until the batch's OWN
    // transaction reaches a terminal state (VISIBLE = readable after all, ABORTED = can
    // never appear).
    @Test
    public void testSharedNanQueryIdCannotProvePublication() throws Exception {
        AuditLoader loader = new AuditLoader();
        setPrivateField(loader, "auditEventQueue", Queues.newLinkedBlockingDeque());
        setRunningLoader(loader);
        String[] status = {"COMMITTED"};
        List<Long> probes = new ArrayList<>();
        AuditLoader.publishVisibilityProbeForTest = (eventTime, queryId) -> {
            probes.add(eventTime);
            return true; // a LATER batch's NaN row is visible - and proves nothing
        };
        AuditLoader.transactionStatusForTest = label -> status[0];
        try {
            Deencapsulation.invoke(loader, "retainPublishFence", 10_000L, "NaN", "lbl-nan", "");
            Deencapsulation.invoke(loader, "confirmPublishFence");
            Assertions.assertEquals(1, loader.pendingPublishFenceCountForTest(),
                    "a probe that only sees the shared NaN id must not release the fence");
            Assertions.assertEquals(0, probes.size(),
                    "the NaN fence must not ask the row probe at all: " + probes);

            // the transaction turns VISIBLE: the label's terminal state releases the fence
            status[0] = "VISIBLE";
            Deencapsulation.invoke(loader, "confirmPublishFence");
            Assertions.assertEquals(0, loader.pendingPublishFenceCountForTest(),
                    "the batch's own terminal transaction state releases the fence");
        } finally {
            AuditLoader.publishVisibilityProbeForTest = null;
            AuditLoader.transactionStatusForTest = null;
            setRunningLoader(null);
        }
    }

    // /: the response is the only evidence of the load's real
    // outcome, and only a COMPLETE, parseable Success response proves publication. An
    // unreadable body says nothing - the earlier "does not contain 'publish timeout'"
    // test treated it as published, so a committed Publish Timeout whose body read
    // failed reset the batch WITHOUT a fence.
    @Test
    public void testBatchPublicationConfirmationReadsTheLoadResponse() {
        Assertions.assertFalse(AuditLoader.batchPublicationConfirmed(null),
                "no response object = nothing confirmed");
        Assertions.assertFalse(AuditLoader.batchPublicationConfirmed(
                new AuditStreamLoader.LoadResponse(200, "OK",
                        "{\"Status\": \"Publish Timeout\", \"TxnId\": 7}")),
                "Publish Timeout is committed but NOT visible: it must fence");
        Assertions.assertFalse(AuditLoader.batchPublicationConfirmed(
                new AuditStreamLoader.LoadResponse(500, "Internal error", "oops")),
                "a non-OK status never proves publication");
        Assertions.assertTrue(AuditLoader.batchPublicationConfirmed(
                new AuditStreamLoader.LoadResponse(200, "OK",
                        "{\"Status\": \"Success\", \"TxnId\": 7}")),
                "a clean parsed success is published");
        Assertions.assertFalse(AuditLoader.batchPublicationConfirmed(
                new AuditStreamLoader.LoadResponse(200, "OK", null)),
                "an OK response WITHOUT a readable body says nothing about the transaction");
        Assertions.assertFalse(AuditLoader.batchPublicationConfirmed(
                new AuditStreamLoader.LoadResponse(200, "OK",
                        "{\"Status\": \"Success\", \"TxnId\": 7}", false)),
                "a body the reader gave up on (incomplete) is AMBIGUOUS, never published");
        Assertions.assertFalse(AuditLoader.batchPublicationConfirmed(
                new AuditStreamLoader.LoadResponse(200, "OK", "{\"Status\": \"Fail\"}")),
                "any non-Success status keeps fencing (bounded by the publish-fence bound)");
        Assertions.assertFalse(AuditLoader.batchPublicationConfirmed(
                new AuditStreamLoader.LoadResponse(200, "OK", "<html>proxy error</html>")),
                "an unparsable body is ambiguous, never published");
    }

    // The zone a row's time column is RENDERED in is the zone that must be
    // registered for it - two independent reads of the global time_zone could observe a
    // `SET GLOBAL time_zone` in between (and back before the next report), leaving the
    // row stored under a zone nobody had registered and a capture skipping it forever.
    @Test
    public void testWriterZoneRegistrationMatchesTheRenderedTime() throws Exception {
        AuditLoader loader = new AuditLoader();
        // simulate the zone switching between what used to be TWO reads: the second
        // "read" returns another zone
        java.util.concurrent.atomic.AtomicInteger reads =
                new java.util.concurrent.atomic.AtomicInteger();
        AuditWriterZones.resetForTest();
        AuditWriterZones.currentWriterZoneForTest =
                () -> reads.getAndIncrement() == 0 ? "UTC" : "Asia/Tokyo";
        try {
            long ts = 1_780_000_000_000L;
            AuditEvent event = new AuditEvent.AuditEventBuilder()
                    .setQueryId("qid-zone").setTimestamp(ts).setStmt("select 1").build();
            StringBuilder buffer = new StringBuilder();
            Deencapsulation.invoke(loader, "fillLogBuffer", event, buffer);
            List<String> fields = Splitter.on(AuditLoader.AUDIT_TABLE_COL_SEPARATOR)
                    .splitToList(buffer.toString());
            Assertions.assertEquals(1, reads.get(),
                    "the writer zone must be read ONCE for both the registration and the"
                            + " rendering");
            Assertions.assertEquals(Set.of("UTC"), AuditWriterZones.zones(),
                    "the zone the row was rendered in is the registered one");
            Assertions.assertEquals(TimeUtils.longToTimeStringWithms(ts, "UTC"), fields.get(1),
                    "the time column is rendered in the SAME zone that was registered: "
                            + fields.get(1));
        } finally {
            AuditWriterZones.currentWriterZoneForTest = null;
            AuditWriterZones.resetForTest();
        }
    }

    // A Publish-Timeout batch is COMMITTED, so closing this FE must not
    // clear its durable fence - the rows may become readable after the FE stopped. With
    // no such batch the row IS cleared (the capture must not wait for a gone FE).
    @Test
    public void testCloseKeepsCommittedFencesUntilTheyPublish() throws Exception {
        AuditLoader loader = new AuditLoader();
        setPrivateField(loader, "auditEventQueue", Queues.newLinkedBlockingDeque());
        setRunningLoader(loader);
        List<Long> reports = new ArrayList<>();
        AuditPublicationHorizon.localHorizonWriterForTest = horizon -> {
            reports.add(horizon);
            return true;
        };
        try {
            // an empty SUCCESSFUL restore read = a live environment whose previous
            // incarnation left no row (see readOwnRowsForRestore): the restore completes,
            // so the close may act on this FE's own obligations
            AuditPublicationHorizon.ownRowRestoreReaderForTest =
                    () -> java.util.Collections.emptyList();
            Deencapsulation.invoke(loader, "retainPublishFence", 10_000L, "qid-timeout");
            Assertions.assertEquals(10_000L, AuditLoader.oldestCommittedPublishFenceEventTime(),
                    "the retained batch is the committed fence of this FE");
            reports.clear();
            loader.close();
            Assertions.assertEquals(Arrays.asList(10_000L), reports,
                    "close() must keep the committed fence instead of clearing the row: "
                            + reports);

            // control: no committed batch -> the row is de-registered on close
            AuditLoader clean = new AuditLoader();
            setPrivateField(clean, "auditEventQueue", Queues.newLinkedBlockingDeque());
            setRunningLoader(clean);
            reports.clear();
            clean.close();
            Assertions.assertTrue(reports.isEmpty(),
                    "with nothing committed the row is DELETED instead of re-reported as a"
                            + " zero fence (round-42 #12: a zero report would upsert the"
                            + " shutting-down FE's row back into the shared table): "
                            + reports);
        } finally {
            AuditPublicationHorizon.localHorizonWriterForTest = null;
            AuditPublicationHorizon.ownRowRestoreReaderForTest = null;
            setRunningLoader(null);
        }
    }

    // The OLDEST pending fence must keep fencing even after it was EVICTED
    // from the bounded list - dropping it released the fence of the oldest committed
    // batch (its rows can still become readable).
    @Test
    public void testEvictedOldestFenceKeepsFencing() throws Exception {
        AuditLoader loader = new AuditLoader();
        setPrivateField(loader, "auditEventQueue", Queues.newLinkedBlockingDeque());
        setRunningLoader(loader);
        try {
            Deencapsulation.invoke(loader, "retainPublishFence", 10_000L, "qid-oldest");
            for (int i = 0; i < AuditLoader.MAX_PENDING_PUBLISH_FENCES + 5; i++) {
                Deencapsulation.invoke(loader, "retainPublishFence", 20_000L + i, "qid-" + i);
            }
            Assertions.assertEquals(AuditLoader.MAX_PENDING_PUBLISH_FENCES,
                    loader.pendingPublishFenceCountForTest(),
                    "the list itself stays bounded");
            Assertions.assertEquals(10_000L, AuditLoader.oldestCommittedPublishFenceEventTime(),
                    "the EVICTED oldest batch keeps fencing: its rows can still publish"
                            + " after this FE stops");
        } finally {
            setRunningLoader(null);
        }
    }

    // Internal statements (e.g. the horizon reporter's own SQL) are never
    // captured, so they must not fence progress - otherwise the reporter's writes keep
    // their own FE's fence (and thereby further writes) alive forever on an idle FE.
    @Test
    public void testInternalEventsDoNotFenceTheLoaderHorizon() throws Exception {
        AuditLoader loader = new AuditLoader();
        BlockingQueue<AuditEvent> queue = Queues.newLinkedBlockingDeque();
        setPrivateField(loader, "auditEventQueue", queue);
        setRunningLoader(loader);
        try {
            queue.add(internalEvent(4000L));
            Assertions.assertEquals(0L, AuditLoader.oldestUnpublishedEventTime(),
                    "a queued INTERNAL event can never be captured and must not fence");

            Deencapsulation.invoke(loader, "transferNextEvent");
            Assertions.assertEquals(0L, AuditLoader.oldestUnpublishedEventTime(),
                    "an internal event entering the assembled batch must not fence either");

            queue.add(event(5000L));
            Deencapsulation.invoke(loader, "transferNextEvent");
            Assertions.assertEquals(5000L, AuditLoader.oldestUnpublishedEventTime(),
                    "a user event in the same batch still fences");
        } finally {
            setRunningLoader(null);
        }
    }

    // (AuditLoader 746): elapsed time is NOT proof of loss - a Publish Timeout
    // batch can stay COMMITTED and unreadable beyond the age bound while the publish
    // daemon keeps retrying. The fence is released by the transaction OUTCOME
    // (VISIBLE / ABORTED); only an unresolvable state keeps the age bound as the last
    // resort, so a genuinely lost batch cannot fence the capture forever.
    @Test
    public void testCommittedFenceOutlivesTheAgeBoundUntilTerminal() throws Exception {
        AuditLoader loader = new AuditLoader();
        setPrivateField(loader, "auditEventQueue", Queues.newLinkedBlockingDeque());
        setRunningLoader(loader);
        java.util.concurrent.atomic.AtomicReference<String> status =
                new java.util.concurrent.atomic.AtomicReference<>("COMMITTED");
        AuditLoader.publishVisibilityProbeForTest = (eventTime, queryId) -> false;
        AuditLoader.transactionStatusForTest = label -> status.get();
        long base = System.currentTimeMillis();
        long pastBound = base + AuditLoader.PUBLISH_FENCE_MAX_MILLIS + 1;
        try {
            AuditLoader.publishFenceClockForTest = () -> base;
            Deencapsulation.invoke(loader, "retainPublishFence", 10_000L, "qid-committed",
                    "audit_log_label");
            AuditLoader.publishFenceClockForTest = () -> pastBound;
            Deencapsulation.invoke(loader, "confirmPublishFence");
            Assertions.assertEquals(10_000L, AuditLoader.oldestUnpublishedEventTime(),
                    "a COMMITTED transaction keeps fencing past the age bound: its publish"
                            + " daemon may still make the rows readable");

            // the daemon gives up / the transaction is aborted: NOW the fence may go
            status.set("ABORTED");
            Deencapsulation.invoke(loader, "confirmPublishFence");
            Assertions.assertEquals(0L, AuditLoader.oldestUnpublishedEventTime(),
                    "an ABORTED transaction can never publish");

            // VISIBLE releases as well - the rows ARE there, a failing probe
            // notwithstanding
            Deencapsulation.invoke(loader, "retainPublishFence", 30_000L, "qid-visible",
                    "audit_log_label");
            status.set("VISIBLE");
            AuditLoader.publishFenceClockForTest =
                    () -> base + 2 * AuditLoader.PUBLISH_FENCE_MAX_MILLIS + 2;
            Deencapsulation.invoke(loader, "confirmPublishFence");
            Assertions.assertEquals(0L, AuditLoader.oldestUnpublishedEventTime(),
                    "a VISIBLE transaction's rows are readable by definition");

            // an UNRESOLVABLE outcome (no transaction manager / no record of the label)
            // falls back to the retention bound
            AuditLoader.publishFenceClockForTest = () -> base + 3 * AuditLoader.PUBLISH_FENCE_MAX_MILLIS;
            Deencapsulation.invoke(loader, "retainPublishFence", 50_000L, "qid-lost",
                    "audit_log_label");
            status.set(null);
            AuditLoader.publishFenceClockForTest =
                    () -> base + 4 * AuditLoader.PUBLISH_FENCE_MAX_MILLIS + 1;
            Deencapsulation.invoke(loader, "confirmPublishFence");
            Assertions.assertEquals(0L, AuditLoader.oldestUnpublishedEventTime(),
                    "an unresolvable outcome keeps the age bound as the last resort");
        } finally {
            AuditLoader.publishVisibilityProbeForTest = null;
            AuditLoader.transactionStatusForTest = null;
            AuditLoader.publishFenceClockForTest = null;
            setRunningLoader(null);
        }
    }

    // (AuditLoader 675): the overflow aggregate cleared EVERY member when the
    // FIRST one's 30-minute window elapsed. A committed batch that overflowed into it at
    // minute 29 lost its fence at minute 30 - without any visibility proof and before its
    // own deadline - and the capture could checkpoint past a row that publishes later.
    // The aggregate must outlive its NEWEST member's own deadline.
    @Test
    public void testOverflowAggregateKeepsEachBatchThroughItsOwnDeadline() throws Exception {
        AuditLoader loader = new AuditLoader();
        setPrivateField(loader, "auditEventQueue", Queues.newLinkedBlockingDeque());
        setRunningLoader(loader);
        AuditLoader.publishVisibilityProbeForTest = (eventTime, queryId) -> false;
        long base = System.currentTimeMillis();
        try {
            AuditLoader.publishFenceClockForTest = () -> base;
            Deencapsulation.invoke(loader, "retainPublishFence", 10_000L, "qid-oldest",
                    "lid-oldest");
            // minute 29: a burst of later, individually unreadable batches overflows the
            // bounded list and pushes the oldest batch into the aggregate
            long overflowAt = base + AuditLoader.PUBLISH_FENCE_MAX_MILLIS - 60_000L;
            AuditLoader.publishFenceClockForTest = () -> overflowAt;
            for (int i = 0; i < AuditLoader.MAX_PENDING_PUBLISH_FENCES + 5; i++) {
                Deencapsulation.invoke(loader, "retainPublishFence", 20_000L + i, "qid-" + i,
                        "lid-" + i);
            }
            Assertions.assertEquals(AuditLoader.MAX_PENDING_PUBLISH_FENCES,
                    loader.pendingPublishFenceCountForTest(),
                    "the retained list itself stays bounded");

            // minute 31: the OLDEST batch's own 30-minute window elapsed, but the batch
            // overflowed at minute 29 keeps its fence until minute 59
            AuditLoader.publishFenceClockForTest =
                    () -> base + AuditLoader.PUBLISH_FENCE_MAX_MILLIS + 60_000L;
            Deencapsulation.invoke(loader, "confirmPublishFence");
            Assertions.assertEquals(10_000L, AuditLoader.oldestCommittedPublishFenceEventTime(),
                    "an overflowed batch keeps its fence through its OWN deadline");
            Assertions.assertEquals(10_000L, AuditLoader.oldestUnpublishedEventTime(),
                    "the LOCAL horizon (the faster of the two capture surfaces) covers the"
                            + " aggregate as well");

            // past EVERY member's deadline the age bound alone must NOT release the
            // aggregate: the retained labels are resolved first, and an unresolvable
            // label keeps it for one more survival window (the bounded-out batches may
            // still publish)
            AuditLoader.transactionStatusForTest = label -> null;
            long expiredAt = base + 2 * AuditLoader.PUBLISH_FENCE_MAX_MILLIS + 60_000L;
            AuditLoader.publishFenceClockForTest = () -> expiredAt;
            Deencapsulation.invoke(loader, "confirmPublishFence");
            Assertions.assertEquals(10_000L, AuditLoader.oldestCommittedPublishFenceEventTime(),
                    "an unresolvable label outcome keeps the aggregate past every deadline");

            // a COMMITTED member keeps it WITHOUT any age bound: its batch may still
            // publish, so neither the deadline nor the survival window may release it
            AuditLoader.transactionStatusForTest = label -> "COMMITTED";
            AuditLoader.publishFenceClockForTest = () -> expiredAt
                    + AuditLoader.PUBLISH_FENCE_MAX_MILLIS
                    + AuditPublicationHorizon.COMMITTED_FENCE_SURVIVAL_MILLIS + 60_000L;
            Deencapsulation.invoke(loader, "confirmPublishFence");
            Assertions.assertEquals(10_000L, AuditLoader.oldestCommittedPublishFenceEventTime(),
                    "a COMMITTED member keeps the aggregate regardless of age");

            // once every retained label resolves to a TERMINAL state the aggregate
            // releases (the label resolution outranks the age/survival fallback)
            AuditLoader.transactionStatusForTest = label -> "VISIBLE";
            AuditLoader.publishFenceClockForTest = () -> expiredAt
                    + 2 * AuditLoader.PUBLISH_FENCE_MAX_MILLIS
                    + 2 * AuditPublicationHorizon.COMMITTED_FENCE_SURVIVAL_MILLIS + 120_000L;
            Deencapsulation.invoke(loader, "confirmPublishFence");
            Assertions.assertEquals(0L, AuditLoader.oldestCommittedPublishFenceEventTime(),
                    "a terminal outcome of every retained label releases the aggregate");
        } finally {
            AuditLoader.publishVisibilityProbeForTest = null;
            AuditLoader.transactionStatusForTest = null;
            AuditLoader.publishFenceClockForTest = null;
            setRunningLoader(null);
        }
    }

    // (AuditLoader 790): the probe must ask for the audit time the row was
    // actually WRITTEN with. The audit table stores the writer's local wall clock, so
    // after `SET GLOBAL time_zone` a bound rendered in the CURRENT zone is hours away from
    // the stored one and a perfectly visible row can never be confirmed - the fence then
    // held the capture for the whole retention window.
    @Test
    public void testFenceKeepsTheWriterZoneAndTheProbeRendersWithIt() throws Exception {
        AuditLoader loader = new AuditLoader();
        setPrivateField(loader, "auditEventQueue", Queues.newLinkedBlockingDeque());
        setRunningLoader(loader);
        long eventTime = 1_780_000_000_000L;
        AuditWriterZones.resetForTest();
        try {
            // the row was rendered while America/New_York was the writer's zone
            AuditWriterZones.note("America/New_York", eventTime + 1_000L);
            Deencapsulation.invoke(loader, "retainPublishFence", eventTime, "qid-zone",
                    "audit_log_zone");
            Assertions.assertEquals("America/New_York",
                    loader.oldestPublishFenceWriterZoneForTest(),
                    "the fence must carry the zone its sample row was rendered in");
            Assertions.assertEquals(
                    TimeUtils.longToTimeStringWithms(eventTime, "America/New_York"),
                    AuditLoader.renderProbeEventTime(eventTime, "America/New_York"),
                    "the probe bound is rendered in the WRITER zone, not the current one");
            Assertions.assertEquals(TimeUtils.longToTimeStringWithms(eventTime),
                    AuditLoader.renderProbeEventTime(eventTime, ""),
                    "an unknown zone falls back to the default rendering");
        } finally {
            AuditWriterZones.resetForTest();
            setRunningLoader(null);
        }
    }

    // The obligation of a batch is recorded BEFORE the load is sent, so the
    // shared horizon row names the possible transaction (its LABEL) from the first moment
    // the request could leave the FE. A crash between the send and its response then
    // leaves a resolvable trace instead of an unfenced batch: the reader resolves the
    // transaction by label even though this FE never processed the outcome.
    @Test
    public void testPreSendObligationCarriesItsLabelImmediately() throws Exception {
        AuditLoader loader = new AuditLoader();
        setPrivateField(loader, "auditEventQueue", Queues.newLinkedBlockingDeque());
        setRunningLoader(loader);
        try {
            Deencapsulation.invoke(loader, "retainPublishFence", 10_000L, "qid-pre-send",
                    "audit_log_l1");
            Assertions.assertEquals(10_000L, AuditLoader.oldestCommittedPublishFenceEventTime());
            Assertions.assertEquals("audit_log_l1",
                    AuditLoader.oldestCommittedPublishFenceLabels(),
                    "the label of the in-flight attempt must be reportable at once");

            // a second, label-less obligation is encoded as an unresolvable slot "-": the
            // dead-FE reader must keep fencing for it until the retention bound
            Deencapsulation.invoke(loader, "retainPublishFence", 20_000L, "qid-legacy");
            Assertions.assertEquals("audit_log_l1;-",
                    AuditLoader.oldestCommittedPublishFenceLabels(),
                    "labels are encoded oldest-first with \"-\" for unknown identities");
        } finally {
            setRunningLoader(null);
        }
    }

    // (comment #3): retaining the obligation is LOCAL. The load worker reports the
    // just-retained fence exactly ONCE, and that CONFIRMED report is the send gate;
    // reporting inside the retain as well paid a second synchronous shared-row write +
    // read-back for every batch and discarded the result.
    @Test
    public void testRetainIsLocalAndOneReportServesTheBatch() throws Exception {
        AuditLoader loader = new AuditLoader();
        setPrivateField(loader, "auditEventQueue", Queues.newLinkedBlockingDeque());
        setRunningLoader(loader);
        java.util.concurrent.atomic.AtomicInteger reports =
                new java.util.concurrent.atomic.AtomicInteger();
        AuditLoader.reportCommittedFenceHookForTest = reports::incrementAndGet;
        try {
            Deencapsulation.invoke(loader, "retainPublishFence", 10_000L, "qid-once",
                    "audit_log_once");
            Assertions.assertEquals(0, reports.get(),
                    "retaining the obligation must not report by itself");
            Assertions.assertTrue(
                    (Boolean) Deencapsulation.invoke(loader, "reportCommittedFence"),
                    "the caller's confirmed report is the single one");
            Assertions.assertEquals(1, reports.get());
        } finally {
            AuditLoader.reportCommittedFenceHookForTest = null;
            setRunningLoader(null);
        }
    }

    // The release paths. A CONFIRMED publication (the rows are readable) and
    // a request that never reached a BE both make the pre-send obligation unnecessary; a
    // FAILED/ambiguous outcome keeps it, exactly as before.
    @Test
    public void testPreSendObligationIsReleasedOnlyForDecidedOutcomes() throws Exception {
        AuditLoader loader = new AuditLoader();
        setPrivateField(loader, "auditEventQueue", Queues.newLinkedBlockingDeque());
        setRunningLoader(loader);
        try {
            Deencapsulation.invoke(loader, "retainPublishFence", 10_000L, "qid-a", "audit_log_a");
            Deencapsulation.invoke(loader, "retainPublishFence", 30_000L, "qid-b", "audit_log_b");
            Assertions.assertEquals("audit_log_a;audit_log_b",
                    AuditLoader.oldestCommittedPublishFenceLabels());

            // outcome A resolved (confirmed publication / never delivered): ONLY A's
            // obligation is released, B keeps fencing with its own label
            Deencapsulation.invoke(loader, "releasePublishAttempt", "audit_log_a",
                    "the stream load CONFIRMED its publication");
            Assertions.assertEquals(30_000L, AuditLoader.oldestCommittedPublishFenceEventTime(),
                    "the OTHER batch must keep fencing");
            Assertions.assertEquals("audit_log_b",
                    AuditLoader.oldestCommittedPublishFenceLabels());

            // an unknown label releases nothing
            Deencapsulation.invoke(loader, "releasePublishAttempt", "no-such-label", "test");
            Assertions.assertEquals(30_000L, AuditLoader.oldestCommittedPublishFenceEventTime());
            Deencapsulation.invoke(loader, "releasePublishAttempt", "", "test");
            Assertions.assertEquals(30_000L, AuditLoader.oldestCommittedPublishFenceEventTime());
        } finally {
            setRunningLoader(null);
        }
    }

    /**
     * The report's snapshot-to-write sequence must be SERIALIZED with the fence mutations
     * (the loader monitor): a batch retained while a report had computed - or was about
     * to write - an OLDER snapshot could otherwise be written over by that report, and if
     * this FE then dies the reader sees committed_fence_ms = 0 and drops the row, letting
     * the capture checkpoint past a still-unreadable audit event.
     */
    @Test
    public void testReportSnapshotAndWriteAreSerializedAgainstFenceMutations() throws Exception {
        AuditLoader loader = new AuditLoader();
        setRunningLoader(loader);
        CountDownLatch insideWriter = new CountDownLatch(1);
        CountDownLatch releaseWriter = new CountDownLatch(1);
        AuditPublicationHorizon.localHorizonWriterForTest = horizon -> {
            insideWriter.countDown();
            try {
                releaseWriter.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return true;
        };
        java.util.concurrent.atomic.AtomicBoolean retained =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        Thread reporter = new Thread(() -> AuditPublicationHorizon.reportLocalHorizon(123L));
        Thread mutator = new Thread(() -> {
            try {
                Deencapsulation.invoke(loader, "retainPublishFence", 5_000L, "qid-x",
                        "label-x");
                retained.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        try {
            // an empty SUCCESSFUL restore read = a live environment whose previous
            // incarnation left no row (see readOwnRowsForRestore): the restore completes,
            // so the report reaches its writer
            AuditPublicationHorizon.ownRowRestoreReaderForTest =
                    () -> java.util.Collections.emptyList();
            reporter.start();
            Assertions.assertTrue(insideWriter.await(10, TimeUnit.SECONDS),
                    "the report must reach its write step");
            mutator.start();
            Thread.sleep(300);
            Assertions.assertFalse(retained.get(),
                    "the fence mutation must WAIT for the report's snapshot-to-write"
                            + " section: an older report may not overwrite a newer fence");
            releaseWriter.countDown();
            reporter.join(10_000);
            mutator.join(10_000);
            Assertions.assertTrue(retained.get(), "the mutation resumes after the report");
            Assertions.assertEquals(5_000L,
                    AuditLoader.oldestCommittedPublishFenceEventTime(),
                    "the retained fence is the surviving obligation");
        } finally {
            releaseWriter.countDown();
            AuditPublicationHorizon.localHorizonWriterForTest = null;
            AuditPublicationHorizon.ownRowRestoreReaderForTest = null;
            setRunningLoader(null);
        }
    }

    /**
     * The overflowed batches' LABELS must survive into the settlement list (see
     * oldestCommittedPublishFenceLabels): without them a dead FE's row could look SETTLED
     * while the overflowed transaction is still COMMITTED and unreadable - the capture
     * then checkpoints past a row that publishes afterwards.
     */
    @Test
    public void testOverflowedFenceLabelsSurviveIntoTheSettlementList() throws Exception {
        AuditLoader loader = new AuditLoader();
        setRunningLoader(loader);
        try {
            for (int i = 0; i <= AuditLoader.MAX_PENDING_PUBLISH_FENCES; i++) {
                Deencapsulation.invoke(loader, "retainPublishFence", 20_000L + i,
                        "qid-" + i, "label-" + i);
            }
            Assertions.assertEquals(20_000L,
                    AuditLoader.oldestCommittedPublishFenceEventTime(),
                    "the overflowed batch keeps fencing");
            String labels = AuditLoader.oldestCommittedPublishFenceLabels();
            Assertions.assertTrue(labels.startsWith("label-0;"),
                    "the overflowed batch's label must come first: " + labels);

            // taking the aggregate past the label bound contributes the OVERFLOWED marker
            // (round-50: distinct from the no-identity marker and never settled by the
            // row's age - the omitted batch may still publish) instead of losing identities
            for (int i = 0; i < AuditLoader.MAX_AGGREGATED_FENCE_LABELS + 1; i++) {
                Deencapsulation.invoke(loader, "retainPublishFence", 50_000L + i,
                        "qid-b" + i, "label-b" + i);
            }
            labels = AuditLoader.oldestCommittedPublishFenceLabels();
            Assertions.assertTrue(Arrays.asList(labels.split(";", -1))
                            .contains(AuditLoader.OVERFLOWED_FENCE_LABEL),
                    "a lost label must be carried as the overflow marker: " + labels);
            Assertions.assertFalse(Arrays.asList(labels.split(";", -1)).contains("-"),
                    "the overflow marker is DISTINCT from the no-identity marker: "
                            + labels);

            // round-50: at the bound the OLDEST identities that already resolved TERMINAL
            // give up their slots first, so a resolvable batch never costs another one's
            // identity - the new labels stay in the settlement list
            AuditLoader.transactionStatusForTest = label -> "VISIBLE";
            for (int i = 0; i < AuditLoader.MAX_AGGREGATED_FENCE_LABELS; i++) {
                Deencapsulation.invoke(loader, "retainPublishFence", 70_000L + i,
                        "qid-c" + i, "label-c" + i);
            }
            labels = AuditLoader.oldestCommittedPublishFenceLabels();
            Assertions.assertTrue(Arrays.asList(labels.split(";", -1))
                            .contains("label-c" + (AuditLoader.MAX_AGGREGATED_FENCE_LABELS - 1)),
                    "a slot freed by a RESOLVED identity must be reused instead of"
                            + " dropping the new label: " + labels);
        } finally {
            AuditLoader.transactionStatusForTest = null;
            setRunningLoader(null);
        }
    }

    /**
     * Round-51 (#7): the overflowed sentinel must NOT pin the aggregate forever. The
     * previous code re-armed the survival window on every expiry, so the shared row's "*"
     * kept a DEAD writer FE's horizon fenced until the end of time - every later capture
     * window stayed pinned even after all resolvable loads turned VISIBLE. The
     * unresolvable obligations (an unknown label, or the sentinel) now share ONE absolute
     * window: past it they are assumed LOST and the aggregate retires.
     */
    @Test
    public void testOverflowedSentinelRetiresAfterItsBoundedWindow() throws Exception {
        AuditLoader loader = new AuditLoader();
        setRunningLoader(loader);
        long base = 1_000_000_000L;
        AuditLoader.publishFenceClockForTest = () -> base;
        try {
            int required = AuditLoader.MAX_AGGREGATED_FENCE_LABELS
                    + AuditLoader.MAX_PENDING_PUBLISH_FENCES + 2;
            for (int i = 0; i < required; i++) {
                Deencapsulation.invoke(loader, "retainPublishFence", 20_000L + i,
                        "qid-" + i, "label-" + i);
            }
            String labels = AuditLoader.oldestCommittedPublishFenceLabels();
            Assertions.assertTrue(Arrays.asList(labels.split(";", -1))
                            .contains(AuditLoader.OVERFLOWED_FENCE_LABEL),
                    "the bound must have overflowed: " + labels);

            // past the re-check window: the first resolution ARMS the absolute window
            AuditLoader.publishFenceClockForTest =
                    () -> base + 2 * AuditLoader.PUBLISH_FENCE_MAX_MILLIS;
            AuditLoader.oldestCommittedPublishFenceEventTime();
            // past the absolute window: the unresolvable obligations (here: all labels are
            // unknown to the transaction manager, plus the sentinel) are assumed LOST
            AuditLoader.publishFenceClockForTest = () -> base
                    + 2 * AuditLoader.PUBLISH_FENCE_MAX_MILLIS
                    + AuditPublicationHorizon.COMMITTED_FENCE_SURVIVAL_MILLIS + 1;
            AuditLoader.oldestCommittedPublishFenceEventTime();
            // The overflowed aggregate retires; the plain pending batches keep their
            // own (unconfirmable here) settlement path - only the SENTINEL must be gone.
            String retired = AuditLoader.oldestCommittedPublishFenceLabels();
            Assertions.assertFalse(Arrays.asList(retired.split(";", -1))
                            .contains(AuditLoader.OVERFLOWED_FENCE_LABEL),
                    "the sentinel must not survive the aggregate: " + retired);
        } finally {
            AuditLoader.publishFenceClockForTest = null;
            setRunningLoader(null);
        }
    }

    /**
     * An idle cluster never changes the horizon (a zero row is the healthy state), so
     * a change-only report let the shared registration go stale while the FE stayed
     * alive: the capture reader then treats a LIVE FE's overdue row as unreadable and
     * fails every capture cycle closed. The keepalive re-reports an unchanged value on
     * the cadence - including the idle zero.
     */
    @Test
    public void testIdleHorizonIsReReportedOnTheKeepaliveCadence() {
        long now = 1_000_000L;
        Assertions.assertTrue(AuditLoader.shouldReportHorizon(true, false, now, now),
                "a changed value is reported immediately");
        Assertions.assertTrue(AuditLoader.shouldReportHorizon(false, true, now, now),
                "a changed writer-zone set is reported immediately");
        long belowCadence = now + AuditLoader.HORIZON_KEEPALIVE_MILLIS - 1;
        Assertions.assertFalse(AuditLoader.shouldReportHorizon(false, false, now, belowCadence),
                "an unchanged value below the cadence is not re-sent");
        long due = now + AuditLoader.HORIZON_KEEPALIVE_MILLIS;
        Assertions.assertTrue(AuditLoader.shouldReportHorizon(false, false, now, due),
                "the idle zero must be re-reported once the cadence elapses");
        Assertions.assertTrue(AuditLoader.shouldReportHorizon(false, false, now, due + 5L),
                "an overdue keepalive stays due until a report confirms");
    }

    private static AuditEvent internalEvent(long timestamp) {
        return new AuditEvent.AuditEventBuilder()
                .setQueryId("internal-" + timestamp)
                .setTimestamp(timestamp)
                .setStmt("insert into __internal_schema.spm_audit_horizon values (...)")
                .setisInternal(true)
                .build();
    }

    private static AuditEvent event(long timestamp) {
        return new AuditEvent.AuditEventBuilder()
                .setQueryId("qid-" + timestamp)
                .setTimestamp(timestamp)
                .setStmt("select 1")
                .build();
    }

    private static void setPrivateField(Object target, String name, Object value) throws Exception {
        java.lang.reflect.Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static void setRunningLoader(AuditLoader loader) throws Exception {
        java.lang.reflect.Field field = AuditLoader.class.getDeclaredField("runningLoader");
        field.setAccessible(true);
        field.set(null, loader);
    }

    private static int count(CharSequence s, char c) {
        int n = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == c) {
                n++;
            }
        }
        return n;
    }
}
