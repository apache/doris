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
     * round-34 #7: the queue -> batch transfer must be ATOMIC with the horizon read. The
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

    // round-32 #12: the SPM capture overlaps its scan window by the LOCAL loader's
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

    // round-36 #2: a stream load can return Publish Timeout AFTER commit while its rows
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

    // round-39 #2: EVERY timed-out batch is retained and confirmed SEPARATELY. Keeping
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

    // round-36 #2 / round-40 #9: the response is the only evidence of the load's real
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

    // round-40 #8: the zone a row's time column is RENDERED in is the zone that must be
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

    // round-40 #10: a Publish-Timeout batch is COMMITTED, so closing this FE must not
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
            setRunningLoader(null);
        }
    }

    // round-42 #7: the OLDEST pending fence must keep fencing even after it was EVICTED
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

    // round-37 #7: internal statements (e.g. the horizon reporter's own SQL) are never
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
