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
import org.apache.doris.plugin.AuditEvent;

import com.google.common.base.Splitter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
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
                        .setSpillWriteBytesToLocalStorage(11L)
                        .setSpillReadBytesFromLocalStorage(12L)
                        .setSpillWriteBytesToRemoteStorage(13L)
                        .setSpillReadBytesFromRemoteStorage(14L)
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
        Assertions.assertEquals("11", columns.get(names.indexOf("spill_write_bytes_from_local_storage")));
        Assertions.assertEquals("12", columns.get(names.indexOf("spill_read_bytes_from_local_storage")));
        Assertions.assertEquals("13", columns.get(names.indexOf("spill_write_bytes_to_remote_storage")));
        Assertions.assertEquals("14", columns.get(names.indexOf("spill_read_bytes_from_remote_storage")));
        Assertions.assertEquals("select 1", columns.get(names.indexOf("stmt")));
        Assertions.assertEquals(names.size() - 1, names.indexOf("stmt"));
    }

    // The SPM capture overlaps its scan window by the LOCAL loader's
    // outstanding queue horizon. The horizon is the OLDEST event the loader has accepted but
    // not published yet - the assembled batch counts as well, and enqueue order is NOT
    // event-time order (the upstream hold releases events by completion), so every queued
    // event is examined.


    // A stream load can return Publish Timeout AFTER commit while its rows
    // stay unreadable. The batch must be fenced until publication is confirmed - the
    // previous unconditional batch reset let the capture advance past those rows, so
    // their late publication landed behind the watermark.


    // EVERY timed-out batch is retained and confirmed SEPARATELY. Keeping
    // only the OLDEST batch's sample released the whole fence when that sample became
    // visible although a newer batch B could still be committed but unreadable; the
    // capture then checkpointed past B and once the watermark moved, later windows could
    // never reach B's rows.


    // The comment-round fix: a parse-failure event is audited under the SHARED query id
    // "NaN" (AuditLogHelper renders a null id as NaN). A LATER batch's visible NaN row
    // satisfies the row probe (query_id = 'NaN' AND time >= sampleTime), so a "visible"
    // answer proves NOTHING about the fenced batch - it could release the fence while the
    // batch is still COMMITTED and unreadable, and the capture would checkpoint past a
    // SELECT the batch still owes. The fence must survive until the batch's OWN
    // transaction reaches a terminal state (VISIBLE = readable after all, ABORTED = can
    // never appear).


    // /: the response is the only evidence of the load's real
    // outcome, and only a COMPLETE, parseable Success response proves publication. An
    // unreadable body says nothing - the earlier "does not contain 'publish timeout'"
    // test treated it as published, so a committed Publish Timeout whose body read
    // failed reset the batch WITHOUT a fence.


    // The zone a row's time column is RENDERED in is the zone that must be
    // registered for it - two independent reads of the global time_zone could observe a
    // `SET GLOBAL time_zone` in between (and back before the next report), leaving the
    // row stored under a zone nobody had registered and a capture skipping it forever.


    // A Publish-Timeout batch is COMMITTED, so closing this FE must not
    // clear its durable fence - the rows may become readable after the FE stopped. With
    // no such batch the row IS cleared (the capture must not wait for a gone FE).


    // The OLDEST pending fence must keep fencing even after it was EVICTED
    // from the bounded list - dropping it released the fence of the oldest committed
    // batch (its rows can still become readable).


    // Internal statements (e.g. the horizon reporter's own SQL) are never
    // captured, so they must not fence progress - otherwise the reporter's writes keep
    // their own FE's fence (and thereby further writes) alive forever on an idle FE.


    // (AuditLoader 746): elapsed time is NOT proof of loss - a Publish Timeout
    // batch can stay COMMITTED and unreadable beyond the age bound while the publish
    // daemon keeps retrying. The fence is released by the transaction OUTCOME
    // (VISIBLE / ABORTED); the age bound is the last resort for a batch whose label is
    // PROVEN absent (no transaction ever carried it), while a lookup that cannot ANSWER
    // (a failing / unavailable manager) keeps the fence - it is no evidence of absence.


    // (AuditLoader 675): the overflow aggregate cleared EVERY member when the
    // FIRST one's 30-minute window elapsed. A committed batch that overflowed into it at
    // minute 29 lost its fence at minute 30 - without any visibility proof and before its
    // own deadline - and the capture could checkpoint past a row that publishes later.
    // The aggregate must outlive its NEWEST member's own deadline.


    // (AuditLoader 790): the probe must ask for the audit time the row was
    // actually WRITTEN with. The audit table stores the writer's local wall clock, so
    // after `SET GLOBAL time_zone` a bound rendered in the CURRENT zone is hours away from
    // the stored one and a perfectly visible row can never be confirmed - the fence then
    // held the capture for the whole retention window.


    // The obligation of a batch is recorded BEFORE the load is sent, so the
    // shared horizon row names the possible transaction (its LABEL) from the first moment
    // the request could leave the FE. A crash between the send and its response then
    // leaves a resolvable trace instead of an unfenced batch: the reader resolves the
    // transaction by label even though this FE never processed the outcome.


    // (comment #3): retaining the obligation is LOCAL. The load worker reports the
    // just-retained fence exactly ONCE, and that CONFIRMED report is the send gate;
    // reporting inside the retain as well paid a second synchronous shared-row write +
    // read-back for every batch and discarded the result.


    // The release paths. A CONFIRMED publication (the rows are readable) and
    // a request that never reached a BE both make the pre-send obligation unnecessary; a
    // FAILED/ambiguous outcome keeps it, exactly as before.


    private static AuditEvent internalEvent(long timestamp) {
        return new AuditEvent.AuditEventBuilder()
                .setQueryId("internal-" + timestamp)
                .setTimestamp(timestamp)
                .setStmt("insert into __internal_schema.spm_baselines values (...)")
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
