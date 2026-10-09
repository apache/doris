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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InternalSchema;
import org.apache.doris.catalog.TokenManager;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.plugin.AuditEvent;

import com.google.common.base.Splitter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class AuditLoaderTest {

    @Test
    public void testFailedStreamLoadKeepsBatchForRetry() throws Exception {
        AuditLoader loader = new AuditLoader();
        StringBuilder batch = new StringBuilder("audit row");
        Deencapsulation.setField(loader, "auditLogBuffer", batch);
        Deencapsulation.setField(loader, "auditLogNum", 1);
        AuditStreamLoader streamLoader = Mockito.mock(AuditStreamLoader.class);
        Deencapsulation.setField(loader, "streamLoader", streamLoader);
        Mockito.when(streamLoader.genLabel()).thenReturn("label-1", "label-2");
        Mockito.when(streamLoader.loadBatch(Mockito.same(batch), Mockito.eq("token"), Mockito.anyString()))
                .thenReturn(new AuditStreamLoader.LoadResponse(200, "OK", "{\"Status\":\"Fail\"}"),
                        new AuditStreamLoader.LoadResponse(200, "OK",
                                "{\"Status\":\"Success\",\"NumberLoadedRows\":1,"
                                        + "\"NumberFilteredRows\":0}"));
        Env env = Mockito.mock(Env.class);
        TokenManager tokenManager = Mockito.mock(TokenManager.class);
        Mockito.when(env.getTokenManager()).thenReturn(tokenManager);
        Mockito.when(tokenManager.acquireToken()).thenReturn("token");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            loader.loadIfNecessary(true);
            Assertions.assertSame(batch, Deencapsulation.getField(loader, "auditLogBuffer"));
            Assertions.assertEquals(1, (int) Deencapsulation.getField(loader, "auditLogNum"));
            loader.loadIfNecessary(true);
        }
        Assertions.assertEquals("", getAuditLogBuffer(loader));
        Mockito.verify(streamLoader).loadBatch(Mockito.same(batch), Mockito.eq("token"), Mockito.eq("label-1"));
        Mockito.verify(streamLoader).loadBatch(Mockito.same(batch), Mockito.eq("token"), Mockito.eq("label-2"));
    }

    @Test
    public void testAmbiguousStreamLoadRetryKeepsLabel() throws Exception {
        AuditLoader loader = new AuditLoader();
        StringBuilder batch = new StringBuilder("audit row");
        Deencapsulation.setField(loader, "auditLogBuffer", batch);
        Deencapsulation.setField(loader, "auditLogNum", 1);
        AuditStreamLoader streamLoader = Mockito.mock(AuditStreamLoader.class);
        Deencapsulation.setField(loader, "streamLoader", streamLoader);
        Mockito.when(streamLoader.genLabel()).thenReturn("stable-label");
        Mockito.when(streamLoader.loadBatch(Mockito.same(batch), Mockito.eq("token"), Mockito.eq("stable-label")))
                .thenReturn(new AuditStreamLoader.LoadResponse(-1, "timeout", ""),
                        new AuditStreamLoader.LoadResponse(200, "OK",
                                "{\"Status\":\"Label Already Exists\",\"ExistingJobStatus\":\"FINISHED\"}"));
        Env env = Mockito.mock(Env.class);
        TokenManager tokenManager = Mockito.mock(TokenManager.class);
        Mockito.when(env.getTokenManager()).thenReturn(tokenManager);
        Mockito.when(tokenManager.acquireToken()).thenReturn("token");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            loader.loadIfNecessary(true);
            Assertions.assertSame(batch, Deencapsulation.getField(loader, "auditLogBuffer"));
            loader.loadIfNecessary(true);
        }
        Assertions.assertEquals("", getAuditLogBuffer(loader));
        Mockito.verify(streamLoader, Mockito.times(2)).loadBatch(Mockito.same(batch), Mockito.eq("token"),
                Mockito.eq("stable-label"));
        Mockito.verify(streamLoader, Mockito.times(1)).genLabel();
    }

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
