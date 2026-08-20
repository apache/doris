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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.plugin.AuditEvent;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
            Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
            Assert.assertTrue(waitForBlocked(assembleThread));
            Assert.assertFalse(getAuditLogBuffer(auditLoader).contains(auditEvent.queryId));
        }

        assembleThread.join(5000);
        Assert.assertFalse(assembleThread.isAlive());
        if (error.get() != null) {
            throw new AssertionError("failed to assemble audit event", error.get());
        }
        Assert.assertTrue(getAuditLogBuffer(auditLoader).contains(auditEvent.queryId));
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
        Assert.assertEquals("injected 0x1E must not add rows",
                count(clean, line), count(evil, line));
        Assert.assertEquals("one row per event", 1, count(evil, line));
        Assert.assertEquals("injected 0x1F must not add columns",
                count(clean, col), count(evil, col));
        // The forged tokens survive only as inert text, never as framing bytes.
        Assert.assertTrue(evil.toString().contains("DROP TABLE finance.ledger"));
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
        Assert.assertTrue(buffer.toString().contains("select * from t where a = 1 and b = 'x'"));
        Assert.assertEquals(1, count(buffer, AuditLoader.AUDIT_TABLE_LINE_DELIMITER));
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
