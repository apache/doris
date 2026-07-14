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

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
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

    @Test
    public void testAuditTableBackendSelectionFieldsMatchSchemaOrder() {
        List<String> schemaNames = InternalSchema.AUDIT_SCHEMA.stream()
                .map(ColumnDef::getName)
                .collect(Collectors.toList());
        int workloadGroupIndex = schemaNames.indexOf("workload_group");
        Assert.assertEquals(workloadGroupIndex + 1, schemaNames.indexOf("compute_group"));
        Assert.assertEquals(workloadGroupIndex + 2, schemaNames.indexOf("backend_selection_preferred_key"));
        Assert.assertEquals(workloadGroupIndex + 3, schemaNames.indexOf("backend_selection_mode"));
        Assert.assertEquals(workloadGroupIndex + 4, schemaNames.indexOf("stmt"));
        Assert.assertEquals(schemaNames.size() - 1, schemaNames.indexOf("stmt"));

        AuditEvent auditEvent = new AuditEvent.AuditEventBuilder()
                .setWorkloadGroup("wg_a")
                .setCloudCluster("compute_a")
                .setBackendSelectionPreferredKey("key_a")
                .setBackendSelectionMode("prefer")
                .setStmt("select 1")
                .build();
        StringBuilder logBuffer = new StringBuilder();
        Deencapsulation.invoke(new AuditLoader(), "fillLogBuffer", auditEvent, logBuffer);

        String logLine = logBuffer.toString();
        Assert.assertTrue(logLine.endsWith(String.valueOf(AuditLoader.AUDIT_TABLE_LINE_DELIMITER)));
        String logLineWithoutDelimiter = logLine.substring(0, logLine.length() - 1);
        String[] fields = logLineWithoutDelimiter.split(Pattern.quote(
                String.valueOf(AuditLoader.AUDIT_TABLE_COL_SEPARATOR)), -1);
        Assert.assertEquals(schemaNames.size(), fields.length);
        Assert.assertEquals("wg_a", fields[workloadGroupIndex]);
        Assert.assertEquals("compute_a", fields[workloadGroupIndex + 1]);
        Assert.assertEquals("key_a", fields[workloadGroupIndex + 2]);
        Assert.assertEquals("prefer", fields[workloadGroupIndex + 3]);
        Assert.assertEquals("select 1", fields[workloadGroupIndex + 4]);
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
}
