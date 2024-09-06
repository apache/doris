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

package org.apache.doris.qe;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.DigitalVersion;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.plugin.audit.AuditEvent;
import org.apache.doris.plugin.audit.AuditEvent.EventType;
import org.apache.doris.plugin.audit.AuditLogBuilder;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class AuditEventProcessorTest {

    private static String runningDir = "fe/mocked/AuditProcessorTest/" + UUID.randomUUID().toString() + "/";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test
    public void testAuditEvent() {
        AuditEvent event = new AuditEvent.AuditEventBuilder().setEventType(EventType.AFTER_QUERY)
                .setTimestamp(System.currentTimeMillis())
                .setClientIp("127.0.0.1")
                .setUser("user1")
                .setDb("db1")
                .setState("EOF")
                .setQueryTime(2000)
                .setScanBytes(100000)
                .setScanRows(200000)
                .setReturnRows(1)
                .setStmtId(1234)
                .setStmtType("SELECT")
                .setStmt("select * from tbl1").build();

        Assert.assertEquals("127.0.0.1", event.clientIp);
        Assert.assertEquals(200000, event.scanRows);
        Assert.assertEquals("SELECT", event.stmtType);
    }

    @Test
    public void testAuditLogBuilder() throws IOException {
        try (AuditLogBuilder auditLogBuilder = new AuditLogBuilder()) {
            PluginInfo pluginInfo = auditLogBuilder.getPluginInfo();
            Assert.assertEquals(DigitalVersion.fromString("0.12.0"), pluginInfo.getVersion());
            Assert.assertEquals(DigitalVersion.fromString("1.8.31"), pluginInfo.getJavaVersion());
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10000; i++) {
                AuditEvent event = new AuditEvent.AuditEventBuilder().setEventType(EventType.AFTER_QUERY)
                        .setTimestamp(System.currentTimeMillis())
                        .setClientIp("127.0.0.1")
                        .setUser("user1")
                        .setDb("db1")
                        .setState("EOF")
                        .setQueryTime(2000)
                        .setScanBytes(100000)
                        .setScanRows(200000)
                        .setReturnRows(i)
                        .setStmtId(1234)
                        .setStmt("select * from tbl1").build();
                if (auditLogBuilder.eventFilter(event.type)) {
                    auditLogBuilder.exec(event);
                }
            }
            long total = System.currentTimeMillis() - start;
            System.out.println("total(ms): " + total + ", avg: " + total / 10000.0);
        }
    }

    @Test
    public void testAuditEventProcessor() throws IOException {
        AuditEventProcessor processor = Env.getCurrentAuditEventProcessor();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            AuditEvent event = new AuditEvent.AuditEventBuilder().setEventType(EventType.AFTER_QUERY)
                    .setTimestamp(System.currentTimeMillis())
                    .setClientIp("127.0.0.1")
                    .setUser("user1")
                    .setDb("db1")
                    .setState("EOF")
                    .setQueryTime(2000)
                    .setScanBytes(100000)
                    .setScanRows(200000)
                    .setReturnRows(i)
                    .setStmtId(1234)
                    .setStmt("select * from tbl1").build();
            processor.handleAuditEvent(event);
        }
        long total = System.currentTimeMillis() - start;
        System.out.println("total(ms): " + total + ", avg: " + total / 10000.0);
    }
}
