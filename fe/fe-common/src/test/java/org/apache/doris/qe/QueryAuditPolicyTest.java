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

import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase.ConfField;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryAuditPolicyTest {
    private boolean alwaysOn;
    private String skipAuditUserList;
    private String[] auditLogModules;
    private int auditEventLogQueueSize;
    private int queryAuditLogTimeoutMs;
    private int beReportQueryStatisticsTimeoutMs;
    private int workloadRuntimeStatusThreadIntervalMs;
    private boolean enableAuditLoader;
    private int maxSqlLength;
    private int maxInsertStmtLength;
    private int loadTimeoutS;
    private long maxBatchBytes;
    private long maxBatchIntervalSec;

    @BeforeEach
    public void save() {
        alwaysOn = Config.query_audit_always_on;
        skipAuditUserList = Config.skip_audit_user_list;
        auditLogModules = Config.audit_log_modules;
        auditEventLogQueueSize = Config.audit_event_log_queue_size;
        queryAuditLogTimeoutMs = Config.query_audit_log_timeout_ms;
        beReportQueryStatisticsTimeoutMs = Config.be_report_query_statistics_timeout_ms;
        workloadRuntimeStatusThreadIntervalMs = Config.workload_runtime_status_thread_interval_ms;
        enableAuditLoader = GlobalVariable.enableAuditLoader;
        maxSqlLength = GlobalVariable.auditPluginMaxSqlLength;
        maxInsertStmtLength = GlobalVariable.auditPluginMaxInsertStmtLength;
        loadTimeoutS = GlobalVariable.auditPluginLoadTimeoutS;
        maxBatchBytes = GlobalVariable.auditPluginMaxBatchBytes;
        maxBatchIntervalSec = GlobalVariable.auditPluginMaxBatchInternalSec;
    }

    @AfterEach
    public void restore() {
        Config.query_audit_always_on = alwaysOn;
        Config.skip_audit_user_list = skipAuditUserList;
        Config.audit_log_modules = auditLogModules;
        Config.audit_event_log_queue_size = auditEventLogQueueSize;
        Config.query_audit_log_timeout_ms = queryAuditLogTimeoutMs;
        Config.be_report_query_statistics_timeout_ms = beReportQueryStatisticsTimeoutMs;
        Config.workload_runtime_status_thread_interval_ms = workloadRuntimeStatusThreadIntervalMs;
        GlobalVariable.enableAuditLoader = enableAuditLoader;
        GlobalVariable.auditPluginMaxSqlLength = maxSqlLength;
        GlobalVariable.auditPluginMaxInsertStmtLength = maxInsertStmtLength;
        GlobalVariable.auditPluginLoadTimeoutS = loadTimeoutS;
        GlobalVariable.auditPluginMaxBatchBytes = maxBatchBytes;
        GlobalVariable.auditPluginMaxBatchInternalSec = maxBatchIntervalSec;
    }

    @Test
    public void testOffUsesTheSettingsAsTheyAre() {
        Config.query_audit_always_on = false;
        setWeakVariables();
        Assertions.assertFalse(QueryAuditPolicy.isAuditLoaderEnabled());
        Assertions.assertEquals(100, QueryAuditPolicy.maxSqlLength());
        Assertions.assertEquals(10, QueryAuditPolicy.maxInsertStmtLength());
        Assertions.assertEquals(1, QueryAuditPolicy.loadTimeoutS());
        Assertions.assertEquals(1, QueryAuditPolicy.maxBatchBytes());
        Assertions.assertEquals(1, QueryAuditPolicy.maxBatchIntervalSec());
        Assertions.assertFalse(QueryAuditPolicy.shouldAuditPreparedExecute(false, false));
        Assertions.assertTrue(QueryAuditPolicy.shouldAuditPreparedExecute(true, false));
        // The master audits a forwarded execution when the forwarded session asks to, as before.
        Assertions.assertTrue(QueryAuditPolicy.shouldAuditPreparedExecute(true, true));

        // The one exception: a negative SQL length counts as 0.
        GlobalVariable.auditPluginMaxSqlLength = -1;
        Assertions.assertEquals(0, QueryAuditPolicy.maxSqlLength());
    }

    @Test
    public void testOnHoldsEachValueToItsDefault() {
        Config.query_audit_always_on = true;
        setWeakVariables();
        Assertions.assertTrue(QueryAuditPolicy.isAuditLoaderEnabled());
        Assertions.assertEquals(2097152, QueryAuditPolicy.maxSqlLength());
        Assertions.assertEquals(Integer.MAX_VALUE, QueryAuditPolicy.maxInsertStmtLength());
        Assertions.assertEquals(600, QueryAuditPolicy.loadTimeoutS());
        Assertions.assertEquals(52428800L, QueryAuditPolicy.maxBatchBytes());
        Assertions.assertEquals(60L, QueryAuditPolicy.maxBatchIntervalSec());
        GlobalVariable.auditPluginMaxSqlLength = -1;
        Assertions.assertEquals(2097152, QueryAuditPolicy.maxSqlLength());

        // Stronger values take effect; the batching knobs are fixed in both directions.
        GlobalVariable.auditPluginMaxSqlLength = 4194304;
        GlobalVariable.auditPluginLoadTimeoutS = 1200;
        GlobalVariable.auditPluginMaxBatchBytes = Long.MAX_VALUE;
        GlobalVariable.auditPluginMaxBatchInternalSec = Long.MAX_VALUE;
        Assertions.assertEquals(4194304, QueryAuditPolicy.maxSqlLength());
        Assertions.assertEquals(1200, QueryAuditPolicy.loadTimeoutS());
        Assertions.assertEquals(52428800L, QueryAuditPolicy.maxBatchBytes());
        Assertions.assertEquals(60L, QueryAuditPolicy.maxBatchIntervalSec());

        // The FE that received an execution audits it whatever the session says; the master leaves a forwarded one
        // to that FE.
        Assertions.assertTrue(QueryAuditPolicy.shouldAuditPreparedExecute(false, false));
        Assertions.assertFalse(QueryAuditPolicy.shouldAuditPreparedExecute(true, true));
        Assertions.assertFalse(QueryAuditPolicy.shouldAuditPreparedExecute(false, true));
    }

    @Test
    public void testCheckVariableValueWhenOff() {
        Config.query_audit_always_on = false;
        Assertions.assertEquals(Optional.empty(), check("enable_audit_plugin", "false"));
        Assertions.assertEquals(Optional.empty(), check("enable_prepared_stmt_audit_log", "false"));
        Assertions.assertEquals(Optional.empty(), check("audit_plugin_max_sql_length", "0"));
        Assertions.assertEquals(Optional.empty(), check("audit_plugin_max_batch_bytes", "1"));
        Assertions.assertEquals(Optional.of("Variable 'audit_plugin_max_sql_length' can't be set to '-1', "
                + "the value must not be negative"), check("audit_plugin_max_sql_length", "-1"));
    }

    @Test
    public void testCheckVariableValueWhenOn() {
        Config.query_audit_always_on = true;
        for (String name : new String[] {"enable_audit_plugin", "enable_prepared_stmt_audit_log"}) {
            for (String off : new String[] {"false", "OFF", "0"}) {
                Assertions.assertEquals(Optional.of("Variable '" + name + "' can't be set to '" + off
                        + "' because query_audit_always_on is enabled"), check(name, off));
            }
            for (String on : new String[] {"true", "ON", "1"}) {
                Assertions.assertEquals(Optional.empty(), check(name, on));
            }
        }
        assertRejected("audit_plugin_max_sql_length", "2097151", "at least 2097152");
        assertRejected("audit_plugin_max_sql_length", "-1", "at least 2097152");
        assertAllowed("audit_plugin_max_sql_length", "2097152", "4194304");
        assertRejected("audit_plugin_max_insert_stmt_length", "1048576", "at least 2147483647");
        assertAllowed("audit_plugin_max_insert_stmt_length", "2147483647");
        assertRejected("audit_plugin_load_timeout", "599", "at least 600");
        assertAllowed("audit_plugin_load_timeout", "600", "1200");
        assertRejected("audit_plugin_max_batch_bytes", "52428799", "52428800");
        assertRejected("audit_plugin_max_batch_bytes", "52428801", "52428800");
        assertAllowed("audit_plugin_max_batch_bytes", "52428800");
        assertRejected("audit_plugin_max_batch_interval_sec", "59", "60");
        assertRejected("audit_plugin_max_batch_interval_sec", "61", "60");
        assertAllowed("audit_plugin_max_batch_interval_sec", "60");
        // Values the setter cannot parse are left to the setter, which reports them; other variables are not checked.
        assertAllowed("audit_plugin_max_sql_length", "abc");
        assertAllowed("enable_audit_plugin", "maybe");
        assertAllowed("exec_mem_limit", "1");
    }

    @Test
    public void testCheckConfigValue() {
        Config.query_audit_always_on = false;
        Assertions.assertEquals(Optional.empty(), QueryAuditPolicy.checkConfigValue("audit_event_log_queue_size", "0"));

        Config.query_audit_always_on = true;
        Assertions.assertEquals(Optional.of("Config 'audit_event_log_queue_size' can't be set to '249999' because "
                        + "query_audit_always_on is enabled, the value must be at least 250000"),
                QueryAuditPolicy.checkConfigValue("audit_event_log_queue_size", " 249999 "));
        Assertions.assertEquals(Optional.empty(),
                QueryAuditPolicy.checkConfigValue("audit_event_log_queue_size", "300000"));
        Assertions.assertTrue(QueryAuditPolicy.checkConfigValue("query_audit_log_timeout_ms", "5001").get()
                .endsWith("the value must be at most 5000"));
        Assertions.assertEquals(Optional.empty(), QueryAuditPolicy.checkConfigValue("query_audit_log_timeout_ms", "1"));
        Assertions.assertTrue(QueryAuditPolicy.checkConfigValue("be_report_query_statistics_timeout_ms", "60001")
                .get().endsWith("the value must be at most 60000"));
        Assertions.assertTrue(QueryAuditPolicy.checkConfigValue("workload_runtime_status_thread_interval_ms", "2001")
                .get().endsWith("the value must be at most 2000"));
        Assertions.assertEquals(Optional.empty(), QueryAuditPolicy.checkConfigValue("qe_slow_log_ms", "1"));
    }

    @Test
    public void testStartupOverridesConflictingConfigs() {
        Config.query_audit_always_on = true;
        Config.skip_audit_user_list = "alice, bob";
        Config.audit_log_modules = new String[] {"slow_query", "load"};
        Config.audit_event_log_queue_size = 10;
        Config.query_audit_log_timeout_ms = 100000;
        Config.be_report_query_statistics_timeout_ms = 60000;
        Config.workload_runtime_status_thread_interval_ms = 1000;

        List<QueryAuditPolicy.StartupOverride> overrides = QueryAuditPolicy.applyStartupOverrides();
        Assertions.assertEquals(Arrays.asList(
                        "skip_audit_user_list: alice, bob -> ",
                        "audit_log_modules: slow_query,load -> slow_query,load,query",
                        "audit_event_log_queue_size: 10 -> 250000",
                        "query_audit_log_timeout_ms: 100000 -> 5000"),
                overrides.stream().map(Object::toString).collect(Collectors.toList()));
        Assertions.assertEquals("", Config.skip_audit_user_list);
        Assertions.assertArrayEquals(new String[] {"slow_query", "load", "query"}, Config.audit_log_modules);
        Assertions.assertEquals(250000, Config.audit_event_log_queue_size);
        Assertions.assertEquals(5000, Config.query_audit_log_timeout_ms);
        // Values inside their bounds stay.
        Assertions.assertEquals(60000, Config.be_report_query_statistics_timeout_ms);
        Assertions.assertEquals(1000, Config.workload_runtime_status_thread_interval_ms);
        Assertions.assertTrue(QueryAuditPolicy.applyStartupOverrides().isEmpty());

        Config.be_report_query_statistics_timeout_ms = 60001;
        Config.workload_runtime_status_thread_interval_ms = 2001;
        Assertions.assertEquals(Arrays.asList(
                        "be_report_query_statistics_timeout_ms: 60001 -> 60000",
                        "workload_runtime_status_thread_interval_ms: 2001 -> 2000"),
                QueryAuditPolicy.applyStartupOverrides().stream().map(Object::toString)
                        .collect(Collectors.toList()));
        Assertions.assertEquals(60000, Config.be_report_query_statistics_timeout_ms);
        Assertions.assertEquals(2000, Config.workload_runtime_status_thread_interval_ms);
    }

    @Test
    public void testStartupKeepsConfigsWhenOff() {
        Config.query_audit_always_on = false;
        Config.skip_audit_user_list = "alice";
        String[] modules = new String[] {"slow_query"};
        Config.audit_log_modules = modules;
        Config.audit_event_log_queue_size = 0;
        Assertions.assertTrue(QueryAuditPolicy.applyStartupOverrides().isEmpty());
        Assertions.assertEquals("alice", Config.skip_audit_user_list);
        Assertions.assertSame(modules, Config.audit_log_modules);
        Assertions.assertEquals(0, Config.audit_event_log_queue_size);
    }

    // A config that is renamed or loses its callback would silently drop its protection: the registry must name
    // exactly the mutable configs whose runtime changes go through ProtectedConfHandler.
    @Test
    public void testProtectedConfigsUseTheHandler() throws Exception {
        Set<String> withHandler = new HashSet<>();
        for (Field field : Config.class.getFields()) {
            ConfField anno = field.getAnnotation(ConfField.class);
            if (anno != null && anno.callback() == QueryAuditPolicy.ProtectedConfHandler.class) {
                Assertions.assertTrue(anno.mutable(), field.getName());
                withHandler.add(field.getName());
            }
        }
        Assertions.assertEquals(QueryAuditPolicy.protectedConfigNames(), withHandler);
        // The switch and the startup-only configs change only on disk.
        for (String name : new String[] {"query_audit_always_on", "skip_audit_user_list", "audit_log_modules"}) {
            Assertions.assertFalse(Config.class.getField(name).getAnnotation(ConfField.class).mutable(), name);
        }
    }

    private static void setWeakVariables() {
        GlobalVariable.enableAuditLoader = false;
        GlobalVariable.auditPluginMaxSqlLength = 100;
        GlobalVariable.auditPluginMaxInsertStmtLength = 10;
        GlobalVariable.auditPluginLoadTimeoutS = 1;
        GlobalVariable.auditPluginMaxBatchBytes = 1;
        GlobalVariable.auditPluginMaxBatchInternalSec = 1;
    }

    private static Optional<String> check(String name, String value) {
        return QueryAuditPolicy.checkVariableValue(name, value);
    }

    // requirement: what the message says the value must be, such as "at least 2097152" or "52428800".
    private static void assertRejected(String name, String value, String requirement) {
        Assertions.assertEquals(Optional.of("Variable '" + name + "' can't be set to '" + value
                + "' because query_audit_always_on is enabled, the value must be " + requirement), check(name, value));
    }

    private static void assertAllowed(String name, String... values) {
        for (String value : values) {
            Assertions.assertEquals(Optional.empty(), check(name, value), name + "=" + value);
        }
    }
}
