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

package org.apache.doris.cloud;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.OnTablesFilter.TableFilterRule;
import org.apache.doris.cloud.OnTablesFilter.TableFilterRule.RuleType;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.WarmUpClusterCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Tests parsing of WARM UP CLUSTER ... ON TABLES (...) grammar.
 * Covers valid syntax, extracted rule types/patterns, and syntax errors.
 */
public class WarmUpClusterOnTablesParseTest {

    private static ConnectContext connectContext;
    private static Env env;
    private static Object originalSystemInfo;

    private static void setField(Object target, Class<?> clazz, String fieldName, Object value) throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Object target, Class<?> clazz, String fieldName) throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    @BeforeAll
    public static void init() throws Exception {
        env = Env.getCurrentEnv();
        originalSystemInfo = getField(env, Env.class, "systemInfo");
        connectContext = new ConnectContext();
        connectContext.setEnv(env);
        connectContext.setThreadLocalInfo();
    }

    @AfterAll
    public static void tearDown() throws Exception {
        setField(env, Env.class, "systemInfo", originalSystemInfo);
        ConnectContext.remove();
    }

    private WarmUpClusterCommand parse(String sql) {
        NereidsParser parser = new NereidsParser();
        LogicalPlan plan = parser.parseSingle(sql);
        Assertions.assertInstanceOf(WarmUpClusterCommand.class, plan);
        return (WarmUpClusterCommand) plan;
    }

    private void mockValidateEnv(String srcCluster, String dstCluster) throws Exception {
        CloudSystemInfoService cloudSys = Mockito.mock(CloudSystemInfoService.class);
        Mockito.when(cloudSys.containClusterName(srcCluster)).thenReturn(true);
        Mockito.when(cloudSys.containClusterName(dstCluster)).thenReturn(true);
        setField(env, Env.class, "systemInfo", cloudSys);
    }

    // ===== Valid syntax: ON TABLES clause is parsed correctly =====

    @Test
    public void testOnTablesSingleInclude() {
        WarmUpClusterCommand cmd = parse(
                "WARM UP CLUSTER dst WITH CLUSTER src "
                + "ON TABLES (INCLUDE 'ods.*') "
                + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')");
        List<TableFilterRule> rules = cmd.getOnTablesRules();
        Assertions.assertNotNull(rules);
        Assertions.assertEquals(1, rules.size());
        Assertions.assertEquals(RuleType.INCLUDE, rules.get(0).getRuleType());
        Assertions.assertEquals("ods.*", rules.get(0).getRawPattern());
    }

    @Test
    public void testOnTablesMultipleRules() {
        WarmUpClusterCommand cmd = parse(
                "WARM UP CLUSTER dst WITH CLUSTER src "
                + "ON TABLES (INCLUDE 'ods.*', INCLUDE 'dw.*', EXCLUDE 'dw.tmp_*') "
                + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')");
        List<TableFilterRule> rules = cmd.getOnTablesRules();
        Assertions.assertNotNull(rules);
        Assertions.assertEquals(3, rules.size());
        Assertions.assertEquals(RuleType.INCLUDE, rules.get(0).getRuleType());
        Assertions.assertEquals("ods.*", rules.get(0).getRawPattern());
        Assertions.assertEquals(RuleType.INCLUDE, rules.get(1).getRuleType());
        Assertions.assertEquals("dw.*", rules.get(1).getRawPattern());
        Assertions.assertEquals(RuleType.EXCLUDE, rules.get(2).getRuleType());
        Assertions.assertEquals("dw.tmp_*", rules.get(2).getRawPattern());
    }

    @Test
    public void testWithoutOnTablesClause() {
        WarmUpClusterCommand cmd = parse(
                "WARM UP CLUSTER dst WITH CLUSTER src "
                + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')");
        Assertions.assertNull(cmd.getOnTablesRules());
    }

    @Test
    public void testOnTablesWithForce() {
        WarmUpClusterCommand cmd = parse(
                "WARM UP CLUSTER dst WITH CLUSTER src FORCE "
                + "ON TABLES (INCLUDE 'ods.*') "
                + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')");
        Assertions.assertTrue(cmd.isForce());
        Assertions.assertNotNull(cmd.getOnTablesRules());
        Assertions.assertEquals(1, cmd.getOnTablesRules().size());
    }

    @Test
    public void testOnTablesWithComputeGroup() {
        WarmUpClusterCommand cmd = parse(
                "WARM UP COMPUTE GROUP dst WITH COMPUTE GROUP src "
                + "ON TABLES (INCLUDE 'db1.*') "
                + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')");
        Assertions.assertNotNull(cmd.getOnTablesRules());
        Assertions.assertEquals(1, cmd.getOnTablesRules().size());
    }

    // ===== Syntax errors =====

    @Test
    public void testOnTablesEmptyParensFails() {
        Assertions.assertThrows(ParseException.class, () ->
                parse("WARM UP CLUSTER dst WITH CLUSTER src "
                    + "ON TABLES () "
                    + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')"));
    }

    @Test
    public void testOnTablesMissingParensFails() {
        Assertions.assertThrows(ParseException.class, () ->
                parse("WARM UP CLUSTER dst WITH CLUSTER src "
                    + "ON TABLES INCLUDE 'ods.*' "
                    + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')"));
    }

    @Test
    public void testOnTablesMissingPatternFails() {
        Assertions.assertThrows(ParseException.class, () ->
                parse("WARM UP CLUSTER dst WITH CLUSTER src "
                    + "ON TABLES (INCLUDE) "
                    + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')"));
    }

    // ===== Validation logic in WarmUpClusterCommand =====

    @Test
    public void testOnTablesExcludeOnlyParsesButLacksInclude() {
        WarmUpClusterCommand cmd = parse(
                "WARM UP CLUSTER dst WITH CLUSTER src "
                + "ON TABLES (EXCLUDE 'ods.tmp_*') "
                + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')");
        List<TableFilterRule> rules = cmd.getOnTablesRules();
        Assertions.assertEquals(1, rules.size());
        Assertions.assertEquals(RuleType.EXCLUDE, rules.get(0).getRuleType());
        boolean hasInclude = rules.stream()
                .anyMatch(r -> r.getRuleType() == RuleType.INCLUDE);
        Assertions.assertFalse(hasInclude, "Exclude-only rules should have no INCLUDE");
    }

    @Test
    public void testOnTablesNonEventDrivenSyncModeParses() {
        WarmUpClusterCommand cmd = parse(
                "WARM UP CLUSTER dst WITH CLUSTER src "
                + "ON TABLES (INCLUDE 'ods.*') "
                + "PROPERTIES('sync_mode'='once')");
        Assertions.assertNotNull(cmd.getOnTablesRules());
        Assertions.assertEquals("once", cmd.getProperties().get("sync_mode"));
    }

    @Test
    public void testOnTablesExcludeOnlyValidateFails() {
        String originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "test_cloud";
        try {
            mockValidateEnv("src", "dst");
            WarmUpClusterCommand cmd = parse(
                    "WARM UP CLUSTER dst WITH CLUSTER src "
                    + "ON TABLES (EXCLUDE 'ods.tmp_*') "
                    + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')");
            Assertions.assertThrows(AnalysisException.class, () -> cmd.validate(new ConnectContext()));
        } catch (Exception e) {
            Assertions.fail(e);
        } finally {
            try {
                setField(env, Env.class, "systemInfo", originalSystemInfo);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    @Test
    public void testOnTablesNonEventDrivenValidateFails() {
        String originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "test_cloud";
        try {
            mockValidateEnv("src", "dst");
            WarmUpClusterCommand cmd = parse(
                    "WARM UP CLUSTER dst WITH CLUSTER src "
                    + "ON TABLES (INCLUDE 'ods.*') "
                    + "PROPERTIES('sync_mode'='once')");
            Assertions.assertThrows(AnalysisException.class, () -> cmd.validate(new ConnectContext()));
        } catch (Exception e) {
            Assertions.fail(e);
        } finally {
            try {
                setField(env, Env.class, "systemInfo", originalSystemInfo);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    @Test
    public void testOnTablesPatternWithoutDbTableFormatValidateFails() {
        String originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "test_cloud";
        try {
            mockValidateEnv("src", "dst");
            WarmUpClusterCommand cmd = parse(
                    "WARM UP CLUSTER dst WITH CLUSTER src "
                    + "ON TABLES (INCLUDE 'orders') "
                    + "PROPERTIES('sync_mode'='event_driven', 'sync_event'='LOAD')");
            Assertions.assertThrows(AnalysisException.class, () -> cmd.validate(new ConnectContext()));
        } catch (Exception e) {
            Assertions.fail(e);
        } finally {
            try {
                setField(env, Env.class, "systemInfo", originalSystemInfo);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }
}
