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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.VariableAnnotation;
import org.apache.doris.common.util.DatasourcePrintableMap;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.system.NodeType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class AdminSetFrontendConfigCommandTest extends TestWithFeService {
    @Test
    public void testForwardedRegexConfigPreservesMasking() throws Exception {
        long originalMode = connectContext.getSessionVariable().getSqlMode();
        String originalPattern = Config.datasource_sensitive_property_pattern;
        try {
            for (long mode : new long[] {SqlModeHelper.MODE_DEFAULT, SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES}) {
                connectContext.getSessionVariable().setSqlMode(mode);
                String inputPattern = mode == SqlModeHelper.MODE_DEFAULT
                        ? "^custom[.]pin\\\\d+$" : "^custom[.]pin\\d+$";
                String sql = "ADMIN SET ALL FRONTENDS CONFIG (\"datasource_sensitive_property_pattern\" = \""
                        + inputPattern + "\")";
                NereidsParser parser = new NereidsParser();
                AdminSetFrontendConfigCommand local = (AdminSetFrontendConfigCommand) parser.parseSingle(sql);
                Assertions.assertEquals("^custom[.]pin\\d+$",
                        local.getConfigs().get("datasource_sensitive_property_pattern"));
                Env.getCurrentEnv().setConfig(local, true);
                Assertions.assertEquals(DatasourcePrintableMap.PASSWORD_MASK,
                        DatasourcePrintableMap.redactProperty("custom.pin1", "fixture"));

                AdminSetFrontendConfigCommand peer = (AdminSetFrontendConfigCommand) parser.parseSingle(
                        local.getLocalSetStmt().originStmt);
                Env.getCurrentEnv().setConfig(peer, true);
                Assertions.assertEquals(DatasourcePrintableMap.PASSWORD_MASK,
                        DatasourcePrintableMap.redactProperty("custom.pin1", "fixture"));
                Assertions.assertEquals("fixture", DatasourcePrintableMap.redactProperty("custom.pind", "fixture"));
                Assertions.assertEquals(local.getConfigs(), peer.getConfigs());
                Assertions.assertTrue(peer.isApplyToAll());
            }
        } finally {
            Config.datasource_sensitive_property_pattern = originalPattern;
            connectContext.getSessionVariable().setSqlMode(originalMode);
        }
    }

    @Test
    public void testForwardedConfigStringLiteralsRoundTrip() {
        long originalMode = connectContext.getSessionVariable().getSqlMode();
        try {
            for (long mode : new long[] {SqlModeHelper.MODE_DEFAULT, SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES}) {
                connectContext.getSessionVariable().setSqlMode(mode);
                for (boolean applyToAll : new boolean[] {false, true}) {
                    for (String value : List.of("\\d+", "one\"two", "one'two", "quote\\\"and\\\\tail\\",
                            "\\n\\t\\r\\b\\0\\Z", "\n\t\r\b\0\032", "percent%_\\%", "")) {
                        AdminSetFrontendConfigCommand command = new AdminSetFrontendConfigCommand(NodeType.FRONTEND,
                                Map.of("datasource_sensitive_property_pattern", value), applyToAll);
                        AdminSetFrontendConfigCommand peer = (AdminSetFrontendConfigCommand) new NereidsParser()
                                .parseSingle(command.getLocalSetStmt().originStmt);
                        Assertions.assertEquals(command.getConfigs(), peer.getConfigs());
                        Assertions.assertEquals(applyToAll, peer.isApplyToAll());
                    }
                }
            }
        } finally {
            connectContext.getSessionVariable().setSqlMode(originalMode);
        }
    }

    @Test
    public void testNormal() throws Exception {
        String sql = "admin set frontend config(\"alter_table_timeout_second\" = \"60\");";
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof AdminSetFrontendConfigCommand);
        Assertions.assertDoesNotThrow(() -> ((AdminSetFrontendConfigCommand) plan).validate());
        Assertions.assertTrue(((AdminSetFrontendConfigCommand) plan).getLocalSetStmt().originStmt
                .startsWith("ADMIN SET FRONTEND CONFIG"));
    }

    @Test
    public void testCloudAdminCannotSetFrontendConfig() {
        String originalDeployMode = Config.deploy_mode;
        String originalCloudUniqueId = Config.cloud_unique_id;
        boolean originalEnableUdfInLoad = Config.enable_udf_in_load;
        UserIdentity originalUserIdentity = connectContext.getCurrentUserIdentity();
        try {
            Config.deploy_mode = "cloud";
            Config.cloud_unique_id = "";
            connectContext.setCurrentUserIdentity(UserIdentity.ADMIN);

            IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                    () -> executeSql("admin set frontend config(\"enable_udf_in_load\" = \"true\");"));
            Assertions.assertEquals("errCode = 2, detailMessage = Unsupported operation", exception.getMessage());

            connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
            Assertions.assertDoesNotThrow(
                    () -> executeSql("admin set frontend config(\"enable_udf_in_load\" = \"true\");"));
        } finally {
            connectContext.setCurrentUserIdentity(originalUserIdentity);
            Config.deploy_mode = originalDeployMode;
            Config.cloud_unique_id = originalCloudUniqueId;
            Config.enable_udf_in_load = originalEnableUdfInLoad;
        }
    }

    @Test
    public void testRedirectStatus() {
        String sql = "admin set frontend config(\"alter_table_timeout_second\" = \"60\");";
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof AdminSetFrontendConfigCommand);
        AdminSetFrontendConfigCommand command = (AdminSetFrontendConfigCommand) plan;
        Assertions.assertEquals(command.toRedirectStatus(), RedirectStatus.FORWARD_NO_SYNC);
        Assertions.assertDoesNotThrow(() -> ((AdminSetFrontendConfigCommand) plan).validate());

        sql = "admin set frontend config('workload_runtime_status_thread_interval_ms' = '100');";
        LogicalPlan plan1 = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan1 instanceof AdminSetFrontendConfigCommand);
        command = (AdminSetFrontendConfigCommand) plan1;
        Assertions.assertEquals(command.toRedirectStatus(), RedirectStatus.NO_FORWARD);
        Assertions.assertDoesNotThrow(() -> ((AdminSetFrontendConfigCommand) plan1).validate());
    }

    @Test
    public void testEmptyConfig() {
        String sql = "admin set frontend config;";
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof AdminSetFrontendConfigCommand);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> ((AdminSetFrontendConfigCommand) plan).validate());
        Assertions.assertEquals("errCode = 2, detailMessage = config parameter size is not equal to 1",
                exception.getMessage());
    }

    @Test
    public void testExperimentalConfig() throws Exception {
        // show config
        int num = ConfigBase.getConfigNumByVariableAnnotation(VariableAnnotation.EXPERIMENTAL);
        PatternMatcher matcher = PatternMatcherWrapper.createMysqlPattern("%experimental%",
                CaseSensibility.CONFIG.getCaseSensibility());
        List<List<String>> results = ConfigBase.getConfigInfo(matcher);
        Assertions.assertEquals(num, results.size());

        num = ConfigBase.getConfigNumByVariableAnnotation(VariableAnnotation.DEPRECATED);
        matcher = PatternMatcherWrapper.createMysqlPattern("%deprecated%",
                CaseSensibility.CONFIG.getCaseSensibility());
        results = ConfigBase.getConfigInfo(matcher);
        Assertions.assertEquals(num, results.size());
    }

    @Test
    public void testTrimPropertyKey() throws Exception {
        String sql = "admin set frontend config(\" alter_table_timeout_second \" = \"60\");";
        LogicalPlan plan = new NereidsParser().parseSingle(sql);

        Assertions.assertTrue(plan instanceof AdminSetFrontendConfigCommand);
        Assertions.assertEquals("60", ((AdminSetFrontendConfigCommand) plan).getConfigs().get("alter_table_timeout_second"));
    }

    @Test
    public void testSetAllFrontendsConfig() throws Exception {
        String sql = "admin set all frontends config(\" alter_table_timeout_second \" = \"77\");";
        LogicalPlan plan = new NereidsParser().parseSingle(sql);

        Assertions.assertInstanceOf(AdminSetFrontendConfigCommand.class, plan);
        AdminSetFrontendConfigCommand command = (AdminSetFrontendConfigCommand) plan;
        Assertions.assertTrue(command.isApplyToAll());
        Assertions.assertEquals(command.toRedirectStatus(), RedirectStatus.NO_FORWARD);
        Assertions.assertTrue(command.getLocalSetStmt().originStmt.startsWith("ADMIN SET ALL FRONTENDS CONFIG"));

        Env.getCurrentEnv().setConfig(command, true);
        Assertions.assertEquals(77, Config.alter_table_timeout_second);
    }
}
