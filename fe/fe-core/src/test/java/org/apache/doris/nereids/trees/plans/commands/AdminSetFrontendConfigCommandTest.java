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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.VariableAnnotation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AdminSetFrontendConfigCommandTest extends TestWithFeService {
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
