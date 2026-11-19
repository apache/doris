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

package org.apache.doris.nereids;

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.common.Config;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class NereidsPlannerFeResultTest extends TestWithFeService {
    @Test
    void testCloudExplainDoesNotFormatTimestampResults() throws Exception {
        String originalCloudId = Config.cloud_unique_id;
        String originalZone = connectContext.getSessionVariable().getTimeZone();
        boolean originalFold = connectContext.getSessionVariable().isEnableFoldConstantByBe();
        MysqlCommand originalCommand = connectContext.getCommand();
        ConnectContext originalContext = connectContext;
        connectContext = Mockito.spy(originalContext);
        Mockito.doThrow(new ComputeGroupException("No compute group",
                ComputeGroupException.FailedTypeEnum.SYSTEM_NOT_HAVE_COMPUTE_GROUP))
                .when(connectContext).getCloudCluster(false);
        Mockito.doThrow(new AssertionError("A constant FE plan must not require a compute group"))
                .when(connectContext).getComputeGroup();
        connectContext.setThreadLocalInfo();
        try {
            Config.cloud_unique_id = "fe_result_planning_test";
            connectContext.setCommand(MysqlCommand.COM_QUERY);
            connectContext.getSessionVariable().setEnableFoldConstantByBe(false);
            for (String[] boundary : new String[][] {
                    {"+08:00", "9999-12-31 23:59:59+00:00"},
                    {"-08:00", "0000-01-01 00:00:00+00:00"}}) {
                connectContext.getSessionVariable().setTimeZone(boundary[0]);
                String timestamp = "cast('" + boundary[1] + "' as timestamptz(0))";
                for (String value : new String[] {timestamp, "array(" + timestamp + ", null)"}) {
                    for (ExplainLevel level : new ExplainLevel[] {
                            ExplainLevel.ALL_PLAN, ExplainLevel.DISTRIBUTED_PLAN}) {
                        String sql = "select " + value;
                        try (StatementContext statementContext = createStatementCtx(sql)) {
                            NereidsPlanner planner = new NereidsPlanner(statementContext);
                            LogicalPlanAdapter statement = new LogicalPlanAdapter(
                                    new NereidsParser().parseSingle(sql), statementContext);
                            ExplainOptions options = new ExplainOptions(level, false);
                            statement.setIsExplain(options);
                            // EXPLAIN must not serialize a value that only SELECT needs to display.
                            Assertions.assertDoesNotThrow(() -> planner.plan(statement), level + ": " + sql);
                            Assertions.assertFalse(planner.getExplainString(options).isEmpty());
                        }
                    }
                    String sql = "select " + value;
                    try (StatementContext statementContext = createStatementCtx(sql)) {
                        NereidsPlanner planner = new NereidsPlanner(statementContext);
                        LogicalPlanAdapter statement = new LogicalPlanAdapter(
                                new NereidsParser().parseSingle(sql), statementContext);
                        planner.plan(statement);
                        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                                () -> planner.handleQueryInFe(statement));
                        Assertions.assertTrue(error.getMessage().contains(
                                "TIMESTAMPTZ local year is outside [0, 9999]"));
                    }
                }
            }
        } finally {
            Config.cloud_unique_id = originalCloudId;
            connectContext.getSessionVariable().setTimeZone(originalZone);
            connectContext.getSessionVariable().setEnableFoldConstantByBe(originalFold);
            connectContext.setCommand(originalCommand);
            connectContext = originalContext;
            connectContext.setThreadLocalInfo();
        }
    }
}
