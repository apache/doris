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

package org.apache.doris.plugin;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.Dialect;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class DialectPluginTest extends TestWithFeService {

    private static final TestDialectPlugin1 sparkPlugin = new TestDialectPlugin1();
    private static final TestDialectPlugin2 hivePlugin = new TestDialectPlugin2();
    private static final String TEST_SQL = "select * from test_hive_table";

    @Override
    public void runBeforeAll() throws IOException, InterruptedException {
        PluginInfo sparkPluginInfo = new PluginInfo("sparkDialectPlugin", PluginInfo.PluginType.DIALECT, "test");
        Env.getCurrentEnv().getPluginMgr().registerBuiltinPlugin(sparkPluginInfo, sparkPlugin);

        PluginInfo hivePluginInfo = new PluginInfo("hiveDialectPlugin", PluginInfo.PluginType.DIALECT, "test");
        Env.getCurrentEnv().getPluginMgr().registerBuiltinPlugin(hivePluginInfo, hivePlugin);
    }

    @Test
    public void testHivePlugin() {
        ConnectContext.get().getSessionVariable().setSqlDialect(Dialect.HIVE.getDialectName());
        NereidsParser parser = new NereidsParser();
        List<StatementBase> stmts = parser.parseSQL(TEST_SQL, ConnectContext.get().getSessionVariable());
        Assertions.assertEquals(1, stmts.size());
        Assertions.assertTrue(stmts.get(0) instanceof LogicalPlanAdapter);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) stmts.get(0)).getLogicalPlan();
        String convertedSql = hivePlugin.convertSql(TEST_SQL, ConnectContext.get().getSessionVariable());
        Assertions.assertEquals(logicalPlan, parser.parseSingle(convertedSql));
    }

    @Test
    public void testSparkPlugin() {
        ConnectContext.get().getSessionVariable().setSqlDialect(Dialect.SPARK.getDialectName());
        NereidsParser parser = new NereidsParser();
        List<StatementBase> stmts = parser.parseSQL(TEST_SQL, ConnectContext.get().getSessionVariable());
        Assertions.assertEquals(1, stmts.size());
        Assertions.assertTrue(stmts.get(0) instanceof LogicalPlanAdapter);
        LogicalPlan logicalPlan = ((LogicalPlanAdapter) stmts.get(0)).getLogicalPlan();
        List<StatementBase> expectedStmts = sparkPlugin.parseSqlWithDialect(TEST_SQL,
                    ConnectContext.get().getSessionVariable());
        Assertions.assertTrue(expectedStmts != null && expectedStmts.size() == 1);
        Assertions.assertTrue(expectedStmts.get(0) instanceof LogicalPlanAdapter);
        Assertions.assertEquals(logicalPlan, ((LogicalPlanAdapter) expectedStmts.get(0)).getLogicalPlan());
    }
}
