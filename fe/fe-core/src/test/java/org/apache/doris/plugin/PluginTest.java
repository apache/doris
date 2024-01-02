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
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.parser.Dialect;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class PluginTest extends TestWithFeService {
    @Override
    public void runBeforeAll() throws IOException, InterruptedException {
        connectContext.getState().setNereids(true);
        connectContext.getSessionVariable().enableFallbackToOriginalPlanner = false;
        connectContext.getSessionVariable().enableNereidsTimeout = false;
        connectContext.getSessionVariable().enableNereidsDML = true;
        FeConstants.runningUnitTest = true;

        TestDialectPlugin2 hivePlugin = new TestDialectPlugin2();
        PluginInfo hivePluginInfo = new PluginInfo("hiveDialectPlugin", PluginInfo.PluginType.DIALECT, "test");
        TestDialectPlugin1 sparkPlugin = new TestDialectPlugin1();
        PluginInfo sparkPluginInfo = new PluginInfo("sparkDialectPlugin", PluginInfo.PluginType.DIALECT, "test");
        Env.getCurrentEnv().getPluginMgr().registerBuiltinPlugin(hivePluginInfo, hivePlugin);
        Env.getCurrentEnv().getPluginMgr().registerBuiltinPlugin(sparkPluginInfo, sparkPlugin);
    }

    @Test
    public void testHivePlugin() {
        connectContext.getSessionVariable().setSqlDialect(Dialect.HIVE.getDialectName());
        NereidsParser parser = new NereidsParser();
        List<StatementBase> stmts = parser.parseSQL("select * from test_hive_table",
                    connectContext.getSessionVariable());
        Assertions.assertEquals(1, stmts.size());
        Assertions.assertTrue(stmts.get(0) instanceof LogicalPlan);
        Assertions.assertTrue(stmts.get(0).toString().contains("select 1"));
    }

    @Test
    public void testSparkPlugin() {
        connectContext.getSessionVariable().setSqlDialect(Dialect.SPARK_SQL.getDialectName());
        NereidsParser parser = new NereidsParser();
        List<StatementBase> stmts = parser.parseSQL("select * from test_hive_table",
                    connectContext.getSessionVariable());
        Assertions.assertEquals(1, stmts.size());
        Assertions.assertTrue(stmts.get(0) instanceof LogicalPlan);
        Assertions.assertTrue(stmts.get(0).toString().contains("select 2"));
    }
}
