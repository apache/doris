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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanPatternMatchSupported;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WarmUpSelectTest extends TestWithFeService implements PlanPatternMatchSupported {
    private final NereidsParser parser = new NereidsParser();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");

        createTables(
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    id bigint,\n"
                        + "    score bigint,\n"
                        + "    name varchar(20)\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
    }

    @Test
    public void testWarmUpSelectStar() throws Exception {
        // Enable file cache for warm up select
        connectContext.getSessionVariable().setEnableFileCache(true);
        connectContext.getSessionVariable().setDisableFileCache(false);

        String sql = "warm up select * from T1";
        Assertions.assertTrue(getOutputFragment(sql).getExplainString(TExplainLevel.BRIEF)
                .contains("BLACKHOLE SINK"));
    }

    @Test
    public void testWarmUpSelectSpecificColumns() throws Exception {
        // Enable file cache for warm up select
        connectContext.getSessionVariable().setEnableFileCache(true);
        connectContext.getSessionVariable().setDisableFileCache(false);

        String sql = "warm up select id, score from T1";
        Assertions.assertTrue(getOutputFragment(sql).getExplainString(TExplainLevel.BRIEF)
                .contains("BLACKHOLE SINK"));
    }

    @Test
    public void testWarmUpSelectWithWhereClause() throws Exception {
        // Enable file cache for warm up select
        connectContext.getSessionVariable().setEnableFileCache(true);
        connectContext.getSessionVariable().setDisableFileCache(false);

        String sql = "warm up select id from T1 where id > 0";
        Assertions.assertTrue(getOutputFragment(sql).getExplainString(TExplainLevel.BRIEF)
                .contains("BLACKHOLE SINK"));
    }

    @Test
    public void testWarmUpSelectWithLiteralShouldFail() throws Exception {
        // Enable file cache for warm up select
        connectContext.getSessionVariable().setEnableFileCache(true);
        connectContext.getSessionVariable().setDisableFileCache(false);

        String sql = "warm up select 1 from T1";
        Assertions.assertThrows(Exception.class, () -> getOutputFragment(sql));
    }

    @Test
    public void testWarmUpSelectWithoutFileCache() throws Exception {
        // Disable file cache - should throw exception
        connectContext.getSessionVariable().setEnableFileCache(false);
        connectContext.getSessionVariable().setDisableFileCache(true);

        String sql = "warm up select * from T1";
        Assertions.assertThrows(Exception.class, () -> getOutputFragment(sql));
    }

    @Test
    public void testWarmUpSelectWithComplexExpressionShouldFail() throws Exception {
        // Enable file cache for warm up select
        connectContext.getSessionVariable().setEnableFileCache(true);
        connectContext.getSessionVariable().setDisableFileCache(false);

        // This should fail because complex expressions are not allowed
        String sql = "warm up select id + 1 from T1";
        Assertions.assertThrows(Exception.class, () -> getOutputFragment(sql));
    }

    @Test
    public void testWarmUpSelectWithFunctionShouldFail() throws Exception {
        // Enable file cache for warm up select
        connectContext.getSessionVariable().setEnableFileCache(true);
        connectContext.getSessionVariable().setDisableFileCache(false);

        // This should fail because functions are not allowed
        String sql = "warm up select upper(name) from T1";
        Assertions.assertThrows(Exception.class, () -> getOutputFragment(sql));
    }

    @Test
    public void testWarmUpSelectWithoutTableRefShouldFail() throws Exception {
        // Enable file cache for warm up select
        connectContext.getSessionVariable().setEnableFileCache(true);
        connectContext.getSessionVariable().setDisableFileCache(false);

        // This tests that warm up select without table reference fails
        String sql = "warm up select 1";
        Assertions.assertThrows(Exception.class, () -> getOutputFragment(sql));
    }

    private PlanFragment getOutputFragment(String sql) throws Exception {
        StatementScopeIdGenerator.clear();
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        PhysicalPlan plan = planner.planWithLock(
                parser.parseSingle(sql),
                PhysicalProperties.ANY
        );
        return new PhysicalPlanTranslator(new PlanTranslatorContext(planner.getCascadesContext())).translatePlan(plan);
    }
}

