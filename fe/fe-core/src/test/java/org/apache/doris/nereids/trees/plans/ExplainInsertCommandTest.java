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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExplainInsertCommandTest extends TestWithFeService {
    private final NereidsParser parser = new NereidsParser();

    @Override
    public void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");
        createTable("create table t1 (\n"
                + "    k1 int,\n"
                + "    k2 int,\n"
                + "    v1 int,\n"
                + "    v2 int\n"
                + ")\n"
                + "duplicate key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table t2 (\n"
                + "    k1 int,\n"
                + "    k2 int,\n"
                + "    v1 int,\n"
                + "    v2 int\n"
                + ")\n"
                + "unique key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");
        createTable("create table src (\n"
                + "    k1 int,\n"
                + "    k2 int,\n"
                + "    v1 int,\n"
                + "    v2 int\n"
                + ")\n"
                + "duplicate key(k1, k2)\n"
                + "distributed by hash(k1) buckets 4\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ")");

        createTable("create table agg_have_dup_base(\n"
                + "    k1 int null,\n"
                + "    k2 int not null,\n"
                + "    k3 bigint null,\n"
                + "    k4 varchar(100) null\n"
                + "    )\n"
                + "duplicate key (k1,k2,k3)\n"
                + "distributed BY hash(k1) buckets 3\n"
                + "properties(\"replication_num\" = \"1\")");

        createMv("create materialized view k12s3m as select k1,sum(k2),max(k2) from agg_have_dup_base group by k1");
        createMv("create materialized view mv3 as select k1, k2 + k3 from agg_have_dup_base group by k1, k2 + k3");
    }

    @Test
    public void testInsertIntoDuplicateKeyTable() throws Exception {
        String sql = "explain insert into t1 select * from src";
        Assertions.assertEquals(4, getOutputFragment(sql).getOutputExprs().size());

    }

    @Test
    public void testInsertIntoUniqueKeyTable() throws Exception {
        String sql = "explain insert into t2 select * from src";
        Assertions.assertEquals(6, getOutputFragment(sql).getOutputExprs().size());

    }

    @Test
    public void testInsertIntoSomeColumns() throws Exception {
        String sql = "explain insert into t1 (v1, v2) select v1 + 1, v2 + 4 from src";
        Assertions.assertEquals(4, getOutputFragment(sql).getOutputExprs().size());
    }

    @Test
    public void testInsertIntoValues() throws Exception {
        String sql = "explain insert into t1 values(1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)";
        Assertions.assertEquals(4, getOutputFragment(sql).getOutputExprs().size());
        sql = "explain insert into t2 values(1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)";
        Assertions.assertEquals(6, getOutputFragment(sql).getOutputExprs().size());
        sql = "explain insert into agg_have_dup_base values(-4, -4, -4, 'd')";
        Assertions.assertEquals(8, getOutputFragment(sql).getOutputExprs().size());
    }

    @Test
    public void testAnalysisException() {
        String sql = "explain insert into t1(v1, v2) select k2 * 2, v1 + 1, v2 + 4 from src";
        Assertions.assertThrows(AnalysisException.class, () -> getOutputFragment(sql));
    }

    @Test
    public void testWithMV() throws Exception {
        String sql = "explain insert into agg_have_dup_base select -4, -4, -4, 'd'";
        Assertions.assertEquals(8, getOutputFragment(sql).getOutputExprs().size());
        sql = "explain insert into agg_have_dup_base select -4, k2, -4, 'd' from agg_have_dup_base";
        Assertions.assertEquals(8, getOutputFragment(sql).getOutputExprs().size());
    }

    private PlanFragment getOutputFragment(String sql) throws Exception {
        StatementScopeIdGenerator.clear();
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        PhysicalPlan plan = planner.plan(
                ((ExplainCommand) parser.parseSingle(sql)).getLogicalPlan(),
                PhysicalProperties.ANY
        );
        return new PhysicalPlanTranslator(new PlanTranslatorContext(planner.getCascadesContext())).translatePlan(plan);
    }
}
