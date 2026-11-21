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

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdfBuilder;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.insert.FastInsertIntoValuesPlanner;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExplainInsertCommandTest extends TestWithFeService {
    private final NereidsParser parser = new NereidsParser();

    @Override
    public void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
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
        createTable("create table row_ttl_dup (\n"
                + "    k int,\n"
                + "    event_time datetimev2(6),\n"
                + "    v int\n"
                + ")\n"
                + "duplicate key(k)\n"
                + "distributed by hash(k) buckets 1\n"
                + "properties(\n"
                + "    \"replication_num\"=\"1\",\n"
                + "    \"function_column.enable_row_ttl\"=\"true\",\n"
                + "    \"function_column.ttl_col\"=\"event_time\",\n"
                + "    \"function_column.ttl\"=\"1 day\",\n"
                + "    \"function_column.ttl_time_zone\"=\"+08:00\"\n"
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

        createMv("create materialized view k12s3m as select k1 as a1,sum(k2) as a2,max(k2) as a3 from agg_have_dup_base group by k1");
        createMv("create materialized view mv3 as select k1 as a4, k2 + k3 as a5 from agg_have_dup_base group by k1, k2 + k3");
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
        Assertions.assertEquals(9, getOutputFragment(sql).getOutputExprs().size());
    }

    @Test
    public void testInsertAggStateCasts() throws Exception {
        boolean enabled = connectContext.getSessionVariable().enableAggState;
        connectContext.getSessionVariable().enableAggState = true;
        try {
            createTable("create table values_avg (k int, s agg_state<avg(bigint not null)> generic) "
                    + "aggregate key(k) distributed by hash(k) buckets 1 properties('replication_num'='1')");
            String insert = "insert into values_avg ";
            getOutputFragment("explain " + insert + "select 1, avg_combine(cast(1 as bigint))");
            AnalysisException mismatch = Assertions.assertThrows(AnalysisException.class,
                    () -> getOutputFragment("explain " + insert + "select 1, avg_combine(cast(1 as int))"));
            Assertions.assertTrue(mismatch.getMessage().contains("cannot cast agg_state<avg("), mismatch.getMessage());

            String valid = "(1, avg_state(cast(1 as int)))";
            String invalid = "(2, if(rand() < 0.5, avg_state(cast(2 as int)), avg_state(cast(3 as int))))";
            for (boolean batch : new boolean[] {false, true}) {
                planFastValues(insert + "values " + valid, batch);
                for (String rows : new String[] {invalid, valid + "," + invalid}) {
                    AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                            () -> planFastValues(insert + "values " + rows, batch));
                    Assertions.assertTrue(error.getMessage().contains("cannot cast agg_state<avg("), error.getMessage());
                }
            }
        } finally {
            connectContext.getSessionVariable().enableAggState = enabled;
        }
    }

    @Test
    public void testMergeGuardedAggStateCast() throws Exception {
        boolean enableAggState = connectContext.getSessionVariable().enableAggState;
        String disabled = String.join(",", connectContext.getSessionVariable().getDisableNereidsRuleNames());
        try {
            // The mock tables have no rowsets. Keep their scans and the guarded assignment in the plan.
            connectContext.getSessionVariable().setDisableNereidsRules(disabled + ",PRUNE_EMPTY_PARTITION");
            connectContext.getSessionVariable().enableAggState = true;
            createTable("create table merge_min_target (k int, v bigint) "
                    + "unique key(k) distributed by hash(k) buckets 1 "
                    + "properties('replication_num'='1', 'enable_unique_key_merge_on_write'='true')");
            createTable("create table merge_min_src (k int, d double not null, b bigint not null) "
                    + "duplicate key(k) distributed by hash(k) buckets 1 properties('replication_num'='1')");
            String merge = "explain merge into merge_min_target t using "
                    + "(select k, min_combine(%s) as s from merge_min_src group by k) q on t.k=q.k "
                    + "when matched and t.k >= 0 then update set v=min_finalize(cast(q.s as "
                    + "agg_state<min(bigint not null)>)) when matched then update set v=t.v";
            getOutputFragment(String.format(merge, "b"));
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> getOutputFragment(String.format(merge, "d")));
            Assertions.assertTrue(error.getMessage().contains("cannot cast agg_state<min("), error.getMessage());
        } finally {
            connectContext.getSessionVariable().setDisableNereidsRules(disabled);
            connectContext.getSessionVariable().enableAggState = enableAggState;
        }
    }

    @Test
    public void testFastValuesAggStateCasts() throws Exception {
        boolean enableAggState = connectContext.getSessionVariable().enableAggState;
        boolean enableDecimal256 = connectContext.getSessionVariable().enableDecimal256;
        try {
            connectContext.getSessionVariable().enableAggState = true;
            connectContext.getSessionVariable().enableDecimal256 = false;
            createTable("create table values_max (k int, m agg_state<max(bigint not null)> generic) aggregate key(k) "
                    + "distributed by hash(k) buckets 1 properties('replication_num'='1')");
            createTable("create table values_sum (k int, "
                    + "s agg_state<sum(decimal(18,2) not null)> generic) aggregate key(k) "
                    + "distributed by hash(k) buckets 1 properties('replication_num'='1')");
            connectContext.getSessionVariable().enableDecimal256 = true;
            for (boolean batch : new boolean[] {false, true}) {
                String max = "(1, max_state(cast(7 as int)))";
                String sum = "(1, sum_state(cast(7 as decimal(18,2))))";
                for (boolean multiRow : new boolean[] {false, true}) {
                    planFastValues("insert into values_max values " + max + (multiRow ? "," + max : ""), batch);
                    AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                            () -> planFastValues("insert into values_sum values "
                                    + sum + (multiRow ? "," + sum : ""), batch));
                    Assertions.assertTrue(error.getMessage().contains("cannot cast agg_state<sum"), error.getMessage());
                }
            }
        } finally {
            connectContext.getSessionVariable().enableAggState = enableAggState;
            connectContext.getSessionVariable().enableDecimal256 = enableDecimal256;
        }
    }

    @Test
    public void testHighScaleMapStateConsumers() throws Exception {
        boolean enableAggState = connectContext.getSessionVariable().enableAggState;
        boolean enableDecimal256 = connectContext.getSessionVariable().enableDecimal256;
        try {
            connectContext.getSessionVariable().enableAggState = true;
            connectContext.getSessionVariable().enableDecimal256 = true;
            createTable("create table high_scale_map_state (k int, "
                    + "s agg_state<avg_map(map<int,decimal(76,40)> not null)> generic) aggregate key(k) "
                    + "distributed by hash(k) buckets 1 properties('replication_num'='1')");
            Column column = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test")
                    .getTableOrAnalysisException("high_scale_map_state").getColumn("s");
            AggStateType saved = (AggStateType) column.getType();
            connectContext.getSessionVariable().enableDecimal256 = false;
            for (boolean legacy : new boolean[] {false, true}) {
                if (legacy) {
                    column.setType(new AggStateType(saved.getFunctionName(), saved.getResultIsNullable(),
                            saved.getSubTypes(), saved.getSubTypeNullables()));
                }
                for (String expression : new String[] {"avg_map_merge(s)", "avg_map_union(s)",
                        "avg_map_finalize(avg_map_union(s))", "avg_map_finalize(s)"}) {
                    PlanFragment fragment = getOutputFragment(
                            "explain select " + expression + " from high_scale_map_state");
                    Assertions.assertEquals(1, fragment.getOutputExprs().size());
                    Assertions.assertEquals(expression.equals("avg_map_union(s)")
                            ? column.getType() : saved.getResultType(), fragment.getOutputExprs().get(0).getType());
                }
            }
        } finally {
            connectContext.getSessionVariable().enableAggState = enableAggState;
            connectContext.getSessionVariable().enableDecimal256 = enableDecimal256;
        }
    }

    @Test
    public void testInsertIntoLegacyState() throws Exception {
        boolean enableAggState = connectContext.getSessionVariable().enableAggState;
        boolean enableDecimal256 = connectContext.getSessionVariable().enableDecimal256;
        try {
            connectContext.getSessionVariable().enableAggState = true;
            connectContext.getSessionVariable().enableDecimal256 = false;
            createTable("create table legacy_sum (k int, s agg_state<sum(decimal(18,2) not null)> generic) "
                    + "aggregate key(k) distributed by hash(k) buckets 1 properties('replication_num'='1')");
            Column column = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test")
                    .getTableOrAnalysisException("legacy_sum").getColumn("s");
            AggStateType saved = (AggStateType) column.getType();
            column.setType(new AggStateType(saved.getFunctionName(), saved.getResultIsNullable(),
                    saved.getSubTypes(), saved.getSubTypeNullables()));

            String values = "insert into legacy_sum values (1, sum_state(cast(7 as decimal(18,2))))";
            String select = "explain insert into legacy_sum select 1, sum_state(cast(7 as decimal(18,2)))";
            FastInsertIntoValuesPlanner planner = planFastValues(values, false);
            PlanFragment fragment = new PhysicalPlanTranslator(new PlanTranslatorContext(planner.getCascadesContext()))
                    .translatePlan(planner.getPhysicalPlan());
            TExpr stateValue = fragment.getPlanRoot().treeToThrift().getNodes().get(0)
                    .getUnionNode().getConstExprLists().get(0).get(1);
            // The BE must receive the cast to the legacy slot type when it materializes this constant row.
            Assertions.assertEquals(TExprNodeType.CAST_EXPR, stateValue.getNodes().get(0).getNodeType());
            getOutputFragment(select);
            connectContext.getSessionVariable().enableDecimal256 = true;
            getOutputFragment("explain insert into legacy_sum select k, s from legacy_sum");
            Assertions.assertTrue(Assertions.assertThrows(AnalysisException.class,
                    () -> planFastValues(values, false)).getMessage().contains("cannot cast agg_state<sum"));
            Assertions.assertTrue(Assertions.assertThrows(AnalysisException.class,
                    () -> getOutputFragment(select)).getMessage().contains("cannot cast agg_state<sum"));
            Assertions.assertNull(((AggStateType) column.getType()).getResultType());
        } finally {
            connectContext.getSessionVariable().enableAggState = enableAggState;
            connectContext.getSessionVariable().enableDecimal256 = enableDecimal256;
        }
    }

    @Test
    public void testAggStateBindingIgnoresAliasFunction() throws Exception {
        boolean enableAggState = connectContext.getSessionVariable().enableAggState;
        boolean preferUdf = connectContext.getSessionVariable().preferUdfOverBuiltin;
        connectContext.getSessionVariable().enableAggState = true;
        try {
            createTable("create table legacy_sum_shadowed (k int, s agg_state<sum(int not null)> generic) "
                    + "aggregate key(k) distributed by hash(k) buckets 1 properties('replication_num'='1')");
            Column column = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test")
                    .getTableOrAnalysisException("legacy_sum_shadowed").getColumn("s");
            AggStateType saved = (AggStateType) column.getType();
            column.setType(new AggStateType(saved.getFunctionName(), saved.getResultIsNullable(),
                    saved.getSubTypes(), saved.getSubTypeNullables()));

            createFunction("CREATE ALIAS FUNCTION sum(INT) WITH PARAMETER(x) AS abs(x)");
            try {
                connectContext.getSessionVariable().preferUdfOverBuiltin = true;
                Assertions.assertInstanceOf(AliasUdfBuilder.class, Env.getCurrentEnv().getFunctionRegistry()
                        .findFunctionBuilder("sum", new SlotReference("x", IntegerType.INSTANCE, false)));

                Assertions.assertAll(
                        () -> {
                            createTable("create table new_sum_shadowed (k int, s agg_state<sum(int not null)> generic) "
                                    + "aggregate key(k) distributed by hash(k) buckets 1 "
                                    + "properties('replication_num'='1')");
                            AggStateType state = (AggStateType) Env.getCurrentInternalCatalog()
                                    .getDbOrAnalysisException("test").getTableOrAnalysisException("new_sum_shadowed")
                                    .getColumn("s").getType();
                            Assertions.assertEquals(Type.BIGINT, state.getResultType());
                        },
                        () -> Assertions.assertEquals(Type.BIGINT, getOutputFragment(
                                "explain select sum_merge(s) from legacy_sum_shadowed")
                                .getOutputExprs().get(0).getType()));
            } finally {
                connectContext.getSessionVariable().preferUdfOverBuiltin = preferUdf;
                executeSql("DROP FUNCTION sum(INT)");
            }
        } finally {
            connectContext.getSessionVariable().enableAggState = enableAggState;
        }
    }

    private FastInsertIntoValuesPlanner planFastValues(String sql, boolean batch) throws Exception {
        StatementContext context = MemoTestUtils.createStatementContext(connectContext, sql);
        LogicalPlan logical = (LogicalPlan) ((Explainable) parser.parseSingle(sql)).getExplainPlan(connectContext);
        FastInsertIntoValuesPlanner planner = new FastInsertIntoValuesPlanner(context, true, batch);
        planner.planWithLock(logical, PhysicalProperties.ANY);
        return planner;
    }

    @Test
    public void testInsertIntoRowTtlTable() throws Exception {
        String sql = "explain insert into row_ttl_dup(k, event_time, v) values"
                + "(1, now(6) - interval 2 day, 10),"
                + "(2, null, 20),"
                + "(3, now(6), 30)";
        Assertions.assertEquals(4, getOutputFragment(sql).getOutputExprs().size());
    }

    @Test
    public void testRowTtlRejectsPartialColumnInsertBeforeBindingDefaults() throws Exception {
        createTable("create table row_ttl_partial_source (k int, "
                + "event_time datetimev2(6) default '2099-01-01', v int) unique key(k) "
                + "distributed by hash(k) buckets 1 properties('replication_num'='1', "
                + "'enable_unique_key_merge_on_write'='true', 'function_column.enable_row_ttl'='true', "
                + "'function_column.ttl_col'='event_time', 'function_column.ttl'='0', "
                + "'function_column.ttl_time_zone'='+00:00')");
        createTable("create table row_ttl_partial_direct (k int, v int) unique key(k) "
                + "distributed by hash(k) buckets 1 properties('replication_num'='1', "
                + "'enable_unique_key_merge_on_write'='true', 'function_column.enable_row_ttl'='true')");
        boolean previous = connectContext.getSessionVariable().isEnableUniqueKeyPartialUpdate();
        connectContext.getSessionVariable().setEnableUniqueKeyPartialUpdate(true);
        try {
            for (String insert : new String[] {
                    "row_ttl_partial_source(k,v) values(1,2)",
                    "row_ttl_partial_source(k,event_time) values(1,'2099-01-01')",
                    "row_ttl_partial_source(k,v) select 1,2",
                    "row_ttl_partial_direct(k,v) values(1,2)",
                    "row_ttl_partial_direct(k,__DORIS_TTL_COL__) values(1,4102444800000000)"}) {
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        () -> getOutputFragment("explain insert into " + insert));
                Assertions.assertTrue(error.getMessage().contains(
                        "Partial update is not supported on tables with row TTL"), error.getMessage());
            }
            // The session setting must not reject an actual full write.
            Assertions.assertDoesNotThrow(() -> getOutputFragment(
                    "explain insert into row_ttl_partial_source values(1,'2099-01-01',2)"));
            Assertions.assertDoesNotThrow(() -> getOutputFragment(
                    "explain insert into row_ttl_partial_direct values(1,2)"));
            connectContext.getSessionVariable().setEnableUniqueKeyPartialUpdate(false);
            Assertions.assertDoesNotThrow(() -> getOutputFragment(
                    "explain insert into row_ttl_partial_source(k,v) values(1,2)"));
        } finally {
            connectContext.getSessionVariable().setEnableUniqueKeyPartialUpdate(previous);
        }
    }

    @Test
    public void testInsertDirectRowTtlExpressions() throws Exception {
        createTable("create table row_ttl_direct_insert (k int, v int) duplicate key(k) "
                + "distributed by hash(k) buckets 1 properties('replication_num'='1', "
                + "'function_column.enable_row_ttl'='true')");
        String target = "explain insert into row_ttl_direct_insert(k, v, __DORIS_TTL_COL__) ";
        for (String expiration : new String[] {"NULL", "DEFAULT", "9223372036854775807",
                "cast('2039-01-01' as date)", "cast('2039-01-01 00:00:00.123456' as datetime(6))",
                "cast('2039-01-01 00:00:00.123456+08:00' as timestamptz(6))"}) {
            Assertions.assertEquals(3, getOutputFragment(target + "values(1, 2, " + expiration + ")")
                    .getOutputExprs().size(), expiration);
        }
        Assertions.assertEquals(3, getOutputFragment(target + "values "
                + "(1, 2, cast('2039-01-01' as date)), (2, 3, 9223372036854775807), (3, 4, NULL)")
                .getOutputExprs().size());
        Assertions.assertEquals(3, getOutputFragment(target
                + "select k1, k2, cast('2039-01-01' as datetime(6)) from src").getOutputExprs().size());
        Assertions.assertEquals(3, getOutputFragment("explain insert into row_ttl_direct_insert values(1, 2)")
                .getOutputExprs().size());
    }

    @Test
    public void testAnalysisException() {
        String sql = "explain insert into t1(v1, v2) select k2 * 2, v1 + 1, v2 + 4 from src";
        Assertions.assertThrows(AnalysisException.class, () -> getOutputFragment(sql));
    }

    @Test
    public void testWithMV() throws Exception {
        String sql = "explain insert into agg_have_dup_base select -4, -4, -4, 'd'";
        Assertions.assertEquals(9, getOutputFragment(sql).getOutputExprs().size());
        sql = "explain insert into agg_have_dup_base select -4, k2, -4, 'd' from agg_have_dup_base";
        Assertions.assertEquals(9, getOutputFragment(sql).getOutputExprs().size());
    }

    @Test
    public void explainNone() throws Exception {
        String sql = "explain insert into agg_have_dup_base select -4, -4, -4, 'd'";
        connectContext.getSessionVariable().enableExplainNone = false;
        StmtExecutor sqlStmtExecutor = getSqlStmtExecutor(sql);
        String explainString = sqlStmtExecutor.planner()
                .getExplainString(new ExplainOptions(ExplainLevel.VERBOSE, false));
        Assertions.assertNotEquals("", explainString);

        connectContext.getSessionVariable().enableExplainNone = true;
        sqlStmtExecutor = getSqlStmtExecutor(sql);
        explainString = sqlStmtExecutor.planner()
                .getExplainString(new ExplainOptions(ExplainLevel.VERBOSE, false));
        Assertions.assertEquals("", explainString);
    }

    private PlanFragment getOutputFragment(String sql) throws Exception {
        StatementScopeIdGenerator.clear();
        StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        LogicalPlan logicalPlan = (LogicalPlan) ((Explainable) (((ExplainCommand) parser.parseSingle(sql))
                .getLogicalPlan())).getExplainPlan(connectContext);
        PhysicalPlan plan = planner.planWithLock(logicalPlan, PhysicalProperties.ANY);
        return new PhysicalPlanTranslator(new PlanTranslatorContext(planner.getCascadesContext())).translatePlan(plan);
    }
}
