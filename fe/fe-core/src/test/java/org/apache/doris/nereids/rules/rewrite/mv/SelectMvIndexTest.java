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

package org.apache.doris.nereids.rules.rewrite.mv;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnionAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.DorisAssert;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests ported from {@link org.apache.doris.planner.MaterializedViewFunctionTest}
 */
class SelectMvIndexTest extends BaseMaterializedIndexSelectTest implements MemoPatternMatchSupported {

    private static final String EMPS_TABLE_NAME = "emps";
    private static final String EMPS_MV_NAME = "emps_mv";
    private static final String HR_DB_NAME = "db1";
    private static final String DEPTS_TABLE_NAME = "depts";
    private static final String DEPTS_MV_NAME = "depts_mv";
    private static final String USER_TAG_TABLE_NAME = "user_tags";
    private static final String TEST_TABLE_NAME = "test_tb";
    private static final String USER_TAG_MV_NAME = "user_tags_mv";

    private static final String ADVANCE_TABLE_NAME = "advance";

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(HR_DB_NAME);
        useDatabase(HR_DB_NAME);
        connectContext.getSessionVariable().enableNereidsTimeout = false;
    }

    @BeforeEach
    void before() throws Exception {
        // NOTICE: make COMMISSION as uppercase to test select mv with uppercase column name.
        createTable("create table " + HR_DB_NAME + "." + EMPS_TABLE_NAME + " (time_col date, empid int, "
                + "name varchar, deptno int, salary int, COMMISSION int) partition by range (time_col) "
                + "(partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3"
                + " properties('replication_num' = '1');");

        createTable("create table " + HR_DB_NAME + "." + DEPTS_TABLE_NAME
                + " (time_col date, deptno int, name varchar, cost int) partition by range (time_col) "
                + "(partition p1 values less than MAXVALUE) "
                + "distributed by hash(time_col) buckets 3 properties('replication_num' = '1');");

        createTable("create table " + HR_DB_NAME + "." + USER_TAG_TABLE_NAME
                + " (time_col date, user_id int, user_name varchar(20), tag_id int) partition by range (time_col) "
                + " (partition p1 values less than MAXVALUE) "
                + "distributed by hash(time_col) buckets 3 properties('replication_num' = '1');");

        createTable("create table " + HR_DB_NAME + "." + ADVANCE_TABLE_NAME
                + "(  a int, \n"
                + "  b int, \n"
                + "  c int\n"
                + ")ENGINE=OLAP \n"
                + "DISTRIBUTED BY HASH(a) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");
    }

    @AfterEach
    void after() throws Exception {
        dropTable(EMPS_TABLE_NAME, true);
        dropTable(DEPTS_TABLE_NAME, true);
        dropTable(USER_TAG_TABLE_NAME, true);
        dropTable(ADVANCE_TABLE_NAME, true);
    }

    @Test
    void testProjectionMV1() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String query = "select empid, deptno from " + EMPS_TABLE_NAME + ";";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testProjectionMV2() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String query1 = "select empid + 1 from " + EMPS_TABLE_NAME + " where deptno = 10;";
        createMv(createMVSql);
        testMv(query1, EMPS_MV_NAME);
        String query2 = "select name from " + EMPS_TABLE_NAME + " where deptno -10 = 0;";
        testMv(query2, EMPS_TABLE_NAME);
    }

    @Test
    void testProjectionMV3() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, name from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String query1 = "select empid +1, name from " + EMPS_TABLE_NAME + " where deptno = 10;";
        createMv(createMVSql);
        testMv(query1, EMPS_MV_NAME);
        String query2 = "select name from " + EMPS_TABLE_NAME + " where deptno - 10 = 0;";
        testMv(query2, EMPS_MV_NAME);
    }

    @Test
    void testProjectionMV4() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select name, deptno, salary from "
                + EMPS_TABLE_NAME + ";";
        String query1 = "select name from " + EMPS_TABLE_NAME + " where deptno > 30 and salary > 3000;";
        createMv(createMVSql);
        testMv(query1, EMPS_MV_NAME);
    }

    @Test
    void testFilterMV4() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select name, deptno, salary from "
                + EMPS_TABLE_NAME + " where deptno > 0;";
        String query1 = "select name from " + EMPS_TABLE_NAME + " where " + EMPS_TABLE_NAME + ".deptno > 0;";
        createMv(createMVSql);
        ConnectContext.get().getState().setNereids(true);
        Env.getCurrentEnv().getCurrentCatalog().getDbOrAnalysisException("default_cluster:db1")
                        .getOlapTableOrDdlException(EMPS_TABLE_NAME).getIndexIdToMeta()
                        .forEach((id, meta) -> {
                            if (meta.getWhereClause() != null) {
                                meta.getWhereClause().setDisableTableName(false);
                            }
                        });
        testMv(query1, EMPS_MV_NAME);
        ConnectContext.get().getState().setNereids(false);
        Env.getCurrentEnv().getCurrentCatalog().getDbOrAnalysisException("default_cluster:db1")
                .getOlapTableOrDdlException(EMPS_TABLE_NAME).getIndexIdToMeta()
                .forEach((id, meta) -> {
                    if (meta.getWhereClause() != null) {
                        meta.getWhereClause().setDisableTableName(true);
                    }
                });
    }

    @Test
    void testUnionQueryOnProjectionMV() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String union = "select empid from " + EMPS_TABLE_NAME + " where deptno > 300" + " union all select empid from"
                + " " + EMPS_TABLE_NAME + " where deptno < 200";
        createMv(createMVSql);
        testMvWithTwoTable(union, EMPS_MV_NAME, EMPS_MV_NAME);
    }

    @Test
    void testAggQueryOnAggMV1() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary), "
                + "max(commission) from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select sum(salary), deptno from " + EMPS_TABLE_NAME + " group by deptno;";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testAggQueryOnAggMV2() throws Exception {
        String agg = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " group by deptno";
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as " + agg + ";";
        String query = "select * from (select deptno, sum(salary) as sum_salary from " + EMPS_TABLE_NAME + " group "
                + "by" + " deptno) a where (sum_salary * 2) > 3;";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testAggQueryOnAggMV3() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary)"
                + " from " + EMPS_TABLE_NAME + " group by deptno, commission;";
        String query = "select commission, sum(salary) from " + EMPS_TABLE_NAME + " where "
                + "commission = 100 group by commission;";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    /**
     * Matching failed because the filtering condition under Aggregate
     * references columns for aggregation.
     */
    @Test
    void testAggQueryOnAggMV4() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary)"
                + " from " + EMPS_TABLE_NAME + " group by deptno, commission;";
        String query = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " where salary>1000 group by deptno;";
        createMv(createMVSql);
        testMv(query, EMPS_TABLE_NAME);
    }

    /**
     * There will be a compensating Project added after matching of the Aggregate.
     */
    @Test
    void testAggQuqeryOnAggMV5() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary)"
                + " from " + EMPS_TABLE_NAME + " group by deptno, commission;";
        String query = "select * from (select deptno, sum(salary) as sum_salary from " + EMPS_TABLE_NAME
                + " group by deptno) a where sum_salary>10;";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    /**
     * There will be a compensating Project + Filter added after matching of the Aggregate.
     */
    @Test
    void testAggQuqeryOnAggMV6() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary)"
                + " from " + EMPS_TABLE_NAME + " group by deptno, commission;";
        String query = "select * from (select deptno, sum(salary) as sum_salary from " + EMPS_TABLE_NAME
                + " where deptno>=20 group by deptno) a where sum_salary>10;";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    /**
     * Aggregation query with groupSets at coarser level of aggregation than
     * aggregation materialized view.
     */
    @Test
    void testGroupingSetQueryOnAggMV() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select sum(salary), empid, deptno from " + EMPS_TABLE_NAME + " group by rollup(empid,deptno);";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    /**
     * Aggregation query at coarser level of aggregation than aggregation materialized view.
     */
    @Test
    void testAggQuqeryOnAggMV7() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " " + "group by deptno, commission;";
        String query = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " where deptno>=20 group by deptno;";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testAggQueryOnAggMV8() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select deptno, sum(salary) + 1 from " + EMPS_TABLE_NAME + " group by deptno;";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    /**
     * Query with cube and arithmetic expr
     */
    @Test
    void testAggQueryOnAggMV9() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno, commission;";
        String query = "select deptno, commission, sum(salary) + 1 from " + EMPS_TABLE_NAME
                + " group by cube(deptno,commission);";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    /**
     * Query with rollup and arithmetic expr
     */
    @Test
    void testAggQueryOnAggMV10() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno, commission;";
        String query = "select deptno, commission, sum(salary) + 1 from " + EMPS_TABLE_NAME
                + " group by rollup (deptno, commission);";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    /**
     * Aggregation query with two aggregation operators
     */
    @Test
    void testAggQueryOnAggMV11() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select deptno, sum(salary) + sum(1) from " + EMPS_TABLE_NAME
                + " group by deptno;";
        createMv(createMVSql);
        testMv(query, EMPS_TABLE_NAME);
    }

    /**
     * Aggregation query with distinct on value
     */
    @Test
    void testAggQueryOnAggMV12() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select deptno, sum(distinct salary) from " + EMPS_TABLE_NAME
                + " group by deptno;";
        createMv(createMVSql);
        testMv(query, EMPS_TABLE_NAME);
    }

    /**
     * Aggregation query with distinct on key
     */
    @Test
    void testAggQueryOnAggMV13() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select deptno, min(distinct deptno), sum(salary) from " + EMPS_TABLE_NAME
                + " group by deptno;";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testAggQueryWithSetOperandOnAggMV() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, count(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select deptno, count(salary) + count(1) from " + EMPS_TABLE_NAME
                + " group by deptno union "
                + "select deptno, count(salary) + count(1) from " + EMPS_TABLE_NAME
                + " group by deptno;";
        createMv(createMVSql);
        testMvWithTwoTable(query, EMPS_TABLE_NAME, EMPS_TABLE_NAME);
    }

    @Test
    void testJoinOnLeftProjectToJoin() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME
                + " as select deptno, sum(salary), sum(commission) from " + EMPS_TABLE_NAME + " group by deptno;";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno, max(cost) from "
                + DEPTS_TABLE_NAME + " group by deptno;";
        String query = "select * from (select deptno , sum(salary) from " + EMPS_TABLE_NAME + " group by deptno) A "
                + "join (select deptno, max(cost) from " + DEPTS_TABLE_NAME + " group by deptno ) B on A.deptno = B"
                + ".deptno;";
        createMv(createEmpsMVsql);
        createMv(createDeptsMVSQL);
        testMv(query, ImmutableMap.of(EMPS_TABLE_NAME, EMPS_MV_NAME, DEPTS_TABLE_NAME, DEPTS_MV_NAME));
    }

    @Test
    void testJoinOnRightProjectToJoin() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary), sum"
                + "(commission) from " + EMPS_TABLE_NAME + " group by deptno;";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno, max(cost) from "
                + DEPTS_TABLE_NAME + " group by deptno;";
        String query = "select * from (select deptno , sum(salary), sum(commission) from " + EMPS_TABLE_NAME
                + " group by deptno) A join (select deptno from " + DEPTS_TABLE_NAME + " group by deptno ) B on A"
                + ".deptno = B.deptno;";
        createMv(createEmpsMVsql);
        createMv(createDeptsMVSQL);
        testMv(query, ImmutableMap.of(EMPS_TABLE_NAME, EMPS_MV_NAME, DEPTS_TABLE_NAME, DEPTS_MV_NAME));
    }

    @Test
    void testJoinOnProjectsToJoin() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary), sum"
                + "(commission) from " + EMPS_TABLE_NAME + " group by deptno;";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno, max(cost) from "
                + DEPTS_TABLE_NAME + " group by deptno;";
        String query = "select * from (select deptno , sum(salary) from " + EMPS_TABLE_NAME + " group by deptno) A "
                + "join (select deptno from " + DEPTS_TABLE_NAME + " group by deptno ) B on A.deptno = B.deptno;";
        createMv(createEmpsMVsql);
        createMv(createDeptsMVSQL);
        testMv(query, ImmutableMap.of(EMPS_TABLE_NAME, EMPS_MV_NAME, DEPTS_TABLE_NAME, DEPTS_MV_NAME));
    }

    @Test
    void testJoinOnCalcToJoin0() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + ";";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno from "
                + DEPTS_TABLE_NAME + ";";
        String query = "select * from (select empid, deptno from " + EMPS_TABLE_NAME + " where deptno > 10 ) A "
                + "join (select deptno from " + DEPTS_TABLE_NAME + " ) B on A.deptno = B.deptno;";
        // createMv(createEmpsMVsql);
        // createMv(createDeptsMVSQL);
        new DorisAssert(connectContext).withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVsql);
        testMv(query, ImmutableMap.of(EMPS_TABLE_NAME, EMPS_MV_NAME, DEPTS_TABLE_NAME, DEPTS_MV_NAME));
    }

    @Test
    void testJoinOnCalcToJoin1() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + ";";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno from "
                + DEPTS_TABLE_NAME + ";";
        String query = "select * from (select empid, deptno from " + EMPS_TABLE_NAME + " ) A join (select "
                + "deptno from " + DEPTS_TABLE_NAME + " where deptno > 10 ) B on A.deptno = B.deptno;";
        createMv(createEmpsMVsql);
        createMv(createDeptsMVSQL);
        testMv(query, ImmutableMap.of(EMPS_TABLE_NAME, EMPS_MV_NAME, DEPTS_TABLE_NAME, DEPTS_MV_NAME));
    }

    @Test
    void testJoinOnCalcToJoin2() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + ";";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno from "
                + DEPTS_TABLE_NAME + ";";
        String query = "select * from (select empid, deptno from " + EMPS_TABLE_NAME + " where empid >10 ) A "
                + "join (select deptno from " + DEPTS_TABLE_NAME + " where deptno > 10 ) B on A.deptno = B.deptno;";
        createMv(createEmpsMVsql);
        createMv(createDeptsMVSQL);
        testMv(query, ImmutableMap.of(EMPS_TABLE_NAME, EMPS_MV_NAME, DEPTS_TABLE_NAME, DEPTS_MV_NAME));
    }

    @Test
    void testJoinOnCalcToJoin3() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + ";";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno from "
                + DEPTS_TABLE_NAME + ";";
        String query = "select * from (select empid, deptno + 1 deptno from " + EMPS_TABLE_NAME + " where empid >10 )"
                + " A join (select deptno from " + DEPTS_TABLE_NAME
                + " where deptno > 10 ) B on A.deptno = B.deptno;";
        createMv(createEmpsMVsql);
        createMv(createDeptsMVSQL);
        testMv(query, ImmutableMap.of(EMPS_TABLE_NAME, EMPS_MV_NAME, DEPTS_TABLE_NAME, DEPTS_MV_NAME));
    }

    /**
     * TODO: enable this when implicit case is fully developed.
     */
    void testJoinOnCalcToJoin4() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + ";";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno from "
                + DEPTS_TABLE_NAME + ";";
        String query = "select * from (select empid, deptno + 1 deptno from " + EMPS_TABLE_NAME
                + " where empid is not null ) A full join (select deptno from " + DEPTS_TABLE_NAME
                + " where deptno is not null ) B on A.deptno = B.deptno;";
        createMv(createEmpsMVsql);
        createMv(createDeptsMVSQL);
        testMv(query, ImmutableMap.of(EMPS_TABLE_NAME, EMPS_MV_NAME, DEPTS_TABLE_NAME, DEPTS_MV_NAME));
    }

    /**
     * TODO: enable this when order by column not in project is supported.
     */
    void testOrderByQueryOnProjectView() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + ";";
        String query = "select empid from " + EMPS_TABLE_NAME + " order by deptno";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    /**
     * TODO: enable this when order by column not in select is supported.
     */
    void testOrderByQueryOnOrderByView() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String query = "select empid from " + EMPS_TABLE_NAME + " order by deptno";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testAggregateMVAggregateFuncs1() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno from " + EMPS_TABLE_NAME + " group by deptno";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testAggregateMVAggregateFuncs2() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " group by deptno";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testAggregateMVAggregateFuncs3() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, empid, sum(salary) from " + EMPS_TABLE_NAME + " group by deptno, empid";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testAggregateMVAggregateFuncs4() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " where deptno > 10 group by deptno";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testAggregateMVAggregateFuncs5() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 group by deptno";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testAggregateMVCalcGroupByQuery1() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno+1, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 "
                + "group by deptno+1;";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testAggregateMVCalcGroupByQuery2() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno * empid, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 "
                + "group by deptno * empid;";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testAggregateMVCalcGroupByQuery3() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select empid, deptno * empid, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 "
                + "group by empid, deptno * empid;";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testAggregateMVCalcAggFunctionQuery() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary + 1) from " + EMPS_TABLE_NAME + " where deptno > 10 "
                + "group by deptno;";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_TABLE_NAME);
    }

    /**
     * TODO: enable this when estimate stats bug fixed.
     */
    @Test
    void testSubQuery() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid "
                + "from " + EMPS_TABLE_NAME + ";";
        createMv(createEmpsMVsql);
        String query = "select empid, deptno, salary from " + EMPS_TABLE_NAME + " e1 where empid = (select max(empid)"
                + " from " + EMPS_TABLE_NAME + " where deptno = e1.deptno);";
        testMvWithTwoTable(query, "emps_mv", "emps");
    }

    /**
     * TODO: enable this when sum(distinct xxx) is supported.
     */
    void testDistinctQuery() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query1 = "select distinct deptno from " + EMPS_TABLE_NAME + ";";
        createMv(createEmpsMVsql);
        testMv(query1, EMPS_MV_NAME);
        String query2 = "select deptno, sum(distinct salary) from " + EMPS_TABLE_NAME + " group by deptno;";
        testMv(query2, EMPS_MV_NAME);
    }

    @Test
    void testSingleMVMultiUsage() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, salary "
                + "from " + EMPS_TABLE_NAME + " order by deptno;";
        String query = "select * from (select deptno, empid from " + EMPS_TABLE_NAME + " where deptno>100) A join "
                + "(select deptno, empid from " + EMPS_TABLE_NAME + " where deptno >200) B on A.deptno=B.deptno;";
        createMv(createEmpsMVsql);
        // both of the 2 table scans should use mv index.
        testMv(query, ImmutableMap.of(EMPS_TABLE_NAME, EMPS_MV_NAME));
    }

    @Test
    void testMultiMVMultiUsage() throws Exception {
        String createEmpsMVSql01 = "create materialized view emp_mv_01 as select deptno, empid, salary "
                + "from " + EMPS_TABLE_NAME + " order by deptno;";
        String createEmpsMVSql02 = "create materialized view emp_mv_02 as select deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        createMv(createEmpsMVSql01);
        createMv(createEmpsMVSql02);
        String query = "select * from (select deptno, empid from " + EMPS_TABLE_NAME + " where deptno>100) A join "
                + "(select deptno, sum(salary) from " + EMPS_TABLE_NAME + " where deptno >200 group by deptno) B "
                + "on A.deptno=B.deptno";
        testMvWithTwoTable(query, "emp_mv_02", "emp_mv_01");
    }

    @Test
    void testMVOnJoinQuery() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select salary, empid, deptno from "
                + EMPS_TABLE_NAME + " order by salary;";
        createMv(createEmpsMVsql);
        String query = "select empid, salary from " + EMPS_TABLE_NAME + " join " + DEPTS_TABLE_NAME
                + " on emps.deptno=depts.deptno where salary > 300;";
        testMv(query, ImmutableMap.of(EMPS_TABLE_NAME, EMPS_MV_NAME, DEPTS_TABLE_NAME, DEPTS_TABLE_NAME));
    }

    @Test
    void testAggregateMVOnCountDistinctQuery1() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, count(distinct empid) from " + EMPS_TABLE_NAME + " group by deptno;";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testQueryAfterTrimingOfUnusedFields() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + " order by empid, deptno;";
        String query = "select empid, deptno from (select empid, deptno, salary from " + EMPS_TABLE_NAME + ") A;";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    void testUnionAll() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + " order by empid, deptno;";
        String query = "select empid, deptno from " + EMPS_TABLE_NAME + " where empid >1 union all select empid,"
                + " deptno from " + EMPS_TABLE_NAME + " where empid <0;";
        createMv(createEmpsMVsql);
        testMvWithTwoTable(query, EMPS_MV_NAME, EMPS_MV_NAME);
    }

    @Test
    void testUnionDistinct() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + " order by empid, deptno;";
        createMv(createEmpsMVsql);
        String query = "select empid, deptno from " + EMPS_TABLE_NAME + " where empid >1 union select empid,"
                + " deptno from " + EMPS_TABLE_NAME + " where empid <0;";
        testMvWithTwoTable(query, EMPS_MV_NAME, EMPS_MV_NAME);
    }

    /**
     * Only key columns rollup for aggregate-keys table.
     */
    @Test
    void testDeduplicateQueryInAgg() throws Exception {
        String aggregateTable = "create table dup_agg_table (k1 int, k2 int, v1 bigint sum) aggregate key (k1, k2) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        createTable(aggregateTable);

        // don't use rollup k1_v1
        addRollup("alter table dup_agg_table add rollup k1_v1(k1, v1)");
        // use rollup only_keys
        addRollup("alter table dup_agg_table add rollup only_keys (k2, k1) properties ('replication_num' = '1')");

        String query = "select k1, k2 from dup_agg_table;";
        // todo: `preagg` should be ture when rollup could be used.
        singleTableTest(query, "only_keys", false);
        dropTable("dup_agg_table", true);
    }

    /**
     * Group by only mv for duplicate-keys table.
     * duplicate table (k1, k2, v1 sum)
     * aggregate mv index (k1, k2)
     */
    @Test
    void testGroupByOnlyForDuplicateTable() throws Exception {
        createTable("create table t (k1 int, k2 int, v1 bigint) duplicate key(k1, k2, v1)"
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1')");
        createMv("create materialized view k1_k2 as select k1, k2 from t group by k1, k2");
        singleTableTest("select k1, k2 from t group by k1, k2", "k1_k2", true);
        dropTable("t", true);
    }

    @Test
    void testAggFunctionInHaving() throws Exception {
        String duplicateTable = "CREATE TABLE " + TEST_TABLE_NAME + " ( k1 int(11) NOT NULL ,  k2  int(11) NOT NULL ,"
                + "v1  varchar(4096) NOT NULL, v2  float NOT NULL , v3  decimal(20, 7) NOT NULL ) ENGINE=OLAP "
                + "DUPLICATE KEY( k1 ,  k2 ) DISTRIBUTED BY HASH( k1 ,  k2 ) BUCKETS 3 "
                + "PROPERTIES ('replication_num' = '1'); ";
        createTable(duplicateTable);
        String createK1K2MV = "create materialized view k1_k2 as select k1,k2 from " + TEST_TABLE_NAME + " group by "
                + "k1,k2;";
        createMv(createK1K2MV);
        String query = "select k1 from " + TEST_TABLE_NAME + " group by k1 having max(v1) > 10;";
        testMv(query, TEST_TABLE_NAME);
        dropTable(TEST_TABLE_NAME, true);
    }

    /**
     * TODO: enable this when order by aggregate function is supported.
     */
    void testAggFunctionInOrder() throws Exception {
        String duplicateTable = "CREATE TABLE " + TEST_TABLE_NAME + " ( k1 int(11) NOT NULL ,  k2  int(11) NOT NULL ,"
                + "v1  varchar(4096) NOT NULL, v2  float NOT NULL , v3  decimal(20, 7) NOT NULL ) ENGINE=OLAP "
                + "DUPLICATE KEY( k1 ,  k2 ) DISTRIBUTED BY HASH( k1 ,  k2 ) BUCKETS 3 "
                + "PROPERTIES ('replication_num' = '1'); ";
        createTable(duplicateTable);
        String createK1K2MV = "create materialized view k1_k2 as select k1,k2 from " + TEST_TABLE_NAME + " group by "
                + "k1,k2;";
        createMv(createK1K2MV);
        String query = "select k1 from " + TEST_TABLE_NAME + " group by k1 order by max(v1);";
        testMv(query, TEST_TABLE_NAME);
        dropTable(TEST_TABLE_NAME, true);
    }

    /**
     * TODO: enable when window is supported.
     */
    @Test
    void testWindowsFunctionInQuery() throws Exception {
        // String duplicateTable = "CREATE TABLE " + TEST_TABLE_NAME + " ( k1 int(11) NOT NULL ,  k2  int(11) NOT NULL ,"
        //         + "v1  varchar(4096) NOT NULL, v2  float NOT NULL , v3  decimal(20, 7) NOT NULL ) ENGINE=OLAP "
        //         + "DUPLICATE KEY( k1 ,  k2 ) DISTRIBUTED BY HASH( k1 ,  k2 ) BUCKETS 3 "
        //         + "PROPERTIES ('replication_num' = '1'); ";
        // dorisAssert.withTable(duplicateTable);
        // String createK1K2MV = "create materialized view k1_k2 as select k1,k2 from " + TEST_TABLE_NAME + " group by "
        //         + "k1,k2;";
        // String query = "select k1 , sum(k2) over (partition by v1 ) from " + TEST_TABLE_NAME + ";";
        // dorisAssert.withMaterializedView(createK1K2MV).query(query).explainWithout("k1_k2");
        // dorisAssert.dropTable(TEST_TABLE_NAME, true);
    }

    @Test
    void testUniqueTableInQuery() throws Exception {
        String uniqueTable = "CREATE TABLE " + TEST_TABLE_NAME + " (k1 int, k2 int, v1 int) UNIQUE KEY (k1, k2) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ('replication_num' = '1','enable_unique_key_merge_on_write' = 'false');";
        createTable(uniqueTable);
        String createK1MV = "create materialized view only_k1 as select k2 from " + TEST_TABLE_NAME;
        createMv(createK1MV);
        String query = "select * from " + TEST_TABLE_NAME + ";";
        singleTableTest(query, TEST_TABLE_NAME, false);
    }

    /**
     * bitmap_union_count(to_bitmap()) -> bitmap_union_count without having
     */
    @Test
    void testBitmapUnionRewrite() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME
                + " as select user_id, bitmap_union(to_bitmap(tag_id)) from "
                + USER_TAG_TABLE_NAME + " group by user_id;";
        createMv(createUserTagMVSql);
        String query = "select user_id, bitmap_union_count(to_bitmap(tag_id)) a from " + USER_TAG_TABLE_NAME
                + " group by user_id";
        singleTableTest(query, USER_TAG_MV_NAME, true);
    }

    /**
     * bitmap_union_count(bitmap_hash()) -> bitmap_union_count without having
     */
    @Test
    void testBitmapUnionBitmapHashRewrite() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME
                + " as select user_id, bitmap_union(bitmap_hash(tag_id)) from "
                + USER_TAG_TABLE_NAME + " group by user_id;";
        createMv(createUserTagMVSql);
        String query = "select user_id, bitmap_union_count(bitmap_hash(tag_id)) a from " + USER_TAG_TABLE_NAME
                + " group by user_id";
        singleTableTest(query, USER_TAG_MV_NAME, true);
    }

    /**
     * bitmap_union_count(to_bitmap()) -> bitmap_union_count with having
     */
    @Test
    void testBitmapUnionInQuery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME
                + " as select user_id, bitmap_union(to_bitmap(tag_id)) from "
                + USER_TAG_TABLE_NAME + " group by user_id;";
        createMv(createUserTagMVSql);
        String query = "select user_id, bitmap_union_count(to_bitmap(tag_id)) a from " + USER_TAG_TABLE_NAME
                + " group by user_id having a>1 order by a;";
        singleTableTest(query, USER_TAG_MV_NAME, true);
    }

    @Test
    void testBitmapUnionInSubquery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        createMv(createUserTagMVSql);
        String query = "select user_id from " + USER_TAG_TABLE_NAME + " where user_id in (select user_id from "
                + USER_TAG_TABLE_NAME + " group by user_id having bitmap_union_count(to_bitmap(tag_id)) >1 ) ;";
        testMvWithTwoTable(query, "user_tags_mv", "user_tags");
    }

    @Test
    void testIncorrectMVRewriteInQuery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        createMv(createUserTagMVSql);
        String createEmpMVSql = "create materialized view " + EMPS_MV_NAME + " as select name, deptno from "
                + EMPS_TABLE_NAME + ";";
        createMv(createEmpMVSql);
        String query = "select user_name, bitmap_union_count(to_bitmap(tag_id)) a from " + USER_TAG_TABLE_NAME + ", "
                + "(select name, deptno from " + EMPS_TABLE_NAME + ") a" + " where user_name=a.name group by "
                + "user_name having a>1 order by a;";
        testMv(query, ImmutableMap.of("user_tags", "user_tags", "emps", "emps_mv"));
    }

    /**
     * bitmap_union_count(to_bitmap(tag_id)) in subquery
     */
    @Test
    void testIncorrectMVRewriteInSubquery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        createMv(createUserTagMVSql);
        String query = "select user_id, bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " where "
                + "user_name in (select user_name from " + USER_TAG_TABLE_NAME + " group by user_name having "
                + "bitmap_union_count(to_bitmap(tag_id)) >1 )" + " group by user_id;";
        // can't use mv index because it has no required column `user_name`
        testMv(query, ImmutableMap.of("user_tags", "user_tags"));
    }

    @Test
    void testTwoTupleInQuery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        createMv(createUserTagMVSql);
        String query = "select * from (select user_id, bitmap_union_count(to_bitmap(tag_id)) x from "
                + USER_TAG_TABLE_NAME + " group by user_id) a, (select user_name, bitmap_union_count(to_bitmap(tag_id))"
                + "" + " y from " + USER_TAG_TABLE_NAME + " group by user_name) b where a.x=b.y;";
        PlanChecker.from(connectContext)
                .analyze(query)
                .rewrite()
                .matches(logicalJoin(
                        logicalProject(
                                logicalAggregate(
                                        logicalOlapScan().when(scan -> "user_tags_mv".equals(
                                                scan.getSelectedMaterializedIndexName().get())))),
                        logicalAggregate(
                                logicalProject(
                                        logicalOlapScan().when(scan -> "user_tags".equals(
                                                scan.getSelectedMaterializedIndexName().get()))))));

    }

    /**
     * count(distinct v) -> bitmap_union_count(v) without mv index.
     */
    @Test
    void testAggTableCountDistinctInBitmapType() throws Exception {
        String aggTable = "CREATE TABLE " + TEST_TABLE_NAME + " (k1 int, v1 bitmap bitmap_union) Aggregate KEY (k1) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ('replication_num' = '1');";
        createTable(aggTable);
        String query = "select k1, count(distinct v1) from " + TEST_TABLE_NAME + " group by k1;";
        PlanChecker.from(connectContext)
                .analyze(query)
                .rewrite()
                .matches(logicalAggregate().when(agg -> {
                    assertOneAggFuncType(agg, BitmapUnionCount.class);
                    return true;
                }));
        dropTable(TEST_TABLE_NAME, true);
    }

    @Test
    void testAggTableCountDistinctInHllType() throws Exception {
        String aggTable = "CREATE TABLE " + TEST_TABLE_NAME + " (k1 int, v1 hll " + FunctionSet.HLL_UNION
                + ") Aggregate KEY (k1) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ('replication_num' = '1');";
        createTable(aggTable);
        String query = "select k1, count(distinct v1) from " + TEST_TABLE_NAME + " group by k1;";
        PlanChecker.from(connectContext)
                .analyze(query)
                .rewrite()
                .matches(logicalAggregate(logicalOlapScan()).when(agg -> {
                    // k1#0, hll_union_agg(v1#1) AS `count(distinct v1)`#2
                    List<NamedExpression> output = agg.getOutputExpressions();
                    Assertions.assertEquals(2, output.size());
                    NamedExpression output1 = output.get(1);
                    Assertions.assertTrue(output1 instanceof Alias);
                    Alias alias = (Alias) output1;
                    Expression aliasChild = alias.child();
                    Assertions.assertTrue(aliasChild instanceof HllUnionAgg);
                    HllUnionAgg hllUnionAgg = (HllUnionAgg) aliasChild;
                    Assertions.assertEquals("v1", ((Slot) hllUnionAgg.child()).getName());
                    return true;
                }));
        dropTable(TEST_TABLE_NAME, true);
    }

    /**
     * count distinct to bitmap_union_count in mv
     */
    @Test
    void testCountDistinctToBitmap() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        createMv(createUserTagMVSql);
        String query = "select count(distinct tag_id) from " + USER_TAG_TABLE_NAME + ";";
        PlanChecker.from(connectContext)
                .analyze(query)
                .rewrite()
                .matches(logicalAggregate().when(agg -> {
                    assertOneAggFuncType(agg, BitmapUnionCount.class);
                    return true;
                }));
        testMv(query, USER_TAG_MV_NAME);
    }

    @Test
    void testIncorrectRewriteCountDistinct() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        createMv(createUserTagMVSql);
        String query = "select user_name, count(distinct tag_id) from " + USER_TAG_TABLE_NAME + " group by user_name;";
        PlanChecker.from(connectContext)
                .analyze(query)
                .rewrite()
                .matches(logicalAggregate().when(agg -> {
                    assertOneAggFuncType(agg, Count.class);
                    return true;
                }));
        testMv(query, USER_TAG_TABLE_NAME);
    }

    @Test
    void testNDVToHll() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "`" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME
                + " group by user_id;";
        createMv(createUserTagMVSql);
        String query = "select ndv(tag_id) from " + USER_TAG_TABLE_NAME + ";";
        testMv(query, USER_TAG_MV_NAME);
    }

    /**
     * TODO: enable this when hll is supported.
     */
    void testApproxCountDistinctToHll() throws Exception {
        // String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
        //         + "`" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME
        //         + " group by user_id;";
        // dorisAssert.withMaterializedView(createUserTagMVSql);
        // String query = "select approx_count_distinct(tag_id) from " + USER_TAG_TABLE_NAME + ";";
        // dorisAssert.query(query).explainContains(USER_TAG_MV_NAME, "hll_union_agg");
    }

    @Test
    void testHLLUnionFamilyRewrite() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "`" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME
                + " group by user_id;";
        createMv(createUserTagMVSql);

        String query = "select `" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id)) from "
                + USER_TAG_TABLE_NAME + ";";
        PlanChecker.from(connectContext)
                .analyze(query)
                .rewrite()
                .matches(logicalAggregate().when(agg -> {
                    assertOneAggFuncType(agg, HllUnion.class);
                    return true;
                }));
        testMv(query, USER_TAG_MV_NAME);

        query = "select hll_union_agg(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME + ";";
        PlanChecker.from(connectContext)
                .analyze(query)
                .rewrite()
                .matches(logicalAggregate().when(agg -> {
                    assertOneAggFuncType(agg, HllUnionAgg.class);
                    return true;
                }));
        testMv(query, USER_TAG_MV_NAME);

        query = "select hll_raw_agg(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME + ";";
        PlanChecker.from(connectContext)
                .analyze(query)
                .rewrite()
                .matches(logicalAggregate().when(agg -> {
                    assertOneAggFuncType(agg, HllUnion.class);
                    return true;
                }));
        testMv(query, USER_TAG_MV_NAME);
    }

    @Test
    void testAggInHaving() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + " group by empid, deptno;";
        createMv(createMVSql);
        String query = "select empid from " + EMPS_TABLE_NAME + " group by empid having max(salary) > 1;";
        testMv(query, EMPS_TABLE_NAME);
    }

    @Test
    void testCountFieldInQuery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "count(tag_id) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        createMv(createUserTagMVSql);
        String query = "select count(tag_id) from " + USER_TAG_TABLE_NAME + ";";
        PlanChecker.from(connectContext)
                .analyze(query)
                .rewrite()
                .matches(logicalAggregate().when(agg -> {
                    assertOneAggFuncType(agg, Sum.class);
                    return true;
                }));
        testMv(query, USER_TAG_MV_NAME);
    }

    @Test
    void testCreateMVBaseBitmapAggTable() throws Exception {
        String createTableSQL = "create table " + HR_DB_NAME + ".agg_table "
                + "(empid int, name varchar, salary bitmap " + FunctionSet.BITMAP_UNION + ") "
                + "aggregate key (empid, name) "
                + "partition by range (empid) "
                + "(partition p1 values less than MAXVALUE) "
                + "distributed by hash(empid) buckets 3 properties('replication_num' = '1');";
        createTable(createTableSQL);
        String createMVSql = "create materialized view mv as select empid, " + FunctionSet.BITMAP_UNION
                + "(salary) from agg_table "
                + "group by empid;";
        createMv(createMVSql);
        String query = "select count(distinct salary) from agg_table;";
        testMv(query, "mv");
        dropTable("agg_table", true);
    }

    @Test
    void testSelectMVWithTableAlias() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "count(tag_id) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        createMv(createUserTagMVSql);
        String query = "select count(tag_id) from " + USER_TAG_TABLE_NAME + " t ;";
        PlanChecker.from(connectContext)
                .analyze(query)
                .rewrite()
                .matches(logicalAggregate().when(agg -> {
                    assertOneAggFuncType(agg, Sum.class);
                    return true;
                }));
        testMv(query, USER_TAG_MV_NAME);
    }

    @Test
    void selectBitmapMvWithProjectTest1() throws Exception {
        createTable("create table t(\n"
                + "  a int, \n"
                + "  b int, \n"
                + "  c int\n"
                + ")ENGINE=OLAP \n"
                + "DISTRIBUTED BY HASH(a) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");
        createMv("create materialized view mv as"
                + "  select a, bitmap_union(to_bitmap(b)) from t group by a;");

        testMv("select a, count(distinct v) as cnt from (select a, b as v from t) t group by a", "mv");
        dropTable("t", true);
    }

    @Test
    void selectBitmapMvWithProjectTest2() throws Exception {
        createTable("create table t(\n"
                + "  a int, \n"
                + "  b int, \n"
                + "  c int\n"
                + ")ENGINE=OLAP \n"
                + "DISTRIBUTED BY HASH(a) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");
        createMv("create materialized view mv as"
                + "  select a, bitmap_union(to_bitmap(b)) from t group by a;");

        testMv("select a, bitmap_union_count(to_bitmap(v)) as cnt from (select a, b as v from t) t group by a", "mv");
        dropTable("t", true);
    }

    @Test
    void selectBitmapMvWithProjectMultiMv() throws Exception {
        createTable("create table selectBitmapMvWithProjectMultiMv(\n"
                + "  a int, \n"
                + "  b int, \n"
                + "  c int\n"
                + ")ENGINE=OLAP \n"
                + "DISTRIBUTED BY HASH(a) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");
        createMv("create materialized view mv as"
                + "  select a, bitmap_union(to_bitmap(b)) from selectBitmapMvWithProjectMultiMv group by a;");
        createMv("create materialized view mv1 as"
                + "  select c, bitmap_union(to_bitmap(b)) from selectBitmapMvWithProjectMultiMv group by c;");
        createMv("create materialized view mv2 as"
                + "  select a, c, bitmap_union(to_bitmap(b)) from selectBitmapMvWithProjectMultiMv group by a, c;");

        testMv("select a, bitmap_union_count(to_bitmap(b)) as cnt from selectBitmapMvWithProjectMultiMv group by a", "mv");
        dropTable("selectBitmapMvWithProjectMultiMv", true);
    }

    @Test
    void advanceMvAggWithExprTest() throws Exception {
        createMv("create materialized view mv1 as"
                + "  select abs(a)+1 tmp, sum(abs(b+2)) from " + ADVANCE_TABLE_NAME + " group by tmp;");

        testMv("select abs(a)+1 tmp, sum(abs(b+2)) from " + ADVANCE_TABLE_NAME + " group by tmp", "mv1");
    }

    @Test
    void advanceMvDupColTest() throws Exception {
        createMv("create materialized view mv2 as"
                + "  select a, sum(b), max(b) from " + ADVANCE_TABLE_NAME + " group by a;");

        testMv("select a, sum(b), max(b) as cnt from " + ADVANCE_TABLE_NAME + " group by a", "mv2");
        testMv("select a, sum(b) as cnt from " + ADVANCE_TABLE_NAME + " group by a", "mv2");
        testMv("select a, max(b) as cnt from " + ADVANCE_TABLE_NAME + " group by a", "mv2");
        testMv("select unix_timestamp(a) tmp, max(b) as cnt from " + ADVANCE_TABLE_NAME + " group by tmp", "mv2");
    }

    @Test
    void advanceMvDupColTest1() throws Exception {
        createMv("create materialized view mv2 as"
                + "  select b, sum(a), max(a) from " + ADVANCE_TABLE_NAME + " group by b;");

        testMv("select b, sum(a), max(a) as cnt from " + ADVANCE_TABLE_NAME + " group by b", "mv2");
        testMv("select b, sum(a) as cnt from " + ADVANCE_TABLE_NAME + " group by b", "mv2");
        testMv("select b, max(a) as cnt from " + ADVANCE_TABLE_NAME + " group by b", "mv2");
        testMv("select unix_timestamp(b) tmp, max(a) as cnt from " + ADVANCE_TABLE_NAME + " group by tmp", "mv2");
    }

    @Test
    void advanceMvMultiSlotTest() throws Exception {
        createMv("create materialized view mv3 as"
                + "  select abs(a)+b+1,abs(b+2)+c+3 from " + ADVANCE_TABLE_NAME);

        testMv("select abs(a)+b+1,abs(b+2)+c+3 from " + ADVANCE_TABLE_NAME, "mv3");
    }

    @Test
    void advanceMvMultiSlotWithAggTest() throws Exception {
        createMv("create materialized view mv4 as"
                + "  select abs(a)+b+1 tmp, sum(abs(b+2)+c+3) from " + ADVANCE_TABLE_NAME + " group by tmp");

        testMv("select abs(a)+b+1 tmp, sum(abs(b+2)+c+3) from " + ADVANCE_TABLE_NAME + " group by tmp", "mv4");
    }

    private void testMv(String sql, Map<String, String> tableToIndex) {
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        PlanChecker.from(connectContext).checkPlannerResult(sql, planner -> {
            List<ScanNode> scans = planner.getScanNodes();
            for (ScanNode scanNode : scans) {
                Assertions.assertTrue(scanNode instanceof OlapScanNode);
                OlapScanNode olapScan = (OlapScanNode) scanNode;
                Assertions.assertTrue(olapScan.isPreAggregation());
                Assertions.assertEquals(tableToIndex.get(olapScan.getOlapTable().getName()),
                        olapScan.getSelectedIndexName());
            }
        });
    }

    private void testMv(String sql, String indexName) {
        singleTableTest(sql, indexName, true);
    }

    private void assertOneAggFuncType(LogicalAggregate<? extends Plan> agg, Class<?> aggFuncType) {
        Set<AggregateFunction> aggFuncs = agg.getOutputExpressions()
                .stream()
                .flatMap(e -> e.<Set<AggregateFunction>>collect(AggregateFunction.class::isInstance)
                        .stream())
                .collect(Collectors.toSet());
        Assertions.assertEquals(1, aggFuncs.size());
        AggregateFunction aggFunc = aggFuncs.iterator().next();
        Assertions.assertTrue(aggFuncType.isInstance(aggFunc));
    }

    private void testMvWithTwoTable(String sql, String firstTableIndexName, String secondTableIndexName) {
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        PlanChecker.from(connectContext).checkPlannerResult(sql, planner -> {
            List<ScanNode> scans = planner.getScanNodes();
            Assertions.assertEquals(2, scans.size());

            ScanNode scanNode0 = scans.get(0);
            Assertions.assertTrue(scanNode0 instanceof OlapScanNode);
            OlapScanNode scan0 = (OlapScanNode) scanNode0;
            Assertions.assertTrue(scan0.isPreAggregation());
            Assertions.assertEquals(firstTableIndexName, scan0.getSelectedIndexName());

            ScanNode scanNode1 = scans.get(1);
            Assertions.assertTrue(scanNode1 instanceof OlapScanNode);
            OlapScanNode scan1 = (OlapScanNode) scanNode1;
            Assertions.assertTrue(scan1.isPreAggregation());
            Assertions.assertEquals(secondTableIndexName, scan1.getSelectedIndexName());
        });
    }
}
