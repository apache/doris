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

package org.apache.doris.nereids.rules.mv;

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
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.utframe.DorisAssert;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests ported from {@link org.apache.doris.planner.MaterializedViewFunctionTest}
 */
public class SelectMvIndexTest extends BaseMaterializedIndexSelectTest implements PatternMatchSupported {

    private static final String EMPS_TABLE_NAME = "emps";
    private static final String EMPS_MV_NAME = "emps_mv";
    private static final String HR_DB_NAME = "db1";
    private static final String DEPTS_TABLE_NAME = "depts";
    private static final String DEPTS_MV_NAME = "depts_mv";
    private static final String USER_TAG_TABLE_NAME = "user_tags";
    private static final String TEST_TABLE_NAME = "test_tb";
    private static final String USER_TAG_MV_NAME = "user_tags_mv";

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(HR_DB_NAME);
        useDatabase(HR_DB_NAME);
    }

    @BeforeEach
    void before() throws Exception {
        createTable("create table " + HR_DB_NAME + "." + EMPS_TABLE_NAME + " (time_col date, empid int, "
                + "name varchar, deptno int, salary int, commission int) partition by range (time_col) "
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
    }

    @AfterEach
    public void after() throws Exception {
        dropTable(EMPS_TABLE_NAME, true);
        dropTable(DEPTS_TABLE_NAME, true);
        dropTable(USER_TAG_TABLE_NAME, true);
    }

    @Test
    public void testProjectionMV1() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String query = "select empid, deptno from " + EMPS_TABLE_NAME + ";";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    public void testProjectionMV2() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String query1 = "select empid + 1 from " + EMPS_TABLE_NAME + " where deptno = 10;";
        createMv(createMVSql);
        testMv(query1, EMPS_MV_NAME);
        String query2 = "select name from " + EMPS_TABLE_NAME + " where deptno -10 = 0;";
        testMv(query2, EMPS_TABLE_NAME);
    }

    @Test
    public void testProjectionMV3() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, name from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String query1 = "select empid +1, name from " + EMPS_TABLE_NAME + " where deptno = 10;";
        createMv(createMVSql);
        testMv(query1, EMPS_MV_NAME);
        String query2 = "select name from " + EMPS_TABLE_NAME + " where deptno - 10 = 0;";
        testMv(query2, EMPS_MV_NAME);
    }

    // @Test
    // public void testProjectionMV4() throws Exception {
    //     String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select name, deptno, salary from "
    //             + EMPS_TABLE_NAME + ";";
    //     String query1 = "select name from " + EMPS_TABLE_NAME + " where deptno > 30 and salary > 3000;";
    //     createMv(createMVSql);
    //     testMv(query1, EMPS_MV_NAME);
    //     String query2 = "select empid from " + EMPS_TABLE_NAME + " where deptno > 30 and empid > 10;";
    //     dorisAssert.query(query2).explainWithout(QUERY_USE_EMPS_MV);
    // }

    @Test
    public void testUnionQueryOnProjectionMV() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String union = "select empid from " + EMPS_TABLE_NAME + " where deptno > 300" + " union all select empid from"
                + " " + EMPS_TABLE_NAME + " where deptno < 200";
        createMv(createMVSql);
        testMvWithTwoTable(union, EMPS_MV_NAME, EMPS_MV_NAME);
    }

    @Test
    public void testAggQueryOnAggMV1() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary), "
                + "max(commission) from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select sum(salary), deptno from " + EMPS_TABLE_NAME + " group by deptno;";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    public void testAggQueryOnAggMV2() throws Exception {
        String agg = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " group by deptno";
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as " + agg + ";";
        String query = "select * from (select deptno, sum(salary) as sum_salary from " + EMPS_TABLE_NAME + " group "
                + "by" + " deptno) a where (sum_salary * 2) > 3;";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    public void testAggQueryOnAggMV3() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary)"
                + " from " + EMPS_TABLE_NAME + " group by deptno, commission;";
        String query = "select commission, sum(salary) from " + EMPS_TABLE_NAME + " where commission * (deptno + "
                + "commission) = 100 group by commission;";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    /**
     * Matching failed because the filtering condition under Aggregate
     * references columns for aggregation.
     */
    @Test
    public void testAggQueryOnAggMV4() throws Exception {
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
    public void testAggQuqeryOnAggMV5() throws Exception {
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
    public void testAggQuqeryOnAggMV6() throws Exception {
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
    public void testGroupingSetQueryOnAggMV() throws Exception {
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
    public void testAggQuqeryOnAggMV7() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " " + "group by deptno, commission;";
        String query = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " where deptno>=20 group by deptno;";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    public void testAggQueryOnAggMV8() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select deptno, sum(salary) + 1 from " + EMPS_TABLE_NAME + " group by deptno;";
        createMv(createMVSql);
        testMv(query, EMPS_MV_NAME);
    }

    /**
     * Query with cube and arithmetic expr
     * TODO: enable this when group by cube is supported.
     */
    @Disabled
    public void testAggQueryOnAggMV9() throws Exception {
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
    public void testAggQueryOnAggMV10() throws Exception {
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
    public void testAggQueryOnAggMV11() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, count(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select deptno, count(salary) + count(1) from " + EMPS_TABLE_NAME
                + " group by deptno;";
        createMv(createMVSql);
        testMv(query, EMPS_TABLE_NAME);
    }

    @Test
    public void testAggQueryWithSetOperandOnAggMV() throws Exception {
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
    public void testJoinOnLeftProjectToJoin() throws Exception {
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
    public void testJoinOnRightProjectToJoin() throws Exception {
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
    public void testJoinOnProjectsToJoin() throws Exception {
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
    public void testJoinOnCalcToJoin0() throws Exception {
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
    public void testJoinOnCalcToJoin1() throws Exception {
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
    public void testJoinOnCalcToJoin2() throws Exception {
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
    public void testJoinOnCalcToJoin3() throws Exception {
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
    @Disabled
    public void testJoinOnCalcToJoin4() throws Exception {
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
    @Disabled
    public void testOrderByQueryOnProjectView() throws Exception {
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
    @Disabled
    public void testOrderByQueryOnOrderByView() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String query = "select empid from " + EMPS_TABLE_NAME + " order by deptno";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    public void testAggregateMVAggregateFuncs1() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno from " + EMPS_TABLE_NAME + " group by deptno";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    public void testAggregateMVAggregateFuncs2() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " group by deptno";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    public void testAggregateMVAggregateFuncs3() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, empid, sum(salary) from " + EMPS_TABLE_NAME + " group by deptno, empid";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    public void testAggregateMVAggregateFuncs4() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " where deptno > 10 group by deptno";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    public void testAggregateMVAggregateFuncs5() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 group by deptno";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    public void testAggregateMVCalcGroupByQuery1() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno+1, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 "
                + "group by deptno+1;";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    public void testAggregateMVCalcGroupByQuery2() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno * empid, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 "
                + "group by deptno * empid;";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    public void testAggregateMVCalcGroupByQuery3() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select empid, deptno * empid, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 "
                + "group by empid, deptno * empid;";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    public void testAggregateMVCalcAggFunctionQuery() throws Exception {
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
    @Disabled
    public void testSubQuery() throws Exception {
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
    @Disabled
    public void testDistinctQuery() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query1 = "select distinct deptno from " + EMPS_TABLE_NAME + ";";
        createMv(createEmpsMVsql);
        testMv(query1, EMPS_MV_NAME);
        String query2 = "select deptno, sum(distinct salary) from " + EMPS_TABLE_NAME + " group by deptno;";
        testMv(query2, EMPS_MV_NAME);
    }

    @Test
    public void testSingleMVMultiUsage() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, salary "
                + "from " + EMPS_TABLE_NAME + " order by deptno;";
        String query = "select * from (select deptno, empid from " + EMPS_TABLE_NAME + " where deptno>100) A join "
                + "(select deptno, empid from " + EMPS_TABLE_NAME + " where deptno >200) B on A.deptno=B.deptno;";
        createMv(createEmpsMVsql);
        // both of the 2 table scans should use mv index.
        testMv(query, ImmutableMap.of(EMPS_TABLE_NAME, EMPS_MV_NAME));
    }

    @Test
    public void testMultiMVMultiUsage() throws Exception {
        String createEmpsMVSql01 = "create materialized view emp_mv_01 as select deptno, empid, salary "
                + "from " + EMPS_TABLE_NAME + " order by deptno;";
        String createEmpsMVSql02 = "create materialized view emp_mv_02 as select deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        createMv(createEmpsMVSql01);
        createMv(createEmpsMVSql02);
        String query = "select * from (select deptno, empid from " + EMPS_TABLE_NAME + " where deptno>100) A join "
                + "(select deptno, sum(salary) from " + EMPS_TABLE_NAME + " where deptno >200 group by deptno) B "
                + "on A.deptno=B.deptno";
        testMvWithTwoTable(query, "emp_mv_01", "emp_mv_02");
    }

    @Test
    public void testMVOnJoinQuery() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select salary, empid, deptno from "
                + EMPS_TABLE_NAME + " order by salary;";
        createMv(createEmpsMVsql);
        String query = "select empid, salary from " + EMPS_TABLE_NAME + " join " + DEPTS_TABLE_NAME
                + " on emps.deptno=depts.deptno where salary > 300;";
        testMv(query, ImmutableMap.of(EMPS_TABLE_NAME, EMPS_MV_NAME, DEPTS_TABLE_NAME, DEPTS_TABLE_NAME));
    }

    @Test
    public void testAggregateMVOnCountDistinctQuery1() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, count(distinct empid) from " + EMPS_TABLE_NAME + " group by deptno;";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    public void testQueryAfterTrimingOfUnusedFields() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + " order by empid, deptno;";
        String query = "select empid, deptno from (select empid, deptno, salary from " + EMPS_TABLE_NAME + ") A;";
        createMv(createEmpsMVsql);
        testMv(query, EMPS_MV_NAME);
    }

    @Test
    public void testUnionAll() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + " order by empid, deptno;";
        String query = "select empid, deptno from " + EMPS_TABLE_NAME + " where empid >1 union all select empid,"
                + " deptno from " + EMPS_TABLE_NAME + " where empid <0;";
        createMv(createEmpsMVsql);
        testMvWithTwoTable(query, EMPS_MV_NAME, EMPS_MV_NAME);
    }

    @Test
    public void testUnionDistinct() throws Exception {
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
    public void testDeduplicateQueryInAgg() throws Exception {
        String aggregateTable = "create table agg_table (k1 int, k2 int, v1 bigint sum) aggregate key (k1, k2) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        createTable(aggregateTable);

        // don't use rollup k1_v1
        addRollup("alter table agg_table add rollup k1_v1(k1, v1)");
        // use rollup only_keys
        addRollup("alter table agg_table add rollup only_keys (k1, k2) properties ('replication_num' = '1')");

        String query = "select k1, k2 from agg_table;";
        // todo: `preagg` should be ture when rollup could be used.
        singleTableTest(query, "only_keys", false);
        dropTable("agg_table", true);
    }

    /**
     * Group by only mv for duplicate-keys table.
     * duplicate table (k1, k2, v1 sum)
     * aggregate mv index (k1, k2)
     */
    @Test
    public void testGroupByOnlyForDuplicateTable() throws Exception {
        createTable("create table t (k1 int, k2 int, v1 bigint) duplicate key(k1, k2, v1)"
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1')");
        createMv("create materialized view k1_k2 as select k1, k2 from t group by k1, k2");
        singleTableTest("select k1, k2 from t group by k1, k2", "k1_k2", true);
        dropTable("t", true);
    }

    @Test
    public void testAggFunctionInHaving() throws Exception {
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
    @Disabled
    public void testAggFunctionInOrder() throws Exception {
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
    public void testWindowsFunctionInQuery() throws Exception {
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
    public void testUniqueTableInQuery() throws Exception {
        String uniqueTable = "CREATE TABLE " + TEST_TABLE_NAME + " (k1 int, v1 int) UNIQUE KEY (k1) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ('replication_num' = '1');";
        createTable(uniqueTable);
        String createK1MV = "create materialized view only_k1 as select k1 from " + TEST_TABLE_NAME + " group by "
                + "k1;";
        createMv(createK1MV);
        String query = "select * from " + TEST_TABLE_NAME + ";";
        singleTableTest(query, TEST_TABLE_NAME, false);
    }

    /**
     * bitmap_union_count(to_bitmap()) -> bitmap_union_count without having
     */
    @Test
    public void testBitmapUnionRewrite() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME
                + " as select user_id, bitmap_union(to_bitmap(tag_id)) from "
                + USER_TAG_TABLE_NAME + " group by user_id;";
        createMv(createUserTagMVSql);
        String query = "select user_id, bitmap_union_count(to_bitmap(tag_id)) a from " + USER_TAG_TABLE_NAME
                + " group by user_id";
        singleTableTest(query, USER_TAG_MV_NAME, true);
    }

    /**
     * bitmap_union_count(to_bitmap()) -> bitmap_union_count with having
     */
    @Test
    public void testBitmapUnionInQuery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME
                + " as select user_id, bitmap_union(to_bitmap(tag_id)) from "
                + USER_TAG_TABLE_NAME + " group by user_id;";
        createMv(createUserTagMVSql);
        String query = "select user_id, bitmap_union_count(to_bitmap(tag_id)) a from " + USER_TAG_TABLE_NAME
                + " group by user_id having a>1 order by a;";
        singleTableTest(query, USER_TAG_MV_NAME, true);
    }

    @Test
    public void testBitmapUnionInSubquery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        createMv(createUserTagMVSql);
        String query = "select user_id from " + USER_TAG_TABLE_NAME + " where user_id in (select user_id from "
                + USER_TAG_TABLE_NAME + " group by user_id having bitmap_union_count(to_bitmap(tag_id)) >1 ) ;";
        testMvWithTwoTable(query, "user_tags", "user_tags_mv");
    }

    @Test
    public void testIncorrectMVRewriteInQuery() throws Exception {
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
    public void testIncorrectMVRewriteInSubquery() throws Exception {
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
    public void testTwoTupleInQuery() throws Exception {
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
                        logicalAggregate(
                                logicalProject(
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
    public void testAggTableCountDistinctInBitmapType() throws Exception {
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
    public void testAggTableCountDistinctInHllType() throws Exception {
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
    public void testCountDistinctToBitmap() throws Exception {
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
    public void testIncorrectRewriteCountDistinct() throws Exception {
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
    public void testNDVToHll() throws Exception {
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
    @Disabled
    public void testApproxCountDistinctToHll() throws Exception {
        // String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
        //         + "`" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME
        //         + " group by user_id;";
        // dorisAssert.withMaterializedView(createUserTagMVSql);
        // String query = "select approx_count_distinct(tag_id) from " + USER_TAG_TABLE_NAME + ";";
        // dorisAssert.query(query).explainContains(USER_TAG_MV_NAME, "hll_union_agg");
    }

    @Test
    public void testHLLUnionFamilyRewrite() throws Exception {
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
    public void testAggInHaving() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + " group by empid, deptno;";
        createMv(createMVSql);
        String query = "select empid from " + EMPS_TABLE_NAME + " group by empid having max(salary) > 1;";
        testMv(query, EMPS_TABLE_NAME);
    }

    @Test
    public void testCountFieldInQuery() throws Exception {
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
    public void testCreateMVBaseBitmapAggTable() throws Exception {
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
    public void testSelectMVWithTableAlias() throws Exception {
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
    public void selectBitmapMvWithProjectTest1() throws Exception {
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
    public void selectBitmapMvWithProjectTest2() throws Exception {
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

    private void testMv(String sql, Map<String, String> tableToIndex) {
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
