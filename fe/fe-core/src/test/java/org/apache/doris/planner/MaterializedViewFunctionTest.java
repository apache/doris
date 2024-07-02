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

package org.apache.doris.planner;

import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.common.FeConstants;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class MaterializedViewFunctionTest {
    private static String baseDir = "fe";
    private static String runningDir =
            baseDir + "/mocked/MaterializedViewFunctionTest/" + UUID.randomUUID().toString() + "/";
    private static final String EMPS_TABLE_NAME = "emps";
    private static final String EMPS_MV_NAME = "emps_mv";
    private static final String HR_DB_NAME = "db1";
    private static final String QUERY_USE_EMPS_MV = "(" + EMPS_MV_NAME + ")";
    private static final String QUERY_USE_EMPS = "(" + EMPS_TABLE_NAME + ")";
    private static final String DEPTS_TABLE_NAME = "depts";
    private static final String DEPTS_MV_NAME = "depts_mv";
    private static final String QUERY_USE_DEPTS_MV = "(" + DEPTS_MV_NAME + ")";
    private static final String QUERY_USE_DEPTS = "(" + DEPTS_TABLE_NAME + ")";
    private static final String USER_TAG_TABLE_NAME = "user_tags";
    private static final String USER_TAG_MV_NAME = "user_tags_mv";
    private static final String QUERY_USE_USER_TAG_MV = "(" + USER_TAG_MV_NAME + ")";
    private static final String QUERY_USE_USER_TAG = "(" + USER_TAG_TABLE_NAME + ")";
    private static final String TEST_TABLE_NAME = "test_tb";
    private static DorisAssert dorisAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createDorisCluster(runningDir);
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase(HR_DB_NAME).useDatabase(HR_DB_NAME);
        dorisAssert.getSessionVariable().setEnableSyncMvCostBasedRewrite(false);
    }

    @Before
    public void beforeMethod() throws Exception {
        String createTableSQL = "create table " + HR_DB_NAME + "." + EMPS_TABLE_NAME + " (time_col date, empid int, "
                + "name varchar, deptno int, salary int, commission int) partition by range (time_col) "
                + "(partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3"
                + " properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
        createTableSQL = "create table " + HR_DB_NAME + "." + DEPTS_TABLE_NAME
                + " (time_col date, deptno int, name varchar, cost int) partition by range (time_col) "
                + "(partition p1 values less than MAXVALUE) "
                + "distributed by hash(time_col) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
        createTableSQL = "create table " + HR_DB_NAME + "." + USER_TAG_TABLE_NAME
                + " (time_col date, user_id int, user_name varchar(20), tag_id int) partition by range (time_col) "
                + " (partition p1 values less than MAXVALUE) "
                + "distributed by hash(time_col) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
    }

    @After
    public void afterMethod() throws Exception {
        dorisAssert.dropTable(EMPS_TABLE_NAME, true);
        dorisAssert.dropTable(DEPTS_TABLE_NAME, true);
        dorisAssert.dropTable(USER_TAG_TABLE_NAME, true);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        UtFrameUtils.cleanDorisFeDir(baseDir);
    }

    @Test
    public void testProjectionMV1() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String query = "select empid, deptno from " + EMPS_TABLE_NAME + ";";
        dorisAssert.withMaterializedView(createMVSql);
        dorisAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testProjectionMV2() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String query1 = "select empid + 1 from " + EMPS_TABLE_NAME + " where deptno = 10;";
        dorisAssert.withMaterializedView(createMVSql);
        dorisAssert.query(query1).explainContains(QUERY_USE_EMPS_MV);
        String query2 = "select name from " + EMPS_TABLE_NAME + " where deptno -10 = 0;";
        dorisAssert.query(query2).explainWithout(QUERY_USE_EMPS_MV);

    }

    @Test
    public void testProjectionMV3() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, name from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String query1 = "select empid +1, name from " + EMPS_TABLE_NAME + " where deptno = 10;";
        dorisAssert.withMaterializedView(createMVSql);
        dorisAssert.query(query1).explainContains(QUERY_USE_EMPS_MV);
        String query2 = "select name from " + EMPS_TABLE_NAME + " where deptno - 10 = 0;";
        dorisAssert.query(query2).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testProjectionMV4() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select name, deptno, salary from "
                + EMPS_TABLE_NAME + ";";
        String query1 = "select name from " + EMPS_TABLE_NAME + " where deptno > 30 and salary > 3000;";
        dorisAssert.withMaterializedView(createMVSql);
        dorisAssert.query(query1).explainContains(QUERY_USE_EMPS_MV);
        String query2 = "select empid from " + EMPS_TABLE_NAME + " where deptno > 30 and empid > 10;";
        dorisAssert.query(query2).explainWithout(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testUnionQueryOnProjectionMV() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String union = "select empid from " + EMPS_TABLE_NAME + " where deptno > 300" + " union all select empid from"
                + " " + EMPS_TABLE_NAME + " where deptno < 200";
        dorisAssert.withMaterializedView(createMVSql).query(union).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggQueryOnAggMV1() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary), "
                + "max(commission) from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select sum(salary), deptno from " + EMPS_TABLE_NAME + " group by deptno;";
        dorisAssert.withMaterializedView(createMVSql).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggQueryOnAggMV2() throws Exception {
        String agg = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " group by deptno";
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as " + agg + ";";
        String query = "select * from (select deptno, sum(salary) as sum_salary from " + EMPS_TABLE_NAME + " group "
                + "by" + " deptno) a where (sum_salary * 2) > 3;";
        dorisAssert.withMaterializedView(createMVSql).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    /*
    TODO
    The deduplicate materialized view is not yet supported
    @Test
    public void testAggQueryOnDeduplicatedMV() throws Exception {
        String deduplicateSQL = "select deptno, empid, name, salary, commission from " + EMPS_TABLE_NAME + " group "
                + "by" + " deptno, empid, name, salary, commission";
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as " + deduplicateSQL + ";";
        String query1 = "select deptno, sum(salary) from (" + deduplicateSQL + ") A group by deptno;";
        dorisAssert.withMaterializedView(createMVSql);
        dorisAssert.query(query1).explainContains(QUERY_USE_EMPS_MV);
        String query2 = "select deptno, empid from " + EMPS_TABLE_NAME + ";";
        dorisAssert.query(query2).explainWithout(QUERY_USE_EMPS_MV);
    }
    */

    @Test
    public void testAggQueryOnAggMV3() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary)"
                + " from " + EMPS_TABLE_NAME + " group by deptno, commission;";
        String query = "select commission, sum(salary) from " + EMPS_TABLE_NAME + " where commission * (deptno + "
                + "commission) = 100 group by commission;";
        dorisAssert.withMaterializedView(createMVSql);
        dorisAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
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
        dorisAssert.withMaterializedView(createMVSql);
        dorisAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);
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
        dorisAssert.withMaterializedView(createMVSql);
        dorisAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    /**
     * There will be a compensating Project + Filter added after matching of the Aggregate.
     *
     * @throws Exception
     */
    @Test
    public void testAggQuqeryOnAggMV6() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary)"
                + " from " + EMPS_TABLE_NAME + " group by deptno, commission;";
        String query = "select * from (select deptno, sum(salary) as sum_salary from " + EMPS_TABLE_NAME
                + " where deptno>=20 group by deptno) a where sum_salary>10;";
        dorisAssert.withMaterializedView(createMVSql);
        dorisAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
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
        dorisAssert.withMaterializedView(createMVSql);
        dorisAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    /**
     * Aggregation query at coarser level of aggregation than aggregation materialized view.
     */
    @Test
    public void testAggQuqeryOnAggMV7() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " " + "group by deptno, commission;";
        String query = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " where deptno>=20 group by deptno;";
        dorisAssert.withMaterializedView(createMVSql);
        dorisAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggQueryOnAggMV8() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select deptno, sum(salary) + 1 from " + EMPS_TABLE_NAME + " group by deptno;";
        dorisAssert.withMaterializedView(createMVSql);
        dorisAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    /**
     * Query with cube and arithmetic expr
     */
    @Test
    public void testAggQueryOnAggMV9() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno, commission;";
        String query = "select deptno, commission, sum(salary) + 1 from " + EMPS_TABLE_NAME
                + " group by cube(deptno,commission);";
        dorisAssert.withMaterializedView(createMVSql);
        dorisAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
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
        try {
            dorisAssert.withMaterializedView(createMVSql);
            dorisAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
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
        dorisAssert.withMaterializedView(createMVSql);
        dorisAssert.query(query).explainContains(QUERY_USE_EMPS);
    }

    /**
     * Aggregation query with set operand
     */
    @Test
    public void testAggQueryWithSetOperandOnAggMV() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select deptno, count(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select deptno, count(salary) + count(1) from " + EMPS_TABLE_NAME
                + " group by deptno union "
                + "select deptno, count(salary) + count(1) from " + EMPS_TABLE_NAME
                + " group by deptno;";
        dorisAssert.withMaterializedView(createMVSql);
        dorisAssert.query(query).explainContains(QUERY_USE_EMPS);
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
        dorisAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVsql).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
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
        dorisAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVsql).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
    }

    @Test
    public void testJoinOnProjectsToJoin() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary), sum"
                + "(commission) from " + EMPS_TABLE_NAME + " group by deptno;";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno, max(cost) from "
                + DEPTS_TABLE_NAME + " group by deptno;";
        String query = "select * from (select deptno , sum(salary) from " + EMPS_TABLE_NAME + " group by deptno) A "
                + "join (select deptno from " + DEPTS_TABLE_NAME + " group by deptno ) B on A.deptno = B.deptno;";
        dorisAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVsql).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
    }

    @Test
    public void testJoinOnCalcToJoin0() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + ";";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno from "
                + DEPTS_TABLE_NAME + ";";
        String query = "select * from (select empid, deptno from " + EMPS_TABLE_NAME + " where deptno > 10 ) A "
                + "join (select deptno from " + DEPTS_TABLE_NAME + " ) B on A.deptno = B.deptno;";
        dorisAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVsql).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
    }

    @Test
    public void testJoinOnCalcToJoin1() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + ";";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno from "
                + DEPTS_TABLE_NAME + ";";
        String query = "select * from (select empid, deptno from " + EMPS_TABLE_NAME + " ) A join (select "
                + "deptno from " + DEPTS_TABLE_NAME + " where deptno > 10 ) B on A.deptno = B.deptno;";
        dorisAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVsql).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
    }

    @Test
    public void testJoinOnCalcToJoin2() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + ";";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno from "
                + DEPTS_TABLE_NAME + ";";
        String query = "select * from (select empid, deptno from " + EMPS_TABLE_NAME + " where empid >10 ) A "
                + "join (select deptno from " + DEPTS_TABLE_NAME + " where deptno > 10 ) B on A.deptno = B.deptno;";
        dorisAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVsql).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
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
        dorisAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVsql).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
    }

    @Test
    public void testJoinOnCalcToJoin4() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + ";";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno from "
                + DEPTS_TABLE_NAME + ";";
        String query = "select * from (select empid, deptno + 1 deptno from " + EMPS_TABLE_NAME
                + " where empid is not null ) A full join (select deptno from " + DEPTS_TABLE_NAME
                + " where deptno is not null ) B on A.deptno = B.deptno;";
        dorisAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVsql).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
    }

    @Test
    public void testOrderByQueryOnProjectView() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + ";";
        String query = "select empid from " + EMPS_TABLE_NAME + " order by deptno";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testOrderByQueryOnOrderByView() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String query = "select empid from " + EMPS_TABLE_NAME + " order by deptno";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testQueryOnStar() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select time_col, deptno,"
                + "empid, name, salary, commission from " + EMPS_TABLE_NAME + " order by time_col, deptno, empid;";
        String query = "select * from " + EMPS_TABLE_NAME + " where deptno = 1";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVAggregateFuncs1() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno from " + EMPS_TABLE_NAME + " group by deptno";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVAggregateFuncs2() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " group by deptno";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVAggregateFuncs3() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, empid, sum(salary) from " + EMPS_TABLE_NAME + " group by deptno, empid";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVAggregateFuncs4() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " where deptno > 10 group by deptno";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVAggregateFuncs5() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 group by deptno";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVCalcGroupByQuery1() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno+1, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 "
                + "group by deptno+1;";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVCalcGroupByQuery2() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno * empid, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 "
                + "group by deptno * empid;";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVCalcGroupByQuery3() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select empid, deptno * empid, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 "
                + "group by empid, deptno * empid;";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVCalcAggFunctionQuery() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary + 1) from " + EMPS_TABLE_NAME + " where deptno > 10 "
                + "group by deptno;";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainWithout(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testSubQuery() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid "
                + "from " + EMPS_TABLE_NAME + ";";
        String query = "select empid, deptno, salary from " + EMPS_TABLE_NAME + " e1 where empid = (select max(empid)"
                + " from " + EMPS_TABLE_NAME + " where deptno = e1.deptno);";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV,
                QUERY_USE_EMPS);
    }

    @Test
    public void testDistinctQuery() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query1 = "select distinct deptno from " + EMPS_TABLE_NAME + ";";
        dorisAssert.withMaterializedView(createEmpsMVsql);
        dorisAssert.query(query1).explainContains(QUERY_USE_EMPS_MV);
        String query2 = "select /*+ SET_VAR(enable_nereids_planner=false) */ deptno, sum(distinct salary) from " + EMPS_TABLE_NAME + " group by deptno;";
        dorisAssert.query(query2).explainWithout(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testSingleMVMultiUsage() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, salary "
                + "from " + EMPS_TABLE_NAME + " order by deptno;";
        String query = "select * from (select deptno, empid from " + EMPS_TABLE_NAME + " where deptno>100) A join "
                + "(select deptno, empid from " + EMPS_TABLE_NAME + " where deptno >200) B using (deptno);";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV, 2);
    }

    @Test
    public void testMultiMVMultiUsage() throws Exception {
        String createEmpsMVSql01 = "create materialized view emp_mv_01 as select deptno, empid, salary "
                + "from " + EMPS_TABLE_NAME + " order by deptno;";
        String createEmpsMVSql02 = "create materialized view emp_mv_02 as select deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select * from (select deptno, empid from " + EMPS_TABLE_NAME + " where deptno>100) A join "
                + "(select deptno, sum(salary) from " + EMPS_TABLE_NAME + " where deptno >200 group by deptno) B "
                + "using (deptno);";
        dorisAssert.withMaterializedView(createEmpsMVSql01).withMaterializedView(createEmpsMVSql02).query(query)
                .explainContains("(emp_mv_01)", "(emp_mv_02)");
    }

    @Test
    public void testMVOnJoinQuery() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select salary, empid, deptno from "
                + EMPS_TABLE_NAME + " order by salary;";
        String query = "select empid, salary from " + EMPS_TABLE_NAME + " join " + DEPTS_TABLE_NAME
                + " using (deptno) where salary > 300;";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV,
                QUERY_USE_DEPTS);
    }

    // TODO: should be support
    @Test
    public void testAggregateMVOnCountDistinctQuery1() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, count(distinct empid) from " + EMPS_TABLE_NAME + " group by deptno;";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testQueryAfterTrimingOfUnusedFields() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + " order by empid, deptno;";
        String query = "select empid, deptno from (select empid, deptno, salary from " + EMPS_TABLE_NAME + ") A;";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testUnionAll() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + " order by empid, deptno;";
        String query = "select empid, deptno from " + EMPS_TABLE_NAME + " where empid >1 union all select empid,"
                + " deptno from " + EMPS_TABLE_NAME + " where empid <0;";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV, 2);
    }

    @Test
    public void testUnionDistinct() throws Exception {
        String createEmpsMVsql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + " order by empid, deptno;";
        String query = "select empid, deptno from " + EMPS_TABLE_NAME + " where empid >1 union select empid,"
                + " deptno from " + EMPS_TABLE_NAME + " where empid <0;";
        dorisAssert.withMaterializedView(createEmpsMVsql).query(query).explainContains(QUERY_USE_EMPS_MV, 2);
    }

    @Test
    public void testDeduplicateQueryInAgg() throws Exception {
        String aggregateTable = "create table agg_table (k1 int, k2 int, v1 bigint sum) aggregate key (k1, k2) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(aggregateTable);
        String createRollupSQL = "alter table agg_table add rollup old_key (k2, k1) "
                + "properties ('replication_num' = '1');";
        String query = "select k1, k2 from agg_table;";
        dorisAssert.withRollup(createRollupSQL).query(query).explainContains("OFF", "old_key");
    }

    @Test
    public void testAggFunctionInHaving() throws Exception {
        String duplicateTable = "CREATE TABLE " + TEST_TABLE_NAME + " ( k1 int(11) NOT NULL ,  k2  int(11) NOT NULL ,"
                + "v1  varchar(4096) NOT NULL, v2  float NOT NULL , v3  decimal(20, 7) NOT NULL ) ENGINE=OLAP "
                + "DUPLICATE KEY( k1 ,  k2 ) DISTRIBUTED BY HASH( k1 ,  k2 ) BUCKETS 3 "
                + "PROPERTIES ('replication_num' = '1'); ";
        dorisAssert.withTable(duplicateTable);
        String createK1K2MV = "create materialized view k1_k2 as select k1,k2 from " + TEST_TABLE_NAME + " group by "
                + "k1,k2;";
        String query = "select k1 from " + TEST_TABLE_NAME + " group by k1 having max(v1) > 10;";
        dorisAssert.withMaterializedView(createK1K2MV).query(query).explainWithout("k1_k2");
        dorisAssert.dropTable(TEST_TABLE_NAME, true);
    }

    @Test
    public void testAggFunctionInOrder() throws Exception {
        String duplicateTable = "CREATE TABLE " + TEST_TABLE_NAME + " ( k1 int(11) NOT NULL ,  k2  int(11) NOT NULL ,"
                + "v1  varchar(4096) NOT NULL, v2  float NOT NULL , v3  decimal(20, 7) NOT NULL ) ENGINE=OLAP "
                + "DUPLICATE KEY( k1 ,  k2 ) DISTRIBUTED BY HASH( k1 ,  k2 ) BUCKETS 3 "
                + "PROPERTIES ('replication_num' = '1'); ";
        dorisAssert.withTable(duplicateTable);
        String createK1K2MV = "create materialized view k1_k2 as select k1,k2 from " + TEST_TABLE_NAME + " group by "
                + "k1,k2;";
        String query = "select k1 from " + TEST_TABLE_NAME + " group by k1 order by max(v1);";
        dorisAssert.withMaterializedView(createK1K2MV).query(query).explainWithout("k1_k2");
        dorisAssert.dropTable(TEST_TABLE_NAME, true);
    }

    @Test
    public void testWindowsFunctionInQuery() throws Exception {
        String duplicateTable = "CREATE TABLE " + TEST_TABLE_NAME + " ( k1 int(11) NOT NULL ,  k2  int(11) NOT NULL ,"
                + "v1  varchar(4096) NOT NULL, v2  float NOT NULL , v3  decimal(20, 7) NOT NULL ) ENGINE=OLAP "
                + "DUPLICATE KEY( k1 ,  k2 ) DISTRIBUTED BY HASH( k1 ,  k2 ) BUCKETS 3 "
                + "PROPERTIES ('replication_num' = '1'); ";
        dorisAssert.withTable(duplicateTable);
        String createK1K2MV = "create materialized view k1_k2 as select k1,k2 from " + TEST_TABLE_NAME + " group by "
                + "k1,k2;";
        String query = "select k1 , sum(k2) over (partition by v1 ) from " + TEST_TABLE_NAME + ";";
        dorisAssert.withMaterializedView(createK1K2MV).query(query).explainWithout("k1_k2");
        dorisAssert.dropTable(TEST_TABLE_NAME, true);
    }

    @Test
    public void testUniqueTableInQuery() throws Exception {
        String uniqueTable = "CREATE TABLE " + TEST_TABLE_NAME + " (k1 int, k2 int, v1 int) UNIQUE KEY (k1, k2) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'false');";
        dorisAssert.withTable(uniqueTable);
        String createK1K2MV = "create materialized view only_k1 as select k2, k1 from " + TEST_TABLE_NAME;
        String query = "select * from " + TEST_TABLE_NAME + ";";
        dorisAssert.withMaterializedView(createK1K2MV).query(query).explainContains(TEST_TABLE_NAME);
        dorisAssert.dropTable(TEST_TABLE_NAME, true);
    }

    @Test
    public void testBitmapUnionInQuery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME
                + " as select user_id, bitmap_union(to_bitmap(tag_id)) from "
                + USER_TAG_TABLE_NAME + " group by user_id;";
        dorisAssert.withMaterializedView(createUserTagMVSql);
        String query = "select user_id, bitmap_union_count(to_bitmap(tag_id)) a from " + USER_TAG_TABLE_NAME
                + " group by user_id having a>1 order by a;";
        dorisAssert.query(query).explainContains(QUERY_USE_USER_TAG_MV);
    }

    @Test
    public void testBitmapUnionInSubquery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        dorisAssert.withMaterializedView(createUserTagMVSql);
        String query = "select user_id from " + USER_TAG_TABLE_NAME + " where user_id in (select user_id from "
                + USER_TAG_TABLE_NAME + " group by user_id having bitmap_union_count(to_bitmap(tag_id)) >1 ) ;";
        dorisAssert.query(query).explainContains(USER_TAG_MV_NAME, USER_TAG_TABLE_NAME);
    }

    @Test
    public void testIncorrectMVRewriteInQuery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        dorisAssert.withMaterializedView(createUserTagMVSql);
        String createEmpMVSql = "create materialized view " + EMPS_MV_NAME + " as select name, deptno from "
                + EMPS_TABLE_NAME + ";";
        dorisAssert.withMaterializedView(createEmpMVSql);
        String query = "select user_name, bitmap_union_count(to_bitmap(tag_id)) a from " + USER_TAG_TABLE_NAME + ", "
                + "(select name, deptno from " + EMPS_TABLE_NAME + ") a" + " where user_name=a.name group by "
                + "user_name having a>1 order by a;";
        dorisAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
        dorisAssert.query(query).explainWithout(QUERY_USE_USER_TAG_MV);
    }

    @Test
    public void testIncorrectMVRewriteInSubquery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        dorisAssert.withMaterializedView(createUserTagMVSql);
        String query = "select user_id, bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " where "
                + "user_name in (select user_name from " + USER_TAG_TABLE_NAME + " group by user_name having "
                + "bitmap_union_count(to_bitmap(tag_id)) >1 )" + " group by user_id;";
        dorisAssert.query(query).explainContains(QUERY_USE_USER_TAG);
    }

    @Test
    public void testTwoTupleInQuery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        dorisAssert.withMaterializedView(createUserTagMVSql);
        String query = "select * from (select user_id, bitmap_union_count(to_bitmap(tag_id)) x from "
                + USER_TAG_TABLE_NAME + " group by user_id) a, (select user_name, bitmap_union_count(to_bitmap(tag_id))"
                + "" + " y from " + USER_TAG_TABLE_NAME + " group by user_name) b where a.x=b.y;";
        dorisAssert.query(query).explainContains(QUERY_USE_USER_TAG, QUERY_USE_USER_TAG_MV);
    }

    @Test
    public void testAggTableCountDistinctInBitmapType() throws Exception {
        String aggTable = "CREATE TABLE " + TEST_TABLE_NAME + " (k1 int, v1 bitmap bitmap_union) Aggregate KEY (k1) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ('replication_num' = '1');";
        dorisAssert.withTable(aggTable);
        String query = "select k1, count(distinct v1) from " + TEST_TABLE_NAME + " group by k1;";
        dorisAssert.query(query).explainContains(TEST_TABLE_NAME, "bitmap_union_count");
        dorisAssert.dropTable(TEST_TABLE_NAME, true);
    }

    @Test
    public void testAggTableCountDistinctInHllType() throws Exception {
        String aggTable = "CREATE TABLE " + TEST_TABLE_NAME + " (k1 int, v1 hll " + FunctionSet.HLL_UNION + ") Aggregate KEY (k1) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ('replication_num' = '1');";
        dorisAssert.withTable(aggTable);
        String query = "select k1, count(distinct v1) from " + TEST_TABLE_NAME + " group by k1;";
        dorisAssert.query(query).explainContains(TEST_TABLE_NAME, "hll_union_agg");
        dorisAssert.dropTable(TEST_TABLE_NAME, true);
    }

    @Test
    public void testCountDistinctToBitmap() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        dorisAssert.withMaterializedView(createUserTagMVSql);
        String query = "select count(distinct tag_id) from " + USER_TAG_TABLE_NAME + ";";
        dorisAssert.query(query).explainContains(USER_TAG_MV_NAME, "bitmap_union_count");
    }

    @Test
    public void testIncorrectRewriteCountDistinct() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        dorisAssert.withMaterializedView(createUserTagMVSql);
        String query = "select user_name, count(distinct tag_id) from " + USER_TAG_TABLE_NAME + " group by user_name;";
        dorisAssert.query(query).explainContains(USER_TAG_TABLE_NAME, FunctionSet.COUNT);
    }

    @Test
    public void testNDVToHll() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "`" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        dorisAssert.withMaterializedView(createUserTagMVSql);
        String query = "select /*+ SET_VAR(enable_fallback_to_original_planner=false) */ndv(tag_id) from " + USER_TAG_TABLE_NAME + ";";
        dorisAssert.query(query).explainContains(USER_TAG_MV_NAME, "hll_union_agg");
    }

    @Test
    public void testApproxCountDistinctToHll() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "`" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        dorisAssert.withMaterializedView(createUserTagMVSql);
        String query = "select /*+ SET_VAR(enable_fallback_to_original_planner=false) */ approx_count_distinct(tag_id) from " + USER_TAG_TABLE_NAME + ";";
        dorisAssert.query(query).explainContains(USER_TAG_MV_NAME, "hll_union_agg");
    }

    @Test
    public void testHLLUnionFamilyRewrite() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "`" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        dorisAssert.withMaterializedView(createUserTagMVSql);
        String query = "select `" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME + ";";
        dorisAssert.query(query).explainContains(USER_TAG_MV_NAME);
        query = "select hll_union_agg(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME + ";";
        dorisAssert.query(query).explainContains(USER_TAG_MV_NAME);
        query = "select hll_raw_agg(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME + ";";
        dorisAssert.query(query).explainContains(USER_TAG_MV_NAME);
    }

    /*
    ISSUE-3174
     */
    @Test
    public void testAggInHaving() throws Exception {
        String createMVSql = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + " group by empid, deptno;";
        dorisAssert.withMaterializedView(createMVSql);
        String query = "select empid from " + EMPS_TABLE_NAME + " group by empid having max(salary) > 1;";
        dorisAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testCountFieldInQuery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "count(tag_id) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        dorisAssert.withMaterializedView(createUserTagMVSql);
        String query = "select count(tag_id) from " + USER_TAG_TABLE_NAME + ";";
        dorisAssert.query(query).explainContains(USER_TAG_MV_NAME);
        query = "select user_name, count(tag_id) from " + USER_TAG_TABLE_NAME + " group by user_name;";
        dorisAssert.query(query).explainWithout(USER_TAG_MV_NAME);
    }

    @Test
    public void testInvalidColumnInCreateMVStmt() throws Exception {
        String createMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select invalid_column, user_id from "
                + USER_TAG_TABLE_NAME + ";";
        try {
            dorisAssert.withMaterializedView(createMVSql);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void testCreateMVBaseBitmapAggTable() throws Exception {
        String createTableSQL = "create table " + HR_DB_NAME + ".agg_table "
                + "(empid int, name varchar, salary bitmap " + FunctionSet.BITMAP_UNION + ") "
                + "aggregate key (empid, name) "
                + "partition by range (empid) "
                + "(partition p1 values less than MAXVALUE) "
                + "distributed by hash(empid) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
        String createMVSql = "create materialized view mv as select empid, " + FunctionSet.BITMAP_UNION
                + "(salary) from agg_table "
                + "group by empid;";
        dorisAssert.withMaterializedView(createMVSql);
        String query = "select count(distinct salary) from agg_table;";
        dorisAssert.query(query).explainContains("mv");
        dorisAssert.dropTable("agg_table", true);
    }

    @Test
    public void testSelectMVWithTableAlias() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "count(tag_id) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        dorisAssert.withMaterializedView(createUserTagMVSql);
        String query = "select count(tag_id) from " + USER_TAG_TABLE_NAME + " t ;";
        dorisAssert.query(query).explainContains(USER_TAG_MV_NAME);
    }
}
