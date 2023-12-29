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

package org.apache.doris.rewrite;

import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class InferFiltersRuleTest {
    private static final String baseDir = "fe";
    private static final String runningDir = baseDir + "/mocked/InferFiltersRuleTest/"
            + UUID.randomUUID() + "/";
    private static DorisAssert dorisAssert;
    private static final String DB_NAME = "db1";
    private static final String TABLE_NAME_1 = "tb1";
    private static final String TABLE_NAME_2 = "tb2";
    private static final String TABLE_NAME_3 = "tb3";

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createDorisCluster(runningDir);
        dorisAssert = new DorisAssert();
        dorisAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        String createTableSQL = "create table " + DB_NAME + "." + TABLE_NAME_1
                + " (k1 int, k2 int) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
        createTableSQL = "create table " + DB_NAME + "." + TABLE_NAME_2
                + " (k1 int, k2 int) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
        createTableSQL = "create table " + DB_NAME + "." + TABLE_NAME_3
                + " (k1 tinyint, k2 smallint, k3 int, k4 bigint,"
                + " k5 largeint, k6 date, k7 datetime, k8 float, k9 double) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        dorisAssert.withTable(createTableSQL);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        UtFrameUtils.cleanDorisFeDir(baseDir);
    }

    @Test
    //set enableInferPredicate = false;
    public void testWithoutRewritten() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(false);
        Assert.assertFalse(sessionVariable.isEnableInferPredicate());
        String query = "select * from tb1, tb2 where tb1.k1 = 1 and tb1.k1 = tb2.k1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertFalse(planString.contains("`tb2`.`k1` = 1"));
    }

    @Test
    public void testRewritten() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1, tb2 where tb1.k1 = 1 and tb1.k1 = tb2.k1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb2`.`k1` = 1"));
    }

    @Test
    public void testUnequalSlotPredicate() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select * from tb1, tb2 where tb1.k1 = 1 and tb1.k1 > tb2.k1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertFalse(planString.contains("`tb2`.`k1` = 1"));
    }

    @Test
    public void testOn3TablesBothInnerJoinRewritten() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 inner join tb2 inner join tb3"
                + " where tb1.k1 = tb2.k1 and tb2.k1 = tb3.k1 and tb3.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb2`.`k1` = 1"));
        Assert.assertTrue(planString.contains("`tb1`.`k1` = 1"));
    }

    @Test
    public void testOn2TablesLeftSemiJoinEqLiteralAt2nd() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 left semi join tb2 on tb1.k1 = tb2.k1 and tb2.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb1`.`k1` = 1"));
    }

    @Test
    public void testOn2TablesLeftSemiJoinEqLiteralAt1st() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 left semi join tb2 on tb1.k1 = tb2.k1 and tb1.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb2`.`k1` = 1"));
    }

    @Test
    public void testOn2TablesLeftAntiJoinEqLiteralAt2nd() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select * from tb1 left anti join tb2 on tb1.k1 = tb2.k1 and tb2.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertFalse(planString.contains("`tb1`.`k1` = 1"));
    }

    @Test
    public void testOn2TablesLeftJoinNotInferable() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select * from tb1 left join tb2 on tb1.k1 = tb2.k1 and tb2.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertFalse(planString.contains("`tb1`.`k1` = 1"));
    }

    /*
    the following 3 test case is valid. But we cannot tell them from other incorrect inferences.
    In origin design we made a mistake: we assume inference is symmetrical.
    For example, t1.x=t2.x and t1.x=1 => t2.x=1
    this is not always true.
    if this is left join, t1 is left, t2.x=1 is not valid.
    However, in inferFilterRule, we do not know whether t1.x is from left or right table.
    And hence, we have to skip inference for outer/anti join for quick fix.

    @Test
    public void testOn3Tables1stInner2ndRightJoinEqLiteralAt2nd() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select * from tb1 inner join tb2 on tb1.k1 = tb2.k1"
                + " right outer join tb3 on tb2.k1 = tb3.k1 and tb2.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb1`.`k1` = 1"));
        Assert.assertFalse(planString.contains("`tb3`.`k1` = 1"));
    }

    @Test
    public void testOn3Tables1stInner2ndRightJoinEqLiteralAt3rd() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select * from tb1 inner join tb2 on tb1.k1 = tb2.k1"
                + " right outer join tb3 on tb2.k1 = tb3.k1 and tb3.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb2`.`k1` = 1"));
        Assert.assertTrue(planString.contains("`tb1`.`k1` = 1"));
    }
    @Test
    public void testOn2TablesLeftAntiJoinEqLiteralAt1st() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select * from tb1 left anti join tb2 on tb1.k1 = tb2.k1 and tb1.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb2`.`k1` = 1"));
    }
     */
    @Test
    public void testOnIsNotNullPredicate() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 inner join tb2 on tb1.k1 = tb2.k1 right outer join tb3 on tb2.k1 = tb3.k1"
                + " where tb1.k1 = tb2.k1 and tb2.k1 = tb3.k1 and tb2.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb1`.`k1` = 1"));
        Assert.assertTrue(planString, planString.contains("CAST(`tb3`.`k1` AS INT)"));
    }

    @Test
    public void testOnBetweenPredicate() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 inner join tb2 on tb1.k1 = tb2.k1 and tb1.k1 between 1 and 2";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb2`.`k1` >= 1"));
        Assert.assertTrue(planString.contains("`tb2`.`k1` <= 2"));
    }

    @Test
    public void testOnInPredicate() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 inner join tb2 on tb1.k1 = tb2.k1 and tb1.k1 in (2)";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb2`.`k1` IN (2)"));
    }

    @Test
    public void testWhere3TablesInnerJoinRewritten() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1, tb2, tb3 where tb1.k1 = tb2.k1 and tb2.k1 = tb3.k1 and tb3.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb2`.`k1` = 1"));
        Assert.assertTrue(planString.contains("`tb1`.`k1` = 1"));
    }

    @Test
    public void testWhere3TablesBothInnerJoinRewritten() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 inner join tb2 inner join tb3"
                + " where tb1.k1 = tb2.k1 and tb2.k1 = tb3.k1 and tb3.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb2`.`k1` = 1"));
        Assert.assertTrue(planString.contains("`tb1`.`k1` = 1"));
    }

    @Test
    public void testWhere3Tables1stInner2ndLeftJoinEqLiteralAt3rd() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 inner join tb2 left outer join tb3 on tb3.k1 = tb2.k1"
                + " where tb1.k1 = tb2.k1 and tb2.k1 = tb3.k1 and tb3.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb2`.`k1` = 1"));
        Assert.assertTrue(planString.contains("`tb1`.`k1` = 1"));
    }

    @Test
    public void testWhere3Tables1stInner2ndLeftJoinEqLiteralAt2nd() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 inner join tb2 left outer join tb3 on tb3.k1 = tb2.k1"
                + " where tb1.k1 = tb2.k1 and tb2.k1 = tb3.k1 and tb2.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb1`.`k1` = 1"));
        Assert.assertFalse(planString.contains("`tb3`.`k1` = 1"));
    }

    @Test
    public void testWhere3Tables1stInner2ndRightJoinEqLiteralAt2nd() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 inner join tb2 on tb1.k1 = tb2.k1 right outer join tb3 on tb2.k1 = tb3.k1"
                + " where tb1.k1 = tb2.k1 and tb2.k1 = tb3.k1 and tb2.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString, planString.contains("`tb1`.`k1` = 1"));
        Assert.assertTrue(planString, planString.contains("CAST(`tb3`.`k1` AS INT) = 1"));
    }

    @Test
    public void testWhere3Tables1stInner2ndRightJoinEqLiteralAt3rd() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select * from tb1 inner join tb2 on tb1.k1 = tb2.k1 right outer join tb3 on tb2.k1 = tb3.k1"
                + " where tb1.k1 = tb2.k1 and tb2.k1 = tb3.k1 and tb3.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertFalse(planString.contains("`tb2`.`k1` = 1"));
        Assert.assertFalse(planString.contains("`tb1`.`k1` = 1"));
    }

    @Test
    public void testWhereIsNotNullPredicate() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        sessionVariable.setEnableRewriteElementAtToSlot(false);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 inner join tb2 inner join tb3"
                + " where tb1.k1 = tb3.k1 and tb2.k1 = tb3.k1 and tb1.k1 is not null";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb3`.`k1` IS NOT NULL"));
        Assert.assertTrue(planString.contains("`tb2`.`k1` IS NOT NULL"));
    }

    @Test
    public void testWhereInPredicate() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 inner join tb2 where tb1.k1 = tb2.k1 and tb1.k1 in (2)";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb2`.`k1` IN (2)"));
    }

    @Test
    public void testWhereBetweenPredicate() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 inner join tb2 where tb1.k1 = tb2.k1 and tb1.k1 between 1 and 2";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb2`.`k1` >= 1"));
        Assert.assertTrue(planString.contains("`tb2`.`k1` <= 2"));
    }

    @Test
    public void testOnAndWhere2TablesLeftJoin2ndIsLiteral() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 left join tb2 on tb1.k1 = tb2.k1 where tb2.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb1`.`k1` = 1"));
    }

    @Test
    public void testOnAndWhere2TablesInnerJoin2ndIsLiteral() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 inner join tb2 on tb1.k1 = tb2.k1 where tb2.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb1`.`k1` = 1"));
    }

    @Test
    public void testOnAndWhere2TableLeftJoin1stIsLiteral() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 left join tb2 on tb1.k1 = tb2.k1 where tb1.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb2`.`k1` = 1"));
    }

    @Test
    public void testOnAndWhere2TablesInnerJoin1stIsLiteral() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from tb1 inner join tb2 on tb1.k1 = tb2.k1 where tb1.k1 = 1";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertTrue(planString.contains("`tb2`.`k1` = 1"));
    }

    @Test
    public void testSameAliasWithSlotEqualToLiteralInDifferentUnionChildren() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select * from tb1 inner join tb2 on tb1.k1 = tb2.k1"
                + " union select * from tb1 inner join tb2 on tb1.k2 = tb2.k2 where tb1.k1 = 3";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertFalse(planString.contains("`tb2`.`k1` = 3"));
    }

    @Test
    public void testSameAliasWithSlotInPredicateInDifferentUnionChildren() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select * from tb1 inner join tb2 on tb1.k1 = tb2.k1"
                + " union select * from tb1 inner join tb2 on tb1.k2 = tb2.k2 where tb1.k1 in (3, 4, 5)";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertFalse(planString.contains("`tb2`.`k1` IN (3, 4, 5)"));
    }

    @Test
    public void testSameAliasWithSlotIsNullInDifferentUnionChildren() throws Exception {
        SessionVariable sessionVariable = dorisAssert.getSessionVariable();
        sessionVariable.setEnableInferPredicate(true);
        Assert.assertTrue(sessionVariable.isEnableInferPredicate());
        String query = "select * from tb1 inner join tb2 on tb1.k1 = tb2.k1"
                + " union select * from tb1 inner join tb2 on tb1.k2 = tb2.k2 where tb1.k1 is not null";
        String planString = dorisAssert.query(query).explainQuery();
        Assert.assertFalse(planString.contains("`tb2`.`k1` IS NOT NULL"));
    }
}
