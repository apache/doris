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

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PlannerTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {

        connectContext.getSessionVariable().setEnableNereidsPlanner(false);

        // Create database `db1`.
        createDatabase("db1");

        // Create tables.
        String tbl1 = "create table db1.tbl1(" + "k1 varchar(32), " + "k2 varchar(32), " + "k3 varchar(32), "
                + "k4 int) " + "AGGREGATE KEY(k1, k2,k3,k4) " + "distributed by hash(k1) buckets 3 "
                + "properties('replication_num' = '1');";

        String tbl2 = "create table db1.tbl2(" + "k1 int, " + "k2 int sum) " + "AGGREGATE KEY(k1) "
                + "partition by range(k1) () " + "distributed by hash(k1) buckets 3 "
                + "properties('replication_num' = '1');";

        String tbl3 = "create table db1.tbl3 (" + "k1 date, " + "k2 varchar(128) NULL, " + "k3 varchar(5000) NULL) "
                + "DUPLICATE KEY(k1, k2, k3) " + "distributed by hash(k1) buckets 1 "
                + "properties ('replication_num' = '1');";

        String tbl4 = "create table db1.tbl4(" + "k1 int," + " k2 int," + " v1 int)" + " distributed by hash(k1)"
                + " properties('replication_num' = '1');";

        String tbl5 = "create table db1.tbl5(" + "k1 int," + "k2 int) " + "DISTRIBUTED BY HASH(k2) "
                + "BUCKETS 3 PROPERTIES ('replication_num' = '1');";

        String tbl6 = "create table db1.tbl6(" + "k1 int," + "k2 int, " + "v1 int)" + "UNIQUE KEY (k1, k2)"
                + "DISTRIBUTED BY HASH(k2) " + "BUCKETS 3 PROPERTIES ('replication_num' = '1');";

        createTables(tbl1, tbl2, tbl3, tbl4, tbl5, tbl6);
    }

    @Test
    public void testSetOperation() throws Exception {
        // union
        String sql1 = "explain select * from\n"
                + "  (select k1, k2 from db1.tbl1\n"
                + "   union all\n"
                + "   select k1, k2 from db1.tbl1) a\n"
                + "  inner join\n"
                + "  db1.tbl1 b\n"
                + "  on (a.k1 = b.k1)\n"
                + "where b.k1 = 'a'";
        StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
        stmtExecutor1.execute();
        Planner planner1 = stmtExecutor1.planner();
        String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertEquals(1, StringUtils.countMatches(plan1, "UNION"));
        String sql2 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from db1.tbl1 where k1='a' and k4=1\n"
                + "union distinct\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=2\n"
                + "   union all\n"
                + "   select * from db1.tbl1 where k1='b' and k4=2)\n"
                + "union distinct\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=2\n"
                + "   union all\n"
                + "   (select * from db1.tbl1 where k1='b' and k4=3)\n"
                + "   order by 3 limit 3)\n"
                + "union all\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=3\n"
                + "   union all\n"
                + "   select * from db1.tbl1 where k1='b' and k4=4)\n"
                + "union all\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=3\n"
                + "   union all\n"
                + "   (select * from db1.tbl1 where k1='b' and k4=5)\n"
                + "   order by 3 limit 3)";
        StmtExecutor stmtExecutor2 = new StmtExecutor(connectContext, sql2);
        stmtExecutor2.execute();
        Planner planner2 = stmtExecutor2.planner();
        String plan2 = planner2.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertEquals(4, StringUtils.countMatches(plan2, "UNION"));

        // intersect
        String sql3 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from\n"
                + "  (select k1, k2 from db1.tbl1\n"
                + "   intersect\n"
                + "   select k1, k2 from db1.tbl1) a\n"
                + "  inner join\n"
                + "  db1.tbl1 b\n"
                + "  on (a.k1 = b.k1)\n"
                + "where b.k1 = 'a'";
        StmtExecutor stmtExecutor3 = new StmtExecutor(connectContext, sql3);
        stmtExecutor3.execute();
        Planner planner3 = stmtExecutor3.planner();
        String plan3 = planner3.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertEquals(1, StringUtils.countMatches(plan3, "INTERSECT"));
        String sql4 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from db1.tbl1 where k1='a' and k4=1\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=2\n"
                + "   intersect\n"
                + "   select * from db1.tbl1 where k1='b' and k4=2)\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=2\n"
                + "   intersect\n"
                + "   (select * from db1.tbl1 where k1='b' and k4=3)\n"
                + "   order by 3 limit 3)\n"
                + "intersect\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   select * from db1.tbl1 where k1='b' and k4=4)\n"
                + "intersect\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   (select * from db1.tbl1 where k1='b' and k4=5)\n"
                + "   order by 3 limit 3)";

        StmtExecutor stmtExecutor4 = new StmtExecutor(connectContext, sql4);
        stmtExecutor4.execute();
        Planner planner4 = stmtExecutor4.planner();
        String plan4 = planner4.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertEquals(3, StringUtils.countMatches(plan4, "INTERSECT"));

        // except
        String sql5 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from\n"
                + "  (select k1, k2 from db1.tbl1\n"
                + "   except\n"
                + "   select k1, k2 from db1.tbl1) a\n"
                + "  inner join\n"
                + "  db1.tbl1 b\n"
                + "  on (a.k1 = b.k1)\n"
                + "where b.k1 = 'a'";
        StmtExecutor stmtExecutor5 = new StmtExecutor(connectContext, sql5);
        stmtExecutor5.execute();
        Planner planner5 = stmtExecutor5.planner();
        String plan5 = planner5.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertEquals(1, StringUtils.countMatches(plan5, "EXCEPT"));

        String sql6 = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from db1.tbl1 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl1 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl1 where k1='a' and k4=2\n"
                + "except distinct\n"
                + "(select * from db1.tbl1 where k1='a' and k4=2)\n"
                + "order by 3 limit 3";
        StmtExecutor stmtExecutor6 = new StmtExecutor(connectContext, sql6);
        stmtExecutor6.execute();
        Planner planner6 = stmtExecutor6.planner();
        String plan6 = planner6.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertEquals(1, StringUtils.countMatches(plan6, "EXCEPT"));

        String sql7 = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from db1.tbl1 where k1='a' and k4=1\n"
                + "except distinct\n"
                + "select * from db1.tbl1 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl1 where k1='a' and k4=2\n"
                + "except\n"
                + "(select * from db1.tbl1 where k1='a' and k4=2)\n"
                + "order by 3 limit 3";
        StmtExecutor stmtExecutor7 = new StmtExecutor(connectContext, sql7);
        stmtExecutor7.execute();
        Planner planner7 = stmtExecutor7.planner();
        String plan7 = planner7.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertEquals(1, StringUtils.countMatches(plan7, "EXCEPT"));

        // mixed
        String sql8 = "select /*+ SET_VAR(enable_nereids_planner=false) */ * from db1.tbl1 where k1='a' and k4=1\n"
                + "union\n"
                + "select * from db1.tbl1 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl1 where k1='a' and k4=2\n"
                + "intersect\n"
                + "(select * from db1.tbl1 where k1='a' and k4=2)\n"
                + "order by 3 limit 3";
        StmtExecutor stmtExecutor8 = new StmtExecutor(connectContext, sql8);
        stmtExecutor8.execute();
        Planner planner8 = stmtExecutor8.planner();
        String plan8 = planner8.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertEquals(1, StringUtils.countMatches(plan8, "UNION"));
        Assertions.assertEquals(1, StringUtils.countMatches(plan8, "INTERSECT"));
        Assertions.assertEquals(1, StringUtils.countMatches(plan8, "EXCEPT"));

        String sql9 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ * from db1.tbl1 where k1='a' and k4=1\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=2\n"
                + "   union all\n"
                + "   select * from db1.tbl1 where k1='b' and k4=2)\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=2\n"
                + "   except\n"
                + "   (select * from db1.tbl1 where k1='b' and k4=3)\n"
                + "   order by 3 limit 3)\n"
                + "union all\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   select * from db1.tbl1 where k1='b' and k4=4)\n"
                + "except\n"
                + "  (select * from db1.tbl1 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   (select * from db1.tbl1 where k1='b' and k4=5)\n"
                + "   order by 3 limit 3)";

        StmtExecutor stmtExecutor9 = new StmtExecutor(connectContext, sql9);
        stmtExecutor9.execute();
        Planner planner9 = stmtExecutor9.planner();
        String plan9 = planner9.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertEquals(2, StringUtils.countMatches(plan9, "UNION"));
        Assertions.assertEquals(3, StringUtils.countMatches(plan9, "INTERSECT"));
        Assertions.assertEquals(2, StringUtils.countMatches(plan9, "EXCEPT"));

        String sql10 = "select /*+ SET_VAR(enable_nereids_planner=false) */ 499 union select 670 except select 499";
        StmtExecutor stmtExecutor10 = new StmtExecutor(connectContext, sql10);
        stmtExecutor10.execute();
        Planner planner10 = stmtExecutor10.planner();
        List<PlanFragment> fragments10 = planner10.getFragments();
        Assertions.assertTrue(fragments10.get(0).getPlanRoot().getFragment()
                .getPlanRoot().getChild(0) instanceof AggregationNode);
        Assertions.assertTrue(fragments10.get(0).getPlanRoot()
                .getFragment().getPlanRoot().getChild(1) instanceof UnionNode);

        String sql11 = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ a.x FROM\n"
                + "(SELECT '01' x) a \n"
                + "INNER JOIN\n"
                + "(SELECT '01' x UNION all SELECT '02') b";
        StmtExecutor stmtExecutor11 = new StmtExecutor(connectContext, sql11);
        stmtExecutor11.execute();
        Planner planner11 = stmtExecutor11.planner();
        SetOperationNode setNode11 = (SetOperationNode) (planner11.getFragments().get(1).getPlanRoot());
        Assertions.assertEquals(2, setNode11.getMaterializedConstExprLists().size());

        String sql12 = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ a.x \n"
                + "FROM (SELECT '01' x) a \n"
                + "INNER JOIN \n"
                + "(SELECT k1 from db1.tbl1 \n"
                + "UNION all \n"
                + "SELECT k1 from db1.tbl1) b;";
        StmtExecutor stmtExecutor12 = new StmtExecutor(connectContext, sql12);
        stmtExecutor12.execute();
        Planner planner12 = stmtExecutor12.planner();
        SetOperationNode setNode12 = (SetOperationNode) (planner12.getFragments().get(1).getPlanRoot());
        Assertions.assertEquals(2, setNode12.getMaterializedResultExprLists().size());
    }

    @Test
    public void testPushDown() throws Exception {
        String sql1 =
                "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ \n"
                + "    IF(k2 IS NULL, 'ALL', k2) AS k2,\n"
                + "    IF(k3 IS NULL, 'ALL', k3) AS k3,\n"
                + "    k4\n"
                + "FROM\n"
                + "(\n"
                + "    SELECT\n"
                + "        k1,\n"
                + "        k2,\n"
                + "        k3,\n"
                + "        SUM(k4) AS k4\n"
                + "    FROM  db1.tbl1\n"
                + "    WHERE k1 = 0\n"
                + "        AND k4 = 1\n"
                + "        AND k3 = 'foo'\n"
                + "    GROUP BY \n"
                + "    GROUPING SETS (\n"
                + "        (k1),\n"
                + "        (k1, k2),\n"
                + "        (k1, k3),\n"
                + "        (k1, k2, k3)\n"
                + "    )\n"
                + ") t\n"
                + "WHERE IF(k2 IS NULL, 'ALL', k2) = 'ALL'";
        StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
        stmtExecutor1.execute();
        Planner planner1 = stmtExecutor1.planner();
        List<PlanFragment> fragments1 = planner1.getFragments();
        Assertions.assertEquals("if",
                fragments1.get(0).getPlanRoot().conjuncts.get(0).getChild(0).getFn().functionName());
        Assertions.assertEquals(3, fragments1.get(0).getPlanRoot().getChild(0).getChild(0).conjuncts.size());

        String sql2 =
                "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ \n"
                        + "    IF(k2 IS NULL, 'ALL', k2) AS k2,\n"
                        + "    IF(k3 IS NULL, 'ALL', k3) AS k3,\n"
                        + "    k4\n"
                        + "FROM\n"
                        + "(\n"
                        + "    SELECT\n"
                        + "        k1,\n"
                        + "        k2,\n"
                        + "        k3,\n"
                        + "        SUM(k4) AS k4\n"
                        + "    FROM  db1.tbl1\n"
                        + "    WHERE k1 = 0\n"
                        + "        AND k4 = 1\n"
                        + "        AND k3 = 'foo'\n"
                        + "    GROUP BY k1, k2, k3\n"
                        + ") t\n"
                        + "WHERE IF(k2 IS NULL, 'ALL', k2) = 'ALL'";
        StmtExecutor stmtExecutor2 = new StmtExecutor(connectContext, sql2);
        stmtExecutor2.execute();
        Planner planner2 = stmtExecutor2.planner();
        List<PlanFragment> fragments2 = planner2.getFragments();
        Assertions.assertEquals(4, fragments2.get(0).getPlanRoot().getChild(0).conjuncts.size());

    }

    @Test
    public void testWithStmtSlotIsAllowNull() throws Exception {
        // union
        String sql1 = "with a as (select NULL as user_id ), "
                + "b as ( select '543' as user_id) "
                + "select /*+ SET_VAR(enable_nereids_planner=false) */ user_id from a union all select user_id from b";

        StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
        stmtExecutor1.execute();
        Planner planner1 = stmtExecutor1.planner();
        String plan1 = planner1.getExplainString(new ExplainOptions(true, false, false));
        Assertions.assertEquals(2, StringUtils.countMatches(plan1, "nullable=true"));
    }

    @Test
    public void testAccessingVisibleColumnWithoutPartition() throws Exception {
        String sql = "select count(k1) from db1.tbl2";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        stmtExecutor.execute();
        Assertions.assertNotNull(stmtExecutor.planner());
    }

    @Test
    public void testAnalyticSortNodeLeftJoin() throws Exception {
        String sql = "SELECT /*+ SET_VAR(enable_nereids_planner=false) */ a.k1, a.k3, SUM(COUNT(t.k2)) OVER (PARTITION BY a.k3 ORDER BY a.k1) AS c\n"
                + "FROM ( SELECT k1, k3 FROM db1.tbl3) a\n"
                + "LEFT JOIN (SELECT 1 AS line, k1, k2, k3 FROM db1.tbl3) t\n"
                + "ON t.k1 = a.k1 AND t.k3 = a.k3\n"
                + "GROUP BY a.k1, a.k3";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        stmtExecutor.execute();
        Assertions.assertNotNull(stmtExecutor.planner());
        Planner planner = stmtExecutor.planner();
        List<PlanFragment> fragments = planner.getFragments();
        Assertions.assertTrue(fragments.size() > 0);
        PlanNode node = fragments.get(0).getPlanRoot().getChild(0);
        Assertions.assertTrue(node.getChildren().size() > 0);
        Assertions.assertTrue(node instanceof SortNode);
        SortNode sortNode = (SortNode) node;
        List<Expr> tupleExprs = sortNode.resolvedTupleExprs;
        List<Expr> sortTupleExprs = sortNode.getSortInfo().getSortTupleSlotExprs();
        for (Expr expr : tupleExprs) {
            expr.isBoundByTupleIds(sortNode.getChild(0).tupleIds);
        }
        for (Expr expr : sortTupleExprs) {
            expr.isBoundByTupleIds(sortNode.getChild(0).tupleIds);
        }
    }


    @Test
    public void testBigintSlotRefCompareDecimalLiteral() {
        java.util.function.BiConsumer<String, String> compare = (sql1, sql2) -> {
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            try {
                stmtExecutor1.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));

            StmtExecutor stmtExecutor2 = new StmtExecutor(connectContext, sql2);
            try {
                stmtExecutor2.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
            Planner planner2 = stmtExecutor2.planner();
            String plan2 = planner2.getExplainString(new ExplainOptions(false, false, false));

            Assertions.assertEquals(plan1, plan2);
        };

        compare.accept("select /*+ SET_VAR(enable_nereids_planner=false) */ * from db1.tbl2 where k1 = 2.0", "select /*+ SET_VAR(enable_nereids_planner=false) */ * from db1.tbl2 where k1 = 2");
        compare.accept("select /*+ SET_VAR(enable_nereids_planner=false) */ * from db1.tbl2 where k1 = 2.1", "select /*+ SET_VAR(enable_nereids_planner=false) */ * from db1.tbl2 where 2 = 2.1");
        compare.accept("select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 != 2.0", "select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 != 2");
        compare.accept("select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 != 2.1", "select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where TRUE");
        compare.accept("select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 <= 2.0", "select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 <= 2");
        compare.accept("select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 <= 2.1", "select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 <= 2");
        compare.accept("select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 >= 2.0", "select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 >= 2");
        compare.accept("select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 >= 2.1", "select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 > 2");
        compare.accept("select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 < 2.0", "select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 < 2");
        compare.accept("select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 < 2.1", "select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 <= 2");
        compare.accept("select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 > 2.0", "select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 > 2");
        compare.accept("select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 > 2.1", "select /*+ SET_VAR(enable_nereids_planner=false) */* from db1.tbl2 where k1 > 2");
    }

    @Test
    public void testStringType() {
        String createTbl1 = "create table db1.tbl1(k1 string, k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1')";
        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, () -> parseAndAnalyzeStmt(createTbl1));
        Assertions.assertTrue(exception.getMessage().contains("String Type should not be used in key column[k1]."));
    }

    @Test
    public void testPushDownPredicateOnGroupingSetAggregate() throws Exception {
        String sql = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ k1, k2, count(distinct v1) from db1.tbl4"
                + " group by grouping sets((k1), (k1, k2)) having k1 = 1 and k2 = 1";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        String plan = planner.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertTrue(plan.contains("`k1` = 1"));
    }

    @Test
    public void testPushDownPredicateOnRollupAggregate() throws Exception {
        String sql = "explain select k1, k2, count(distinct v1) from db1.tbl4"
                + " group by rollup(k1, k2) having k1 = 1 and k2 = 1";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        String plan = planner.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertFalse(plan.contains("PREDICATES:"));
    }

    @Test
    public void testPushDownPredicateOnNormalAggregate() throws Exception {
        String sql = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ k1, k2, count(distinct v1) from db1.tbl4"
                + " group by k1, k2 having k1 = 1 and k2 = 1";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        String plan = planner.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertTrue(plan.contains("(`k1` = 1) AND (`k2` = 1)"));
    }

    @Test
    public void testPushDownPredicateOnWindowFunction() throws Exception {
        String sql = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ v1, k1,"
                + " sum(v1) over (partition by k1 order by v1 rows between 1 preceding and 1 following)"
                + " as 'moving total' from db1.tbl4 where k1 = 1";
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        stmtExecutor.execute();
        Planner planner = stmtExecutor.planner();
        String plan = planner.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertTrue(plan.contains("`k1` = 1"));
    }

    @Test
    public void testRewriteNestedUnionStmt() throws Exception {
        String qSQL = "SELECT k1 FROM db1.tbl5 WHERE k1 IN "
                + "( SELECT k1 FROM ( SELECT k1 FROM db1.tbl5 ORDER BY k2 DESC, k1 DESC LIMIT 300 INTERSECT "
                + "(SELECT k1 FROM db1.tbl5 ORDER BY k2 DESC, k1 DESC LIMIT 9 EXCEPT SELECT k1 "
                + "FROM db1.tbl5 ORDER BY k2 DESC, k1 DESC LIMIT 2) ) t )";

        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, qSQL);
        stmtExecutor.execute();
    }

    @Test
    public void testUpdateUnique() throws Exception {
        String qSQL = "update db1.tbl6 set v1=5 where k1=5";
        UserIdentity user1 = new UserIdentity("cmy", "%");
        user1.setIsAnalyzed();
        // check user priv
        connectContext.setCurrentUserIdentity(user1);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, qSQL);
        stmtExecutor.execute();
        QueryState state = connectContext.getState();
        Assertions.assertEquals(MysqlStateType.ERR, state.getStateType());
        Assertions.assertTrue(state.getErrorMessage()
                .contains("you need (at least one of) the (LOAD) privilege(s) for this operation"));
        // set to admin user
        connectContext.setCurrentUserIdentity(UserIdentity.ADMIN);
    }

    @Test
    public void testPushSortToOlapScan() throws Exception {
        // Push sort fail without limit
        String sql1 = "explain select k1 from db1.tbl3 order by k1, k2";
        StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
        stmtExecutor1.execute();
        Planner planner1 = stmtExecutor1.planner();
        String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertFalse(plan1.contains("SORT INFO:\n          `k1`\n          `k2`"));
        Assertions.assertFalse(plan1.contains("SORT LIMIT:"));

        // Push sort fail limit > topnOptLimitThreshold
        sql1 = "explain select k1 from db1.tbl3 order by k1, k2 limit "
            + (connectContext.getSessionVariable().topnOptLimitThreshold + 1);
        stmtExecutor1 = new StmtExecutor(connectContext, sql1);
        stmtExecutor1.execute();
        planner1 = stmtExecutor1.planner();
        plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertFalse(plan1.contains("SORT INFO:\n          `k1`\n          `k2`"));
        Assertions.assertFalse(plan1.contains("SORT LIMIT:"));

        // Push sort success limit = topnOptLimitThreshold
        sql1 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ k1 from db1.tbl3 order by k1, k2 limit "
            + (connectContext.getSessionVariable().topnOptLimitThreshold);
        stmtExecutor1 = new StmtExecutor(connectContext, sql1);
        stmtExecutor1.execute();
        planner1 = stmtExecutor1.planner();
        plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertTrue(plan1.contains("SORT INFO:\n          `k1`\n          `k2`"));
        Assertions.assertTrue(plan1.contains("SORT LIMIT:"));

        // Push sort success limit < topnOptLimitThreshold
        if (connectContext.getSessionVariable().topnOptLimitThreshold > 1) {
            sql1 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ k1 from db1.tbl3 order by k1, k2 limit "
                + (connectContext.getSessionVariable().topnOptLimitThreshold - 1);
            stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            planner1 = stmtExecutor1.planner();
            plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertTrue(plan1.contains("SORT INFO:\n          `k1`\n          `k2`"));
            Assertions.assertTrue(plan1.contains("SORT LIMIT:"));
        }

        // Push sort failed
        String sql2 = "explain select k1, k2, k3 from db1.tbl3 order by k1, k3, k2";
        StmtExecutor stmtExecutor2 = new StmtExecutor(connectContext, sql2);
        stmtExecutor2.execute();
        Planner planner2 = stmtExecutor2.planner();
        String plan2 = planner2.getExplainString(new ExplainOptions(false, false, false));
        Assertions.assertFalse(plan2.contains("SORT INFO:"));
        Assertions.assertFalse(plan2.contains("SORT LIMIT:"));
    }

    @Test
    public void testEliminatingSortNode() throws Exception {
            // fail case 1
            {
            String sql1 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ k1 from db1.tbl1 where k1 = 1 order by k1, k2";
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertTrue(plan1.contains("order by:"));
            }

            // fail case 2
            {
            String sql1 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ k1 from db1.tbl1 where k1 = 1 and k3 = 2 order by k1, k2";
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertTrue(plan1.contains("order by:"));
            }

            // fail case 3
            {
            String sql1 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ k1 from db1.tbl1 where k1 = 1 and k2 != 2 order by k1, k2";
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertTrue(plan1.contains("order by:"));
            }

            // fail case 4
            {
            String sql1 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ k1 from db1.tbl1 where k1 = 1 or k2 = 2 order by k1, k2";
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertTrue(plan1.contains("order by:"));
            }

            // fail case 5
            {
            String sql1 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ k1 from db1.tbl1 where k1 = 1 and k2 = 2 or k3 = 3 order by k1, k2";
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertTrue(plan1.contains("order by:"));
            }

            // fail case 6
            // TODO, support: in (select 1)
            {
            String sql1 = "explain select k1 from db1.tbl1 where k1 in (select 1) and k2 = 2 order by k1, k2";
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertTrue(plan1.contains("order by:"));
            }

            // fail case 7
            {
            String sql1 = "explain select k1 from db1.tbl1 where k1 not in (1) and k2 = 2 order by k1, k2";
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertTrue(plan1.contains("order by:"));
            }

            // success case 1
            {
            String sql1 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ k1 from db1.tbl1 where k1 = 1 and k2 = 2 order by k1, k2";
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertFalse(plan1.contains("order by:"));
            }

            // success case 2
            {
            String sql1 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ k1 from db1.tbl1 where k3 = 3 and k2 = 2 and k1 = 1 order by k1, k2";
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertFalse(plan1.contains("order by:"));
            }

            // success case 3
            {
            String sql1 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ k1 from db1.tbl1 where k1 in (1) and k2 in (2) and k2 !=2 order by k1, k2";
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertFalse(plan1.contains("order by:"));
            }

            // success case 4
            {
            String sql1 = "explain select /*+ SET_VAR(enable_nereids_planner=false) */ k1 from db1.tbl1 where k1 in (concat('1','2')) and k2 = 2 order by k1, k2";
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertFalse(plan1.contains("order by:"));
            }

            // success case 5
            {
            String sql1 = "explain select tbl1.k1 from db1.tbl1 join db1.tbl2 on tbl1.k1 = tbl2.k1"
                    + " where tbl1.k1 = 1 and tbl2.k1 = 2 and tbl1.k2 = 3 order by tbl1.k1, tbl2.k1";
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertFalse(plan1.contains("order by:"));
            }
    }

    @Test
    public void testInsertPlan() throws Exception {
        FeConstants.runningUnitTest = true;
        // 1. should not contains exchange node in old planner
        boolean v = connectContext.getSessionVariable().isEnableNereidsPlanner();
        try {
            connectContext.getSessionVariable().setEnableNereidsPlanner(false);
            String sql1 = "explain insert into db1.tbl1 select * from db1.tbl1";
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertFalse(plan1.contains("VEXCHANGE"));
        } finally {
            connectContext.getSessionVariable().setEnableNereidsPlanner(v);
        }

        // 2. should not contains exchange node in new planner
        v = connectContext.getSessionVariable().isEnableNereidsPlanner();
        boolean v2 = connectContext.getSessionVariable().isEnableStrictConsistencyDml();
        try {
            connectContext.getSessionVariable().setEnableNereidsPlanner(true);
            connectContext.getSessionVariable().setEnableStrictConsistencyDml(false);
            String sql1 = "explain insert into db1.tbl1 select * from db1.tbl1";
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertFalse(plan1.contains("VEXCHANGE"));
        } finally {
            connectContext.getSessionVariable().setEnableNereidsPlanner(v);
            connectContext.getSessionVariable().setEnableStrictConsistencyDml(v2);
        }

        // 3. should contain exchange node in new planner if enable strict consistency dml
        v = connectContext.getSessionVariable().isEnableNereidsPlanner();
        v2 = connectContext.getSessionVariable().isEnableStrictConsistencyDml();
        try {
            connectContext.getSessionVariable().setEnableNereidsPlanner(true);
            connectContext.getSessionVariable().setEnableStrictConsistencyDml(true);
            String sql1 = "explain insert into db1.tbl1 select * from db1.tbl1";
            StmtExecutor stmtExecutor1 = new StmtExecutor(connectContext, sql1);
            stmtExecutor1.execute();
            Planner planner1 = stmtExecutor1.planner();
            String plan1 = planner1.getExplainString(new ExplainOptions(false, false, false));
            Assertions.assertTrue(plan1.contains("VEXCHANGE"));
        } finally {
            connectContext.getSessionVariable().setEnableNereidsPlanner(v);
            connectContext.getSessionVariable().setEnableStrictConsistencyDml(v2);
        }
    }
}
