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

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.utframe.UtFrameUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;

public class PlannerTest {
    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(runningDir));
    }

    @Test
    public void testSetOperation() throws Exception {
        // union

        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.createMinDorisCluster(runningDir);
        String createDbStmtStr = "create database db1;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        // 3. create table tbl1
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStmtStr, ctx);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
        String sql1 = "explain select * from\n"
                + "  (select k1, k2 from db1.tbl1\n"
                + "   union all\n"
                + "   select k1, k2 from db1.tbl1) a\n"
                + "  inner join\n"
                + "  db1.tbl1 b\n"
                + "  on (a.k1 = b.k1)\n"
                + "where b.k1 = 'a'";
        String plan1 = "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:<slot 4> | <slot 5> | `b`.`k1` | `b`.`k2` | `b`.`k3` | `b`.`k4`\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: Node type not match\n" +
                "(<slot 4> = `b`.`k1`)\n" +
                "  |  tuple ids: 2 4 \n" +
                "  |  \n" +
                "  |----7:EXCHANGE\n" +
                "  |       tuple ids: 4 \n" +
                "  |    \n" +
                "  0:UNION\n" +
                "  |  child exprs: \n" +
                "  |      `k1` | `k2`\n" +
                "  |      `k1` | `k2`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 2 \n" +
                "  |  \n" +
                "  |----6:EXCHANGE\n" +
                "  |       tuple ids: 1 \n" +
                "  |    \n" +
                "  5:EXCHANGE\n" +
                "     tuple ids: 0 \n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  3:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: null\n" +
                "     PREDICATES: `b`.`k1` = 'a'\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 4 \n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    RANDOM\n" +
                "\n" +
                "  2:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 1 \n" +
                "\n" +
                "PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 05\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 0 \n";
        StmtExecutor stmtExecutor1 = new StmtExecutor(ctx, sql1);
        stmtExecutor1.execute();
        Planner planner1 = stmtExecutor1.planner();
        List<PlanFragment> fragments1 = planner1.getFragments();
        Assert.assertEquals(plan1, planner1.getExplainString(fragments1, TExplainLevel.VERBOSE));
        String sql2 = "explain select * from db1.tbl1 where k1='a' and k4=1\n"
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

        String plan2 = "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:<slot 60> | <slot 61> | <slot 62> | <slot 63>\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  30:EXCHANGE\n" +
                "     tuple ids: 15 \n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 30\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  9:UNION\n" +
                "  |  child exprs: \n" +
                "  |      <slot 60> | <slot 61> | <slot 62> | <slot 63>\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      <slot 57> | <slot 58> | <slot 56> | <slot 59>\n" +
                "  |  pass-through-operands: 26,27,28\n" +
                "  |  tuple ids: 15 \n" +
                "  |  \n" +
                "  |----27:EXCHANGE\n" +
                "  |       tuple ids: 8 \n" +
                "  |    \n" +
                "  |----28:EXCHANGE\n" +
                "  |       tuple ids: 9 \n" +
                "  |    \n" +
                "  |----29:EXCHANGE\n" +
                "  |       limit: 3\n" +
                "  |       tuple ids: 14 \n" +
                "  |    \n" +
                "  26:EXCHANGE\n" +
                "     tuple ids: 15 \n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 29\n" +
                "    RANDOM\n" +
                "\n" +
                "  25:MERGING-EXCHANGE\n" +
                "     limit: 3\n" +
                "     tuple ids: 14 \n" +
                "\n" +
                "PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 25\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  15:TOP-N\n" +
                "  |  order by: <slot 56> ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 3\n" +
                "  |  tuple ids: 14 \n" +
                "  |  \n" +
                "  12:UNION\n" +
                "  |  child exprs: \n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 13 \n" +
                "  |  \n" +
                "  |----24:EXCHANGE\n" +
                "  |       tuple ids: 12 \n" +
                "  |    \n" +
                "  23:EXCHANGE\n" +
                "     tuple ids: 11 \n" +
                "\n" +
                "PLAN FRAGMENT 4\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 24\n" +
                "    RANDOM\n" +
                "\n" +
                "  14:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 5\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 12 \n" +
                "\n" +
                "PLAN FRAGMENT 5\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 23\n" +
                "    RANDOM\n" +
                "\n" +
                "  13:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 3\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 11 \n" +
                "\n" +
                "PLAN FRAGMENT 6\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 28\n" +
                "    RANDOM\n" +
                "\n" +
                "  11:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 4\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 9 \n" +
                "\n" +
                "PLAN FRAGMENT 7\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 27\n" +
                "    RANDOM\n" +
                "\n" +
                "  10:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 3\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 8 \n" +
                "\n" +
                "PLAN FRAGMENT 8\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 26\n" +
                "    RANDOM\n" +
                "\n" +
                "  8:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "  |  tuple ids: 15 \n" +
                "  |  \n" +
                "  0:UNION\n" +
                "  |  child exprs: \n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      <slot 29> | <slot 30> | <slot 28> | <slot 31>\n" +
                "  |  pass-through-operands: 19,20,21\n" +
                "  |  tuple ids: 15 \n" +
                "  |  \n" +
                "  |----20:EXCHANGE\n" +
                "  |       tuple ids: 1 \n" +
                "  |    \n" +
                "  |----21:EXCHANGE\n" +
                "  |       tuple ids: 2 \n" +
                "  |    \n" +
                "  |----22:EXCHANGE\n" +
                "  |       limit: 3\n" +
                "  |       tuple ids: 7 \n" +
                "  |    \n" +
                "  19:EXCHANGE\n" +
                "     tuple ids: 0 \n" +
                "\n" +
                "PLAN FRAGMENT 9\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 22\n" +
                "    RANDOM\n" +
                "\n" +
                "  18:MERGING-EXCHANGE\n" +
                "     limit: 3\n" +
                "     tuple ids: 7 \n" +
                "\n" +
                "PLAN FRAGMENT 10\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 18\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  7:TOP-N\n" +
                "  |  order by: <slot 28> ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 3\n" +
                "  |  tuple ids: 7 \n" +
                "  |  \n" +
                "  4:UNION\n" +
                "  |  child exprs: \n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 6 \n" +
                "  |  \n" +
                "  |----17:EXCHANGE\n" +
                "  |       tuple ids: 5 \n" +
                "  |    \n" +
                "  16:EXCHANGE\n" +
                "     tuple ids: 4 \n" +
                "\n" +
                "PLAN FRAGMENT 11\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 17\n" +
                "    RANDOM\n" +
                "\n" +
                "  6:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 3\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 5 \n" +
                "\n" +
                "PLAN FRAGMENT 12\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 16\n" +
                "    RANDOM\n" +
                "\n" +
                "  5:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 2\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 4 \n" +
                "\n" +
                "PLAN FRAGMENT 13\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 21\n" +
                "    RANDOM\n" +
                "\n" +
                "  3:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 2\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 2 \n" +
                "\n" +
                "PLAN FRAGMENT 14\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 20\n" +
                "    RANDOM\n" +
                "\n" +
                "  2:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 2\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 1 \n" +
                "\n" +
                "PLAN FRAGMENT 15\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 19\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'a', `k4` = 1\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 0 \n";
        StmtExecutor stmtExecutor2 = new StmtExecutor(ctx, sql2);
        stmtExecutor2.execute();
        Planner planner2 = stmtExecutor2.planner();
        List<PlanFragment> fragments2 = planner2.getFragments();
        Assert.assertEquals(plan2, planner2.getExplainString(fragments2, TExplainLevel.VERBOSE));
        // intersect
        String sql3 = "explain select * from\n"
                + "  (select k1, k2 from db1.tbl1\n"
                + "   intersect\n"
                + "   select k1, k2 from db1.tbl1) a\n"
                + "  inner join\n"
                + "  db1.tbl1 b\n"
                + "  on (a.k1 = b.k1)\n"
                + "where b.k1 = 'a'";
        String plan3 = "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:<slot 4> | <slot 5> | `b`.`k1` | `b`.`k2` | `b`.`k3` | `b`.`k4`\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  5:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: Node type not match\n" +
                "(<slot 4> = `b`.`k1`)\n" +
                "  |  tuple ids: 2 4 \n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |       tuple ids: 4 \n" +
                "  |    \n" +
                "  3:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 4>, <slot 5>\n" +
                "  |  tuple ids: 2 \n" +
                "  |  \n" +
                "  0:INTERSECT\n" +
                "  |  child exprs: \n" +
                "  |      `k1` | `k2`\n" +
                "  |      `k1` | `k2`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 2 \n" +
                "  |  \n" +
                "  |----7:EXCHANGE\n" +
                "  |       tuple ids: 1 \n" +
                "  |    \n" +
                "  6:EXCHANGE\n" +
                "     tuple ids: 0 \n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 08\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  4:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: null\n" +
                "     PREDICATES: `b`.`k1` = 'a'\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 4 \n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    RANDOM\n" +
                "\n" +
                "  2:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 1 \n" +
                "\n" +
                "PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 0 \n";
        StmtExecutor stmtExecutor3 = new StmtExecutor(ctx, sql3);
        stmtExecutor3.execute();
        Planner planner3 = stmtExecutor3.planner();
        List<PlanFragment> fragments3 = planner3.getFragments();
        Assert.assertEquals(plan3, planner3.getExplainString(fragments3, TExplainLevel.VERBOSE));
        String sql4 = "explain select * from db1.tbl1 where k1='a' and k4=1\n"
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

        String plan4 = "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:<slot 60> | <slot 61> | <slot 62> | <slot 63>\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  32:EXCHANGE\n" +
                "     tuple ids: 15 \n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: HASH_PARTITIONED: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 32\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  31:AGGREGATE (merge finalize)\n" +
                "  |  group by: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "  |  tuple ids: 15 \n" +
                "  |  \n" +
                "  30:EXCHANGE\n" +
                "     tuple ids: 15 \n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 30\n" +
                "    HASH_PARTITIONED: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "\n" +
                "  16:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "  |  tuple ids: 15 \n" +
                "  |  \n" +
                "  0:INTERSECT\n" +
                "  |  child exprs: \n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      <slot 29> | <slot 30> | <slot 28> | <slot 31>\n" +
                "  |      <slot 57> | <slot 58> | <slot 56> | <slot 59>\n" +
                "  |  pass-through-operands: 23,24,25,27,28\n" +
                "  |  tuple ids: 15 \n" +
                "  |  \n" +
                "  |----24:EXCHANGE\n" +
                "  |       tuple ids: 1 \n" +
                "  |    \n" +
                "  |----25:EXCHANGE\n" +
                "  |       tuple ids: 2 \n" +
                "  |    \n" +
                "  |----27:EXCHANGE\n" +
                "  |       tuple ids: 8 \n" +
                "  |    \n" +
                "  |----28:EXCHANGE\n" +
                "  |       tuple ids: 9 \n" +
                "  |    \n" +
                "  |----26:EXCHANGE\n" +
                "  |       limit: 3\n" +
                "  |       tuple ids: 7 \n" +
                "  |    \n" +
                "  |----29:EXCHANGE\n" +
                "  |       limit: 3\n" +
                "  |       tuple ids: 14 \n" +
                "  |    \n" +
                "  23:EXCHANGE\n" +
                "     tuple ids: 0 \n" +
                "\n" +
                "PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 29\n" +
                "    RANDOM\n" +
                "\n" +
                "  22:MERGING-EXCHANGE\n" +
                "     limit: 3\n" +
                "     tuple ids: 14 \n" +
                "\n" +
                "PLAN FRAGMENT 4\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 22\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  15:TOP-N\n" +
                "  |  order by: <slot 56> ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 3\n" +
                "  |  tuple ids: 14 \n" +
                "  |  \n" +
                "  14:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 52>, <slot 53>, <slot 54>, <slot 55>\n" +
                "  |  tuple ids: 13 \n" +
                "  |  \n" +
                "  11:INTERSECT\n" +
                "  |  child exprs: \n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 13 \n" +
                "  |  \n" +
                "  |----21:EXCHANGE\n" +
                "  |       tuple ids: 12 \n" +
                "  |    \n" +
                "  20:EXCHANGE\n" +
                "     tuple ids: 11 \n" +
                "\n" +
                "PLAN FRAGMENT 5\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 21\n" +
                "    RANDOM\n" +
                "\n" +
                "  13:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 5\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 12 \n" +
                "\n" +
                "PLAN FRAGMENT 6\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 20\n" +
                "    RANDOM\n" +
                "\n" +
                "  12:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 3\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 11 \n" +
                "\n" +
                "PLAN FRAGMENT 7\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 28\n" +
                "    RANDOM\n" +
                "\n" +
                "  10:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 4\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 9 \n" +
                "\n" +
                "PLAN FRAGMENT 8\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 27\n" +
                "    RANDOM\n" +
                "\n" +
                "  9:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 3\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 8 \n" +
                "\n" +
                "PLAN FRAGMENT 9\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 26\n" +
                "    RANDOM\n" +
                "\n" +
                "  19:MERGING-EXCHANGE\n" +
                "     limit: 3\n" +
                "     tuple ids: 7 \n" +
                "\n" +
                "PLAN FRAGMENT 10\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 19\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  8:TOP-N\n" +
                "  |  order by: <slot 28> ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 3\n" +
                "  |  tuple ids: 7 \n" +
                "  |  \n" +
                "  7:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 24>, <slot 25>, <slot 26>, <slot 27>\n" +
                "  |  tuple ids: 6 \n" +
                "  |  \n" +
                "  4:INTERSECT\n" +
                "  |  child exprs: \n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 6 \n" +
                "  |  \n" +
                "  |----18:EXCHANGE\n" +
                "  |       tuple ids: 5 \n" +
                "  |    \n" +
                "  17:EXCHANGE\n" +
                "     tuple ids: 4 \n" +
                "\n" +
                "PLAN FRAGMENT 11\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 18\n" +
                "    RANDOM\n" +
                "\n" +
                "  6:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 3\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 5 \n" +
                "\n" +
                "PLAN FRAGMENT 12\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 17\n" +
                "    RANDOM\n" +
                "\n" +
                "  5:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 2\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 4 \n" +
                "\n" +
                "PLAN FRAGMENT 13\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 25\n" +
                "    RANDOM\n" +
                "\n" +
                "  3:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 2\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 2 \n" +
                "\n" +
                "PLAN FRAGMENT 14\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 24\n" +
                "    RANDOM\n" +
                "\n" +
                "  2:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 2\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 1 \n" +
                "\n" +
                "PLAN FRAGMENT 15\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 23\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'a', `k4` = 1\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 0 \n";
        StmtExecutor stmtExecutor4 = new StmtExecutor(ctx, sql4);
        stmtExecutor4.execute();
        Planner planner4 = stmtExecutor4.planner();
        List<PlanFragment> fragments4 = planner4.getFragments();
        Assert.assertEquals(plan4, planner4.getExplainString(fragments4, TExplainLevel.VERBOSE));

        // except
        String sql5 = "explain select * from\n"
                + "  (select k1, k2 from db1.tbl1\n"
                + "   except\n"
                + "   select k1, k2 from db1.tbl1) a\n"
                + "  inner join\n"
                + "  db1.tbl1 b\n"
                + "  on (a.k1 = b.k1)\n"
                + "where b.k1 = 'a'";
        String plan5 = "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:<slot 4> | <slot 5> | `b`.`k1` | `b`.`k2` | `b`.`k3` | `b`.`k4`\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  5:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: Node type not match\n" +
                "(<slot 4> = `b`.`k1`)\n" +
                "  |  tuple ids: 2 4 \n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |       tuple ids: 4 \n" +
                "  |    \n" +
                "  3:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 4>, <slot 5>\n" +
                "  |  tuple ids: 2 \n" +
                "  |  \n" +
                "  0:EXCEPT\n" +
                "  |  child exprs: \n" +
                "  |      `k1` | `k2`\n" +
                "  |      `k1` | `k2`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 2 \n" +
                "  |  \n" +
                "  |----7:EXCHANGE\n" +
                "  |       tuple ids: 1 \n" +
                "  |    \n" +
                "  6:EXCHANGE\n" +
                "     tuple ids: 0 \n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 08\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  4:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: null\n" +
                "     PREDICATES: `b`.`k1` = 'a'\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 4 \n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    RANDOM\n" +
                "\n" +
                "  2:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 1 \n" +
                "\n" +
                "PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 0 \n";
        StmtExecutor stmtExecutor5 = new StmtExecutor(ctx, sql5);
        stmtExecutor5.execute();
        Planner planner5 = stmtExecutor5.planner();
        List<PlanFragment> fragments5 = planner5.getFragments();
        Assert.assertEquals(plan5, planner5.getExplainString(fragments5, TExplainLevel.VERBOSE));

        String sql6 = "select * from db1.tbl1 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl1 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl1 where k1='a' and k4=2\n"
                + "except distinct\n"
                + "(select * from db1.tbl1 where k1='a' and k4=2)\n"
                + "order by 3 limit 3";
        String plan6 = "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:<slot 21> | <slot 22> | <slot 20> | <slot 23>\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  11:MERGING-EXCHANGE\n" +
                "     limit: 3\n" +
                "     tuple ids: 5 \n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 11\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  6:TOP-N\n" +
                "  |  order by: <slot 20> ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 3\n" +
                "  |  tuple ids: 5 \n" +
                "  |  \n" +
                "  5:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 16>, <slot 17>, <slot 18>, <slot 19>\n" +
                "  |  tuple ids: 4 \n" +
                "  |  \n" +
                "  0:EXCEPT\n" +
                "  |  child exprs: \n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 4 \n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |       tuple ids: 1 \n" +
                "  |    \n" +
                "  |----9:EXCHANGE\n" +
                "  |       tuple ids: 2 \n" +
                "  |    \n" +
                "  |----10:EXCHANGE\n" +
                "  |       tuple ids: 3 \n" +
                "  |    \n" +
                "  7:EXCHANGE\n" +
                "     tuple ids: 0 \n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 10\n" +
                "    RANDOM\n" +
                "\n" +
                "  4:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'a', `k4` = 2\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 3 \n" +
                "\n" +
                "PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 09\n" +
                "    RANDOM\n" +
                "\n" +
                "  3:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'a', `k4` = 2\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 2 \n" +
                "\n" +
                "PLAN FRAGMENT 4\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 08\n" +
                "    RANDOM\n" +
                "\n" +
                "  2:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'a', `k4` = 1\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 1 \n" +
                "\n" +
                "PLAN FRAGMENT 5\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'a', `k4` = 1\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 0 \n";
        StmtExecutor stmtExecutor6 = new StmtExecutor(ctx, sql6);
        stmtExecutor6.execute();
        Planner planner6 = stmtExecutor6.planner();
        List<PlanFragment> fragments6 = planner6.getFragments();
        Assert.assertEquals(plan6, planner6.getExplainString(fragments6, TExplainLevel.VERBOSE));

        String sql7 = "select * from db1.tbl1 where k1='a' and k4=1\n"
                + "except distinct\n"
                + "select * from db1.tbl1 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl1 where k1='a' and k4=2\n"
                + "except\n"
                + "(select * from db1.tbl1 where k1='a' and k4=2)\n"
                + "order by 3 limit 3";
        String plan7 = "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:<slot 21> | <slot 22> | <slot 20> | <slot 23>\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  11:MERGING-EXCHANGE\n" +
                "     limit: 3\n" +
                "     tuple ids: 5 \n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 11\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  6:TOP-N\n" +
                "  |  order by: <slot 20> ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 3\n" +
                "  |  tuple ids: 5 \n" +
                "  |  \n" +
                "  5:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 16>, <slot 17>, <slot 18>, <slot 19>\n" +
                "  |  tuple ids: 4 \n" +
                "  |  \n" +
                "  0:EXCEPT\n" +
                "  |  child exprs: \n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 4 \n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |       tuple ids: 1 \n" +
                "  |    \n" +
                "  |----9:EXCHANGE\n" +
                "  |       tuple ids: 2 \n" +
                "  |    \n" +
                "  |----10:EXCHANGE\n" +
                "  |       tuple ids: 3 \n" +
                "  |    \n" +
                "  7:EXCHANGE\n" +
                "     tuple ids: 0 \n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 10\n" +
                "    RANDOM\n" +
                "\n" +
                "  4:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'a', `k4` = 2\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 3 \n" +
                "\n" +
                "PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 09\n" +
                "    RANDOM\n" +
                "\n" +
                "  3:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'a', `k4` = 2\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 2 \n" +
                "\n" +
                "PLAN FRAGMENT 4\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 08\n" +
                "    RANDOM\n" +
                "\n" +
                "  2:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'a', `k4` = 1\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 1 \n" +
                "\n" +
                "PLAN FRAGMENT 5\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'a', `k4` = 1\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 0 \n";
        StmtExecutor stmtExecutor7 = new StmtExecutor(ctx, sql7);
        stmtExecutor7.execute();
        Planner planner7 = stmtExecutor7.planner();
        List<PlanFragment> fragments7 = planner7.getFragments();
        Assert.assertEquals(plan7, planner7.getExplainString(fragments7, TExplainLevel.VERBOSE));

        // mixed
        String sql8 = "select * from db1.tbl1 where k1='a' and k4=1\n"
                + "union\n"
                + "select * from db1.tbl1 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl1 where k1='a' and k4=2\n"
                + "intersect\n"
                + "(select * from db1.tbl1 where k1='a' and k4=2)\n"
                + "order by 3 limit 3";
        String plan8 = "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:<slot 21> | <slot 22> | <slot 20> | <slot 23>\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  17:MERGING-EXCHANGE\n" +
                "     limit: 3\n" +
                "     tuple ids: 5 \n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 17\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  10:TOP-N\n" +
                "  |  order by: <slot 20> ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 3\n" +
                "  |  tuple ids: 5 \n" +
                "  |  \n" +
                "  9:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 16>, <slot 17>, <slot 18>, <slot 19>\n" +
                "  |  tuple ids: 4 \n" +
                "  |  \n" +
                "  7:INTERSECT\n" +
                "  |  child exprs: \n" +
                "  |      <slot 16> | <slot 17> | <slot 18> | <slot 19>\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 4 \n" +
                "  |  \n" +
                "  |----16:EXCHANGE\n" +
                "  |       tuple ids: 3 \n" +
                "  |    \n" +
                "  15:EXCHANGE\n" +
                "     tuple ids: 4 \n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 16\n" +
                "    RANDOM\n" +
                "\n" +
                "  8:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'a', `k4` = 2\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 3 \n" +
                "\n" +
                "PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 15\n" +
                "    RANDOM\n" +
                "\n" +
                "  6:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 16>, <slot 17>, <slot 18>, <slot 19>\n" +
                "  |  tuple ids: 4 \n" +
                "  |  \n" +
                "  4:EXCEPT\n" +
                "  |  child exprs: \n" +
                "  |      <slot 16> | <slot 17> | <slot 18> | <slot 19>\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 4 \n" +
                "  |  \n" +
                "  |----14:EXCHANGE\n" +
                "  |       tuple ids: 2 \n" +
                "  |    \n" +
                "  13:EXCHANGE\n" +
                "     tuple ids: 4 \n" +
                "\n" +
                "PLAN FRAGMENT 4\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 14\n" +
                "    RANDOM\n" +
                "\n" +
                "  5:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'a', `k4` = 2\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 2 \n" +
                "\n" +
                "PLAN FRAGMENT 5\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 13\n" +
                "    RANDOM\n" +
                "\n" +
                "  3:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 16>, <slot 17>, <slot 18>, <slot 19>\n" +
                "  |  tuple ids: 4 \n" +
                "  |  \n" +
                "  0:UNION\n" +
                "  |  child exprs: \n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 4 \n" +
                "  |  \n" +
                "  |----12:EXCHANGE\n" +
                "  |       tuple ids: 1 \n" +
                "  |    \n" +
                "  11:EXCHANGE\n" +
                "     tuple ids: 0 \n" +
                "\n" +
                "PLAN FRAGMENT 6\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 12\n" +
                "    RANDOM\n" +
                "\n" +
                "  2:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'a', `k4` = 1\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 1 \n" +
                "\n" +
                "PLAN FRAGMENT 7\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 11\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'a', `k4` = 1\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 0 \n";
        StmtExecutor stmtExecutor8 = new StmtExecutor(ctx, sql8);
        stmtExecutor8.execute();
        Planner planner8 = stmtExecutor8.planner();
        List<PlanFragment> fragments8 = planner8.getFragments();
        Assert.assertEquals(plan8, planner8.getExplainString(fragments8, TExplainLevel.VERBOSE));

        String sql9 = "explain select * from db1.tbl1 where k1='a' and k4=1\n"
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

        String plan9 = "PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:<slot 60> | <slot 61> | <slot 62> | <slot 63>\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  47:EXCHANGE\n" +
                "     tuple ids: 15 \n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: HASH_PARTITIONED: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 47\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  46:AGGREGATE (merge finalize)\n" +
                "  |  group by: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "  |  tuple ids: 15 \n" +
                "  |  \n" +
                "  45:EXCHANGE\n" +
                "     tuple ids: 15 \n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 45\n" +
                "    HASH_PARTITIONED: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "\n" +
                "  23:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "  |  tuple ids: 15 \n" +
                "  |  \n" +
                "  17:EXCEPT\n" +
                "  |  child exprs: \n" +
                "  |      <slot 60> | <slot 61> | <slot 62> | <slot 63>\n" +
                "  |      <slot 57> | <slot 58> | <slot 56> | <slot 59>\n" +
                "  |  pass-through-operands: 43\n" +
                "  |  tuple ids: 15 \n" +
                "  |  \n" +
                "  |----44:EXCHANGE\n" +
                "  |       limit: 3\n" +
                "  |       tuple ids: 14 \n" +
                "  |    \n" +
                "  43:EXCHANGE\n" +
                "     tuple ids: 15 \n" +
                "\n" +
                "PLAN FRAGMENT 3\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 44\n" +
                "    RANDOM\n" +
                "\n" +
                "  42:MERGING-EXCHANGE\n" +
                "     limit: 3\n" +
                "     tuple ids: 14 \n" +
                "\n" +
                "PLAN FRAGMENT 4\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 42\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  22:TOP-N\n" +
                "  |  order by: <slot 56> ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 3\n" +
                "  |  tuple ids: 14 \n" +
                "  |  \n" +
                "  21:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 52>, <slot 53>, <slot 54>, <slot 55>\n" +
                "  |  tuple ids: 13 \n" +
                "  |  \n" +
                "  18:INTERSECT\n" +
                "  |  child exprs: \n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 13 \n" +
                "  |  \n" +
                "  |----41:EXCHANGE\n" +
                "  |       tuple ids: 12 \n" +
                "  |    \n" +
                "  40:EXCHANGE\n" +
                "     tuple ids: 11 \n" +
                "\n" +
                "PLAN FRAGMENT 5\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 41\n" +
                "    RANDOM\n" +
                "\n" +
                "  20:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 5\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 12 \n" +
                "\n" +
                "PLAN FRAGMENT 6\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 40\n" +
                "    RANDOM\n" +
                "\n" +
                "  19:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 3\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 11 \n" +
                "\n" +
                "PLAN FRAGMENT 7\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: HASH_PARTITIONED: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 43\n" +
                "    RANDOM\n" +
                "\n" +
                "  39:AGGREGATE (merge finalize)\n" +
                "  |  group by: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "  |  tuple ids: 15 \n" +
                "  |  \n" +
                "  38:EXCHANGE\n" +
                "     tuple ids: 15 \n" +
                "\n" +
                "PLAN FRAGMENT 8\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 38\n" +
                "    HASH_PARTITIONED: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "\n" +
                "  16:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "  |  tuple ids: 15 \n" +
                "  |  \n" +
                "  11:UNION\n" +
                "  |  child exprs: \n" +
                "  |      <slot 40> | <slot 41> | <slot 42> | <slot 43>\n" +
                "  |      <slot 60> | <slot 61> | <slot 62> | <slot 63>\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 15 \n" +
                "  |  \n" +
                "  |----37:EXCHANGE\n" +
                "  |       tuple ids: 15 \n" +
                "  |    \n" +
                "  36:EXCHANGE\n" +
                "     tuple ids: 10 \n" +
                "\n" +
                "PLAN FRAGMENT 9\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: HASH_PARTITIONED: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 37\n" +
                "    RANDOM\n" +
                "\n" +
                "  35:AGGREGATE (merge finalize)\n" +
                "  |  group by: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "  |  tuple ids: 15 \n" +
                "  |  \n" +
                "  34:EXCHANGE\n" +
                "     tuple ids: 15 \n" +
                "\n" +
                "PLAN FRAGMENT 10\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 34\n" +
                "    HASH_PARTITIONED: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "\n" +
                "  10:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: <slot 60>, <slot 61>, <slot 62>, <slot 63>\n" +
                "  |  tuple ids: 15 \n" +
                "  |  \n" +
                "  0:INTERSECT\n" +
                "  |  child exprs: \n" +
                "  |      <slot 12> | <slot 13> | <slot 14> | <slot 15>\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      <slot 29> | <slot 30> | <slot 28> | <slot 31>\n" +
                "  |  pass-through-operands: 31,32\n" +
                "  |  tuple ids: 15 \n" +
                "  |  \n" +
                "  |----32:EXCHANGE\n" +
                "  |       tuple ids: 0 \n" +
                "  |    \n" +
                "  |----33:EXCHANGE\n" +
                "  |       limit: 3\n" +
                "  |       tuple ids: 7 \n" +
                "  |    \n" +
                "  31:EXCHANGE\n" +
                "     tuple ids: 3 \n" +
                "\n" +
                "PLAN FRAGMENT 11\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 33\n" +
                "    RANDOM\n" +
                "\n" +
                "  30:MERGING-EXCHANGE\n" +
                "     limit: 3\n" +
                "     tuple ids: 7 \n" +
                "\n" +
                "PLAN FRAGMENT 12\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 30\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  9:TOP-N\n" +
                "  |  order by: <slot 28> ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 3\n" +
                "  |  tuple ids: 7 \n" +
                "  |  \n" +
                "  8:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 24>, <slot 25>, <slot 26>, <slot 27>\n" +
                "  |  tuple ids: 6 \n" +
                "  |  \n" +
                "  5:EXCEPT\n" +
                "  |  child exprs: \n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 6 \n" +
                "  |  \n" +
                "  |----29:EXCHANGE\n" +
                "  |       tuple ids: 5 \n" +
                "  |    \n" +
                "  28:EXCHANGE\n" +
                "     tuple ids: 4 \n" +
                "\n" +
                "PLAN FRAGMENT 13\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 29\n" +
                "    RANDOM\n" +
                "\n" +
                "  7:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 3\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 5 \n" +
                "\n" +
                "PLAN FRAGMENT 14\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 28\n" +
                "    RANDOM\n" +
                "\n" +
                "  6:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 2\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 4 \n" +
                "\n" +
                "PLAN FRAGMENT 15\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 32\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'a', `k4` = 1\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 0 \n" +
                "\n" +
                "PLAN FRAGMENT 16\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 31\n" +
                "    RANDOM\n" +
                "\n" +
                "  2:UNION\n" +
                "  |  child exprs: \n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 3 \n" +
                "  |  \n" +
                "  |----27:EXCHANGE\n" +
                "  |       tuple ids: 2 \n" +
                "  |    \n" +
                "  26:EXCHANGE\n" +
                "     tuple ids: 1 \n" +
                "\n" +
                "PLAN FRAGMENT 17\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 27\n" +
                "    RANDOM\n" +
                "\n" +
                "  4:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 2\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 2 \n" +
                "\n" +
                "PLAN FRAGMENT 18\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 26\n" +
                "    RANDOM\n" +
                "\n" +
                "  3:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 2\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 1 \n" +
                "\n" +
                "PLAN FRAGMENT 19\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 36\n" +
                "    RANDOM\n" +
                "\n" +
                "  15:AGGREGATE (update finalize)\n" +
                "  |  group by: <slot 40>, <slot 41>, <slot 42>, <slot 43>\n" +
                "  |  tuple ids: 10 \n" +
                "  |  \n" +
                "  12:INTERSECT\n" +
                "  |  child exprs: \n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |      `default_cluster:db1.tbl1`.`k1` | `default_cluster:db1.tbl1`.`k2` | `default_cluster:db1.tbl1`.`k3` | `default_cluster:db1.tbl1`.`k4`\n" +
                "  |  pass-through-operands: all\n" +
                "  |  tuple ids: 10 \n" +
                "  |  \n" +
                "  |----25:EXCHANGE\n" +
                "  |       tuple ids: 9 \n" +
                "  |    \n" +
                "  24:EXCHANGE\n" +
                "     tuple ids: 8 \n" +
                "\n" +
                "PLAN FRAGMENT 20\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 25\n" +
                "    RANDOM\n" +
                "\n" +
                "  14:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 4\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 9 \n" +
                "\n" +
                "PLAN FRAGMENT 21\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 24\n" +
                "    RANDOM\n" +
                "\n" +
                "  13:OlapScanNode\n" +
                "     TABLE: tbl1\n" +
                "     PREAGGREGATION: OFF. Reason: No AggregateInfo\n" +
                "     PREDICATES: `k1` = 'b', `k4` = 3\n" +
                "     partitions=0/1\n" +
                "     rollup: null\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=-1\n" +
                "     avgRowSize=0.0\n" +
                "     numNodes=0\n" +
                "     tuple ids: 8 \n";
        StmtExecutor stmtExecutor9 = new StmtExecutor(ctx, sql9);
        stmtExecutor9.execute();
        Planner planner9 = stmtExecutor9.planner();
        List<PlanFragment> fragments9 = planner9.getFragments();
        Assert.assertEquals(plan9, planner9.getExplainString(fragments9, TExplainLevel.VERBOSE));
    }

}
