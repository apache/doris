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

import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class SelectRollupTest extends TestWithFeService implements PatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");

        createTable("CREATE TABLE `t` (\n"
                + "  `k1` int(11) NULL,\n"
                + "  `k2` int(11) NULL,\n"
                + "  `k3` int(11) NULL,\n"
                + "  `v1` int(11) SUM NULL\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`k1`, `k2`, `k3`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");
        addRollup("alter table t add rollup r1(k2, v1)");
        addRollup("alter table t add rollup r2(k2, k3, v1)");

        //
        // createTable("CREATE TABLE `only_base` (\n"
        //         + "  `k1` int(11) NULL,\n"
        //         + "  `k2` int(11) NULL,\n"
        //         + "  `k3` int(11) NULL,\n"
        //         + "  `v1` int(11) SUM NULL\n"
        //         + ") ENGINE=OLAP\n"
        //         + "AGGREGATE KEY(`k1`, `k2`, k3)\n"
        //         + "COMMENT 'OLAP'\n"
        //         + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
        //         + "PROPERTIES (\n"
        //         + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
        //         + "\"in_memory\" = \"false\",\n"
        //         + "\"storage_format\" = \"V2\"\n"
        //         + ")");
        //
        // createTable("CREATE TABLE `dup_tbl` (\n"
        //         + "  `k1` int(11) NULL,\n"
        //         + "  `k2` int(11) NULL,\n"
        //         + "  `k3` int(11) NULL,\n"
        //         + "  `v1` int(11) NULL\n"
        //         + ") ENGINE=OLAP\n"
        //         + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n"
        //         + "COMMENT 'OLAP'\n"
        //         + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
        //         + "PROPERTIES (\n"
        //         + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
        //         + "\"in_memory\" = \"false\",\n"
        //         + "\"storage_format\" = \"V2\",\n"
        //         + "\"disable_auto_compaction\" = \"false\"\n"
        //         + ");");
        //
        // createTable("create table t0 (\n"
        //         + "     user_id int, \n"
        //         + "     date date, \n"
        //         + "     timestamp datetime,\n"
        //         + "     city varchar,\n"
        //         + "     age int,\n"
        //         + "     gender int,\n"
        //         + "     cost bigint sum,\n"
        //         + "     last_visit_time   datetime replace,\n"
        //         + "     max_dwell_time int max,\n"
        //         + "     min_dwell_time int min\n"
        //         + "     )\n"
        //         + "aggregate key (user_id, date, timestamp, city, age, gender)\n"
        //         + "distributed by hash(user_id) buckets 3 \n"
        //         + "properties('replication_num' = '1')");
        //
        //
        // // addRollup("ALTER TABLE t1 ADD ROLLUP r1(user_id, cost)");
        //
        // createTable("CREATE TABLE `t1` (\n"
        //         + "  `k1` int(11) NULL,\n"
        //         + "  `k2` int(11) NULL,\n"
        //         + "  `k3` int(11) NULL,\n"
        //         + "  `k4` int(11) NULL,\n"
        //         + "  `k5` int(11) NULL,\n"
        //         + "  `v1` int(11) SUM NULL,\n"
        //         + "  `v2` int(11) SUM NULL\n"
        //         + ") ENGINE=OLAP\n"
        //         + "AGGREGATE KEY(`k1`, `k2`, k3, k4, k5)\n"
        //         + "COMMENT 'OLAP'\n"
        //         + "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n"
        //         + "PROPERTIES (\n"
        //         + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
        //         + "\"in_memory\" = \"false\",\n"
        //         + "\"storage_format\" = \"V2\"\n"
        //         + ")");
        // addRollup("ALTER TABLE t1 ADD ROLLUP r1(k1, k2, k3, v1)");
        // addRollup("ALTER TABLE t1 ADD ROLLUP r2(k1, k2, v1)");
        // addRollup("ALTER TABLE t1 ADD ROLLUP r3(k1, v1)");
        //
        // createTable("CREATE TABLE `t2` (\n"
        //         + "  `k1` int(11) NULL,\n"
        //         + "  `k2` int(11) NULL,\n"
        //         + "  `k3` int(11) NULL,\n"
        //         + "  `k4` int(11) NULL,\n"
        //         + "  `k5` int(11) NULL,\n"
        //         + "  `v1` int(11) SUM NULL,\n"
        //         + "  `v2` int(11) SUM NULL\n"
        //         + ") ENGINE=OLAP\n"
        //         + "AGGREGATE KEY(`k1`, `k2`, k3, k4, k5)\n"
        //         + "COMMENT 'OLAP'\n"
        //         + "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n"
        //         + "PROPERTIES (\n"
        //         + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
        //         + "\"in_memory\" = \"false\",\n"
        //         + "\"storage_format\" = \"V2\"\n"
        //         + ")");
        // addRollup("ALTER TABLE t2 ADD ROLLUP r1(k1, k2, k3, v1)");
        // addRollup("ALTER TABLE t2 ADD ROLLUP r2(k1, k2, v1)");
        // addRollup("ALTER TABLE t2 ADD ROLLUP r3(k1, v1)");
    }

    @Test
    public void testAggMatching() {
        PlanChecker.from(connectContext)
                .analyze(" select k2, sum(v1) from t group by k2")
                .applyTopDown(new SelectRollup())
                .matches(logicalOlapScan().when(scan -> "r1".equals(scan.getSelectRollupName().get())));
    }

    @Test
    public void testMatchingBase() {
        PlanChecker.from(connectContext)
                .analyze(" select k1, sum(v1) from t group by k1")
                .applyTopDown(new SelectRollup())
                .matches(logicalOlapScan().when(scan -> "t".equals(scan.getSelectRollupName().get())));
    }

    @Test
    void testAggFilterScan() {
        PlanChecker.from(connectContext)
                .analyze("select k2, sum(v1) from t where k3=0 group by k2")
                .applyTopDown(new SelectRollup())
                .matches(logicalOlapScan().when(scan -> "r2".equals(scan.getSelectRollupName().get())));
    }

    @Test
    void testTranslate() {
        PlanChecker.from(connectContext)
                .checkPlannerResult(
                        " select k2, sum(v1) from t group by k2",
                        planner -> {
                        }
                );
    }

    @Disabled
    @Test
    public void testDup() throws Exception {
        // System.out.println(getSQLPlanOrErrorMsg("select k2, sum(v1) from dup_tbl group by k2"));
        PlanChecker.from(connectContext)
                .checkPlannerResult(
                        "select k2, sum(v1) from dup_tbl group by k2",
                        planner -> {
                        })
        ;
    }

    @Disabled
    @Test
    void testTranslate1() {
        PlanChecker.from(connectContext)
                .checkPlannerResult(
                        "select k1, v1 from only_base group by k1",
                        planner -> {
                        }
                );
    }

    ///////////////////////////////////////////////////////////////////////////
    // For debugging legacy rollup selecting.
    // Add these cases for nereids rollup select in the future.
    ///////////////////////////////////////////////////////////////////////////
    @Disabled
    @Test
    public void test() throws Exception {
        System.out.println(getSQLPlanOrErrorMsg("select k1, sum(v1) from only_base group by k1"));
    }

    @Disabled
    @Test
    public void testLegacyRollupSelect() throws Exception {
        String explain = getSQLPlanOrErrorMsg("SELECT user_id, sum(cost) FROM t0 GROUP BY user_id");
        System.out.println(explain);
    }

    @Disabled
    @Test
    public void testDuplicateKey() throws Exception {
        System.out.println(getSQLPlanOrErrorMsg("select k1, sum(v1) from detail group by k1"));
    }

    // todo: add test cases
    //  1. equal case: k2=0
    //  2. not equal case: k2 !=0
    @Disabled
    @Test
    public void test1() throws Exception {
        // predicates pushdown scan: k2>0
        // rollup: r2
        String sql1 = "select k1, sum(v1) from t2 where k2>0 group by k1";
        System.out.println(getSQLPlanOrErrorMsg(sql1));
    }

    @Disabled
    @Test
    public void test2() throws Exception {
        // pushdown: k2>k3
        // rollup: r1
        String sql2 = "select k1, sum(v1) from t2 where k2>k3 group by k1";
        System.out.println(getSQLPlanOrErrorMsg(sql2));
    }

    /**
     * <pre>
     * :VOlapScanNode
     *      TABLE: t2(t2), PREAGGREGATION: OFF. Reason: conjunct on `c1` which is StorageEngine value column
     *      PREDICATES: `k2` > 0
     *      rollup: t2
     *      partitions=1/1, tablets=3/3, tabletList=10017,10019,10021
     *      cardinality=0, avgRowSize=12.0, numNodes=1
     * </pre>
     */
    @Disabled
    @Test
    public void testFilterPushDownProject() throws Exception {
        // failed to select rollup
        String sql = "select c1, sum(v1) from"
                + "(select k1 as c1, k2 as c2, k3 as c3, k4 as c4, v1 as v1 from t2) t"
                + " where c2>0 group by c1";
        System.out.println(getSQLPlanOrErrorMsg(sql));
    }

    /**
     * <pre>
     *   0:VOlapScanNode
     *      TABLE: t2(t2), PREAGGREGATION: OFF. Reason: aggExpr.getChild(0)[(SlotRef{slotDesc=
     *      SlotDescriptor{id=1, parent=0, col=v1, type=INT, materialized=true, byteSize=0,
     *      byteOffset=-1, nullIndicatorByte=0, nullIndicatorBit=0, slotIdx=0}, col=v1, label=`v1`, tblName=null}
     *      SlotRef{slotDesc=SlotDescriptor{id=2, parent=0, col=v2, type=INT, materialized=true,
     *      byteSize=0, byteOffset=-1, nullIndicatorByte=0, nullIndicatorBit=0, slotIdx=0}, col=v2,
     *      label=`v2`, tblName=null})] is not SlotRef or CastExpr|CaseExpr
     *      rollup: t2
     *      partitions=1/1, tablets=3/3, tabletList=10017,10019,10021
     *      cardinality=0, avgRowSize=12.0, numNodes=1
     * </pre>
     */
    @Disabled
    @Test
    public void testMultiAggColumns() throws Exception {
        System.out.println(getSQLPlanOrErrorMsg("select k1, sum(v1 + v2) from t2 group by k1"));
    }


    ///////////////////////////////////////////////////////////////////////////
    // Test turn off pre-agg in legacy logic.
    // Add these cases for nereids rollup select in the future.
    ///////////////////////////////////////////////////////////////////////////

    /**
     * <pre>
     *   0:VOlapScanNode
     *      TABLE: t1(t1), PREAGGREGATION: OFF. Reason: No AggregateInfo
     * </pre>
     */
    @Disabled
    @Test
    public void testNoAgg() throws Exception {
        System.out.println(getSQLPlanOrErrorMsg("select k1, v1 from t1"));
    }

    @Disabled
    @Test
    public void testJoin() throws Exception {
        System.out.println(
                getSQLPlanOrErrorMsg("select t1.k1, t2.k2, sum(t1.v1), sum(t2.v1) from "
                        + "t1 join t2 on t1.k1=t2.k1 group by t1.k1, t2.k2;"));
    }

    @Disabled
    @Test
    public void testOuterJoin() throws Exception {
        System.out.println(getSQLPlanOrErrorMsg(
                "select t1.k1, t2.k2, sum(t1.v1), sum(t2.v1) from t1 outer join t2 on t1.k1=t2.k1"));
    }
}
