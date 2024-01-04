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

import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.rules.analysis.LogicalSubQueryAliasToLogicalProject;
import org.apache.doris.nereids.rules.rewrite.MergeProjects;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughProject;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class SelectRollupIndexTest extends BaseMaterializedIndexSelectTest implements MemoPatternMatchSupported {

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");

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
        // waiting table state to normal
        Thread.sleep(500);
        addRollup("alter table t add rollup r2(k2, k3, v1)");
        addRollup("alter table t add rollup r3(k2)");
        addRollup("alter table t add rollup r4(k2, k3)");

        createTable("CREATE TABLE `t1` (\n"
                + "  `k1` int(11) NULL,\n"
                + "  `k2` int(11) NULL,\n"
                + "  `v1` int(11) SUM NULL\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`k1`, `k2`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");
        addRollup("alter table t1 add rollup r1(k1)");
        addRollup("alter table t1 add rollup r2(k2, v1)");
        addRollup("alter table t1 add rollup r3(k2, k1)");

        createTable("CREATE TABLE `duplicate_tbl` (\n"
                + "  `k1` int(11) NULL,\n"
                + "  `k2` int(11) NULL,\n"
                + "  `k3` int(11) NULL,\n"
                + "  `v1` int(11) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `k2`, `k3`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");
    }

    //@Disabled //ISSUE #18263
    @Test
    public void testAggMatching() {
        singleTableTest("select k2, sum(v1) from t group by k2", "r1", true);
    }

    @Test
    public void testMatchingBase() {
        PlanChecker.from(connectContext)
                .analyze(" select k1, sum(v1) from t group by k1")
                .applyTopDown(new SelectMaterializedIndexWithAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    Assertions.assertTrue(scan.getPreAggStatus().isOn());
                    Assertions.assertEquals("t", scan.getSelectedMaterializedIndexName().get());
                    return true;
                }));
    }

    @Test
    void testAggFilterScan() {
        PlanChecker.from(connectContext)
                .analyze("select k2, sum(v1) from t where k3=0 group by k2")
                .applyTopDown(new SelectMaterializedIndexWithAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    Assertions.assertTrue(scan.getPreAggStatus().isOn());
                    Assertions.assertEquals("r2", scan.getSelectedMaterializedIndexName().get());
                    return true;
                }));
    }

    @Test
    void testTranslate() {
        PlanChecker.from(connectContext).checkPlannerResult("select k2, sum(v1) from t group by k2");
    }

    //@Disabled //ISSUE #18263
    @Test
    public void testTranslateWhenPreAggIsOff() {
        singleTableTest("select k2, min(v1) from t group by k2", scan -> {
            Assertions.assertFalse(scan.isPreAggregation());
            Assertions.assertEquals("Aggregate operator don't match, "
                            + "aggregate function: min(v1), column aggregate type: SUM",
                    scan.getReasonOfPreAggregation());
        });
    }

    @Test
    public void testWithEqualFilter() {
        PlanChecker.from(connectContext)
                .analyze("select k2, sum(v1) from t where k3=0 group by k2")
                .applyTopDown(new SelectMaterializedIndexWithAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    Assertions.assertTrue(scan.getPreAggStatus().isOn());
                    Assertions.assertEquals("r2", scan.getSelectedMaterializedIndexName().get());
                    return true;
                }));
    }

    @Test
    public void testWithNonEqualFilter() {
        PlanChecker.from(connectContext)
                .analyze("select k2, sum(v1) from t where k3>0 group by k2")
                .applyTopDown(new SelectMaterializedIndexWithAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    Assertions.assertTrue(scan.getPreAggStatus().isOn());
                    Assertions.assertEquals("r2", scan.getSelectedMaterializedIndexName().get());
                    return true;
                }));
    }

    @Test
    public void testWithFilter() {
        PlanChecker.from(connectContext)
                .analyze("select k2, sum(v1) from t where k2>3 group by k2")
                .applyTopDown(new SelectMaterializedIndexWithAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    Assertions.assertTrue(scan.getPreAggStatus().isOn());
                    Assertions.assertEquals("r1", scan.getSelectedMaterializedIndexName().get());
                    return true;
                }));
    }

    @Test
    public void testWithFilterAndProject() {
        String sql = "select c2, sum(v1) from"
                + "(select k2 as c2, k3 as c3, v1 as v1 from t) t"
                + " where c3>0 group by c2";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new LogicalSubQueryAliasToLogicalProject())
                .applyTopDown(new PushDownFilterThroughProject())
                .applyBottomUp(new MergeProjects())
                .applyTopDown(new SelectMaterializedIndexWithAggregate())
                .applyTopDown(new SelectMaterializedIndexWithoutAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    Assertions.assertTrue(scan.getPreAggStatus().isOn());
                    Assertions.assertEquals("r2", scan.getSelectedMaterializedIndexName().get());
                    return true;
                }));
    }

    ///////////////////////////////////////////////////////////////////////////
    // Check pre-aggregate status.
    ///////////////////////////////////////////////////////////////////////////

    @Test
    public void testNoAggregate() {
        PlanChecker.from(connectContext)
                .analyze("select k1, v1 from t")
                .applyTopDown(new SelectMaterializedIndexWithAggregate())
                .applyTopDown(new SelectMaterializedIndexWithoutAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    PreAggStatus preAgg = scan.getPreAggStatus();
                    Assertions.assertTrue(preAgg.isOff());
                    Assertions.assertEquals("No aggregate on scan.", preAgg.getOffReason());
                    return true;
                }));
    }

    @Test
    public void testAggregateTypeNotMatch() {
        PlanChecker.from(connectContext)
                .analyze("select k1, min(v1) from t group by k1")
                .applyTopDown(new SelectMaterializedIndexWithAggregate())
                .applyTopDown(new SelectMaterializedIndexWithoutAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    PreAggStatus preAgg = scan.getPreAggStatus();
                    Assertions.assertTrue(preAgg.isOff());
                    Assertions.assertEquals("Aggregate operator don't match, "
                            + "aggregate function: min(v1), column aggregate type: SUM", preAgg.getOffReason());
                    return true;
                }));
    }

    @Test
    public void testInvalidSlotInAggFunction() {
        PlanChecker.from(connectContext)
                .analyze("select k1, sum(v1 + 1) from t group by k1")
                .applyTopDown(new SelectMaterializedIndexWithAggregate())
                .applyTopDown(new SelectMaterializedIndexWithoutAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    PreAggStatus preAgg = scan.getPreAggStatus();
                    Assertions.assertTrue(preAgg.isOff());
                    Assertions.assertEquals("Slot((v1 + 1)) in sum((v1 + 1)) is neither key column nor value column.",
                            preAgg.getOffReason());
                    return true;
                }));
    }

    @Test
    public void testKeyColumnInAggFunction() {
        PlanChecker.from(connectContext)
                .analyze("select k1, sum(k2) from t group by k1")
                .applyTopDown(new SelectMaterializedIndexWithAggregate())
                .applyTopDown(new SelectMaterializedIndexWithoutAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    PreAggStatus preAgg = scan.getPreAggStatus();
                    Assertions.assertTrue(preAgg.isOff());
                    Assertions.assertEquals("Aggregate function sum(k2) contains key column k2.",
                            preAgg.getOffReason());
                    return true;
                }));
    }

    @Disabled("reopen it if we fix rollup select bugs")
    @Test
    public void testMaxCanUseKeyColumn() {
        PlanChecker.from(connectContext)
                .analyze("select k2, max(k3) from t group by k3")
                .applyTopDown(new SelectMaterializedIndexWithAggregate())
                .applyTopDown(new SelectMaterializedIndexWithoutAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    PreAggStatus preAgg = scan.getPreAggStatus();
                    Assertions.assertTrue(preAgg.isOn());
                    Assertions.assertEquals("r4", scan.getSelectedMaterializedIndexName().get());
                    return true;
                }));
    }

    @Disabled("reopen it if we fix rollup select bugs")
    @Test
    public void testMinCanUseKeyColumn() {
        PlanChecker.from(connectContext)
                .analyze("select k2, min(k3) from t group by k3")
                .applyTopDown(new SelectMaterializedIndexWithAggregate())
                .applyTopDown(new SelectMaterializedIndexWithoutAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    PreAggStatus preAgg = scan.getPreAggStatus();
                    Assertions.assertTrue(preAgg.isOn());
                    Assertions.assertEquals("r4", scan.getSelectedMaterializedIndexName().get());
                    return true;
                }));
    }

    @Test
    public void testDuplicatePreAggOn() {
        PlanChecker.from(connectContext)
                .analyze("select k1, sum(k1) from duplicate_tbl group by k1")
                .applyTopDown(new SelectMaterializedIndexWithAggregate())
                .applyTopDown(new SelectMaterializedIndexWithoutAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    PreAggStatus preAgg = scan.getPreAggStatus();
                    Assertions.assertTrue(preAgg.isOn());
                    return true;
                }));
    }

    @Test
    public void testDuplicatePreAggOnEvenWithoutAggregate() {
        PlanChecker.from(connectContext)
                .analyze("select k1, v1 from duplicate_tbl")
                .applyTopDown(new SelectMaterializedIndexWithAggregate())
                .applyTopDown(new SelectMaterializedIndexWithoutAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    PreAggStatus preAgg = scan.getPreAggStatus();
                    Assertions.assertTrue(preAgg.isOn());
                    return true;
                }));
    }

    //@Disabled //ISSUE #18263
    @Test
    public void testKeysOnlyQuery() throws Exception {
        singleTableTest("select k1 from t1", "r3", false);
        singleTableTest("select k2 from t1", "r3", false);
        singleTableTest("select k1, k2 from t1", "r3", false);
        singleTableTest("select k1 from t1 group by k1", "r1", true);
        singleTableTest("select k2 from t1 group by k2", "r2", true);
        singleTableTest("select k1, k2 from t1 group by k1, k2", "r3", true);
    }

    /**
     * Rollup with all the keys should be used.
     */
    //@Disabled //ISSUE #18263
    @Test
    public void testRollupWithAllTheKeys() throws Exception {
        createTable(" CREATE TABLE `t4` (\n"
                + "  `k1` int(11) NULL,\n"
                + "  `k2` int(11) NULL,\n"
                + "  `v1` int(11) SUM NULL,\n"
                + "  `v2` int(11) SUM NULL\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`k1`, `k2`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");
        addRollup("alter table t4 add rollup r1(k2, k1, v1)");

        singleTableTest("select k1, k2, v1 from t4", "r1", false);
        singleTableTest("select k1, k2, sum(v1) from t4 group by k1, k2", "r1", true);
        singleTableTest("select k1, v1 from t4", "r1", false);
        singleTableTest("select k1, sum(v1) from t4 group by k1", "r1", true);
    }

    //@Disabled //ISSUE #18263
    @Test
    public void testComplexGroupingExpr() throws Exception {
        singleTableTest("select k2 + 1, sum(v1) from t group by k2 + 1", "r1", true);
    }

    //@Disabled //ISSUE #18263
    @Test
    public void testCountDistinctKeyColumn() {
        singleTableTest("select k2, count(distinct k3) from t group by k2", "r4", true);
    }

    //@Disabled //ISSUE #18263
    @Test
    public void testCountDistinctValueColumn() {
        singleTableTest("select k1, count(distinct v1) from t group by k1", scan -> {
            Assertions.assertFalse(scan.isPreAggregation());
            Assertions.assertEquals("Count distinct is only valid for key columns, but meet count(DISTINCT v1).",
                    scan.getReasonOfPreAggregation());
            Assertions.assertEquals("t", scan.getSelectedIndexName());
        });
    }

    //@Disabled //ISSUE #18263
    @Test
    public void testOnlyValueColumn1() throws Exception {
        singleTableTest("select sum(v1) from t", "r1", true);
    }

    @Test
    public void testOnlyValueColumn2() throws Exception {
        singleTableTest("select v1 from t", "t", false);
    }

    @Disabled
    @Test
    public void testPreAggHint() throws Exception {
        createTable(" CREATE TABLE `test_preagg_hint` (\n"
                + "  `k1` int(11) NULL,\n"
                + "  `k2` int(11) NULL,\n"
                + "  `v1` int(11) SUM NULL,\n"
                + "  `v2` int(11) SUM NULL\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`k1`, `k2`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        addRollup("alter table test_preagg_hint add rollup r1(k1, k2, v1)");

        // no pre-agg hint
        String queryWithoutHint = "select k1, v1 from test_preagg_hint";
        // legacy planner
        Assertions.assertTrue(getSQLPlanOrErrorMsg(queryWithoutHint).contains(
                "TABLE: test.test_preagg_hint(r1), PREAGGREGATION: OFF. Reason: No AggregateInfo"));
        // nereids planner
        PlanChecker.from(connectContext)
                .analyze(queryWithoutHint)
                .rewrite()
                .matches(logicalOlapScan().when(scan -> {
                    Assertions.assertTrue(scan.getHints().isEmpty());
                    Assertions.assertEquals("r1", scan.getSelectedMaterializedIndexName().get());
                    PreAggStatus preAggStatus = scan.getPreAggStatus();
                    Assertions.assertTrue(preAggStatus.isOff());
                    Assertions.assertEquals("No aggregate on scan.", preAggStatus.getOffReason());
                    return true;
                }));

        // has pre-agg hint
        String queryWithHint = "select k1, v1 from test_preagg_hint /*+ PREAGGOPEN*/";
        // legacy planner
        Assertions.assertTrue(getSQLPlanOrErrorMsg(queryWithHint).contains(
                "TABLE: test.test_preagg_hint(r1), PREAGGREGATION: ON"));
        // nereids planner
        PlanChecker.from(connectContext)
                .analyze(queryWithHint)
                .rewrite()
                .matches(logicalOlapScan().when(scan -> {
                    Assertions.assertEquals(1, scan.getHints().size());
                    Assertions.assertEquals("PREAGGOPEN", scan.getHints().get(0));
                    Assertions.assertEquals("r1", scan.getSelectedMaterializedIndexName().get());
                    PreAggStatus preAggStatus = scan.getPreAggStatus();
                    Assertions.assertTrue(preAggStatus.isOn());
                    return true;
                }));

        dropTable("test_preagg_hint", true);
    }
}
