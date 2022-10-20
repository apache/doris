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
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

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
        // waiting table state to normal
        Thread.sleep(500);
        addRollup("alter table t add rollup r2(k2, k3, v1)");

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

    @Test
    public void testAggMatching() {
        PlanChecker.from(connectContext)
                .analyze(" select k2, sum(v1) from t group by k2")
                .applyTopDown(new SelectRollupWithAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    Assertions.assertTrue(scan.getPreAggStatus().isOn());
                    Assertions.assertEquals("r1", scan.getSelectRollupName().get());
                    return true;
                }));
    }

    @Test
    public void testMatchingBase() {
        PlanChecker.from(connectContext)
                .analyze(" select k1, sum(v1) from t group by k1")
                .applyTopDown(new SelectRollupWithAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    Assertions.assertTrue(scan.getPreAggStatus().isOn());
                    Assertions.assertEquals("t", scan.getSelectRollupName().get());
                    return true;
                }));
    }

    @Test
    void testAggFilterScan() {
        PlanChecker.from(connectContext)
                .analyze("select k2, sum(v1) from t where k3=0 group by k2")
                .applyTopDown(new SelectRollupWithAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    Assertions.assertTrue(scan.getPreAggStatus().isOn());
                    Assertions.assertEquals("r2", scan.getSelectRollupName().get());
                    return true;
                }));
    }

    @Test
    void testTranslate() {
        PlanChecker.from(connectContext).checkPlannerResult("select k2, sum(v1) from t group by k2");
    }

    @Test
    public void testTranslateWhenPreAggIsOff() {
        PlanChecker.from(connectContext).checkPlannerResult(
                "select k2, min(v1) from t group by k2",
                planner -> {
                    List<ScanNode> scans = planner.getScanNodes();
                    Assertions.assertEquals(1, scans.size());
                    ScanNode scanNode = scans.get(0);
                    Assertions.assertTrue(scanNode instanceof OlapScanNode);
                    OlapScanNode olapScan = (OlapScanNode) scanNode;
                    Assertions.assertFalse(olapScan.isPreAggregation());
                    Assertions.assertEquals("Aggregate operator don't match, "
                                    + "aggregate function: min(v1), column aggregate type: SUM",
                            olapScan.getReasonOfPreAggregation());
                });
    }

    @Test
    public void testWithEqualFilter() {
        PlanChecker.from(connectContext)
                .analyze("select k2, sum(v1) from t where k3=0 group by k2")
                .applyTopDown(new SelectRollupWithAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    Assertions.assertTrue(scan.getPreAggStatus().isOn());
                    Assertions.assertEquals("r2", scan.getSelectRollupName().get());
                    return true;
                }));
    }

    @Test
    public void testWithNonEqualFilter() {
        PlanChecker.from(connectContext)
                .analyze("select k2, sum(v1) from t where k3>0 group by k2")
                .applyTopDown(new SelectRollupWithAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    Assertions.assertTrue(scan.getPreAggStatus().isOn());
                    Assertions.assertEquals("r2", scan.getSelectRollupName().get());
                    return true;
                }));
    }

    @Test
    public void testWithFilter() {
        PlanChecker.from(connectContext)
                .analyze("select k2, sum(v1) from t where k2>3 group by k3")
                .applyTopDown(new SelectRollupWithAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    Assertions.assertTrue(scan.getPreAggStatus().isOn());
                    Assertions.assertEquals("r2", scan.getSelectRollupName().get());
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
                .applyTopDown(new SelectRollupWithAggregate())
                .applyTopDown(new SelectRollupWithoutAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    Assertions.assertTrue(scan.getPreAggStatus().isOn());
                    Assertions.assertEquals("r2", scan.getSelectRollupName().get());
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
                .applyTopDown(new SelectRollupWithAggregate())
                .applyTopDown(new SelectRollupWithoutAggregate())
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
                .applyTopDown(new SelectRollupWithAggregate())
                .applyTopDown(new SelectRollupWithoutAggregate())
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
                .applyTopDown(new SelectRollupWithAggregate())
                .applyTopDown(new SelectRollupWithoutAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    PreAggStatus preAgg = scan.getPreAggStatus();
                    Assertions.assertTrue(preAgg.isOff());
                    Assertions.assertEquals("Input of aggregate function sum((v1 + 1)) should be slot or cast on slot.",
                            preAgg.getOffReason());
                    return true;
                }));
    }

    @Test
    public void testKeyColumnInAggFunction() {
        PlanChecker.from(connectContext)
                .analyze("select k1, sum(k2) from t group by k1")
                .applyTopDown(new SelectRollupWithAggregate())
                .applyTopDown(new SelectRollupWithoutAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    PreAggStatus preAgg = scan.getPreAggStatus();
                    Assertions.assertTrue(preAgg.isOff());
                    Assertions.assertEquals("Aggregate function sum(k2) contains key column k2.",
                            preAgg.getOffReason());
                    return true;
                }));
    }

    @Test
    public void testMaxCanUseKeyColumn() {
        PlanChecker.from(connectContext)
                .analyze("select k2, max(k3) from t group by k3")
                .applyTopDown(new SelectRollupWithAggregate())
                .applyTopDown(new SelectRollupWithoutAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    PreAggStatus preAgg = scan.getPreAggStatus();
                    Assertions.assertTrue(preAgg.isOn());
                    Assertions.assertEquals("r2", scan.getSelectRollupName().get());
                    return true;
                }));
    }

    @Test
    public void testMinCanUseKeyColumn() {
        PlanChecker.from(connectContext)
                .analyze("select k2, min(k3) from t group by k3")
                .applyTopDown(new SelectRollupWithAggregate())
                .applyTopDown(new SelectRollupWithoutAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    PreAggStatus preAgg = scan.getPreAggStatus();
                    Assertions.assertTrue(preAgg.isOn());
                    Assertions.assertEquals("r2", scan.getSelectRollupName().get());
                    return true;
                }));
    }

    @Test
    public void testDuplicatePreAggOn() {
        PlanChecker.from(connectContext)
                .analyze("select k1, sum(k1) from duplicate_tbl group by k1")
                .applyTopDown(new SelectRollupWithAggregate())
                .applyTopDown(new SelectRollupWithoutAggregate())
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
                .applyTopDown(new SelectRollupWithAggregate())
                .applyTopDown(new SelectRollupWithoutAggregate())
                .matches(logicalOlapScan().when(scan -> {
                    PreAggStatus preAgg = scan.getPreAggStatus();
                    Assertions.assertTrue(preAgg.isOn());
                    return true;
                }));
    }

}
