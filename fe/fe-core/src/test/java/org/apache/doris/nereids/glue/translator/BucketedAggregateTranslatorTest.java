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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.BucketedAggregationNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class BucketedAggregateTranslatorTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createDatabase("bucketed_aggregate_translator_test");
        createTable("CREATE TABLE bucketed_aggregate_translator_test.agg_group_concat_table ("
                + "kint INT NOT NULL, kbint INT NOT NULL, kstr STRING NOT NULL) "
                + "DISTRIBUTED BY HASH(kint) BUCKETS 4 "
                + "PROPERTIES('replication_num' = '1')");
        createFunction("CREATE AGGREGATE FUNCTION bucketed_aggregate_translator_test.py_udaf_sum(INT) "
                + "RETURNS BIGINT PROPERTIES('type'='PYTHON_UDF', 'symbol'='SumUdaf', "
                + "'runtime_version'='3.10.2')");
    }

    @Test
    public void testPythonUdafIsNotFusedIntoBucketedAggregation() throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldAggPhase = sessionVariable.aggPhase;
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        try {
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = 1.0;
            sessionVariable.enableBucketedHashAgg = true;

            // agg_phase=0 lets the optimizer choose the plan; agg_phase=1 forces the
            // one-phase plan, so only the translator fusion gate can reject the UDAF.
            for (int aggPhase : new int[] {0, 1}) {
                sessionVariable.aggPhase = aggPhase;
                // A builtin aggregate on the same shape is fused, so the UDAF cases below
                // are rejected because of the UDAF rather than the plan shape.
                Assertions.assertFalse(collectBucketedAggregationNodes("sum(kint)").isEmpty());
                assertUsesRegularAggregation(
                        "bucketed_aggregate_translator_test.py_udaf_sum(kint)");
                assertUsesRegularAggregation(
                        "sum(kint), bucketed_aggregate_translator_test.py_udaf_sum(kint)");
            }
        } finally {
            sessionVariable.aggPhase = oldAggPhase;
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
        }
    }

    @Test
    public void testAggregateOrderByIsNotFusedIntoBucketedAggregation() throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldAggPhase = sessionVariable.aggPhase;
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        boolean oldUseOnePhaseAggForGroupConcatWithOrder =
                sessionVariable.useOnePhaseAggForGroupConcatWithOrder;
        try {
            sessionVariable.aggPhase = 1;
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = 1.0;
            sessionVariable.enableBucketedHashAgg = true;
            sessionVariable.useOnePhaseAggForGroupConcatWithOrder = false;

            assertUsesRegularAggregation("group_concat(kstr ORDER BY kint)");
            assertUsesRegularAggregation("multi_distinct_group_concat(kstr ORDER BY kint)");
        } finally {
            sessionVariable.aggPhase = oldAggPhase;
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
            sessionVariable.useOnePhaseAggForGroupConcatWithOrder =
                    oldUseOnePhaseAggForGroupConcatWithOrder;
        }
    }

    @Test
    public void testSpillKeepsRegularAggregation() throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldAggPhase = sessionVariable.aggPhase;
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        boolean oldEnableSpill = sessionVariable.enableSpill;
        boolean oldEnableForceSpill = sessionVariable.enableForceSpill;
        try {
            sessionVariable.aggPhase = 1;
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = 1.0;
            sessionVariable.enableBucketedHashAgg = true;
            sessionVariable.enableSpill = false;
            sessionVariable.enableForceSpill = false;
            Assertions.assertFalse(collectBucketedAggregationNodes("sum(kint)").isEmpty());

            // Bucketed agg cannot spill, so the spillable regular aggregation must be kept.
            sessionVariable.enableSpill = true;
            assertUsesRegularAggregation("sum(kint)");
            sessionVariable.enableSpill = false;
            sessionVariable.enableForceSpill = true;
            assertUsesRegularAggregation("sum(kint)");
        } finally {
            sessionVariable.aggPhase = oldAggPhase;
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
            sessionVariable.enableSpill = oldEnableSpill;
            sessionVariable.enableForceSpill = oldEnableForceSpill;
        }
    }

    private void assertUsesRegularAggregation(String aggregateFunction) throws Exception {
        Planner planner = planAggregate(aggregateFunction);
        Assertions.assertTrue(collectNodes(planner, BucketedAggregationNode.class).isEmpty());
        Assertions.assertFalse(collectNodes(planner, AggregationNode.class).isEmpty());
    }

    private List<BucketedAggregationNode> collectBucketedAggregationNodes(String aggregateFunction)
            throws Exception {
        return collectNodes(planAggregate(aggregateFunction), BucketedAggregationNode.class);
    }

    private Planner planAggregate(String aggregateFunction) throws Exception {
        return getSQLPlanner("SELECT " + aggregateFunction
                + " FROM bucketed_aggregate_translator_test.agg_group_concat_table GROUP BY kbint");
    }

    private <T extends PlanNode> List<T> collectNodes(Planner planner, Class<T> nodeClass) {
        List<T> nodes = Lists.newArrayList();
        for (PlanFragment fragment : planner.getFragments()) {
            PlanNode root = fragment.getPlanRoot();
            if (root != null) {
                root.collect(nodeClass, nodes);
            }
        }
        return nodes;
    }
}
