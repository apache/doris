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

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.planner.AggregationNode;
import org.apache.doris.planner.BucketedAggregationNode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
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

    @Test
    public void testQueryCacheKeepsRegularAggregation() throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        boolean oldEnableSpill = sessionVariable.enableSpill;
        boolean oldEnableForceSpill = sessionVariable.enableForceSpill;
        boolean oldEnableQueryCache = sessionVariable.getEnableQueryCache();
        try {
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = 1.0;
            sessionVariable.enableBucketedHashAgg = true;
            sessionVariable.enableSpill = false;
            sessionVariable.enableForceSpill = false;
            sessionVariable.setEnableQueryCache(false);
            Assertions.assertFalse(collectBucketedAggregationNodes("sum(kint)").isEmpty());

            // The query cache point is the LOCAL AggregationNode above the scan, which the
            // fused bucketed aggregation does not have.
            sessionVariable.setEnableQueryCache(true);
            Planner planner = planAggregate("sum(kint)");
            Assertions.assertTrue(collectNodes(planner, BucketedAggregationNode.class).isEmpty());
            List<AggregationNode> aggregationNodes = collectNodes(planner, AggregationNode.class);
            Assertions.assertTrue(aggregationNodes.stream()
                    .anyMatch(node -> node.isQueryCacheCandidate()));
        } finally {
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
            sessionVariable.enableSpill = oldEnableSpill;
            sessionVariable.enableForceSpill = oldEnableForceSpill;
            sessionVariable.setEnableQueryCache(oldEnableQueryCache);
        }
    }

    @Test
    public void testMixedDistinctDedupAggregateIsNotPlannedAsBucketed() throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        int oldAggPhase = sessionVariable.aggPhase;
        int oldBeNumberForTest = sessionVariable.getBeNumberForTest();
        long oldBucketedAggMinInputRows = sessionVariable.bucketedAggMinInputRows;
        long oldBucketedAggMaxGroupKeys = sessionVariable.bucketedAggMaxGroupKeys;
        double oldBucketedAggHighCardThreshold = sessionVariable.bucketedAggHighCardThreshold;
        boolean oldEnableBucketedHashAgg = sessionVariable.enableBucketedHashAgg;
        try {
            // Let the optimizer choose between the one-phase and the multi-phase plans.
            sessionVariable.aggPhase = 0;
            sessionVariable.setBeNumberForTest(1);
            sessionVariable.bucketedAggMinInputRows = 0;
            sessionVariable.bucketedAggMaxGroupKeys = 0;
            sessionVariable.bucketedAggHighCardThreshold = 1.0;
            sessionVariable.enableBucketedHashAgg = true;

            // The dedup aggregate of a mixed DISTINCT / non-DISTINCT query is a one-phase
            // GLOBAL INPUT_TO_RESULT aggregate whose non-distinct functions are partial, so
            // the translator never fuses it. The regulator and the cost model must treat it
            // the same way: neither exempt its one-phase-with-distribute shape from the ban
            // nor discount its cost, otherwise the plan would exchange raw scan rows instead
            // of deduplicating locally before the exchange.
            Planner planner = planAggregate("stddev_pop(distinct kint), sum(kbint)");
            Assertions.assertTrue(collectNodes(planner, BucketedAggregationNode.class).isEmpty());
            for (ExchangeNode exchange : collectNodes(planner, ExchangeNode.class)) {
                Assertions.assertFalse(exchange.getChild(0) instanceof OlapScanNode,
                        "raw scan rows must not be exchanged: "
                                + planner.getExplainString(new ExplainOptions(false, false, false)));
            }
            Assertions.assertTrue(collectNodes(planner, AggregationNode.class).stream()
                    .anyMatch(node -> node.getChild(0) instanceof OlapScanNode));

            // A fusible aggregate on the same shape is still fused.
            Assertions.assertFalse(collectBucketedAggregationNodes("sum(kint)").isEmpty());
        } finally {
            sessionVariable.aggPhase = oldAggPhase;
            sessionVariable.setBeNumberForTest(oldBeNumberForTest);
            sessionVariable.bucketedAggMinInputRows = oldBucketedAggMinInputRows;
            sessionVariable.bucketedAggMaxGroupKeys = oldBucketedAggMaxGroupKeys;
            sessionVariable.bucketedAggHighCardThreshold = oldBucketedAggHighCardThreshold;
            sessionVariable.enableBucketedHashAgg = oldEnableBucketedHashAgg;
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
