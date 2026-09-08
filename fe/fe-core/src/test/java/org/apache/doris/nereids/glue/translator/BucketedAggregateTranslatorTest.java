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

    private void assertUsesRegularAggregation(String aggregateFunction) throws Exception {
        Planner planner = getSQLPlanner("SELECT " + aggregateFunction
                + " FROM bucketed_aggregate_translator_test.agg_group_concat_table GROUP BY kbint");
        List<BucketedAggregationNode> bucketedAggregationNodes = Lists.newArrayList();
        List<AggregationNode> aggregationNodes = Lists.newArrayList();
        for (PlanFragment fragment : planner.getFragments()) {
            PlanNode root = fragment.getPlanRoot();
            if (root != null) {
                root.collect(BucketedAggregationNode.class, bucketedAggregationNodes);
                root.collect(AggregationNode.class, aggregationNodes);
            }
        }
        Assertions.assertTrue(bucketedAggregationNodes.isEmpty());
        Assertions.assertFalse(aggregationNodes.isEmpty());
    }
}
