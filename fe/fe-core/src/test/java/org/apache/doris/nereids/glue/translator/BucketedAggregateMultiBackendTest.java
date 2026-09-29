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

public class BucketedAggregateMultiBackendTest extends TestWithFeService {

    @Override
    protected int backendNum() {
        return 2;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createDatabase("bucketed_aggregate_multi_be_test");
        createTable("CREATE TABLE bucketed_aggregate_multi_be_test.t ("
                + "kint INT NOT NULL, kbint INT NOT NULL) "
                + "DISTRIBUTED BY HASH(kint) BUCKETS 4 "
                + "PROPERTIES('replication_num' = '1')");
    }

    @Test
    public void testBeNumberForTestCannotEnableBucketedAggOnMultipleBackends() throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        sessionVariable.aggPhase = 1;
        sessionVariable.bucketedAggMinInputRows = 0;
        sessionVariable.bucketedAggMaxGroupKeys = 0;
        sessionVariable.bucketedAggHighCardThreshold = 1.0;
        sessionVariable.enableBucketedHashAgg = true;
        sessionVariable.enableSpill = false;
        sessionVariable.enableForceSpill = false;
        // Scan ranges still go to both real backends, so the test override must not
        // enable the single-BE-only bucketed aggregation.
        sessionVariable.setBeNumberForTest(1);

        Planner planner = getSQLPlanner(
                "SELECT kbint, sum(kint) FROM bucketed_aggregate_multi_be_test.t GROUP BY kbint");
        List<BucketedAggregationNode> nodes = Lists.newArrayList();
        for (PlanFragment fragment : planner.getFragments()) {
            PlanNode root = fragment.getPlanRoot();
            if (root != null) {
                root.collect(BucketedAggregationNode.class, nodes);
            }
        }
        Assertions.assertTrue(nodes.isEmpty());
    }
}
