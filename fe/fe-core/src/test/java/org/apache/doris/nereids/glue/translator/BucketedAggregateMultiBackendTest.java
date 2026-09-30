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

import org.apache.doris.catalog.Env;
import org.apache.doris.planner.BucketedAggregationNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
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
        // Every tablet has a replica on both backends, so the scan could be placed on either.
        createTable("CREATE TABLE bucketed_aggregate_multi_be_test.t_two_replicas ("
                + "kint INT NOT NULL, kbint INT NOT NULL) "
                + "DISTRIBUTED BY HASH(kint) BUCKETS 4 "
                + "PROPERTIES('replication_num' = '2')");
    }

    private void enableBucketedAggregation(SessionVariable sessionVariable) {
        sessionVariable.aggPhase = 1;
        sessionVariable.bucketedAggMinInputRows = 0;
        sessionVariable.bucketedAggMaxGroupKeys = 0;
        sessionVariable.bucketedAggHighCardThreshold = 1.0;
        sessionVariable.enableBucketedHashAgg = true;
        sessionVariable.enableSpill = false;
        sessionVariable.enableForceSpill = false;
    }

    @Test
    public void testBeNumberForTestCannotEnableBucketedAggOnMultipleBackends() throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        enableBucketedAggregation(sessionVariable);
        // Scan ranges still go to both real backends, so the test override must not
        // enable the single-BE-only bucketed aggregation.
        sessionVariable.setBeNumberForTest(1);

        Planner planner = getSQLPlanner(
                "SELECT kbint, sum(kint) FROM bucketed_aggregate_multi_be_test.t GROUP BY kbint");
        Assertions.assertTrue(collectBucketedAggregationNodes(planner).isEmpty());
    }

    @Test
    public void testFusedScanIsPinnedToTheBackendSeenBySingleBackendGate() throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        enableBucketedAggregation(sessionVariable);
        sessionVariable.setBeNumberForTest(-1);
        String sql = "SELECT kbint, sum(kint) FROM bucketed_aggregate_multi_be_test.t_two_replicas"
                + " GROUP BY kbint";

        List<Long> backendIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
        Assertions.assertEquals(2, backendIds.size());
        long pinnedBackendId = backendIds.get(0);

        // Control: with two alive backends there is no fusion, and every tablet offers both
        // replicas to the scan worker selection.
        Planner regularPlanner = getSQLPlanner(sql);
        Assertions.assertTrue(collectBucketedAggregationNodes(regularPlanner).isEmpty());
        OlapScanNode regularScan = singleOlapScanNode(regularPlanner);
        Assertions.assertEquals(-1, regularScan.getPinnedBackendId());
        Assertions.assertFalse(regularScan.getScanRangeLocations(0).isEmpty());
        for (TScanRangeLocations locations : regularScan.getScanRangeLocations(0)) {
            Assertions.assertEquals(2, locations.getLocationsSize());
        }

        // The single-BE gate sees only one alive backend, but the second backend is alive
        // (and offers replicas) by the time the scan range locations are built. Bucketed
        // aggregation merges the groups of its fragment in memory, so the fused scan must
        // still be placed on the backend the gate saw and nowhere else.
        new MockUp<SystemInfoService>() {
            @Mock
            public List<Long> getAllBackendByCurrentCluster(boolean needAlive) {
                return Lists.newArrayList(pinnedBackendId);
            }
        };
        Planner fusedPlanner = getSQLPlanner(sql);
        Assertions.assertFalse(collectBucketedAggregationNodes(fusedPlanner).isEmpty());
        OlapScanNode fusedScan = singleOlapScanNode(fusedPlanner);
        Assertions.assertEquals(pinnedBackendId, fusedScan.getPinnedBackendId());
        Assertions.assertEquals(regularScan.getScanRangeLocations(0).size(),
                fusedScan.getScanRangeLocations(0).size());
        for (TScanRangeLocations locations : fusedScan.getScanRangeLocations(0)) {
            Assertions.assertEquals(1, locations.getLocationsSize());
            Assertions.assertEquals(pinnedBackendId, locations.getLocations().get(0).getBackendId());
        }
    }

    private OlapScanNode singleOlapScanNode(Planner planner) {
        List<ScanNode> scanNodes = planner.getScanNodes();
        Assertions.assertEquals(1, scanNodes.size());
        Assertions.assertTrue(scanNodes.get(0) instanceof OlapScanNode);
        return (OlapScanNode) scanNodes.get(0);
    }

    private List<BucketedAggregationNode> collectBucketedAggregationNodes(Planner planner) {
        List<BucketedAggregationNode> nodes = Lists.newArrayList();
        for (PlanFragment fragment : planner.getFragments()) {
            PlanNode root = fragment.getPlanRoot();
            if (root != null) {
                root.collect(BucketedAggregationNode.class, nodes);
            }
        }
        return nodes;
    }
}
