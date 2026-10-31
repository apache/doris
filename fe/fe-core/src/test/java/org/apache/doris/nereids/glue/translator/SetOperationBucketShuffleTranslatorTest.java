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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.BucketScanSource;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.SetOperationNode;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SetOperationBucketShuffleTranslatorTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("set_operation_bucket_shuffle");
        useDatabase("set_operation_bucket_shuffle");
        for (String table : List.of("t1", "t2")) {
            createTable("CREATE TABLE " + table + " (id INT) DISTRIBUTED BY HASH(id) BUCKETS 4 "
                    + "PROPERTIES('replication_num'='1')");
        }
        for (String table : List.of("t1", "t2")) {
            OlapTable olapTable = (OlapTable) Env.getCurrentInternalCatalog()
                    .getDbOrAnalysisException("set_operation_bucket_shuffle").getTableOrAnalysisException(table);
            for (Partition partition : olapTable.getPartitions()) {
                partition.getBaseIndex().setRowCount(10000);
                partition.getBaseIndex().setRowCountReported(true);
            }
        }
        connectContext.getSessionVariable().aggPhase = 1;
        connectContext.getSessionVariable().setDisableNereidsRules(
                "PRUNE_EMPTY_PARTITION,REWRITE_EXCEPT_TO_JOIN_WHEN_SPILL");
        connectContext.getSessionVariable().setBucketShuffleDowngradeRatio(0);
        connectContext.getSessionVariable().setBeNumberForTest(3);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    public void testPrunedSetOperationRetainsBucketReceivers(boolean localShuffle, boolean localShufflePlanner)
            throws Exception {
        SessionVariable session = connectContext.getSessionVariable();
        session.setEnableLocalShuffle(localShuffle);
        session.setEnableLocalShufflePlanner(localShufflePlanner);
        boolean bucketShuffleAllowed = !localShuffle || localShufflePlanner;
        for (String operation : List.of("UNION ALL", "EXCEPT")) {
            String rightValues = "EXCEPT".equals(operation) ? "1,2" : "3,4";
            String sql = "SELECT id FROM (SELECT id FROM t1 WHERE id IN (1,2) " + operation
                    + " SELECT id FROM t2 WHERE id IN (" + rightValues + ")) u GROUP BY id";
            NereidsPlanner planner = PlanChecker.from(connectContext).plan(sql);
            int setOperations = 0;
            for (DistributedPlan distributed : planner.getDistributedPlans().values()) {
                PipelineDistributedPlan plan = (PipelineDistributedPlan) distributed;
                List<SetOperationNode> nodes = plan.getFragmentJob().getFragment().getPlanRoot()
                        .collectInCurrentFragment(SetOperationNode.class::isInstance);
                for (SetOperationNode node : nodes) {
                    setOperations++;
                    String detail = operation + ": " + plan.toString(0);
                    Assertions.assertEquals(bucketShuffleAllowed, node.isBucketShuffle(), detail);
                    if (!localShuffle && bucketShuffleAllowed) {
                        // The basic scan prunes buckets, but the other input can send to any bucket.
                        List<OlapScanNode> scans = plan.getFragmentJob().getFragment().getPlanRoot()
                                .collectInCurrentFragment(OlapScanNode.class::isInstance);
                        Assertions.assertEquals(1, scans.size(), detail);
                        Assertions.assertTrue(scans.get(0).getScanTabletIds().size() < 4, detail);
                        Assertions.assertTrue(plan.getFragmentJob().getFragment().hasBucketShuffleNode(), detail);
                        Set<Integer> receiverBuckets = new HashSet<>();
                        for (AssignedJob job : plan.getInstanceJobs()) {
                            BucketScanSource source = (BucketScanSource) job.getScanSource();
                            receiverBuckets.addAll(source.bucketIndexToScanNodeToTablets.keySet());
                        }
                        Assertions.assertEquals(Set.of(0, 1, 2, 3), receiverBuckets, detail);
                    }
                }
            }
            Assertions.assertEquals(1, setOperations, planner.getPhysicalPlan().treeString());
        }
    }
}
