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

package org.apache.doris.nereids.trees.plans.distribute.worker.job;

import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorkerManager;
import org.apache.doris.nereids.trees.plans.distribute.worker.ScanWorkerSelector;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * UnassignedScanBucketOlapTableJob.
 * bucket shuffle join olap table, or colocate join olap table
 */
public class UnassignedScanBucketOlapTableJob extends AbstractUnassignedScanJob {
    private final ScanWorkerSelector scanWorkerSelector;
    private final List<OlapScanNode> olapScanNodes;

    /** UnassignedScanNativeTableJob */
    public UnassignedScanBucketOlapTableJob(
            PlanFragment fragment, List<OlapScanNode> olapScanNodes,
            ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob,
            ScanWorkerSelector scanWorkerSelector) {
        super(fragment, (List) olapScanNodes, exchangeToChildJob);
        this.scanWorkerSelector = Objects.requireNonNull(
                scanWorkerSelector, "scanWorkerSelector cat not be null");

        Preconditions.checkArgument(!olapScanNodes.isEmpty(), "OlapScanNode is empty");
        this.olapScanNodes = olapScanNodes;
    }

    public List<OlapScanNode> getOlapScanNodes() {
        return olapScanNodes;
    }

    @Override
    protected Map<DistributedPlanWorker, UninstancedScanSource> multipleMachinesParallelization(
            DistributedPlanWorkerManager workerManager, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        // for every bucket tablet, select its replica and worker.
        // for example, colocate join:
        // {
        //    BackendWorker("172.0.0.1"): {
        //       bucket 0: {
        //         olapScanNode1: ScanRanges([tablet_10001, tablet_10002, tablet_10003, tablet_10004]),
        //         olapScanNode2: ScanRanges([tablet_10009, tablet_10010, tablet_10011, tablet_10012])
        //       },
        //       bucket 1: {
        //         olapScanNode1: ScanRanges([tablet_10005, tablet_10006, tablet_10007, tablet_10008])
        //         olapScanNode2: ScanRanges([tablet_10013, tablet_10014, tablet_10015, tablet_10016])
        //       },
        //       ...
        //    },
        //    BackendWorker("172.0.0.2"): {
        //       ...
        //    }
        // }
        return scanWorkerSelector.selectReplicaAndWorkerWithBucket(this);
    }

    @Override
    protected List<AssignedJob> insideMachineParallelization(
            Map<DistributedPlanWorker, UninstancedScanSource> workerToScanRanges,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs, DistributedPlanWorkerManager workerManager) {
        // separate buckets to instanceNum groups, let one instance process some buckets.
        // for example, colocate join:
        // {
        //    // 172.0.0.1 has two instances
        //    BackendWorker("172.0.0.1"): [
        //       // instance 1 process two buckets
        //       {
        //         bucket 0: {
        //           olapScanNode1: ScanRanges([tablet_10001, tablet_10002, tablet_10003, tablet_10004]),
        //           olapScanNode2: ScanRanges([tablet_10009, tablet_10010, tablet_10011, tablet_10012])
        //         },
        //         bucket 1: {
        //           olapScanNode1: ScanRanges([tablet_10005, tablet_10006, tablet_10007, tablet_10008])
        //           olapScanNode2: ScanRanges([tablet_10013, tablet_10014, tablet_10015, tablet_10016])
        //         }
        //       },
        //       // instance 1 process one bucket
        //       {
        //         bucket 3: ...
        //       }
        //    ]
        //    // instance 4... in "172.0.0.1"
        //    BackendWorker("172.0.0.2"): [
        //       ...
        //    ],
        //    ...
        // }
        List<AssignedJob> assignedJobs = super.insideMachineParallelization(
                workerToScanRanges, inputJobs, workerManager);

        // the case:
        // ```sql
        // SELECT * FROM
        // (select * from tbl1 where c0 =1)a
        // RIGHT OUTER JOIN
        // (select * from tbl2)b
        // ON a.id = b.id;
        // ```
        // contains right outer join and missing instance in left side because of tablet pruner, for example
        // left:  [bucket 1, bucket 3]
        //
        // we should join buckets:
        // [
        //   (left bucket 1) right outer join (exchange right data which should process by bucket 1)
        //   (no any machine) right outer join (exchange right data which should process by bucket 2)
        //   (left bucket 3) right outer join (exchange right data which should process by bucket 3)
        // ]
        // if missing the left bucket 2, it will compute an empty result
        // because right bucket 2 doesn't exist destination instance,
        // so we should fill up this instance
        List<HashJoinNode> hashJoinNodes = fragment.getPlanRoot()
                .collectInCurrentFragment(HashJoinNode.class::isInstance);
        if (shouldFillUpInstances(hashJoinNodes)) {
            return fillUpInstances(assignedJobs);
        }

        return assignedJobs;
    }

    private boolean shouldFillUpInstances(List<HashJoinNode> hashJoinNodes) {
        for (HashJoinNode hashJoinNode : hashJoinNodes) {
            if (!hashJoinNode.isBucketShuffle()) {
                continue;
            }
            JoinOperator joinOp = hashJoinNode.getJoinOp();
            switch (joinOp) {
                case INNER_JOIN:
                case CROSS_JOIN:
                    break;
                default:
                    return true;
            }
        }
        return false;
    }

    private List<AssignedJob> fillUpInstances(List<AssignedJob> instances) {
        Set<Integer> missingBucketIndexes = missingBuckets(instances);
        if (missingBucketIndexes.isEmpty()) {
            return instances;
        }

        ConnectContext context = ConnectContext.get();

        OlapScanNode olapScanNode = (OlapScanNode) scanNodes.get(0);
        MaterializedIndex randomPartition = randomPartition(olapScanNode);
        ListMultimap<DistributedPlanWorker, Integer> missingBuckets = selectWorkerForMissingBuckets(
                olapScanNode, randomPartition, missingBucketIndexes);

        boolean useLocalShuffle = instances.stream().anyMatch(LocalShuffleAssignedJob.class::isInstance);
        List<AssignedJob> newInstances = new ArrayList<>(instances);
        for (Entry<DistributedPlanWorker, Collection<Integer>> workerToBuckets : missingBuckets.asMap().entrySet()) {
            Map<Integer, Map<ScanNode, ScanRanges>> scanEmptyBuckets = Maps.newLinkedHashMap();
            for (Integer bucketIndex : workerToBuckets.getValue()) {
                Map<ScanNode, ScanRanges> scanTableWithEmptyData = Maps.newLinkedHashMap();
                for (ScanNode scanNode : scanNodes) {
                    scanTableWithEmptyData.put(scanNode, new ScanRanges());
                }
                scanEmptyBuckets.put(bucketIndex, scanTableWithEmptyData);
            }

            AssignedJob fillUpInstance = null;
            DistributedPlanWorker worker = workerToBuckets.getKey();
            BucketScanSource scanSource = new BucketScanSource(scanEmptyBuckets);
            if (useLocalShuffle) {
                // when use local shuffle, we should ensure every backend only process one instance!
                // so here we should try to merge the missing buckets into exist instances
                boolean mergedBucketsInSameWorkerInstance = false;
                for (AssignedJob newInstance : newInstances) {
                    if (newInstance.getAssignedWorker().equals(worker)) {
                        BucketScanSource bucketScanSource = (BucketScanSource) newInstance.getScanSource();
                        bucketScanSource.bucketIndexToScanNodeToTablets.putAll(scanEmptyBuckets);
                        mergedBucketsInSameWorkerInstance = true;
                    }
                }
                if (!mergedBucketsInSameWorkerInstance) {
                    fillUpInstance = new LocalShuffleAssignedJob(
                            newInstances.size(), shareScanIdGenerator.getAndIncrement(),
                            context.nextInstanceId(), this, worker, scanSource
                    );
                }
            } else {
                fillUpInstance = assignWorkerAndDataSources(
                        newInstances.size(), context.nextInstanceId(), worker, scanSource
                );
            }
            if (fillUpInstance != null) {
                newInstances.add(fillUpInstance);
            }
        }
        return newInstances;
    }

    private int fullBucketNum() {
        for (ScanNode scanNode : scanNodes) {
            if (scanNode instanceof OlapScanNode) {
                return ((OlapScanNode) scanNode).getBucketNum();
            }
        }
        throw new IllegalStateException("Not support bucket shuffle join with non OlapTable");
    }

    private Set<Integer> missingBuckets(List<AssignedJob> instances) {
        Set<Integer> usedBuckets = usedBuckets(instances);
        int bucketNum = fullBucketNum();
        Set<Integer> missingBuckets = new TreeSet<>();
        for (int i = 0; i < bucketNum; i++) {
            if (!usedBuckets.contains(i)) {
                missingBuckets.add(i);
            }
        }
        return missingBuckets;
    }

    private Set<Integer> usedBuckets(List<AssignedJob> instances) {
        Set<Integer> usedBuckets = new TreeSet<>();
        for (AssignedJob instance : instances) {
            ScanSource scanSource = instance.getScanSource();
            if (scanSource instanceof BucketScanSource) {
                BucketScanSource bucketScanSource = (BucketScanSource) scanSource;
                usedBuckets.addAll(bucketScanSource.bucketIndexToScanNodeToTablets.keySet());
            }
        }
        return usedBuckets;
    }

    private MaterializedIndex randomPartition(OlapScanNode olapScanNode) {
        List<Long> selectedPartitionIds = ImmutableList.copyOf(olapScanNode.getSelectedPartitionIds());
        if (selectedPartitionIds.isEmpty()) {
            throw new IllegalStateException("Missing selected partitions in " + olapScanNode);
        }

        Long randomSelectPartitionId = selectedPartitionIds.get((int) (Math.random() * selectedPartitionIds.size()));
        Partition partition = olapScanNode.getOlapTable().getPartition(randomSelectPartitionId);
        return partition.getBaseIndex();
    }

    private ListMultimap<DistributedPlanWorker, Integer> selectWorkerForMissingBuckets(
            OlapScanNode olapScanNode, MaterializedIndex partition, Set<Integer> selectBucketIndexes) {
        List<Long> tabletIdsInOrder = partition.getTabletIdsInOrder();
        ListMultimap<DistributedPlanWorker, Integer> fillUpWorkerToBuckets = ArrayListMultimap.create();
        for (Integer bucketIndex : selectBucketIndexes) {
            Long tabletIdInBucket = tabletIdsInOrder.get(bucketIndex);
            Tablet tabletInBucket = partition.getTablet(tabletIdInBucket);
            List<DistributedPlanWorker> workers = getWorkersByReplicas(tabletInBucket);
            if (workers.isEmpty()) {
                throw new IllegalStateException("Can not found available replica for bucket " + bucketIndex
                        + ", table: " + olapScanNode);
            }
            DistributedPlanWorker worker = scanWorkerSelector.selectMinWorkloadWorker(workers);
            fillUpWorkerToBuckets.put(worker, bucketIndex);
        }
        return fillUpWorkerToBuckets;
    }

    private List<DistributedPlanWorker> getWorkersByReplicas(Tablet tablet) {
        DistributedPlanWorkerManager workerManager = scanWorkerSelector.getWorkerManager();
        List<Replica> replicas = tablet.getReplicas();
        List<DistributedPlanWorker> workers = Lists.newArrayListWithCapacity(replicas.size());
        for (Replica replica : replicas) {
            DistributedPlanWorker worker = workerManager.getWorker(replica.getBackendId());
            if (worker.available()) {
                workers.add(worker);
            }
        }
        return workers;
    }
}
