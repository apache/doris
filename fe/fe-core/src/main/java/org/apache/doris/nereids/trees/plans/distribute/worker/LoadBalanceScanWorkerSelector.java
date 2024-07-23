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

package org.apache.doris.nereids.trees.plans.distribute.worker;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.BucketScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.DefaultScanSource;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.ScanRanges;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedScanBucketOlapTableJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UninstancedScanSource;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;

/** LoadBalanceScanWorkerSelector */
public class LoadBalanceScanWorkerSelector implements ScanWorkerSelector {
    private final BackendDistributedPlanWorkerManager workerManager = new BackendDistributedPlanWorkerManager();
    private final Map<DistributedPlanWorker, WorkerWorkload> workloads = Maps.newLinkedHashMap();

    @Override
    public DistributedPlanWorkerManager getWorkerManager() {
        return workerManager;
    }

    @Override
    public DistributedPlanWorker selectMinWorkloadWorker(List<DistributedPlanWorker> workers) {
        DistributedPlanWorker minWorkloadWorker = null;
        WorkerWorkload minWorkload = new WorkerWorkload(Integer.MAX_VALUE, Long.MAX_VALUE);
        for (DistributedPlanWorker worker : workers) {
            WorkerWorkload workload = getWorkload(worker);
            if (minWorkload.compareTo(workload) > 0) {
                minWorkloadWorker = worker;
                minWorkload = workload;
            }
        }
        minWorkload.recordOneScanTask(1);
        return minWorkloadWorker;
    }

    @Override
    public Map<DistributedPlanWorker, UninstancedScanSource> selectReplicaAndWorkerWithoutBucket(ScanNode scanNode) {
        Map<DistributedPlanWorker, UninstancedScanSource> workerScanRanges = Maps.newLinkedHashMap();
        // allScanRangesLocations is all scan ranges in all partition which need to scan
        List<TScanRangeLocations> allScanRangesLocations = scanNode.getScanRangeLocations(0);
        for (TScanRangeLocations onePartitionOneScanRangeLocation : allScanRangesLocations) {
            // usually, the onePartitionOneScanRangeLocation is a tablet in one partition
            long bytes = getScanRangeSize(scanNode, onePartitionOneScanRangeLocation);

            WorkerScanRanges assigned = selectScanReplicaAndMinWorkloadWorker(
                    onePartitionOneScanRangeLocation, bytes);
            UninstancedScanSource scanRanges = workerScanRanges.computeIfAbsent(
                    assigned.worker,
                    w -> new UninstancedScanSource(
                            new DefaultScanSource(ImmutableMap.of(scanNode, new ScanRanges()))
                    )
            );
            DefaultScanSource scanSource = (DefaultScanSource) scanRanges.scanSource;
            scanSource.scanNodeToScanRanges.get(scanNode).addScanRanges(assigned.scanRanges);
        }
        return workerScanRanges;
    }

    @Override
    public Map<DistributedPlanWorker, UninstancedScanSource> selectReplicaAndWorkerWithBucket(
            UnassignedScanBucketOlapTableJob unassignedJob) {
        PlanFragment fragment = unassignedJob.getFragment();
        List<ScanNode> scanNodes = unassignedJob.getScanNodes();
        List<OlapScanNode> olapScanNodes = unassignedJob.getOlapScanNodes();

        BiFunction<ScanNode, Integer, List<TScanRangeLocations>> bucketScanRangeSupplier = bucketScanRangeSupplier();
        Function<ScanNode, Map<Integer, Long>> bucketBytesSupplier = bucketBytesSupplier();
        // all are olap scan nodes
        if (!scanNodes.isEmpty() && scanNodes.size() == olapScanNodes.size()) {
            if (olapScanNodes.size() == 1 && fragment.hasBucketShuffleJoin()) {
                return selectForBucket(unassignedJob, scanNodes, bucketScanRangeSupplier, bucketBytesSupplier);
            } else if (fragment.hasColocatePlanNode()) {
                return selectForBucket(unassignedJob, scanNodes, bucketScanRangeSupplier, bucketBytesSupplier);
            }
        } else if (olapScanNodes.isEmpty() && fragment.getDataPartition() == DataPartition.UNPARTITIONED) {
            return selectForBucket(unassignedJob, scanNodes, bucketScanRangeSupplier, bucketBytesSupplier);
        }
        throw new IllegalStateException(
                "Illegal bucket shuffle join or colocate join in fragment:\n"
                        + fragment.getExplainString(TExplainLevel.VERBOSE)
        );
    }

    private BiFunction<ScanNode, Integer, List<TScanRangeLocations>> bucketScanRangeSupplier() {
        return (scanNode, bucketIndex) -> {
            if (scanNode instanceof OlapScanNode) {
                return (List) ((OlapScanNode) scanNode).bucketSeq2locations.get(bucketIndex);
            } else {
                // the backend is selected by XxxScanNode.createScanRangeLocations()
                return scanNode.getScanRangeLocations(0);
            }
        };
    }

    private Function<ScanNode, Map<Integer, Long>> bucketBytesSupplier() {
        return scanNode -> {
            if (scanNode instanceof OlapScanNode) {
                return ((OlapScanNode) scanNode).bucketSeq2Bytes;
            } else {
                // not supported yet
                return ImmutableMap.of(0, 0L);
            }
        };
    }

    private Map<DistributedPlanWorker, UninstancedScanSource> selectForBucket(
            UnassignedJob unassignedJob, List<ScanNode> scanNodes,
            BiFunction<ScanNode, Integer, List<TScanRangeLocations>> bucketScanRangeSupplier,
            Function<ScanNode, Map<Integer, Long>> bucketBytesSupplier) {
        Map<DistributedPlanWorker, UninstancedScanSource> assignment = Maps.newLinkedHashMap();

        Map<Integer, Long> bucketIndexToBytes = computeEachBucketScanBytes(scanNodes, bucketBytesSupplier);

        for (Entry<Integer, Long> kv : bucketIndexToBytes.entrySet()) {
            Integer bucketIndex = kv.getKey();
            long allScanNodeScanBytesInOneBucket = kv.getValue();

            DistributedPlanWorker selectedWorker = null;
            for (ScanNode scanNode : scanNodes) {
                List<TScanRangeLocations> allPartitionTabletsInOneBucketInOneTable
                        = bucketScanRangeSupplier.apply(scanNode, bucketIndex);
                if (!allPartitionTabletsInOneBucketInOneTable.isEmpty()) {
                    WorkerScanRanges replicaAndWorker = selectScanReplicaAndMinWorkloadWorker(
                            allPartitionTabletsInOneBucketInOneTable.get(0), allScanNodeScanBytesInOneBucket);
                    selectedWorker = replicaAndWorker.worker;
                    break;
                }
                // else: the bucket is pruned, we should use another ScanNode to select worker for this bucket
            }
            if (selectedWorker == null) {
                throw new IllegalStateException("Can not assign worker for bucket: " + bucketIndex);
            }

            long workerId = selectedWorker.id();
            for (ScanNode scanNode : scanNodes) {
                List<TScanRangeLocations> allPartitionTabletsInOneBucket
                        = bucketScanRangeSupplier.apply(scanNode, bucketIndex);
                List<Pair<TScanRangeParams, Long>> selectedReplicasInOneBucket = filterReplicaByWorkerInBucket(
                                scanNode, workerId, bucketIndex, allPartitionTabletsInOneBucket
                );
                UninstancedScanSource bucketIndexToScanNodeToTablets
                        = assignment.computeIfAbsent(
                            selectedWorker,
                            worker -> new UninstancedScanSource(new BucketScanSource(Maps.newLinkedHashMap()))
                        );
                BucketScanSource scanSource = (BucketScanSource) bucketIndexToScanNodeToTablets.scanSource;
                Map<ScanNode, ScanRanges> scanNodeToScanRanges = scanSource.bucketIndexToScanNodeToTablets
                        .computeIfAbsent(bucketIndex, bucket -> Maps.newLinkedHashMap());
                ScanRanges scanRanges = scanNodeToScanRanges.computeIfAbsent(scanNode, node -> new ScanRanges());
                for (Pair<TScanRangeParams, Long> replica : selectedReplicasInOneBucket) {
                    TScanRangeParams replicaParam = replica.first;
                    Long scanBytes = replica.second;
                    scanRanges.addScanRange(replicaParam, scanBytes);
                }
            }
        }
        return assignment;
    }

    private WorkerScanRanges selectScanReplicaAndMinWorkloadWorker(
            TScanRangeLocations tabletLocation, long tabletBytes) {
        List<TScanRangeLocation> replicaLocations = tabletLocation.getLocations();
        int replicaNum = replicaLocations.size();
        WorkerWorkload minWorkload = new WorkerWorkload(Integer.MAX_VALUE, Long.MAX_VALUE);
        DistributedPlanWorker minWorkLoadWorker = null;
        TScanRangeLocation selectedReplicaLocation = null;

        for (int i = 0; i < replicaNum; i++) {
            TScanRangeLocation replicaLocation = replicaLocations.get(i);
            DistributedPlanWorker worker = workerManager.getWorker(replicaLocation.getBackendId());
            if (!worker.available()) {
                continue;
            }

            WorkerWorkload workload = getWorkload(worker);
            if (workload.compareTo(minWorkload) < 0) {
                minWorkLoadWorker = worker;
                minWorkload = workload;
                selectedReplicaLocation = replicaLocation;
            }
        }
        if (minWorkLoadWorker == null) {
            throw new AnalysisException("No available workers");
        } else {
            minWorkload.recordOneScanTask(tabletBytes);
            ScanRanges scanRanges = new ScanRanges();
            TScanRangeParams scanReplicaParams =
                    ScanWorkerSelector.buildScanReplicaParams(tabletLocation, selectedReplicaLocation);
            scanRanges.addScanRange(scanReplicaParams, tabletBytes);
            return new WorkerScanRanges(minWorkLoadWorker, scanRanges);
        }
    }

    private List<Pair<TScanRangeParams, Long>> filterReplicaByWorkerInBucket(
            ScanNode scanNode, long filterWorkerId, int bucketIndex,
            List<TScanRangeLocations> allPartitionTabletsInOneBucket) {
        List<Pair<TScanRangeParams, Long>> selectedReplicasInOneBucket = Lists.newArrayList();
        for (TScanRangeLocations onePartitionOneTabletLocation : allPartitionTabletsInOneBucket) {
            TScanRange scanRange = onePartitionOneTabletLocation.getScanRange();
            if (scanRange.getPaloScanRange() != null) {
                long tabletId = scanRange.getPaloScanRange().getTabletId();
                boolean foundTabletInThisWorker = false;
                for (TScanRangeLocation replicaLocation : onePartitionOneTabletLocation.getLocations()) {
                    if (replicaLocation.getBackendId() == filterWorkerId) {
                        TScanRangeParams scanReplicaParams = ScanWorkerSelector.buildScanReplicaParams(
                                onePartitionOneTabletLocation, replicaLocation);
                        Long replicaSize = ((OlapScanNode) scanNode).getTabletSingleReplicaSize(tabletId);
                        selectedReplicasInOneBucket.add(Pair.of(scanReplicaParams, replicaSize));
                        foundTabletInThisWorker = true;
                        break;
                    }
                }
                if (!foundTabletInThisWorker) {
                    throw new IllegalStateException(
                            "Can not find tablet " + tabletId + " in the bucket: " + bucketIndex);
                }
            } else if (onePartitionOneTabletLocation.getLocations().size() == 1) {
                TScanRangeLocation replicaLocation = onePartitionOneTabletLocation.getLocations().get(0);
                TScanRangeParams scanReplicaParams = ScanWorkerSelector.buildScanReplicaParams(
                        onePartitionOneTabletLocation, replicaLocation);
                Long replicaSize = 0L;
                selectedReplicasInOneBucket.add(Pair.of(scanReplicaParams, replicaSize));
            } else {
                throw new IllegalStateException("Unsupported");
            }
        }
        return selectedReplicasInOneBucket;
    }

    private Map<Integer, Long> computeEachBucketScanBytes(
            List<ScanNode> scanNodes, Function<ScanNode, Map<Integer, Long>> bucketBytesSupplier) {
        Map<Integer, Long> bucketIndexToBytes = Maps.newLinkedHashMap();
        for (ScanNode scanNode : scanNodes) {
            Map<Integer, Long> bucketSeq2Bytes = bucketBytesSupplier.apply(scanNode);
            for (Entry<Integer, Long> bucketSeq2Byte : bucketSeq2Bytes.entrySet()) {
                Integer bucketIndex = bucketSeq2Byte.getKey();
                Long scanBytes = bucketSeq2Byte.getValue();
                bucketIndexToBytes.merge(bucketIndex, scanBytes, Long::sum);
            }
        }
        return bucketIndexToBytes;
    }

    private WorkerWorkload getWorkload(DistributedPlanWorker worker) {
        return workloads.computeIfAbsent(worker, w -> new WorkerWorkload());
    }

    private long getScanRangeSize(ScanNode scanNode, TScanRangeLocations scanRangeLocations) {
        TScanRange scanRange = scanRangeLocations.getScanRange();
        TPaloScanRange paloScanRange = scanRange.getPaloScanRange();
        if (paloScanRange != null) {
            long tabletId = paloScanRange.getTabletId();
            Long tabletBytes = ((OlapScanNode) scanNode).getTabletSingleReplicaSize(tabletId);
            return tabletBytes == null ? 0L : tabletBytes;
        }

        TExternalScanRange extScanRange = scanRange.getExtScanRange();
        if (extScanRange != null) {
            TFileScanRange fileScanRange = extScanRange.getFileScanRange();
            long size = 0;
            for (TFileRangeDesc range : fileScanRange.getRanges()) {
                size += range.getSize();
            }
            return size;
        }

        return 0L;
    }

    private static class WorkerWorkload implements Comparable<WorkerWorkload> {
        private int taskNum;
        private long scanBytes;

        public WorkerWorkload() {
            this(0, 0);
        }

        public WorkerWorkload(int taskNum, long scanBytes) {
            this.taskNum = taskNum;
            this.scanBytes = scanBytes;
        }

        public void recordOneScanTask(long scanBytes) {
            this.scanBytes += scanBytes;
        }

        // order by scanBytes asc, taskNum asc
        @Override
        public int compareTo(WorkerWorkload workerWorkload) {
            int compareScanBytes = Long.compare(this.scanBytes, workerWorkload.scanBytes);
            if (compareScanBytes != 0) {
                return compareScanBytes;
            }
            return taskNum - workerWorkload.taskNum;
        }
    }
}
