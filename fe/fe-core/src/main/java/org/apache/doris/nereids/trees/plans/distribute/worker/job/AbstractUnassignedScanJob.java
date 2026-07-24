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

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.distribute.DistributeContext;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorkerManager;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

/** AbstractUnassignedScanJob */
public abstract class AbstractUnassignedScanJob extends AbstractUnassignedJob {
    protected final AtomicInteger shareScanIdGenerator = new AtomicInteger();

    public AbstractUnassignedScanJob(StatementContext statementContext, PlanFragment fragment,
            List<ScanNode> scanNodes, ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob) {
        super(statementContext, fragment, scanNodes, exchangeToChildJob);
    }

    /**
     * Compute assigned scan jobs using a two-phase parallelization strategy:
     * <ol>
     *   <li><b>Cross-machine parallelization</b> ({@link #multipleMachinesParallelization}):
     *       For each tablet / scan range, select the best replica and its hosting backend worker.
     *       This groups scan ranges by the worker that will process them.</li>
     *   <li><b>Intra-machine parallelization</b> ({@link #insideMachineParallelization}):
     *       Within each worker, split the assigned scan ranges into one or more instances
     *       based on the degree of parallelism. Supports local shuffle mode to further
     *       increase parallelism without rescanning data.</li>
     * </ol>
     * After both phases, {@link #fillUpAssignedJobs} provides a hook for subclasses to
     * supply fallback instances when no workers could be selected (e.g. all tablets pruned).
     *
     * @param distributeContext the distribute context for worker selection and parallelism config
     * @param inputJobs multimap from child exchange nodes to their assigned jobs
     * @return the list of assigned scan jobs, each bound to a worker with its tablet ranges
     */
    @Override
    public List<AssignedJob> computeAssignedJobs(
            DistributeContext distributeContext, ListMultimap<ExchangeNode, AssignedJob> inputJobs) {

        Map<DistributedPlanWorker, UninstancedScanSource> workerToScanSource
                = multipleMachinesParallelization(distributeContext, inputJobs);

        List<AssignedJob> assignedJobs = insideMachineParallelization(workerToScanSource, inputJobs, distributeContext);

        return fillUpAssignedJobs(assignedJobs, distributeContext.workerManager, inputJobs);
    }

    /**
     * Hook for subclasses to supply fallback instances when the normal parallelization
     * produces an empty result. For example, when all tablets of a table have been pruned
     * (e.g. TABLET(1234) with a non-existent tablet id), this method can create a single
     * empty instance to keep the fragment alive and return an empty result set.
     *
     * @param assignedJobs the list produced by {@link #insideMachineParallelization};
     *                     may be empty if no workers could be selected
     * @param workerManager the worker manager used to select a random fallback worker
     * @param inputJobs multimap from child exchange nodes to their assigned jobs
     * @return the (possibly augmented) list of assigned jobs; default returns unchanged
     */
    protected List<AssignedJob> fillUpAssignedJobs(
            List<AssignedJob> assignedJobs,
            DistributedPlanWorkerManager workerManager,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs) {
        return assignedJobs;
    }

    /**
     * Cross-machine parallelization: for each tablet / scan range of the scan nodes
     * in this fragment, select the best replica and its hosting {@link DistributedPlanWorker}.
     * The result groups all scan ranges by the worker that will process them.
     * <p>
     * This is the first phase of the two-phase parallelization. The returned map drives
     * the second phase ({@link #insideMachineParallelization}) where each worker's ranges
     * are further split into individual instances.
     *
     * @param distributeContext the distribute context for worker selection and parallelism config
     * @param inputJobs multimap from child exchange nodes to their assigned jobs
     * @return a map from selected worker to its {@link UninstancedScanSource} containing
     *         the raw scan ranges assigned to that worker, not yet split into instances
     */
    protected abstract Map<DistributedPlanWorker, UninstancedScanSource> multipleMachinesParallelization(
            DistributeContext distributeContext, ListMultimap<ExchangeNode, AssignedJob> inputJobs);

    /**
     * Intra-machine parallelization: for each worker, split its assigned scan ranges
     * into one or more {@link AssignedJob} instances. This is the second phase of
     * the two-phase parallelization, following {@link #multipleMachinesParallelization}.
     * <p>
     * For each worker entry, the method:
     * <ol>
     *   <li>Computes the max parallelism from the scan source (e.g. tablet count).</li>
     *   <li>Determines the final instance count via {@link #degreeOfParallelism},
     *       capped by the fragment's {@code parallelExecNum} and tablet count.</li>
     *   <li>Splits scan ranges evenly across instances (default mode) or creates
     *       local shuffle instances that share a single scan source to add
     *       parallelism without rescanning data ({@link #assignLocalShuffleJobs}).</li>
     * </ol>
     *
     * @param workerToScanRanges map from worker to its un-instanced scan ranges,
     *                           produced by {@link #multipleMachinesParallelization}
     * @param inputJobs multimap from child exchange nodes to their assigned jobs
     * @param distributeContext the distribute context for parallelism configuration
     * @return the list of assigned jobs, each bound to a worker with its portion of scan ranges
     */
    protected List<AssignedJob> insideMachineParallelization(
            Map<DistributedPlanWorker, UninstancedScanSource> workerToScanRanges,
            ListMultimap<ExchangeNode, AssignedJob> inputJobs,
            DistributeContext distributeContext) {

        ConnectContext context = statementContext.getConnectContext();
        boolean useLocalShuffleToAddParallel = useLocalShuffleToAddParallel(distributeContext);
        List<AssignedJob> instances = Lists.newArrayList();
        for (Entry<DistributedPlanWorker, UninstancedScanSource> entry : workerToScanRanges.entrySet()) {
            DistributedPlanWorker worker = entry.getKey();

            // the scanRanges which this worker should scan,
            // for example:
            // {
            //   scan tbl1: [tablet_10001, tablet_10002, tablet_10003, tablet_10004] // no instances
            // }
            ScanSource scanSource = entry.getValue().scanSource;

            // usually, its tablets num, or buckets num
            int scanSourceMaxParallel = scanSource.maxParallel(scanNodes);

            // now we should compute how many instances to process the data,
            // for example: two instances
            int instanceNum = degreeOfParallelism(scanSourceMaxParallel, useLocalShuffleToAddParallel);

            if (useLocalShuffleToAddParallel) {
                assignLocalShuffleJobs(scanSource, instanceNum, instances, context, worker);
            } else {
                assignedDefaultJobs(scanSource, instanceNum, instances, context, worker);
            }
        }

        return instances;
    }

    /**
     * Whether the fragment should use a serial source operator followed by local
     * shuffle to add intra-machine parallelism. When true, data is first gathered
     * through one exchange, then locally shuffled to multiple instances on the same
     * machine, allowing parallel computation without rescanning the source data.
     *
     * @param distributeContext the distribute context; for load jobs, the connect
     *                          context is passed as null to avoid serial source
     * @return true if the fragment has a serial source operator and should use
     *         local shuffle to increase parallelism
     */
    protected boolean useLocalShuffleToAddParallel(DistributeContext distributeContext) {
        return fragment.useSerialSource(distributeContext.isLoadJob ? null : statementContext.getConnectContext());
    }

    /**
     * Split the given scan source evenly into {@code instanceNum} partitions and
     * create one {@link StaticAssignedJob} per partition, all on the same worker.
     * Each instance scans a disjoint subset of the tablet ranges, dividing the
     * total scan workload among the instances.
     *
     * @param scanSource the full scan source (e.g. all tablets assigned to this worker)
     * @param instanceNum the number of instances to split into
     * @param instances the output list receiving newly created assigned jobs
     * @param context the connect context for generating instance IDs
     * @param worker the worker that will host all of the instances
     */
    protected void assignedDefaultJobs(ScanSource scanSource, int instanceNum, List<AssignedJob> instances,
            ConnectContext context, DistributedPlanWorker worker) {
        // split the scanRanges to some partitions, one partition for one instance
        // for example:
        //  [
        //     scan tbl1: [tablet_10001, tablet_10003], // instance 1
        //     scan tbl1: [tablet_10002, tablet_10004]  // instance 2
        //  ]
        List<ScanSource> instanceToScanRanges = scanSource.parallelize(scanNodes, instanceNum);

        for (ScanSource instanceToScanRange : instanceToScanRanges) {
            instances.add(
                    assignWorkerAndDataSources(
                        instances.size(), context.nextInstanceId(), worker, instanceToScanRange
                    )
            );
        }
    }

    /**
     * Create local shuffle instances on the given worker. The first instance scans
     * all data, and remaining instances receive an empty scan source — they share
     * the first instance's scan result via local shuffle on the same BE.
     * This avoids rescanning the same data multiple times while still adding
     * parallelism for downstream operators (e.g. aggregation).
     * <p>
     * All instances share the same {@code shareScanId}, signaling to the backend
     * that they belong to the same shared-scan group.
     *
     * @param scanSource the full scan source (all data for this worker)
     * @param instanceNum the total number of local shuffle instances to create
     * @param instances the output list receiving newly created {@link LocalShuffleAssignedJob}s
     * @param context the connect context for generating instance IDs
     * @param worker the worker that will host all local shuffle instances
     */
    protected void assignLocalShuffleJobs(ScanSource scanSource, int instanceNum, List<AssignedJob> instances,
            ConnectContext context, DistributedPlanWorker worker) {
        // only generate one instance to scan all data, in this step
        List<ScanSource> assignedJoinBuckets = scanSource.parallelize(scanNodes, instanceNum);

        // when data not big, but aggregation too slow, we will use 1 instance to scan data,
        // and use more instances (to ***add parallel***) to process aggregate.
        // We call it `ignore data distribution` of `share scan`. Backend will know this instances
        // share the same ScanSource, and will not scan same data multiple times.
        //
        // +-------------------------------- same fragment in one host -------------------------------------+
        // |                instance1      instance2     instance3     instance4                            |
        // |                    \              \             /            /                                 |
        // |                                                                                                |
        // |                                     OlapScanNode                                               |
        // |(share scan node, instance1 will scan all data and local shuffle to other local instances       |
        // |                           to parallel compute this data)                                       |
        // +------------------------------------------------------------------------------------------------+
        ScanSource shareScanSource = assignedJoinBuckets.get(0);

        // one scan range generate multiple instances,
        // different instances reference the same scan source
        int shareScanId = shareScanIdGenerator.getAndIncrement();
        ScanSource emptyShareScanSource = shareScanSource.newEmpty();
        for (int i = 0; i < instanceNum; i++) {
            LocalShuffleAssignedJob instance = new LocalShuffleAssignedJob(
                    instances.size(), shareScanId, context.nextInstanceId(), this, worker,
                    // only first instance need to scan data
                    i == 0 ? scanSource : emptyShareScanSource
            );
            instances.add(instance);
        }
    }

    /**
     * Compute the number of parallel instances for this fragment.
     * The result is bounded by several constraints:
     * <ul>
     *   <li>If the fragment has unpartitioned data distribution, returns 1.</li>
     *   <li>If query cache is enabled, returns {@code maxParallel} (one instance per
     *       tablet required for cache lookup).</li>
     *   <li>If the single OLAP scan node qualifies for single-instance optimization
     *       (e.g. LIMIT with no conjuncts), returns 1 to save resources.</li>
     *   <li>If local shuffle is active, returns the fragment's {@code parallelExecNum}.</li>
     *   <li>Otherwise, returns {@code min(maxParallel, max(parallelExecNum, 1))},
     *       i.e. capped by the actual tablet count.</li>
     * </ul>
     *
     * @param maxParallel the maximum possible parallelism (e.g. total tablet count
     *                    or bucket count on this worker)
     * @param useLocalShuffleToAddParallel whether local shuffle is active
     * @return the number of instances to create for this worker
     */
    protected int degreeOfParallelism(int maxParallel, boolean useLocalShuffleToAddParallel) {
        Preconditions.checkArgument(maxParallel > 0, "maxParallel must be positive");
        if (!fragment.getDataPartition().isPartitioned()) {
            return 1;
        }
        if (fragment.queryCacheParam != null) {
            // backend need use one instance for one tablet to look up tablet query cache
            return maxParallel;
        }
        if (scanNodes.size() == 1 && scanNodes.get(0) instanceof OlapScanNode) {
            OlapScanNode olapScanNode = (OlapScanNode) scanNodes.get(0);
            // if the scan node have limit and no conjuncts, only need 1 instance to save cpu and mem resource,
            // e.g. select * from tbl limit 10
            ConnectContext connectContext = statementContext.getConnectContext();
            if (connectContext != null && olapScanNode.shouldUseOneInstance(connectContext)) {
                return 1;
            }
        }

        if (useLocalShuffleToAddParallel) {
            return Math.max(fragment.getParallelExecNum(), 1);
        }

        // the scan instance num should not larger than the tablets num
        return Math.min(maxParallel, Math.max(fragment.getParallelExecNum(), 1));
    }

    /**
     * Create a single empty instance assigned to a random available worker.
     * Used by subclasses in {@link #fillUpAssignedJobs} as a fallback when normal
     * parallelization produces no instances (e.g. all tablets/data pruned away),
     * ensuring the fragment can still execute and return an empty result.
     *
     * @param workerManager the worker manager to select a random worker from
     * @return a singleton list containing one empty assigned job
     */
    protected List<AssignedJob> fillUpSingleEmptyInstance(DistributedPlanWorkerManager workerManager) {
        long catalogId = Env.getCurrentInternalCatalog().getId();
        if (scanNodes != null && scanNodes.size() > 0) {
            catalogId = scanNodes.get(0).getCatalogId();
        }
        return ImmutableList.of(
                assignWorkerAndDataSources(0,
                        statementContext.getConnectContext().nextInstanceId(),
                        workerManager.randomAvailableWorker(catalogId),
                        DefaultScanSource.empty())
        );
    }
}
