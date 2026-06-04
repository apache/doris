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

package org.apache.doris.planner;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.LocalShuffleBucketJoinAssignedJob;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
import org.apache.doris.planner.LocalExchangeNode.RequireHash;
import org.apache.doris.qe.ConnectContext;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * FE-side local exchange planner — inserts {@link LocalExchangeNode} into each fragment's
 * plan tree so that within-fragment data redistribution is decided at planning time
 * instead of at BE pipeline-build time.
 *
 * <h3>When this runs</h3>
 * Invoked from {@code NereidsPlanner.addLocalExchangeAfterDistribute()} right after
 * {@code DistributePlanner} has assigned instances to fragments and before the plan is
 * serialized to BE.  Gated by session variable {@code enable_local_shuffle_planner}
 * (default true) and {@code enable_local_shuffle}; when either is off this pass is
 * skipped entirely and BE falls back to its own {@code _plan_local_exchange}.  The two
 * paths are mutually exclusive: BE consults {@code runtime_state.h::plan_local_shuffle()}
 * to know whether it should plan LE itself.
 *
 * <h3>What it changes</h3>
 * <ul>
 *   <li>For each fragment with {@code maxPerBeInstances > 1}, walks the plan tree
 *       bottom-up via {@link PlanNode#enforceAndDeriveLocalExchange} and inserts
 *       LocalExchangeNodes where children's output distribution doesn't satisfy the
 *       parent's requirement.</li>
 *   <li>May wrap the fragment root with an extra PASSTHROUGH LE so the data sink
 *       (DataStreamSink / OlapTableSink) runs with the full instance count even when
 *       the root operator is serial — see {@link #addLocalExchangeForFragment}.</li>
 *   <li>Does NOT modify the fragment sink itself, fragment boundaries, or instance
 *       assignment.</li>
 * </ul>
 *
 * <h3>Per-BE instance semantics</h3>
 * Skips fragments where every BE has at most 1 instance.  Using a global instance count
 * would insert LE for "2 BEs × 1 instance" cases, which BE's own
 * {@code _plan_local_exchange} would not — leading to pipeline task-count mismatch and
 * deadlock.  See {@link #addLocalExchange}.
 *
 * <h3>Reading order</h3>
 * Start with {@link PlanNode#enforceRequire} (the recursion engine), then individual
 * {@code enforceAndDeriveLocalExchange} overrides on PlanNode subclasses.
 */
public class AddLocalExchange {
    /** addLocalExchange with distributed plans, skipping single-instance fragments.
     *  BE's _plan_local_exchange checks _num_instances which is the per-BE instance count.
     *  With _num_instances<=1 all pipelines on that BE have 1 task so local exchange is a no-op.
     *  We must use the same per-BE semantics: skip when every BE has at most 1 instance.
     *  Using global instanceCount would insert LE for fragments where 2 BEs each have 1 instance
     *  (global=2, per-BE=1), causing pipeline task mismatch and deadlock. */
    public void addLocalExchange(FragmentIdMapping<DistributedPlan> distributedPlans,
            PlanTranslatorContext context) {
        for (DistributedPlan plan : distributedPlans.values()) {
            PipelineDistributedPlan pipePlan = (PipelineDistributedPlan) plan;
            long maxPerBeInstances = pipePlan.getInstanceJobs().stream()
                    .collect(java.util.stream.Collectors.groupingBy(
                            j -> j.getAssignedWorker().id(), java.util.stream.Collectors.counting()))
                    .values().stream().mapToLong(Long::longValue).max().orElse(0);
            if (maxPerBeInstances <= 1) {
                continue;
            }
            context.setCurrentFragmentBucketUpgradeEligible(
                    isBucketUpgradeEligible(pipePlan, maxPerBeInstances, context));
            PlanFragment fragment = pipePlan.getFragmentJob().getFragment();
            addLocalExchangeForFragment(fragment, context);
        }
    }

    /**
     * Bucket → local-hash parallelism upgrade eligibility (DORIS-24902 part 2).
     *
     * A pooled bucket-join fragment runs its bucket joins at bucket-count parallelism:
     * each LocalShuffleBucketJoinAssignedJob owns a disjoint set of join buckets and only
     * instances with buckets do join work (e.g. 8 buckets/BE but 16 instances/BE → 8 idle).
     * When nothing above the join needs bucket alignment, HashJoinNode can re-distribute
     * both sides with LOCAL_EXECUTION_HASH_SHUFFLE to use all instances — see
     * {@link HashJoinNode#enforceAndDeriveLocalExchange}.
     *
     * This method computes the per-fragment numeric condition from the actual instance
     * assignment: maxPerBeInstances > maxBucketsWithDataPerWorker × ratio.  The ratio comes
     * from session variable {@code local_shuffle_bucket_upgrade_ratio}; values <= 1 disable
     * the upgrade entirely (a required parallelism gain of at most 1x means no gain).
     */
    private boolean isBucketUpgradeEligible(PipelineDistributedPlan pipePlan,
            long maxPerBeInstances, PlanTranslatorContext context) {
        ConnectContext connectContext = context.getConnectContext();
        if (connectContext == null || connectContext.getSessionVariable() == null) {
            return false;
        }
        double ratio = connectContext.getSessionVariable().getLocalShuffleBucketUpgradeRatio();
        List<AssignedJob> instanceJobs = pipePlan.getInstanceJobs();
        if (instanceJobs.isEmpty()
                || !instanceJobs.stream().allMatch(LocalShuffleBucketJoinAssignedJob.class::isInstance)) {
            // Only pooled bucket-join fragments have the bucket-count parallelism cap.
            return false;
        }
        Map<Long, Set<Integer>> bucketsPerWorker = new HashMap<>();
        for (AssignedJob job : instanceJobs) {
            bucketsPerWorker.computeIfAbsent(job.getAssignedWorker().id(), k -> new HashSet<>())
                    .addAll(((LocalShuffleBucketJoinAssignedJob) job).getAssignedJoinBucketIndexes());
        }
        long maxBucketsPerWorker = bucketsPerWorker.values().stream()
                .mapToLong(Set::size).max().orElse(0);
        return shouldUpgradeBucketParallelism(ratio, maxPerBeInstances, maxBucketsPerWorker);
    }

    /**
     * Pure numeric gate for the bucket → local-hash upgrade.
     * ratio <= 1 (including 0 and negatives) always disables; otherwise upgrade when the
     * per-BE instance count exceeds buckets-with-data × ratio (i.e. the parallelism gain
     * is at least the configured multiple).
     */
    static boolean shouldUpgradeBucketParallelism(double ratio, long maxPerBeInstances,
            long maxBucketsPerWorker) {
        if (ratio <= 1.0) {
            return false;
        }
        return maxBucketsPerWorker > 0 && maxPerBeInstances > maxBucketsPerWorker * ratio;
    }

    private void addLocalExchangeForFragment(PlanFragment fragment, PlanTranslatorContext context) {
        DataSink sink = fragment.getSink();
        LocalExchangeTypeRequire require = sink == null
                ? LocalExchangeTypeRequire.noRequire() : sink.getLocalExchangeTypeRequire();
        PlanNode root = fragment.getPlanRoot();
        context.setHasSerialAncestorInPipeline(root, false);
        Pair<PlanNode, LocalExchangeType> output = root
                .enforceAndDeriveLocalExchange(context, null, require);
        PlanNode newRoot = output.first;
        // The fragment data sink (DataStreamSink, OlapTableSink) runs in the same pipeline
        // as the root. If the root will be serial on BE, the sink pipeline has 1 task —
        // only instance 0 sends data, others hang or miss writes.
        // Insert PASSTHROUGH fan-out so sink runs with _num_instances tasks.
        // This matches BE-native's default required_data_distribution():
        //   _child->is_serial_operator() ? PASSTHROUGH : NOOP
        if (newRoot.isSerialOperatorOnBe(context.getConnectContext())) {
            newRoot = new LocalExchangeNode(context.nextPlanNodeId(), newRoot,
                    LocalExchangeType.PASSTHROUGH, null);
        }
        if (newRoot != root) {
            fragment.setPlanRoot(newRoot);
        }
        validateNoSerialWithoutLocalExchange(fragment.getPlanRoot(), context.getConnectContext());
    }

    /**
     * In a local-shuffle fragment, the root check above guarantees the root pipeline
     * has N tasks. Any serial operator reduces its pipeline to 1 task. If this serial
     * operator feeds into a non-serial parent without LocalExchangeNode in between,
     * some pipelines have 1 task while others have N → shared_state mismatch, data loss.
     *
     * Serial→serial chains are fine (all at 1 task, consistent). Only the transition
     * from serial to non-serial needs LE to restore parallelism.
     */
    private void validateNoSerialWithoutLocalExchange(PlanNode node,
            org.apache.doris.qe.ConnectContext context) {
        for (PlanNode child : node.getChildren()) {
            validateNoSerialWithoutLocalExchange(child, context);
            if (child.isSerialOperatorOnBe(context)
                    && !(child instanceof LocalExchangeNode)
                    && !(node instanceof LocalExchangeNode)
                    && !(node instanceof ExchangeNode)
                    && !node.isSerialOperatorOnBe(context)) {
                org.apache.logging.log4j.LogManager.getLogger(AddLocalExchange.class).warn(
                        "Serial " + child.getClass().getSimpleName() + "(id=" + child.getId()
                        + ") feeds into non-serial " + node.getClass().getSimpleName()
                        + "(id=" + node.getId() + ") without LocalExchangeNode"
                        + " in fragment " + node.getFragment().getFragmentId()
                        + ". FE should insert LocalExchangeNode to restore parallelism.");
            }
        }
    }

    public static boolean isColocated(PlanNode plan) {
        if (plan instanceof AggregationNode) {
            return ((AggregationNode) plan).isColocate() && isColocated(plan.getChild(0));
        } else if (plan instanceof OlapScanNode) {
            return true;
        } else if (plan instanceof SelectNode) {
            return isColocated(plan.getChild(0));
        } else if (plan instanceof HashJoinNode) {
            return ((HashJoinNode) plan).isColocate()
                    && (isColocated(plan.getChild(0)) || isColocated(plan.getChild(1)));
        } else if (plan instanceof SetOperationNode) {
            if (!((SetOperationNode) plan).isColocate()) {
                return false;
            }
            for (PlanNode child : plan.getChildren()) {
                if (isColocated(child)) {
                    return true;
                }
            }
            return false;
        } else {
            return false;
        }
    }

    public static LocalExchangeType resolveExchangeType(LocalExchangeTypeRequire require,
            PlanTranslatorContext translatorContext, PlanNode parent, PlanNode child) {
        // Only generic RequireHash adapts to LOCAL_EXECUTION_HASH_SHUFFLE.
        // Explicit RequireSpecific (GLOBAL_EXECUTION_HASH_SHUFFLE, BUCKET_HASH_SHUFFLE, etc.)
        // must never be degraded — if they appear in an invalid context, the plan is wrong.
        //
        // Always prefer LOCAL_EXECUTION_HASH_SHUFFLE for FE-planned intra-fragment hash exchanges.
        // GLOBAL_EXECUTION_HASH_SHUFFLE requires shuffle_idx_to_instance_idx which may be empty
        // for fragments with non-hash sinks (UNPARTITIONED/MERGE). LOCAL_HASH is always safe
        // since it partitions by local instance count without needing external shuffle maps.
        if (require instanceof RequireHash) {
            return LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE;
        }
        return require.preferType();
    }
}
