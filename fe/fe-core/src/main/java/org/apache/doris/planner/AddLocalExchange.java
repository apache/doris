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
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
import org.apache.doris.planner.LocalExchangeNode.RequireHash;

/** AddLocalExchange */
public class AddLocalExchange {
    /** addLocalExchange with distributed plans, skipping single-instance fragments.
     *  BE's _plan_local_exchange checks _num_instances which is the per-BE instance count.
     *  With _num_instances<=1 all pipelines on that BE have 1 task so local exchange is a no-op.
     *  We must use the same per-BE semantics: skip when every BE has at most 1 instance.
     *  Using global instanceCount would insert LE for fragments where 2 BEs each have 1 instance
     *  (global=2, per-BE=1), causing pipeline task mismatch and deadlock. */
    public void addLocalExchange(FragmentIdMapping<DistributedPlan> distributedPlans,
            PlanTranslatorContext context) {
        String qid = context.getConnectContext() != null
                ? org.apache.doris.common.util.DebugUtil.printId(context.getConnectContext().queryId()) : "?";
        for (DistributedPlan plan : distributedPlans.values()) {
            PipelineDistributedPlan pipePlan = (PipelineDistributedPlan) plan;
            long maxPerBeInstances = pipePlan.getInstanceJobs().stream()
                    .collect(java.util.stream.Collectors.groupingBy(
                            j -> j.getAssignedWorker().id(), java.util.stream.Collectors.counting()))
                    .values().stream().mapToLong(Long::longValue).max().orElse(0);
            PlanFragment fragment = pipePlan.getFragmentJob().getFragment();
            boolean isPooling = pipePlan.getInstanceJobs().stream().anyMatch(
                    org.apache.doris.nereids.trees.plans.distribute.worker.job
                            .LocalShuffleAssignedJob.class::isInstance);
            if (maxPerBeInstances <= 1) {
                continue;
            }
            String rootBefore = fragment.getPlanRoot().getClass().getSimpleName()
                    + "#" + fragment.getPlanRoot().getId();
            addLocalExchangeForFragment(fragment, context);
            String rootAfter = fragment.getPlanRoot().getClass().getSimpleName()
                    + "#" + fragment.getPlanRoot().getId();
            org.apache.logging.log4j.LogManager.getLogger(AddLocalExchange.class).info(
                    "addLocalExchange: qid={} fragment={} maxPerBe={} pooling={} root={}→{} changed={}",
                    qid, fragment.getFragmentId(), maxPerBeInstances, isPooling,
                    rootBefore, rootAfter, !rootBefore.equals(rootAfter));
        }
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
