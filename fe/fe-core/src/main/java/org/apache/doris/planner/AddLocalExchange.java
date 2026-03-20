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

import java.util.List;

/** AddLocalExchange */
public class AddLocalExchange {
    public void addLocalExchange(List<PlanFragment> fragments, PlanTranslatorContext context) {
        for (PlanFragment fragment : fragments) {
            addLocalExchangeForFragment(fragment, context);
        }
    }

    /** addLocalExchange with distributed plans, skipping single-instance fragments (matching BE behavior) */
    public void addLocalExchange(FragmentIdMapping<DistributedPlan> distributedPlans,
            PlanTranslatorContext context) {
        for (DistributedPlan plan : distributedPlans.values()) {
            PipelineDistributedPlan pipePlan = (PipelineDistributedPlan) plan;
            int instanceCount = pipePlan.getInstanceJobs().size();
            // Match BE's early-return: if (_num_instances <= 1) return OK();
            if (instanceCount <= 1) {
                continue;
            }
            PlanFragment fragment = pipePlan.getFragmentJob().getFragment();
            addLocalExchangeForFragment(fragment, context);
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
        if (output.first != root) {
            fragment.setPlanRoot(output.first);
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
        // Only generic RequireHash adapts to LOCAL_EXECUTION_HASH_SHUFFLE based on context.
        // Explicit RequireSpecific (GLOBAL_EXECUTION_HASH_SHUFFLE, BUCKET_HASH_SHUFFLE, etc.)
        // must never be degraded — if they appear in an invalid context, the plan is wrong.
        if (require instanceof RequireHash
                && shouldUseLocalExecutionHash(translatorContext, parent, child)) {
            return LocalExchangeType.LOCAL_EXECUTION_HASH_SHUFFLE;
        }
        return require.preferType();
    }

    private static boolean shouldUseLocalExecutionHash(
            PlanTranslatorContext translatorContext, PlanNode parent, PlanNode child) {
        PlanFragment fragment = null;
        if (parent != null) {
            fragment = parent.getFragment();
        }
        if (fragment == null && child != null) {
            fragment = child.getFragment();
        }

        if (fragment != null && fragment.useSerialSource(translatorContext.getConnectContext())) {
            return true;
        }
        if (child instanceof ScanNode) {
            return true;
        }
        // For FE-planned intra-fragment hash exchanges, always prefer LOCAL_EXECUTION_HASH_SHUFFLE.
        // GLOBAL_EXECUTION_HASH_SHUFFLE requires shuffle_idx_to_instance_idx which may be empty
        // for fragments with non-hash sinks (UNPARTITIONED/MERGE). LOCAL_HASH is always safe
        // since it partitions by local instance count without needing external shuffle maps.
        return true;
    }
}
