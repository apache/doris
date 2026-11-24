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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.ImmutableList;

/** Prefer the IVM delta as the build input after simplifying the join predicates. */
public class PreferIvmDeltaBuildSide extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalJoin()
                .when(join -> join.getJoinType().isLeftOuterJoin() || join.getJoinType().isLeftAntiJoin())
                .whenNot(PreferIvmDeltaBuildSide::shouldKeepJoinDirection)
                .thenApply(ctx -> {
                    LogicalJoin<?, ?> join = ctx.root;
                    if (ctx.cascadesContext.isLeadingDisableJoinReorder()
                            || !ctx.statementContext.getIvmRewriteContext()
                                    .filter(IvmRewriteContext::isIncrementalRefresh).isPresent()
                            || !readsDelta(join.left()) || readsDelta(join.right())) {
                        return null;
                    }
                    LogicalJoin<?, ?> swapped = join.swap();
                    return join.getJoinType().isLeftOuterJoin()
                            ? new LogicalProject<>(ImmutableList.copyOf(join.getOutput()), swapped) : swapped;
                })
                .toRule(RuleType.PREFER_IVM_DELTA_BUILD_SIDE);
    }

    private static boolean shouldKeepJoinDirection(LogicalJoin<?, ?> join) {
        return join.isMarkJoin() || JoinUtils.shouldNestedLoopJoin(join)
                || join.hasDistributeHint() || join.isLeadingJoin();
    }

    private static boolean readsDelta(Plan plan) {
        // Stream lowering can turn both incremental and snapshot reads into ordinary OLAP scans.
        return plan.anyMatch(node -> node instanceof LogicalOlapTableStreamScan
                && ((LogicalOlapTableStreamScan) node).isIncremental());
    }
}
