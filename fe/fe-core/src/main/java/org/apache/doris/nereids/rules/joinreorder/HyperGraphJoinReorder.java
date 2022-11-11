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

package org.apache.doris.nereids.rules.joinreorder;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.joinreorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

/**
 * This rule is for Join Reorder (non Cascades Transfrom Join Reorder).
 */
public class HyperGraphJoinReorder extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        // TODO: we need a pattern to match a subtree of join and mark the node in this tree ordered
        return logicalJoin(
                subTree(LogicalJoin.class, LogicalProject.class),
                subTree(LogicalJoin.class, LogicalProject.class))
                .thenApply(ctx -> {
                    LogicalJoin<? extends Plan, ? extends Plan> rootJoin = ctx.root;
                    // TODO: check mark.
                    HyperGraph graph = HyperGraph.fromPlan(rootJoin);
                    if (graph.optimize()) {
                        return graph.toPlan();
                    }
                    return null;
                }).toRule(RuleType.JOIN_REORDER);
    }
}
