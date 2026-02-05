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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;

/**
 * AbstractMaterializedViewLimitRule
 */
public interface AbstractMaterializedViewLimitOrTopNRule {

    /**
     * Try rewrite limit node
     */
    default Plan tryRewriteLimit(LogicalLimit<Plan> queryLimitNode, LogicalLimit<Plan> viewLimitNode,
            Plan tmpRwritePlan, StructInfo queryStructInfo,
            MaterializationContext materializationContext) {
        if (queryLimitNode == null || viewLimitNode == null) {
            materializationContext.recordFailReason(queryStructInfo,
                    "query limit rewrite fail, queryLimitNode or viewLimitNode is null",
                    () -> String.format("queryLimitNode = %s,\n viewLimitNode = %s,\n",
                            queryLimitNode, viewLimitNode));
            return null;
        }
        Pair<Long, Long> limitAndOffset = rewriteLimitAndOffset(
                Pair.of(queryLimitNode.getLimit(), queryLimitNode.getOffset()),
                Pair.of(viewLimitNode.getLimit(), viewLimitNode.getOffset()));
        if (limitAndOffset == null) {
            materializationContext.recordFailReason(queryStructInfo,
                    "query limit rewrite fail, query limit is not consistent with view limit",
                    () -> String.format("query limit = %s,\n view limit = %s,\n",
                            queryLimitNode.treeString(),
                            viewLimitNode.treeString()));
            return null;
        }
        return new LogicalLimit<>(limitAndOffset.key(), limitAndOffset.value(), LimitPhase.GLOBAL, tmpRwritePlan);
    }

    /**
     * The key of pair is limit, the value of pair is offset
     * if return null, means cannot rewrite
     */
    static Pair<Long, Long> rewriteLimitAndOffset(Pair<Long, Long> queryLimitNode, Pair<Long, Long> viewLimitNode) {
        if (queryLimitNode == null || viewLimitNode == null) {
            return null;
        }
        long queryOffset = queryLimitNode.value();
        long queryLimit = queryLimitNode.key();

        long viewOffset = viewLimitNode.value();
        long viewLimit = viewLimitNode.key();
        if (queryOffset >= viewOffset && queryOffset + queryLimit <= viewOffset + viewLimit) {
            return Pair.of(queryLimit, queryOffset - viewOffset);
        }
        return null;
    }
}
