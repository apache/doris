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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;

/**
 * Implementation rule that convert logical aggregation to physical hash aggregation.
 */
public class LogicalAggToPhysicalHashAgg extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().thenApply(ctx -> {
            boolean useStreamAgg = !ctx.connectContext.getSessionVariable().disableStreamPreaggregations
                    && !ctx.root.getGroupByExpressions().isEmpty()
                    && !ctx.root.isFinalPhase();
            return new PhysicalAggregate<>(
                    ctx.root.getGroupByExpressions(),
                    ctx.root.getOutputExpressions(),
                    ctx.root.getPartitionExpressions(),
                    ctx.root.getAggPhase(),
                    useStreamAgg,
                    ctx.root.isFinalPhase(),
                    ctx.root.getLogicalProperties(),
                    ctx.root.child());
        }).toRule(RuleType.LOGICAL_AGG_TO_PHYSICAL_HASH_AGG_RULE);
    }
}
