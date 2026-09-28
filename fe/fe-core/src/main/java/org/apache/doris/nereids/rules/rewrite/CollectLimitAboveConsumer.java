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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Collect limit rows needed by CTE consumers.
 */
public class CollectLimitAboveConsumer implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalLimit(logicalCTEConsumer()).thenApply(ctx -> {
                    LogicalLimit<LogicalCTEConsumer> limit = ctx.root;
                    collectLimitRows(ctx.cascadesContext, limit, limit.child());
                    return ctx.root;
                }).toRule(RuleType.COLLECT_LIMIT_ABOVE_CTE_CONSUMER),
                logicalLimit(logicalProject(logicalCTEConsumer()))
                        .thenApply(ctx -> {
                            LogicalLimit<LogicalProject<LogicalCTEConsumer>> limit = ctx.root;
                            collectLimitRows(ctx.cascadesContext, limit, limit.child().child());
                            return ctx.root;
                        }).toRule(RuleType.COLLECT_LIMIT_ABOVE_CTE_CONSUMER)
        );
    }

    private void collectLimitRows(CascadesContext cascadesContext, LogicalLimit<?> limit,
            LogicalCTEConsumer cteConsumer) {
        cascadesContext.putConsumerIdToLimitRows(
                cteConsumer.getRelationId(), limit.getLimit() + limit.getOffset());
    }
}
