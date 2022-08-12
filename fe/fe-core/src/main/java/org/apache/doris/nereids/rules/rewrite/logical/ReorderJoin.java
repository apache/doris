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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

/**
 * Try to eliminate cross join via finding join conditions in filters and change the join orders.
 * <p>
 * <pre>
 * For example:
 *
 * input:
 * SELECT * FROM t1, t2, t3 WHERE t1.id=t3.id AND t2.id=t3.id
 *
 * output:
 * SELECT * FROM t1 JOIN t3 ON t1.id=t3.id JOIN t2 ON t2.id=t3.id
 * </pre>
 * </p>
 * TODO: This is tested by SSB queries currently, add more `unit` test for this rule
 * when we have a plan building and comparing framework.
 */
public class ReorderJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(subTree(LogicalJoin.class, LogicalFilter.class)).thenApply(ctx -> {
            LogicalFilter<Plan> filter = ctx.root;
            if (!ctx.cascadesContext.getConnectContext().getSessionVariable()
                    .isEnableNereidsReorderToEliminateCrossJoin()) {
                return filter;
            }
            MultiJoin multiJoin = new MultiJoin();
            filter.accept(multiJoin, null);

            return multiJoin.reorderJoinsAccordingToConditions();
        }).toRule(RuleType.REORDER_JOIN);
    }
}
