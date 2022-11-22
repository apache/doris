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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;

import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * eliminate group by constant, like:
 * select 1, 'str', count(*) from t group by t.id, 1, 'str', 3, 2;
 * transform to:
 * select 1, 'str', count(*) from t group by t.id.
 * firstly, we change the number to slot
 * secondly, we eliminate all constants.
 */
public class EliminateGroupByConstant extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalAggregate().thenApply(ctx -> {
            LogicalAggregate<GroupPlan> aggregate = ctx.root;
            List<Expression> groupByExprs = aggregate.getGroupByExpressions();
            Set<Expression> slotGroupByExprs = Sets.newHashSet();
            // List<Slot> outputExprs = aggregate.getOutput();
            for (Expression expression : groupByExprs) {
                if (expression instanceof IntegerLikeLiteral || expression.isSlot()) {
                    slotGroupByExprs.add(expression);
                }
            }
            return null;
        }).toRule(RuleType.ELIMINATE_GROUP_BY_CONSTANT);
    }
}
