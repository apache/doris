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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.List;

/**
 * Check match expression
 */
public class CheckMatchExpression extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalOlapScan())
                .when(filter -> ExpressionUtils.containsType(filter.getExpressions(), Match.class))
                .then(this::checkChildren)
                .toRule(RuleType.CHECK_MATCH_EXPRESSION);
    }

    private Plan checkChildren(LogicalFilter<? extends Plan> filter) {
        List<Expression> expressions = filter.getExpressions();
        for (Expression expr : expressions) {
            if (expr instanceof Match) {
                Match matchExpression = (Match) expr;
                boolean isSlotReference = matchExpression.left() instanceof SlotReference;
                boolean isCastChildWithSlotReference = (matchExpression.left() instanceof Cast
                            && matchExpression.left().child(0) instanceof SlotReference);
                if (!(isSlotReference || isCastChildWithSlotReference)
                        || !(matchExpression.right() instanceof Literal)) {
                    throw new AnalysisException(String.format("Only support match left operand is SlotRef,"
                            + " right operand is Literal. But meet expression %s", matchExpression));
                }
            }
        }
        return filter;
    }
}
