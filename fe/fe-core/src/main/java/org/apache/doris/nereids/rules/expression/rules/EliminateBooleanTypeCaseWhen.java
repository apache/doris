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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;

/**
 * if case when all branch value are true/false literal, and the ELSE default value can be any expression,
 * then can eliminate this case when.
 *
 * for example:
 * 1. case when c1 then true when c2 then false end => (c1 <=> true or (not (c2 <=> true) and null))
 * 2. if (c1, true, false) => c1 <=> true or false
 */
public class EliminateBooleanTypeCaseWhen implements ExpressionPatternRuleFactory {
    public static EliminateBooleanTypeCaseWhen INSTANCE = new EliminateBooleanTypeCaseWhen();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(CaseWhen.class)
                        .when(this::checkBooleanType)
                        .then(this::rewriteCaseWhen)
                        .toRule(ExpressionRuleType.ELIMINATE_BOOLEAN_TYPE_CASE_WHEN),
                matchesType(If.class)
                        .when(this::checkBooleanType)
                        .then(this::rewriteIf)
                        .toRule(ExpressionRuleType.ELIMINATE_BOOLEAN_TYPE_IF)
        );
    }

    private boolean checkBooleanType(Expression expression) {
        return expression.getDataType().isBooleanType();
    }

    private Expression rewriteCaseWhen(CaseWhen caseWhen) {
        Expression defaultValue = caseWhen.getDefaultValue().orElse(NullLiteral.BOOLEAN_INSTANCE);
        return rewrite(caseWhen.getWhenClauses(), defaultValue).orElse(caseWhen);
    }

    private Expression rewriteIf(If ifExpr) {
        List<WhenClause> whenClauses = ImmutableList.of(new WhenClause(ifExpr.getCondition(), ifExpr.getTrueValue()));
        Expression defaultValue = ifExpr.getFalseValue();
        return rewrite(whenClauses, defaultValue).orElse(ifExpr);
    }

    // for a branch, suppose the branches later it can rewrite to X, then given the branch:
    // 1. when c then true  ...,  will rewrite to (c <=> true  OR X),
    // 2. when c then false ...,  will rewrite to (not(c <=> true) AND X),
    // for the ELSE branch,  it can rewrite to `when true then defaultValue`,
    // process the branches from back to front, the default value process first, while the first when clause will
    // process last.
    private Optional<Expression> rewrite(List<WhenClause> whenClauses, Expression defaultValue) {
        for (WhenClause whenClause : whenClauses) {
            Expression result = whenClause.getResult();
            if (!(result instanceof BooleanLiteral)) {
                return Optional.empty();
            }
        }
        Expression result = defaultValue;
        try {
            for (int i = whenClauses.size() - 1; i >= 0; i--) {
                WhenClause whenClause = whenClauses.get(i);
                Expression condition = new NullSafeEqual(whenClause.getOperand(), BooleanLiteral.TRUE);
                if (whenClause.getResult().equals(BooleanLiteral.TRUE)) {
                    result = new Or(condition, result);
                } else {
                    result = new And(new Not(condition), result);
                }
            }
            result = flattenCompound(result);
        } catch (Exception e) {
            // expression may exceed expression limit
            return Optional.empty();
        }
        return Optional.of(result);
    }

    private Expression flattenCompound(Expression expression) {
        if (expression instanceof CompoundPredicate) {
            List<Expression> newChildren = Lists.newArrayList();
            for (Expression child : expression.children()) {
                Expression newChild = flattenCompound(child);
                if (newChild.getClass() == expression.getClass()) {
                    newChildren.addAll(newChild.children());
                } else {
                    newChildren.add(newChild);
                }
            }
            return ExpressionUtils.compound(expression instanceof And, newChildren);
        } else {
            return expression;
        }
    }
}
