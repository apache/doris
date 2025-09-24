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
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Replace null literal with false literal for condition expression.
 * Because in nereids, we use boolean type to represent three-value logic,
 * so we need to replace null literal with false literal for condition expression.
 * For example, in filter, join condition, case when predicate, etc.
 *
 * rule: if(null and a > 1, ...) => if(false and a > 1, ...)
 *       case when null and a > 1 then ... => case when false and a > 1 then ...
 *       null or (null and a > 1) or not(null) => false or (false and a > 1) or not(null)
 */
public class ReplaceNullWithFalseForCond implements ExpressionPatternRuleFactory {

    public static final ReplaceNullWithFalseForCond INSTANCE = new ReplaceNullWithFalseForCond(false);
    private final boolean replacedCaseThen;

    public ReplaceNullWithFalseForCond(boolean replacedCaseThen) {
        this.replacedCaseThen = replacedCaseThen;
    }

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(CaseWhen.class).then(this::simplify)
                        .toRule(ExpressionRuleType.REPLACE_NULL_WITH_FALSE_FOR_COND),
                matchesTopType(If.class).then(this::simplify)
                        .toRule(ExpressionRuleType.REPLACE_NULL_WITH_FALSE_FOR_COND)
        );
    }

    private Expression simplify(Expression expression) {
        return replace(expression, replacedCaseThen);
    }

    /**
     * replace null which its ancestors are all AND/OR/CASE WHEN/IF CONDITION.
     * NOTICE: NOT's type is boolean too, if replace null to false in NOT, will get NOT(NULL) = NOT(FALSE) = TRUE,
     * but it is wrong,  NOT(NULL) = NULL. For null, only under the AND / OR, can rewrite it as FALSE.
     */
    public static Expression replace(Expression expression, boolean replaceCaseThen) {
        if (!expression.containsType(NullLiteral.class)) {
            return expression;
        }
        if (expression.isNullLiteral()) {
            DataType dataType = expression.getDataType();
            if (dataType.isBooleanType() || dataType.isNullType()) {
                return BooleanLiteral.FALSE;
            }
        } else if (expression instanceof CompoundPredicate) {
            // process AND / OR
            ImmutableList.Builder<Expression> builder
                    = ImmutableList.builderWithExpectedSize(expression.children().size());
            for (Expression child : expression.children()) {
                builder.add(replace(child, replaceCaseThen));
            }
            List<Expression> newChildren = builder.build();
            if (!newChildren.equals(expression.children())) {
                return expression.withChildren(builder.build());
            }
        } else if (expression instanceof CaseWhen) {
            CaseWhen caseWhen = (CaseWhen) expression;
            ImmutableList.Builder<WhenClause> whenClausesBuilder
                    = ImmutableList.builderWithExpectedSize(caseWhen.getWhenClauses().size());
            for (WhenClause whenClause : caseWhen.getWhenClauses()) {
                Expression newOperand = replace(whenClause.getOperand(), true);
                Expression newResult = whenClause.getResult();
                if (replaceCaseThen) {
                    newResult = replace(whenClause.getResult(), true);
                }
                whenClausesBuilder.add(new WhenClause(newOperand, newResult));
            }
            List<WhenClause> newWhenClauses = whenClausesBuilder.build();
            Optional<Expression> newDefaultValueOpt = caseWhen.getDefaultValue();
            if (caseWhen.getDefaultValue().isPresent() && replaceCaseThen) {
                newDefaultValueOpt = Optional.of(replace(caseWhen.getDefaultValue().get(), true));
            }
            if (!newWhenClauses.equals(caseWhen.getWhenClauses())
                    || !newDefaultValueOpt.equals(caseWhen.getDefaultValue())) {
                return newDefaultValueOpt
                        .map(defaultValue -> new CaseWhen(newWhenClauses, defaultValue))
                        .orElseGet(() -> new CaseWhen(newWhenClauses));
            }
        } else if (expression instanceof If) {
            If ifExpr = (If) expression;
            Expression newCondition = replace(ifExpr.getCondition(), true);
            Expression newTrueValue = ifExpr.getTrueValue();
            Expression newFalseValue = ifExpr.getFalseValue();
            if (replaceCaseThen) {
                newTrueValue = replace(newTrueValue, true);
                newFalseValue = replace(newFalseValue, true);
            }
            if (!newCondition.equals(ifExpr.getCondition())
                    || !newTrueValue.equals(ifExpr.getTrueValue())
                    || !newFalseValue.equals(ifExpr.getFalseValue())) {
                return new If(newCondition, newTrueValue, newFalseValue);
            }
        }

        return expression;
    }
}
