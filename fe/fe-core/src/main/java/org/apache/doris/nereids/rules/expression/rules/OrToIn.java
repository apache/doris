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

import org.apache.doris.nereids.rules.expression.ExpressionBottomUpRewriter;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Used to convert multi equalTo which has same slot and compare to a literal of disjunction to a InPredicate so that
 * it could be push down to storage engine.
 * example:
 * col1 = 1 or col1 = 2 or col1 = 3 and (col2 = 4)
 * col1 = 1 and col1 = 3 and col2 = 3 or col2 = 4
 * (col1 = 1 or col1 = 2) and  (col2 = 3 or col2 = 4)
 * <p>
 * would be converted to:
 * col1 in (1, 2) or col1 = 3 and (col2 = 4)
 * col1 = 1 and col1 = 3 and col2 = 3 or col2 = 4
 * (col1 in (1, 2) and (col2 in (3, 4)))
 * The generic type declaration and the overridden 'rewrite' function in this class may appear unconventional
 * because we need to maintain a map passed between methods in this class. But the owner of this module prohibits
 * adding any additional rule-specific fields to the default ExpressionRewriteContext. However, the entire expression
 * rewrite framework always passes an ExpressionRewriteContext of type context to all rules.
 */
public class OrToIn implements ExpressionPatternRuleFactory {

    public static final OrToIn INSTANCE = new OrToIn();

    public static final int REWRITE_OR_TO_IN_PREDICATE_THRESHOLD = 2;

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesTopType(Or.class).then(OrToIn::rewrite)
        );
    }

    public Expression rewriteTree(Expression expr, ExpressionRewriteContext context) {
        ExpressionBottomUpRewriter bottomUpRewriter = ExpressionRewrite.bottomUp(this);
        return bottomUpRewriter.rewrite(expr, context);
    }

    private static Expression rewrite(Or or) {
        // NOTICE: use linked hash map to avoid unstable order or entry.
        //  unstable order entry lead to dead loop since return expression always un-equals to original one.
        Map<NamedExpression, Set<Literal>> slotNameToLiteral = Maps.newLinkedHashMap();
        Map<Expression, NamedExpression> disConjunctToSlot = Maps.newLinkedHashMap();
        List<Expression> expressions = ExpressionUtils.extractDisjunction(or);
        for (Expression expression : expressions) {
            if (expression instanceof EqualTo) {
                handleEqualTo((EqualTo) expression, slotNameToLiteral, disConjunctToSlot);
            } else if (expression instanceof InPredicate) {
                handleInPredicate((InPredicate) expression, slotNameToLiteral, disConjunctToSlot);
            }
        }
        if (disConjunctToSlot.isEmpty()) {
            return or;
        }

        List<Expression> rewrittenOr = new ArrayList<>();
        for (Map.Entry<NamedExpression, Set<Literal>> entry : slotNameToLiteral.entrySet()) {
            Set<Literal> literals = entry.getValue();
            if (literals.size() >= REWRITE_OR_TO_IN_PREDICATE_THRESHOLD) {
                InPredicate inPredicate = new InPredicate(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
                rewrittenOr.add(inPredicate);
            }
        }
        for (Expression expression : expressions) {
            if (disConjunctToSlot.get(expression) == null) {
                rewrittenOr.add(expression);
            } else {
                Set<Literal> literals = slotNameToLiteral.get(disConjunctToSlot.get(expression));
                if (literals.size() < REWRITE_OR_TO_IN_PREDICATE_THRESHOLD) {
                    rewrittenOr.add(expression);
                }
            }
        }

        return ExpressionUtils.or(rewrittenOr);
    }

    private static void handleEqualTo(EqualTo equal, Map<NamedExpression, Set<Literal>> slotNameToLiteral,
                               Map<Expression, NamedExpression> disConjunctToSlot) {
        Expression left = equal.left();
        Expression right = equal.right();
        if (left instanceof NamedExpression && right instanceof Literal) {
            addSlotToLiteral((NamedExpression) left, (Literal) right, slotNameToLiteral);
            disConjunctToSlot.put(equal, (NamedExpression) left);
        } else if (right instanceof NamedExpression && left instanceof Literal) {
            addSlotToLiteral((NamedExpression) right, (Literal) left, slotNameToLiteral);
            disConjunctToSlot.put(equal, (NamedExpression) right);
        }
    }

    private static void handleInPredicate(InPredicate inPredicate, Map<NamedExpression, Set<Literal>> slotNameToLiteral,
                                   Map<Expression, NamedExpression> disConjunctToSlot) {
        // TODO a+b in (1,2,3...) is not supported now
        if (inPredicate.getCompareExpr() instanceof NamedExpression
                && inPredicate.getOptions().stream().allMatch(opt -> opt instanceof Literal)) {
            for (Expression opt : inPredicate.getOptions()) {
                addSlotToLiteral((NamedExpression) inPredicate.getCompareExpr(), (Literal) opt, slotNameToLiteral);
            }
            disConjunctToSlot.put(inPredicate, (NamedExpression) inPredicate.getCompareExpr());
        }
    }

    private static void addSlotToLiteral(NamedExpression namedExpression, Literal literal,
            Map<NamedExpression, Set<Literal>> slotNameToLiteral) {
        Set<Literal> literals = slotNameToLiteral.computeIfAbsent(namedExpression, k -> new LinkedHashSet<>());
        literals.add(literal);
    }
}
