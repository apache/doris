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

import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rules.OrToIn.OrToInContext;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
public class OrToIn extends DefaultExpressionRewriter<OrToInContext> implements
        ExpressionRewriteRule<ExpressionRewriteContext> {

    public static final OrToIn INSTANCE = new OrToIn();

    private static final int REWRITE_OR_TO_IN_PREDICATE_THRESHOLD = 2;

    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        return expr.accept(this, new OrToInContext());
    }

    @Override
    public Expression visitCompoundPredicate(CompoundPredicate compoundPredicate, OrToInContext context) {
        if (compoundPredicate instanceof And) {
            return compoundPredicate.withChildren(compoundPredicate.child(0).accept(new OrToIn(),
                            new OrToInContext()),
                    compoundPredicate.child(1).accept(new OrToIn(),
                            new OrToInContext()));
        }
        List<Expression> expressions = ExpressionUtils.extractDisjunction(compoundPredicate);
        for (Expression expression : expressions) {
            if (expression instanceof EqualTo) {
                addSlotToLiteralMap((EqualTo) expression, context);
            }
        }
        List<Expression> rewrittenOr = new ArrayList<>();
        for (Map.Entry<NamedExpression, Set<Literal>> entry : context.slotNameToLiteral.entrySet()) {
            Set<Literal> literals = entry.getValue();
            if (literals.size() >= REWRITE_OR_TO_IN_PREDICATE_THRESHOLD) {
                InPredicate inPredicate = new InPredicate(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
                rewrittenOr.add(inPredicate);
            }
        }
        for (Expression expression : expressions) {
            if (!ableToConvertToIn(expression, context)) {
                rewrittenOr.add(expression);
            }
        }

        return ExpressionUtils.or(rewrittenOr);
    }

    private void addSlotToLiteralMap(EqualTo equal, OrToInContext context) {
        Expression left = equal.left();
        Expression right = equal.right();
        if (left instanceof NamedExpression && right instanceof Literal) {
            addSlotToLiteral((NamedExpression) left, (Literal) right, context);
        }
        if (right instanceof NamedExpression && left instanceof Literal) {
            addSlotToLiteral((NamedExpression) right, (Literal) left, context);
        }
    }

    private boolean ableToConvertToIn(Expression expression, OrToInContext context) {
        if (!(expression instanceof EqualTo)) {
            return false;
        }
        EqualTo equalTo = (EqualTo) expression;
        Expression left = equalTo.left();
        Expression right = equalTo.right();
        NamedExpression namedExpression = null;
        if (left instanceof NamedExpression && right instanceof Literal) {
            namedExpression = (NamedExpression) left;
        }
        if (right instanceof NamedExpression && left instanceof Literal) {
            namedExpression = (NamedExpression) right;
        }
        return namedExpression != null
                && findSizeOfLiteralThatEqualToSameSlotInOr(namedExpression, context)
                >= REWRITE_OR_TO_IN_PREDICATE_THRESHOLD;
    }

    public void addSlotToLiteral(NamedExpression namedExpression, Literal literal, OrToInContext context) {
        Set<Literal> literals = context.slotNameToLiteral.computeIfAbsent(namedExpression, k -> new HashSet<>());
        literals.add(literal);
    }

    public int findSizeOfLiteralThatEqualToSameSlotInOr(NamedExpression namedExpression, OrToInContext context) {
        return context.slotNameToLiteral.getOrDefault(namedExpression, Collections.emptySet()).size();
    }

    /**
     * Context of OrToIn
     */
    public static class OrToInContext {
        public final Map<NamedExpression, Set<Literal>> slotNameToLiteral = new HashMap<>();

    }
}
