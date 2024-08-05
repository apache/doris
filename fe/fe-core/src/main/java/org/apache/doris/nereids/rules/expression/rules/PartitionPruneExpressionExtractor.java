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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruneExpressionExtractor.Context;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

import com.google.common.annotations.VisibleForTesting;

import java.util.Objects;
import java.util.Set;

/**
 * PartitionPruneExpressionExtractor
 *
 * This rewriter only used to extract the expression that can be used in partition pruning from
 *     the whole predicate expression.
 * The theory of extractor is pretty simple:
 * A:Sort the expression in two kinds:
 *  1. evaluable-expression (let's mark it as E).
 *      Expressions that can be evaluated in the partition pruning stage.
 *      In the other word: not contains non-partition slots or deterministic expression.
 *  2. un-evaluable-expression (let's mark it as UE).
 *      Expressions that can NOT be evaluated in the partition pruning stage.
 *      In the other word: contains non-partition slots or deterministic expression.
 *
 * B: Travel the predicate, only point on AND and OR operator, following the rule:
 *    (E and UE) -> (E and TRUE) -> E
 *    (UE and UE) -> TRUE
 *    (E and E) -> (E and E)
 *    (E or UE) -> TRUE
 *    (UE or UE) -> TRUE
 *    (E or E) -> (E or E)
 *
 * e.g.
 *    (part = 1 and non_part = 'a') or (part = 2)
 * -> (part = 1 and true) or (part = 2)
 * -> (part = 1) or (part = 2)
 *
 * It's better that do some expression optimize(like fold, eliminate etc.) on predicate before this step.
 */
public class PartitionPruneExpressionExtractor extends DefaultExpressionRewriter<Context> {
    private final ExpressionEvaluableDetector expressionEvaluableDetector;

    private PartitionPruneExpressionExtractor(Set<Slot> interestedSlots) {
        this.expressionEvaluableDetector = new ExpressionEvaluableDetector(interestedSlots);
    }

    /**
     * Extract partition prune expression from predicate
     */
    public static Expression extract(Expression predicate,
                                     Set<Slot> partitionSlots,
                                     CascadesContext cascadesContext) {
        predicate = predicate.accept(FoldConstantRuleOnFE.VISITOR_INSTANCE,
            new ExpressionRewriteContext(cascadesContext));
        PartitionPruneExpressionExtractor rewriter = new PartitionPruneExpressionExtractor(partitionSlots);
        Context context = new Context();
        Expression partitionPruneExpression = predicate.accept(rewriter, context);
        if (context.containsUnEvaluableExpression) {
            return BooleanLiteral.TRUE;
        }
        return partitionPruneExpression;
    }

    @Override
    public Expression visit(Expression originExpr, Context parentContext) {
        if (originExpr instanceof And) {
            return this.visitAnd((And) originExpr, parentContext);
        }
        if (originExpr instanceof Or) {
            return this.visitOr((Or) originExpr, parentContext);
        }

        parentContext.containsUnEvaluableExpression = !expressionEvaluableDetector.detect(originExpr);
        return originExpr;
    }

    @Override
    public Expression visitAnd(And node, Context parentContext) {
        // handle left node
        Context leftContext = new Context();
        Expression newLeft = node.left().accept(this, leftContext);
        // handle right node
        Context rightContext = new Context();
        Expression newRight = node.right().accept(this, rightContext);

        // if anyone of them is FALSE, the whole expression should be FALSE.
        if (newLeft == BooleanLiteral.FALSE || newRight == BooleanLiteral.FALSE) {
            return BooleanLiteral.FALSE;
        }

        // If left node contains non-partition slot or is TURE, just discard it.
        if (newLeft == BooleanLiteral.TRUE || leftContext.containsUnEvaluableExpression) {
            return rightContext.containsUnEvaluableExpression ? BooleanLiteral.TRUE : newRight;
        }

        // If right node contains non-partition slot or is TURE, just discard it.
        if (newRight == BooleanLiteral.TRUE || rightContext.containsUnEvaluableExpression) {
            return newLeft;
        }

        // both does not contains non-partition slot.
        return new And(newLeft, newRight);
    }

    @Override
    public Expression visitOr(Or node, Context parentContext) {
        // handle left node
        Context leftContext = new Context();
        Expression newLeft = node.left().accept(this, leftContext);
        // handle right node
        Context rightContext = new Context();
        Expression newRight = node.right().accept(this, rightContext);

        // if anyone of them is TRUE or contains non-partition slot, just return TRUE.
        if (newLeft == BooleanLiteral.TRUE || newRight == BooleanLiteral.TRUE
                || leftContext.containsUnEvaluableExpression || rightContext.containsUnEvaluableExpression) {
            return BooleanLiteral.TRUE;
        }

        return new Or(newLeft, newRight);
    }

    /**
     * Context
     */
    @VisibleForTesting
    public static class Context {
        private boolean containsUnEvaluableExpression;
    }

    /**
     * The detector only indicate that whether a predicate contains interested slots or not,
     * and do not change the predicate.
     */
    @VisibleForTesting
    public static class ExpressionEvaluableDetector extends DefaultExpressionRewriter<Context> {
        private final Set<Slot> partitionSlots;

        public ExpressionEvaluableDetector(Set<Slot> partitionSlots) {
            this.partitionSlots = Objects.requireNonNull(partitionSlots, "partitionSlots can not be null");
        }

        /**
         * Return true if expression does NOT contains un-evaluable expression.
         */
        @VisibleForTesting
        public boolean detect(Expression expression) {
            boolean containsUnEvaluableExpression = expression.anyMatch(
                    expr -> expr instanceof SubqueryExpr || (expr instanceof Slot && !partitionSlots.contains(expr)));
            return !containsUnEvaluableExpression;
        }
    }
}
