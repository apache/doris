package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.BetweenPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;

/**
 * transfer between to compound.
 * for example:
 * a between c and d -> a >= c and a <= d
 */
public class BetweenToCompoundRule extends AbstractExpressionRewriteRule {

    public static final BetweenToCompoundRule INSTANCE = new BetweenToCompoundRule();

    @Override
    public Expression visitBetweenPredicate(BetweenPredicate expr, ExpressionRewriteContext context) {
        return new CompoundPredicate<>(NodeType.AND, new GreaterThanEqual<>(expr.getCompareExpr(), expr.getLowerBound()),
                new LessThanEqual<>(expr.getCompareExpr(), expr.getUpperBound()));
    }

    @Override
    public Expression visitNot(Not expr, ExpressionRewriteContext context) {
        if (expr.child() instanceof BetweenPredicate) {
            BetweenPredicate between = (BetweenPredicate) expr.child();
            return new CompoundPredicate<>(
                    NodeType.OR, new LessThan<>(between.getCompareExpr(), between.getLowerBound()),
                    new GreaterThan<>(between.getCompareExpr(), between.getUpperBound()));
        }
        return expr;
    }
}