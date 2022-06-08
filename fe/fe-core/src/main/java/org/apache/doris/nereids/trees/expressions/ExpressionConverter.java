package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.analysis.Expr;

public class ExpressionConverter {

    public static ExpressionConverter converter = new ExpressionConverter();

    // TODO: implement this, besides if expression is a slot, should set the slotId to
    //       converted the org.apache.doris.analysis.Expr
    public Expr convert(Expression expression) {
        return null;
    }
}
