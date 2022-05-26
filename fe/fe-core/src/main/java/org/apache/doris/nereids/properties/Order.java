package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.trees.expressions.Expression;

public class Order {

    private Expression expr;

    private boolean isAsc;

    private boolean nullFirst;

    public Order(Expression expr, boolean isAsc, boolean nullFirst) {
        this.expr = expr;
        this.isAsc = isAsc;
        this.nullFirst = nullFirst;
    }

}
