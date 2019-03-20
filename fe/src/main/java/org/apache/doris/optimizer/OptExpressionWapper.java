package org.apache.doris.optimizer;

/**
 * For deriving property or statistics when object derived is
 * MultiExpression and handling OptExpression.
 */
public class OptExpressionWapper {

    private final OptExpression expression;

    public OptExpressionWapper(OptExpression expression) {
        this.expression = expression;
    }

    public OptExpression getExpression() {
        return expression;
    }
}
