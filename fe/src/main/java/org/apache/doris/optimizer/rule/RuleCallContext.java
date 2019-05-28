package org.apache.doris.optimizer.rule;

import com.google.common.collect.Lists;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.base.OptColumnRefFactory;

import java.util.List;

public final class RuleCallContext {

    private final OptExpression origin;
    private final List<OptExpression> newExprs;
    private final OptColumnRefFactory factory;

    public RuleCallContext(OptExpression origin, OptColumnRefFactory factory) {
        this.origin = origin;
        this.factory = factory;
        this.newExprs = Lists.newArrayList();
    }

    public OptExpression getOrigin() {
        return origin;
    }

    public void addNewExpr(OptExpression newExpr) {
        this.newExprs.add(newExpr);
    }
    public List<OptExpression> getNewExpr() { return this.newExprs; }
    public OptColumnRefFactory getColumnRefFactor() {
        return factory;
    }
}
