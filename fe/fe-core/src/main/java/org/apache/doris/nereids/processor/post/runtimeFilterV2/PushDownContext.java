package org.apache.doris.nereids.processor.post.runtimeFilterV2;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;

public class PushDownContext {
    private RuntimeFilterV2Context RuntimeFilterV2Context;
    private int exprOrder;
    private boolean isNullSafe;
    private AbstractPhysicalPlan sourceNode;
    private Expression sourceExpression;
    private Expression targetExpression;

    public PushDownContext(
            RuntimeFilterV2Context srfContext,
            AbstractPhysicalPlan sourceNode,
            Expression sourceExpression,
            int exprOrder,
            Expression targetExpression) {
        this(srfContext, sourceNode, sourceExpression, exprOrder, false, targetExpression);
    }

    public PushDownContext(
            RuntimeFilterV2Context srfContext,
            AbstractPhysicalPlan sourceNode,
            Expression sourceExpression,
            int exprOrder,
            boolean isNullSafe,
            Expression targetExpression) {
        this.RuntimeFilterV2Context = srfContext;
        this.sourceNode = sourceNode;
        this.sourceExpression = sourceExpression;
        this.exprOrder = exprOrder;
        this.isNullSafe = isNullSafe;
        this.targetExpression = targetExpression;
    }

    public AbstractPhysicalPlan getSourceNode() {
        return sourceNode;
    }

    public Expression getSourceExpression() {
        return sourceExpression;
    }

    public Expression getTargetExpression() {
        return targetExpression;
    }

    public RuntimeFilterV2Context getRFContext() {
        return RuntimeFilterV2Context;
    }

    public int getExprOrder() {
        return exprOrder;
    }

    public boolean isNullSafe() {
        return isNullSafe;
    }

    public PushDownContext withTarget(Expression newTarget) {
        return new PushDownContext(RuntimeFilterV2Context, sourceNode, sourceExpression, exprOrder, newTarget);
    }
}
