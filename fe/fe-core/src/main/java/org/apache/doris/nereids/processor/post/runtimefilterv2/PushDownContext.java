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

package org.apache.doris.nereids.processor.post.runtimefilterv2;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;

/**
 * PushDownContext
 */
public class PushDownContext {
    private RuntimeFilterContextV2 runtimeFilterContextV2;
    private int exprOrder;
    private boolean isNullSafe;
    private AbstractPhysicalPlan sourceNode;
    private Expression sourceExpression;
    private long buildNdvOrRowCount;
    private Expression targetExpression;

    public PushDownContext(
            RuntimeFilterContextV2 srfContext,
            AbstractPhysicalPlan sourceNode,
            Expression sourceExpression,
            long buildNdvOrRowCount,
            int exprOrder,
            Expression targetExpression) {
        this(srfContext, sourceNode, sourceExpression, buildNdvOrRowCount, exprOrder, false, targetExpression);
    }

    /**
     * constr
     */
    public PushDownContext(
            RuntimeFilterContextV2 srfContext,
            AbstractPhysicalPlan sourceNode,
            Expression sourceExpression,
            long buildNdvOrRowCount,
            int exprOrder,
            boolean isNullSafe,
            Expression targetExpression) {
        this.runtimeFilterContextV2 = srfContext;
        this.sourceNode = sourceNode;
        this.sourceExpression = sourceExpression;
        this.buildNdvOrRowCount = buildNdvOrRowCount;
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

    public RuntimeFilterContextV2 getRFContext() {
        return runtimeFilterContextV2;
    }

    public int getExprOrder() {
        return exprOrder;
    }

    public boolean isNullSafe() {
        return isNullSafe;
    }

    public PushDownContext withTarget(Expression newTarget) {
        return new PushDownContext(runtimeFilterContextV2,
                sourceNode, sourceExpression, buildNdvOrRowCount, exprOrder, newTarget);
    }

    public long getBuildNdvOrRowCount() {
        return buildNdvOrRowCount;
    }
}
