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

import org.apache.doris.analysis.Expr;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterType;

/**
 * RuntimeFilterV2
 */
public class RuntimeFilterV2 {
    private RuntimeFilterId id;
    private AbstractPhysicalPlan sourceNode;
    private Expression sourceExpression;
    private long buildNdvOrRowCount;
    private int exprOrder;
    private AbstractPhysicalPlan targetNode;
    private Expression targetExpression;
    private TRuntimeFilterType type;
    private TMinMaxRuntimeFilterType tMinMaxRuntimeFilterType = TMinMaxRuntimeFilterType.MIN_MAX;

    // translate
    private PlanNode legacyTargetNode;
    private Expr legacyTargetExpr;

    /**
     * constr
     */
    public RuntimeFilterV2(RuntimeFilterId id,
            AbstractPhysicalPlan sourceNode, Expression source, long buildNdvOrRowCount, int exprOrder,
            AbstractPhysicalPlan targetNode, Expression target, TRuntimeFilterType type) {
        this.sourceNode = sourceNode;
        this.sourceExpression = source;
        this.buildNdvOrRowCount = buildNdvOrRowCount;
        this.exprOrder = exprOrder;
        this.targetNode = targetNode;
        this.targetExpression = target;
        this.id = id;
        this.type = type;
    }

    public RuntimeFilterId getId() {
        return id;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RF").append(id.asInt()).append("[").append(type).append("]")
                .append("(").append(sourceExpression)
                .append("->").append(targetExpression).append(")");
        return sb.toString();
    }

    public RuntimeFilterId getRuntimeFilterId() {
        return id;
    }

    public AbstractPhysicalPlan getSourceNode() {
        return sourceNode;
    }

    public Expression getSourceExpression() {
        return sourceExpression;
    }

    public AbstractPhysicalPlan getTargetNode() {
        return targetNode;
    }

    public Expression getTargetExpression() {
        return targetExpression;
    }

    public TRuntimeFilterType getType() {
        return type;
    }

    public int getExprOrder() {
        return exprOrder;
    }

    public long getBuildNdvOrRowCount() {
        return buildNdvOrRowCount;
    }

    public void setMinMaxSubType(TMinMaxRuntimeFilterType tMinMaxRuntimeFilterType) {
        this.tMinMaxRuntimeFilterType = tMinMaxRuntimeFilterType;
    }

    public TMinMaxRuntimeFilterType getTMinMaxRuntimeFilterType() {
        return tMinMaxRuntimeFilterType;
    }

    public PlanNode getLegacyTargetNode() {
        return legacyTargetNode;
    }

    public Expr getLegacyTargetExpr() {
        return legacyTargetExpr;
    }

    public void setLegacyTargetNode(PlanNode legacyTargetNode) {
        this.legacyTargetNode = legacyTargetNode;
    }

    public void setLegacyTargetExpr(Expr legacyTargetExpr) {
        this.legacyTargetExpr = legacyTargetExpr;
    }

    /**
     * used for explain shape plan
     */
    public String shapeInfo() {
        String ignore = "";
        if (ConnectContext.get() != null
                && ConnectContext.get().getSessionVariable()
                .getIgnoredRuntimeFilterIds().contains(id.asInt())) {
            ignore = "(ignored)";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(ignore).append(" RF").append(id.asInt())
                .append("[").append(sourceExpression.toSql()).append("->").append(
                        targetExpression.toSql()).append("]");
        return sb.toString();
    }
}
