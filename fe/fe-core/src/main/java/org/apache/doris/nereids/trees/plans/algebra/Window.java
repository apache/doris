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

package org.apache.doris.nereids.trees.plans.algebra;

import org.apache.doris.analysis.AnalyticWindow;
import org.apache.doris.analysis.Expr;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundType;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameUnitsType;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * interface for LogicalWindow and PhysicalWindow
 */
public interface Window {

    List<NamedExpression> getWindowExpressions();

    /**
     * translate WindowFrame to AnalyticWindow
     */
    default AnalyticWindow translateWindowFrame(WindowFrame windowFrame, PlanTranslatorContext context) {
        FrameUnitsType frameUnits = windowFrame.getFrameUnits();
        FrameBoundary leftBoundary = windowFrame.getLeftBoundary();
        FrameBoundary rightBoundary = windowFrame.getRightBoundary();

        AnalyticWindow.Type type = frameUnits == FrameUnitsType.ROWS
                    ? AnalyticWindow.Type.ROWS : AnalyticWindow.Type.RANGE;

        AnalyticWindow.Boundary left = withFrameBoundary(leftBoundary, context);
        AnalyticWindow.Boundary right = withFrameBoundary(rightBoundary, context);

        return new AnalyticWindow(type, left, right);
    }

    /**
     * translate FrameBoundary to Boundary
     */
    default AnalyticWindow.Boundary withFrameBoundary(FrameBoundary boundary, PlanTranslatorContext context) {
        FrameBoundType boundType = boundary.getFrameBoundType();
        BigDecimal offsetValue = null;
        Expr e = null;
        if (boundary.hasOffset()) {
            Expression boundOffset = boundary.getBoundOffset().get();
            offsetValue = new BigDecimal(((Literal) boundOffset).getDouble());
            e = ExpressionTranslator.translate(boundOffset, context);
        }

        switch (boundType) {
            case UNBOUNDED_PRECEDING:
                return new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING, null);
            case UNBOUNDED_FOLLOWING:
                return new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.UNBOUNDED_FOLLOWING, null);
            case CURRENT_ROW:
                return new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.CURRENT_ROW, null);
            case PRECEDING:
                return new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.PRECEDING, e, offsetValue);
            case FOLLOWING:
                return new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.FOLLOWING, e, offsetValue);
            default:
                throw new AnalysisException("This WindowFrame hasn't be resolved in REWRITE");
        }
    }

    /**
     *
     * select rank() over (partition by A, B) as r, sum(x) over(A, C) as s from T;
     * A is a common partition key for all windowExpressions.
     * for a common Partition key A, we could push filter A=1 through this window.
     */
    default Set<SlotReference> getCommonPartitionKeyFromWindowExpressions() {
        ImmutableSet.Builder<SlotReference> commonPartitionKeySet = ImmutableSet.builder();
        Map<Expression, Integer> partitionKeyCount = Maps.newHashMap();
        for (Expression expr : getWindowExpressions()) {
            if (expr instanceof Alias && expr.child(0) instanceof WindowExpression) {
                WindowExpression winExpr = (WindowExpression) expr.child(0);
                for (Expression partitionKey : winExpr.getPartitionKeys()) {
                    int count = partitionKeyCount.getOrDefault(partitionKey, 0);
                    partitionKeyCount.put(partitionKey, count + 1);
                }
            }
        }
        int winExprCount = getWindowExpressions().size();
        for (Map.Entry<Expression, Integer> entry : partitionKeyCount.entrySet()) {
            if (entry.getValue() == winExprCount && entry.getKey() instanceof SlotReference) {
                SlotReference slot = (SlotReference) entry.getKey();
                if (this instanceof LogicalWindow) {
                    LogicalWindow lw = (LogicalWindow) this;
                    Set<Slot> equalSlots = lw.getLogicalProperties().getTrait().calEqualSet(slot);
                    for (Slot other : equalSlots) {
                        if (other instanceof SlotReference) {
                            commonPartitionKeySet.add((SlotReference) other);
                        }
                    }
                }
                commonPartitionKeySet.add((SlotReference) entry.getKey());
            }
        }
        return commonPartitionKeySet.build();
    }
}
