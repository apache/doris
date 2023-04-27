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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundType;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameBoundary;
import org.apache.doris.nereids.trees.expressions.WindowFrame.FrameUnitsType;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import java.math.BigDecimal;
import java.util.List;

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

}
