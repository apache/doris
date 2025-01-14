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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.Map;

/**
 * cancel load command
 */
public abstract class CancelCommand extends Command implements ForwardWithSync {
    public CancelCommand(PlanType type) {
        super(type);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {

    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return null;
    }

    /**
     * translate to legacy expr, which do not need complex expression and table columns
     */
    public Expr translateToLegacyExpr(ConnectContext ctx, Expression expression) {
        LogicalEmptyRelation plan = new LogicalEmptyRelation(
                ConnectContext.get().getStatementContext().getNextRelationId(),
                new ArrayList<>());
        CascadesContext cascadesContext = CascadesContext.initContext(ctx.getStatementContext(), plan,
                PhysicalProperties.ANY);
        PlanTranslatorContext planTranslatorContext = new PlanTranslatorContext(cascadesContext);
        ExpressionToExpr translator = new ExpressionToExpr();
        return expression.accept(translator, planTranslatorContext);
    }

    private static class ExpressionToExpr extends ExpressionTranslator {
        @Override
        public Expr visitUnboundSlot(UnboundSlot unboundSlot, PlanTranslatorContext context) {
            String inputCol = unboundSlot.getName();
            return new SlotRef(null, inputCol);
        }
    }

    /**
     * check where filter for cancel load/export commands
     * @param expression where clause
     * @param supportedColumns only these kind of columns is supported
     * @throws AnalysisException analyze exceptions
     */
    public void checkWhereFilter(Expression expression, Map<String, String> supportedColumns) throws AnalysisException {
        if (null == expression) {
            throw new AnalysisException("Where clause can't be null");
        } else if (expression instanceof Like) {
            likeCheck(expression, supportedColumns);
        } else if (expression instanceof BinaryOperator) {
            binaryCheck(expression, supportedColumns);
        } else if (expression instanceof CompoundPredicate) {
            compoundCheck(expression, supportedColumns);
        } else {
            throw new AnalysisException("Only support like/binary/compound predicate");
        }
    }

    private void checkColumn(Expression expr, boolean like, Map<String, String> supportedColumns)
            throws AnalysisException {
        if (!(expr.child(0) instanceof UnboundSlot)) {
            throw new AnalysisException("Current only support label and state, invalid column: "
                + expr.child(0).toSql());
        }
        String inputCol = ((UnboundSlot) expr.child(0)).getName();
        if (!supportedColumns.keySet().contains(inputCol.toLowerCase())) {
            throw new AnalysisException("Current only support label and state, invalid column: " + inputCol);
        }
        if (!(expr.child(1) instanceof StringLikeLiteral)) {
            throw new AnalysisException("Value must be a string");
        }

        String inputValue = ((StringLikeLiteral) expr.child(1)).getStringValue();
        if (Strings.isNullOrEmpty(inputValue)) {
            throw new AnalysisException("Value can't be null");
        }

        if (inputCol.equalsIgnoreCase("label")) {
            supportedColumns.put("label", inputValue);
        }

        if (inputCol.equalsIgnoreCase("state")) {
            if (like) {
                throw new AnalysisException("Only label can use like");
            }
            supportedColumns.put("state", inputValue);
        }
    }

    private void likeCheck(Expression expr, Map<String, String> supportedColumns) throws AnalysisException {
        checkColumn(expr, true, supportedColumns);
    }

    private void binaryCheck(Expression expr, Map<String, String> supportedColumns) throws AnalysisException {
        checkColumn(expr, false, supportedColumns);
    }

    private void compoundCheck(Expression expr, Map<String, String> supportedColumns) throws AnalysisException {
        // current only support label and state
        if (expr instanceof Not) {
            throw new AnalysisException("not support NOT operator");
        }
        for (int i = 0; i < 2; i++) {
            Expression child = expr.child(i);
            if (child instanceof CompoundPredicate) {
                throw new AnalysisException("not support where clause: " + expr.toSql());
            } else if (child instanceof Like) {
                likeCheck(child, supportedColumns);
            } else if (child instanceof BinaryOperator) {
                binaryCheck(child, supportedColumns);
            } else {
                throw new AnalysisException("Only support like/equalTo/And/Or predicate");
            }
        }
    }
}
