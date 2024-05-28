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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.PreparedStatementContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Prepared Statement
 */
public class ExecuteCommand extends Command {
    private final String stmtName;
    private final PreparedCommand preparedCommand;
    private final StatementContext statementContext;

    public ExecuteCommand(String stmtName, PreparedCommand preparedCommand, StatementContext statementContext) {
        super(PlanType.UNKNOWN);
        this.stmtName = stmtName;
        this.preparedCommand = preparedCommand;
        this.statementContext = statementContext;
    }

    public String getStmtName() {
        return stmtName;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visit(this, context);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        PreparedStatementContext preparedStmtCtx = ctx.getPreparedStementContext(stmtName);
        if (null == preparedStmtCtx) {
            throw new AnalysisException(
                    "prepare statement " + stmtName + " not found,  maybe expired");
        }
        PreparedCommand preparedCommand = (PreparedCommand) preparedStmtCtx.command;
        LogicalPlanAdapter planAdapter = new LogicalPlanAdapter(preparedCommand.getInnerPlan(), executor.getContext()
                .getStatementContext());
        executor.setParsedStmt(planAdapter);
        // execute real statement
        executor.execute();
    }

    /**
     * return the sql representation contains real expr instead of placeholders
     */
    public String toSql() {
        // maybe slow
        List<Expression> realValueExpr = preparedCommand.params().stream()
                .map(placeholder -> statementContext.getIdToPlaceholderRealExpr().get(placeholder.getExprId()))
                .collect(Collectors.toList());
        return "EXECUTE `" + stmtName + "`"
                + realValueExpr.stream().map(Expression::toSql).collect(Collectors.joining(", ", " USING ", ""));
    }
}
