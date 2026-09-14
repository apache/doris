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

import org.apache.doris.analysis.Queriable;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.OlapGroupCommitInsertExecutor;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSqlCache;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.PointQueryExecutor;
import org.apache.doris.qe.PreparedStatementContext;
import org.apache.doris.qe.ShortCircuitQueryContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Prepared Statement
 */
public class ExecuteCommand extends Command {
    private final String stmtName;
    private final PrepareCommand prepareCommand;
    private final StatementContext statementContext;

    public ExecuteCommand(String stmtName, PrepareCommand prepareCommand, StatementContext statementContext) {
        super(PlanType.EXECUTE_COMMAND);
        this.stmtName = stmtName;
        this.prepareCommand = prepareCommand;
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
        PrepareCommand prepareCommand = preparedStmtCtx.command;
        // Allocate a fresh StatementContext per EXECUTE so the per-statement state accumulated by
        // prior executions (bound tables, CTE maps, statistics, snapshots, ...) is released
        // promptly instead of living as long as the connection, which can OOM long-lived
        // connections. The necessary cross-execution state (placeholder bindings, comparison
        // slots, id generator positions, short-circuit flags) is carried over to the new context.
        StatementContext statementContext = preparedStmtCtx.nextStatementContext();
        statementContext.setPrepareStage(false);
        statementContext.setIsInsert(false);
        LogicalPlan logicalPlan = prepareCommand.getLogicalPlan();
        List<LogicalPlan> relationRoots = new ArrayList<>();
        if (logicalPlan instanceof InsertIntoTableCommand) {
            relationRoots.add(((InsertIntoTableCommand) logicalPlan).getLogicalQuery());
        } else if (logicalPlan instanceof InsertOverwriteTableCommand) {
            relationRoots.add(((InsertOverwriteTableCommand) logicalPlan).getLogicalQuery());
        } else if (logicalPlan instanceof UpdateCommand) {
            relationRoots.add(((UpdateCommand) logicalPlan).getLogicalQuery());
        } else if (logicalPlan instanceof DeleteFromUsingCommand) {
            relationRoots.add(((DeleteFromUsingCommand) logicalPlan).getLogicalQuery());
        } else if (logicalPlan instanceof DeleteFromCommand) {
            relationRoots.add(((DeleteFromCommand) logicalPlan).logicalQuery);
        } else if (logicalPlan instanceof MergeIntoCommand) {
            relationRoots.addAll(((MergeIntoCommand) logicalPlan).getRelationRoots());
        } else if (!(logicalPlan instanceof Command)) {
            relationRoots.add(logicalPlan);
        }
        // Commands hide their retained query trees from normal plan traversal. Reset every exposed
        // root so a later EXECUTE cannot reuse a relation-local snapshot from an earlier execution.
        for (int rootIndex = 0; rootIndex < relationRoots.size(); rootIndex++) {
            LogicalPlan relationRoot = relationRoots.get(rootIndex);
            for (UnboundRelation relation : relationRoot.<UnboundRelation>collectToList(
                    UnboundRelation.class::isInstance)) {
                TableScanParams scanParams = relation.getScanParams();
                if (scanParams != null) {
                    scanParams.resetResolvedMapParams();
                }
            }
            for (LogicalPlan plan : relationRoot.<LogicalPlan>collectToList(node -> true)) {
                for (Expression expression : plan.getExpressions()) {
                    for (SubqueryExpr subquery : expression.<SubqueryExpr>collectToList(
                            SubqueryExpr.class::isInstance)) {
                        // SubqueryExpr owns its query plan outside Plan.children(), so retained prepared
                        // commands need this explicit edge to clear nested relation-local snapshot state.
                        relationRoots.add(subquery.getQueryPlan());
                    }
                }
            }
        }
        if (logicalPlan instanceof LogicalSqlCache) {
            throw new AnalysisException("Unsupported sql cache for server prepared statement");
        }
        if (logicalPlan instanceof InsertIntoTableCommand
                || logicalPlan instanceof InsertOverwriteTableCommand
                || logicalPlan instanceof UpdateCommand) {
            ctx.getStatementContext().setIsInsert(true);
        }
        LogicalPlanAdapter planAdapter = new LogicalPlanAdapter(
                logicalPlan, executor.getContext().getStatementContext());
        executor.setParsedStmt(planAdapter);
        boolean hasShortCircuitContext = preparedStmtCtx.shortCircuitQueryContext.isPresent();
        boolean shortCircuitContextReusable = hasShortCircuitContext
                && preparedStmtCtx.shortCircuitQueryContext.get().isReusable(ctx);
        // Reuse the cached short-circuit plan only when table metadata is unchanged and the statement
        // has no nondeterministic functions. Otherwise fall back to the normal execution path below.
        if (statementContext.isShortCircuitQuery()
                && hasShortCircuitContext
                && shortCircuitContextReusable
                && !statementContext.hasNondeterministic()) {
            // The fresh per-execution context carries the short-circuit flag but not the cached plan.
            // Install the just-validated cache before the direct path: result sending reads it via
            // statementContext.getShortCircuitQueryContext(), and the fallback (building one from a
            // null planner, since this path skips planning) would NPE.
            statementContext.setShortCircuitQueryContext(preparedStmtCtx.shortCircuitQueryContext.get());
            PointQueryExecutor.directExecuteShortCircuitQuery(executor, preparedStmtCtx, statementContext);
            return;
        }
        if (ctx.getSessionVariable().enableGroupCommitFullPrepare) {
            if (preparedStmtCtx.groupCommitPlanner.isPresent()) {
                OlapGroupCommitInsertExecutor.fastAnalyzeGroupCommit(ctx, prepareCommand);
            } else {
                OlapGroupCommitInsertExecutor.analyzeGroupCommit(ctx, prepareCommand);
            }
            if (ctx.isGroupCommit()) {
                GroupCommitPlanner.executeGroupCommitInsert(ctx, preparedStmtCtx, statementContext);
                return;
            }
        }
        // execute real statement
        preparedStmtCtx.shortCircuitQueryContext = Optional.empty();
        statementContext.setShortCircuitQueryContext(null);
        executor.execute();
        if (executor.getContext().getStatementContext().isShortCircuitQuery()) {
            // cache short-circuit plan
            preparedStmtCtx.shortCircuitQueryContext = Optional.of(
                    new ShortCircuitQueryContext(executor.planner(), (Queriable) executor.getParsedStmt()));
            statementContext.setShortCircuitQueryContext(preparedStmtCtx.shortCircuitQueryContext.get());
        }
    }

    /**
     * return the sql representation contains real expr instead of placeholders
     */
    public String toSql() {
        // maybe slow
        List<Expression> realValueExpr = prepareCommand.getPlaceholders().stream()
                .map(placeholder -> statementContext.getIdToPlaceholderRealExpr().get(placeholder.getPlaceholderId()))
                .collect(Collectors.toList());
        return "EXECUTE `" + stmtName + "`"
                + realValueExpr.stream().map(Expression::toSql).collect(Collectors.joining(", ", " USING ", ""));
    }

    @Override
    public StmtType stmtType() {
        return StmtType.EXECUTE;
    }
}
