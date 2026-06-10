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
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.MysqlColType;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.OlapGroupCommitInsertExecutor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSqlCache;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.PointQueryExecutor;
import org.apache.doris.qe.PreparedStatementContext;
import org.apache.doris.qe.ShortCircuitQueryContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Prepared Statement
 */
public class ExecuteCommand extends Command {
    private static final Logger LOG = LogManager.getLogger(ExecuteCommand.class);

    private final String stmtName;
    private final PrepareCommand prepareCommand;
    private final StatementContext statementContext;
    private Instant executionStartTime;

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
        StatementContext statementContext = preparedStmtCtx.getStatementContext();
        if (executionStartTime == null) {
            executionStartTime = Instant.now();
        }
        statementContext.resetStatementStartTime(executionStartTime);
        statementContext.setPrepareStage(false);
        statementContext.setIsInsert(false);
        LogicalPlan logicalPlan = prepareCommand.getLogicalPlan();
        if (logicalPlan instanceof LogicalSqlCache) {
            throw new AnalysisException("Unsupported sql cache for server prepared statement");
        }
        if (logicalPlan instanceof InsertIntoTableCommand
                || logicalPlan instanceof InsertOverwriteTableCommand
                || logicalPlan instanceof UpdateCommand) {
            statementContext.setIsInsert(true);
        }
        LogicalPlanAdapter planAdapter = new LogicalPlanAdapter(logicalPlan, statementContext);
        executor.setStatementContext(statementContext);
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
            applySetVarHints(logicalPlan, statementContext);
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
        if (statementContext.isShortCircuitQuery() && hasShortCircuitContext && !shortCircuitContextReusable) {
            statementContext = refreshPreparedPlan(preparedStmtCtx, executor, prepareCommand, statementContext);
        } else {
            statementContext.setShortCircuitQueryContext(null);
        }
        // Drop the previously cached short-circuit context: either it was reusable and returned
        // early above, has just been refreshed here, or is stale and we are about to re-plan.
        preparedStmtCtx.shortCircuitQueryContext = Optional.empty();
        executor.execute();
        if (executor.getContext().getStatementContext().isShortCircuitQuery()) {
            // cache short-circuit plan
            preparedStmtCtx.shortCircuitQueryContext = Optional.of(
                    new ShortCircuitQueryContext(executor.planner(), (Queriable) executor.getParsedStmt()));
            statementContext.setShortCircuitQueryContext(preparedStmtCtx.shortCircuitQueryContext.get());
        }
    }

    static void applySetVarHints(LogicalPlan logicalPlan, StatementContext statementContext) {
        logicalPlan.foreach(plan -> {
            if (plan instanceof LogicalSelectHint) {
                for (SelectHint hint : ((LogicalSelectHint<?>) plan).getHints()) {
                    if (hint instanceof SelectHintSetVar) {
                        ((SelectHintSetVar) hint).setVarOnceInSql(statementContext);
                    }
                }
            }
            return false;
        });
    }

    private StatementContext refreshPreparedPlan(PreparedStatementContext preparedStmtCtx, StmtExecutor executor,
            PrepareCommand currentCommand, StatementContext currentStatementContext) {
        Map<PlaceholderId, Expression> boundPlaceholderValues = new HashMap<>(
                currentStatementContext.getIdToPlaceholderRealExpr());
        StatementContext originalStatementContext = executor.getContext().getStatementContext();
        StatementBase originalParsedStmt = executor.getParsedStmt();
        PrepareCommand originalCommand = preparedStmtCtx.command;
        StatementContext originalPreparedStatementContext = preparedStmtCtx.getStatementContext();
        boolean refreshed = false;
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("refresh prepared statement plan for short-circuit query, stmtName={}",
                        currentCommand.getName());
            }
            List<StatementBase> reparsedStatements = new NereidsParser().parseSQL(
                    currentCommand.getOriginalStmt().originStmt, executor.getContext().getSessionVariable());
            if (reparsedStatements.size() <= currentCommand.getOriginalStmt().idx) {
                throw new AnalysisException("Nereids parse failed. Parser get " + reparsedStatements.size()
                        + " statements, but we need at least " + (currentCommand.getOriginalStmt().idx + 1)
                        + " statements.");
            }
            StatementBase reparsedStmt = reparsedStatements.get(currentCommand.getOriginalStmt().idx);
            if (!(reparsedStmt instanceof LogicalPlanAdapter)) {
                throw new AnalysisException("Prepared statement must be parsed as LogicalPlanAdapter, but get "
                        + reparsedStmt.getClass().getName());
            }

            LogicalPlanAdapter reparsedAdapter = (LogicalPlanAdapter) reparsedStmt;
            StatementContext reparsedStatementContext = reparsedAdapter.getStatementContext();
            reparsedStatementContext.resetStatementStartTime(executionStartTime);
            List<Placeholder> reparsedPlaceholders = reparsedStatementContext.getPlaceholders();
            List<Placeholder> currentPlaceholders = currentCommand.getPlaceholders();
            if (reparsedPlaceholders.size() != currentPlaceholders.size()) {
                throw new AnalysisException("Prepared statement placeholder count changed after reparse, old: "
                        + currentPlaceholders.size() + ", new: " + reparsedPlaceholders.size());
            }

            reparsedStatementContext.setConnectContext(executor.getContext());
            reparsedStatementContext.setOriginStatement(currentCommand.getOriginalStmt());
            reparsedStatementContext.getIdToPlaceholderRealExpr().clear();

            List<Placeholder> refreshedPlaceholders = new ArrayList<>(reparsedPlaceholders.size());
            for (int i = 0; i < reparsedPlaceholders.size(); i++) {
                Placeholder oldPlaceholder = currentPlaceholders.get(i);
                Placeholder refreshedPlaceholder = withMysqlType(reparsedPlaceholders.get(i), oldPlaceholder);
                refreshedPlaceholders.add(refreshedPlaceholder);

                Expression boundValue = boundPlaceholderValues.get(oldPlaceholder.getPlaceholderId());
                if (boundValue != null) {
                    reparsedStatementContext.getIdToPlaceholderRealExpr().put(
                            refreshedPlaceholder.getPlaceholderId(), boundValue);
                }
            }
            reparsedStatementContext.setPlaceholders(refreshedPlaceholders);

            reparsedAdapter.setOrigStmt(currentCommand.getOriginalStmt());
            executor.setStatementContext(reparsedStatementContext);
            executor.setParsedStmt(reparsedAdapter);
            PrepareCommand refreshedCommand = new PrepareCommand(currentCommand.getName(),
                    reparsedAdapter.getLogicalPlan(), refreshedPlaceholders, currentCommand.getOriginalStmt());
            preparedStmtCtx.command = refreshedCommand;
            preparedStmtCtx.setStatementContext(reparsedStatementContext);
            refreshed = true;
            if (LOG.isDebugEnabled()) {
                LOG.debug("refreshed prepared statement plan for short-circuit query, stmtName={}",
                        currentCommand.getName());
            }
            return reparsedStatementContext;
        } finally {
            if (!refreshed) {
                executor.setStatementContext(originalStatementContext);
                executor.setParsedStmt(originalParsedStmt);
                preparedStmtCtx.command = originalCommand;
                preparedStmtCtx.setStatementContext(originalPreparedStatementContext);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("rollback prepared statement plan refresh for short-circuit query, stmtName={}",
                            currentCommand.getName());
                }
            }
        }
    }

    private Placeholder withMysqlType(Placeholder refreshedPlaceholder, Placeholder oldPlaceholder) {
        if (!oldPlaceholder.hasMysqlColType()) {
            return refreshedPlaceholder;
        }
        int mysqlTypeCode = oldPlaceholder.getMysqlTypeCode() & MysqlColType.MYSQL_CODE_MASK;
        if (oldPlaceholder.isUnsigned()) {
            mysqlTypeCode |= MysqlColType.UNSIGNED_MASK;
        }
        return refreshedPlaceholder.withNewMysqlColType(mysqlTypeCode);
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
