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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.ResultRow;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents the command for SHOW CHARACTER SET
 */
public class ShowCharsetCommand extends ShowCommand {
    private static final ShowResultSetMetaData CHARSET_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Charset", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Description", ScalarType.createVarchar(60)))
                    .addColumn(new Column("Default collation", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Maxlen", ScalarType.createVarchar(10)))
                    .build();

    private final String likePattern;
    private final Expression whereClause;

    public ShowCharsetCommand(String likePattern, Expression whereClause) {
        super(PlanType.SHOW_CHARSET_COMMAND);
        this.likePattern = likePattern;
        this.whereClause = whereClause;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return CHARSET_META_DATA;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (whereClause != null) {
            return new ShowResultSet(getMetaData(), filterRows(ctx, executor));
        }
        List<List<String>> rows = Lists.newArrayList();
        if (likePattern != null && !PatternMatcherWrapper.createMysqlPattern(likePattern, false)
                .match(ctx.getSessionVariable().getCharsetServer())) {
            return new ShowResultSet(getMetaData(), rows);
        }
        List<String> row = Lists.newArrayList();
        // | utf8mb4 | UTF-8 Unicode | utf8mb4_general_ci | 4|
        row.add(ctx.getSessionVariable().getCharsetServer());
        row.add("UTF-8 Unicode");
        row.add(ctx.getSessionVariable().getCollationConnection());
        row.add("4");
        rows.add(row);

        return new ShowResultSet(getMetaData(), rows);
    }

    private List<List<String>> filterRows(ConnectContext ctx, StmtExecutor executor) throws UserException {
        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(ctx.createInternalQueryContext());
                StatementContext statementContext = ctx.getStatementContext()
                        .createShowQueryContext(scope.connectContext)) {
            ConnectContext internalContext = scope.connectContext;
            internalContext.setStatementContext(statementContext);

            List<NamedExpression> projects = ImmutableList.of(
                    new Alias(new StringLiteral(ctx.getSessionVariable().getCharsetServer()), "Charset"),
                    new Alias(new StringLiteral("UTF-8 Unicode"), "Description"),
                    new Alias(new StringLiteral(ctx.getSessionVariable().getCollationConnection()),
                            "Default collation"),
                    new Alias(new IntegerLiteral(4), "Maxlen"));
            LogicalOneRowRelation relation = new LogicalOneRowRelation(
                    StatementScopeIdGenerator.newRelationId(), projects);
            LogicalFilter<LogicalOneRowRelation> filter = new LogicalFilter<>(
                    ExpressionUtils.extractConjunctionToSet(whereClause), relation);
            LogicalPlanAdapter adapter = new LogicalPlanAdapter(new UnboundResultSink<>(filter), statementContext);
            adapter.setOrigStmt(executor.getOriginStmt());
            StmtExecutor internalExecutor = new StmtExecutor(internalContext, adapter);
            internalContext.setExecutor(internalExecutor);
            executor.setCancelDelegate(internalExecutor::cancel);
            // Publish the delegate before checking for cancellation that arrived before publication.
            Status cancelReason = executor.getPendingCancelReason();
            if (cancelReason != null) {
                throw new UserException(cancelReason.getErrorMsg());
            }
            return internalExecutor.executeInternalQuery().stream()
                    .map(ResultRow::getValues)
                    .collect(Collectors.toList());
        } finally {
            executor.clearCancelDelegate();
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCharsetCommand(this, context);
    }
}
