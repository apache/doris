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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundInlineTable;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Represents the command for show all catalog or desc the specific catalog.
 */
public class ShowCatalogCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA_ALL =
            ShowResultSetMetaData.builder().addColumn(new Column("CatalogId", ScalarType.BIGINT))
                    .addColumn(new Column("CatalogName", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Type", ScalarType.createStringType()))
                    .addColumn(new Column("IsCurrent", ScalarType.createStringType()))
                    .addColumn(new Column("CreateTime", ScalarType.createStringType()))
                    .addColumn(new Column("LastUpdateTime", ScalarType.createStringType()))
                    .addColumn(new Column("Comment", ScalarType.createStringType()))
                    .addColumn(new Column("ErrorMsg", ScalarType.createStringType()))
                    .build();

    private static final ShowResultSetMetaData META_DATA_SPECIFIC =
            ShowResultSetMetaData.builder()
                .addColumn(new Column("Key", ScalarType.createStringType()))
                .addColumn(new Column("Value", ScalarType.createStringType()))
                .build();

    private final String catalogName;
    private final String pattern;
    private final Expression whereClause;

    public ShowCatalogCommand(String catalogName, String pattern, Expression whereClause) {
        super(PlanType.SHOW_CATALOG_COMMAND);
        this.catalogName = catalogName;
        this.pattern = pattern;
        this.whereClause = whereClause;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        List<List<String>> rows = Env.getCurrentEnv().getCatalogMgr()
                .showCatalogs(catalogName, pattern, ctx.getCurrentCatalog() != null
                    ? ctx.getCurrentCatalog().getName() : null);

        if (whereClause == null) {
            return new ShowResultSet(getMetaData(), rows);
        }

        rows = executeFilter(ctx, rows);
        return new ShowResultSet(getMetaData(), rows);
    }

    private List<NamedExpression> toExpressions(List<String> row) {
        return ImmutableList.of(
                new Alias(new BigIntLiteral(Long.parseLong(row.get(0))), "CatalogId"),
                stringAlias(row.get(1), "CatalogName"),
                stringAlias(row.get(2), "Type"),
                stringAlias(row.get(3), "IsCurrent"),
                stringAlias(row.get(4), "CreateTime"),
                stringAlias(row.get(5), "LastUpdateTime"),
                stringAlias(row.get(6), "Comment"),
                stringAlias(row.get(7), "ErrorMsg"));
    }

    private List<NamedExpression> nullRow() {
        return ImmutableList.of(
                new Alias(new NullLiteral(BigIntType.INSTANCE), "CatalogId"),
                new Alias(new NullLiteral(StringType.INSTANCE), "CatalogName"),
                new Alias(new NullLiteral(StringType.INSTANCE), "Type"),
                new Alias(new NullLiteral(StringType.INSTANCE), "IsCurrent"),
                new Alias(new NullLiteral(StringType.INSTANCE), "CreateTime"),
                new Alias(new NullLiteral(StringType.INSTANCE), "LastUpdateTime"),
                new Alias(new NullLiteral(StringType.INSTANCE), "Comment"),
                new Alias(new NullLiteral(StringType.INSTANCE), "ErrorMsg"));
    }

    private NamedExpression stringAlias(String value, String name) {
        Expression literal = value == null || FeConstants.null_string.equals(value)
                ? new NullLiteral(StringType.INSTANCE) : new StringLiteral(value);
        return new Alias(literal, name);
    }

    private List<List<String>> executeFilter(ConnectContext outerContext, List<List<String>> rows) throws Exception {
        ConnectContext filterContext = buildFilterContext(outerContext);
        try (AutoCloseConnectContext ignored = new AutoCloseConnectContext(filterContext)) {
            // The SHOW predicate must not replace the statement, state, or query id being audited by the caller.
            if (rows.isEmpty()) {
                executeFilterPlan(filterContext, filterPlan(nullRow(), true));
                return rows;
            }

            // Multi-row VALUES plans require BE execution. Filtering one row at a time keeps this path in FE
            // and preserves the CatalogMgr order instead of applying a different SQL string collation.
            List<List<String>> filteredRows = new ArrayList<>(rows.size());
            for (List<String> row : rows) {
                filteredRows.addAll(executeFilterPlan(filterContext, filterPlan(toExpressions(row), false)));
            }
            return filteredRows;
        }
    }

    private LogicalPlan filterPlan(List<NamedExpression> value, boolean empty) {
        LogicalPlan input = new UnboundInlineTable(ImmutableList.of(value));
        if (empty) {
            // Keep a typed zero-row relation so invalid WHERE expressions are still rejected.
            input = new LogicalLimit<>(0, 0, LimitPhase.ORIGIN, input);
        }
        return new UnboundResultSink<>(new LogicalFilter<>(ImmutableSet.of(whereClause), input));
    }

    private List<List<String>> executeFilterPlan(ConnectContext filterContext, LogicalPlan plan) throws Exception {
        StatementContext statementContext = new StatementContext(
                filterContext, new OriginStatement(toString(), 0));
        filterContext.setStatementContext(statementContext);
        LogicalPlanAdapter adapter = new LogicalPlanAdapter(plan, statementContext);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        planner.plan(adapter, filterContext.getSessionVariable().toThrift());
        Optional<ResultSet> resultSet = planner.handleQueryInFe(adapter);
        if (!resultSet.isPresent()) {
            throw new IllegalStateException("SHOW CATALOGS filter must be executable in FE");
        }
        return resultSet.get().getResultRows();
    }

    private ConnectContext buildFilterContext(ConnectContext outerContext) {
        ConnectContext filterContext = new ConnectContext();
        filterContext.setSessionVariable(VariableMgr.cloneSessionVariable(outerContext.getSessionVariable()));
        filterContext.setEnv(Env.getCurrentEnv());
        filterContext.changeDefaultCatalog(outerContext.getDefaultCatalog());
        filterContext.setDatabase(outerContext.getDatabase());
        filterContext.setCurrentUserIdentity(outerContext.getCurrentUserIdentity());
        filterContext.setAuthenticatedPrincipal(outerContext.getAuthenticatedPrincipal());
        filterContext.setAuthenticatedRoles(outerContext.getAuthenticatedRoles());
        filterContext.setRemoteIP(outerContext.getRemoteIP());
        filterContext.setNoAuth(outerContext.getNoAuth());
        filterContext.setIsTempUser(outerContext.getIsTempUser());
        UUID uuid = UUID.randomUUID();
        filterContext.setQueryId(new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
        filterContext.setStartTime();
        return filterContext;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCatalogCommand(this, context);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW");

        if (catalogName != null) {
            sb.append(" CATALOG ");
            sb.append(catalogName);
        } else {
            sb.append(" CATALOGS");

            if (pattern != null) {
                sb.append(" LIKE ");
                sb.append("'");
                sb.append(pattern);
                sb.append("'");
            } else if (whereClause != null) {
                sb.append(" WHERE ");
                sb.append(whereClause.toSql());
            }
        }

        return sb.toString();
    }

    public ShowResultSetMetaData getMetaData() {
        if (catalogName == null) {
            return META_DATA_ALL;
        } else {
            return META_DATA_SPECIFIC;
        }
    }
}
