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
import org.apache.doris.nereids.analyzer.UnboundInlineTable;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;

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

        if (whereClause == null || rows.isEmpty()) {
            return new ShowResultSet(getMetaData(), rows);
        }

        // Apply WHERE only after the existing catalog privilege filter has produced the rows.
        List<List<NamedExpression>> values = new ArrayList<>(rows.size());
        for (List<String> row : rows) {
            values.add(ImmutableList.of(
                    new Alias(new BigIntLiteral(Long.parseLong(row.get(0))), "CatalogId"),
                    new Alias(new StringLiteral(row.get(1)), "CatalogName"),
                    new Alias(new StringLiteral(row.get(2)), "Type"),
                    new Alias(new StringLiteral(row.get(3)), "IsCurrent"),
                    new Alias(new StringLiteral(row.get(4)), "CreateTime"),
                    new Alias(new StringLiteral(row.get(5)), "LastUpdateTime"),
                    new Alias(new StringLiteral(row.get(6)), "Comment"),
                    new Alias(new StringLiteral(row.get(7)), "ErrorMsg")));
        }
        LogicalPlan plan = new LogicalFilter<>(ImmutableSet.of(whereClause), new UnboundInlineTable(values));
        plan = new LogicalSort<>(ImmutableList.of(new OrderKey(new UnboundSlot("CatalogName"), true, true)), plan);
        rows = Utils.executePlan(ctx, executor, new UnboundResultSink<>(plan));
        return new ShowResultSet(getMetaData(), rows);
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
