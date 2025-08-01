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
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.AliasInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * ShowDatabasesCommand
 */
public class ShowDatabasesCommand extends ShowCommand {
    private static final String DB_COL = "Database";
    private static final String ORI_DB_COL = "SCHEMA_NAME";
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
                    .addColumn(new Column(DB_COL, ScalarType.createVarchar(20)))
                    .build();

    private String catalog;
    private final String likePattern;
    private final Expression whereClause;

    /**
     * ShowDatabasesCommand
     */
    public ShowDatabasesCommand(String catalog, String likePattern, Expression whereClause) {
        super(PlanType.SHOW_DATABASES_COMMAND);
        this.catalog = catalog;
        this.likePattern = likePattern;
        this.whereClause = whereClause;
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(catalog)) {
            catalog = ctx.getDefaultCatalog();
            if (Strings.isNullOrEmpty(catalog)) {
                catalog = InternalCatalog.INTERNAL_CATALOG_NAME;
            }
        }
    }

    /**
     * replaceColumnNameVisitor
     * replace column name to real column name
     */
    private static class ReplaceColumnNameVisitor extends DefaultExpressionRewriter<Void> {
        @Override
        public Expression visitUnboundSlot(UnboundSlot slot, Void context) {
            if (slot.getName().toLowerCase(Locale.ROOT).equals(DB_COL.toLowerCase(Locale.ROOT))) {
                return UnboundSlot.quoted(ORI_DB_COL);
            }
            return slot;
        }
    }

    private ShowResultSet execute(ConnectContext ctx, StmtExecutor executor, String whereClause) {
        List<AliasInfo> selectList = new ArrayList<>();
        selectList.add(AliasInfo.of(ORI_DB_COL, DB_COL));

        TableNameInfo fullTblName = new TableNameInfo(catalog, InfoSchemaDb.DATABASE_NAME, "schemata");

        LogicalPlan plan = Utils.buildLogicalPlan(selectList, fullTblName, whereClause);
        List<List<String>> rows = Utils.executePlan(ctx, executor, plan);
        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        if (whereClause != null) {
            Expression rewrited = whereClause.accept(new ReplaceColumnNameVisitor(), null);
            String whereCondition = " WHERE `CATALOG_NAME` = '" + catalog
                    + "' AND " + rewrited.toSql();
            return execute(ctx, executor, whereCondition);
        }
        List<List<String>> rows = Lists.newArrayList();
        CatalogIf catalogIf = ctx.getCatalog(catalog);
        if (catalogIf == null) {
            throw new AnalysisException("No catalog found with name " + catalog);
        }
        List<String> dbNames = catalogIf.getDbNames();
        PatternMatcher matcher = null;
        if (likePattern != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(likePattern,
                    CaseSensibility.DATABASE.getCaseSensibility());
        }
        Set<String> dbNameSet = Sets.newTreeSet();
        for (String fullName : dbNames) {
            final String db = ClusterNamespace.getNameFromFullName(fullName);
            // Filter dbname
            if (matcher != null && !matcher.match(db)) {
                continue;
            }

            if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(), catalog,
                    fullName, PrivPredicate.SHOW)) {
                continue;
            }

            dbNameSet.add(db);
        }

        for (String dbName : dbNameSet) {
            rows.add(Lists.newArrayList(dbName));
        }

        rows.sort(Comparator.comparing(x -> x.get(0)));
        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowDatabasesCommand(this, context);
    }
}
