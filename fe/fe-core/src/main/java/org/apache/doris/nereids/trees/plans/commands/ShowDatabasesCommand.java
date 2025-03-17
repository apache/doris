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
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Comparator;
import java.util.List;
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
    private String dbName;

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
    private void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(catalog)) {
            catalog = ctx.getDefaultCatalog();
            if (Strings.isNullOrEmpty(catalog)) {
                catalog = InternalCatalog.INTERNAL_CATALOG_NAME;
            }
        }

        // extract dbName from where clause
        if (whereClause != null) {
            if (whereClause instanceof EqualTo) {
                EqualTo equalTo = (EqualTo) whereClause;
                if (equalTo.left() instanceof UnboundSlot && equalTo.right() instanceof Literal) {
                    dbName = ((Literal) equalTo.right()).getValue().toString();
                } else {
                    throw new AnalysisException("Only support where clause with schema_name");
                }
            } else {
                throw new AnalysisException("Only support where clause with schema_name");
            }
        }
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        List<List<String>> rows = Lists.newArrayList();
        CatalogIf catalogIf = ctx.getCatalog(catalog);
        if (catalogIf == null) {
            throw new AnalysisException("No catalog found with name " + catalog);
        }
        List<String> dbNames = catalogIf.getDbNames();
        if (!Strings.isNullOrEmpty(dbName)) {
            if (dbNames.contains(dbName)) {
                rows.add(Lists.newArrayList(dbName));
            }
            return new ShowResultSet(META_DATA, rows);
        }

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
        return new ShowResultSet(META_DATA, rows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowDatabasesCommand(this, context);
    }
}
