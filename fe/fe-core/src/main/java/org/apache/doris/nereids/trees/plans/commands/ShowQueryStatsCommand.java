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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.query.QueryStatsUtil;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * show query stats command
 */
public class ShowQueryStatsCommand extends ShowCommand {
    private static final ShowResultSetMetaData SHOW_QUERY_STATS_CATALOG_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Database", ScalarType.createVarchar(20)))
            .addColumn(new Column("QueryCount", ScalarType.createVarchar(30))).build();
    private static final ShowResultSetMetaData SHOW_QUERY_STATS_DATABASE_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
            .addColumn(new Column("QueryCount", ScalarType.createVarchar(30))).build();
    private static final ShowResultSetMetaData SHOW_QUERY_STATS_TABLE_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Field", ScalarType.createVarchar(20)))
            .addColumn(new Column("QueryCount", ScalarType.createVarchar(30)))
            .addColumn(new Column("FilterCount", ScalarType.createVarchar(30))).build();
    private static final ShowResultSetMetaData SHOW_QUERY_STATS_TABLE_ALL_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("IndexName", ScalarType.createVarchar(20)))
            .addColumn(new Column("QueryCount", ScalarType.createVarchar(30))).build();
    private static final ShowResultSetMetaData SHOW_QUERY_STATS_TABLE_ALL_VERBOSE_META_DATA
            = ShowResultSetMetaData.builder().addColumn(new Column("IndexName", ScalarType.createVarchar(20)))
            .addColumn(new Column("Field", ScalarType.createVarchar(20)))
            .addColumn(new Column("QueryCount", ScalarType.createVarchar(30)))
            .addColumn(new Column("FilterCount", ScalarType.createVarchar(30))).build();

    private final String database;
    private final String table;
    private final boolean isAll;
    private final boolean isVerbose;

    /**
     * Constructor
     */
    public ShowQueryStatsCommand(String database, String table, boolean isAll, boolean isVerbose) {
        super(PlanType.SHOW_QUERY_STATS_COMMAND);
        this.database = database;
        this.table = table;
        this.isAll = isAll;
        this.isVerbose = isVerbose;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        String catalog = ctx.getCurrentCatalog().getName();

        if (database == null && table == null) {
            Map<String, Long> stats = QueryStatsUtil.getMergedCatalogStats(catalog);
            List<List<String>> rows = Lists.newArrayList();
            for (Map.Entry<String, Long> entry : stats.entrySet()) {
                List<String> row = Lists.newArrayList();
                row.add(entry.getKey());
                row.add(String.valueOf(entry.getValue()));
                rows.add(row);
            }
            return new ShowResultSet(SHOW_QUERY_STATS_CATALOG_META_DATA, rows);
        }

        if (table == null) {
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkDbPriv(ctx, catalog, database, PrivPredicate.SHOW)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR,
                        ctx.getQualifiedUser(), database);
            }

            Map<String, Long> stats = QueryStatsUtil.getMergedDatabaseStats(catalog, database);
            List<List<String>> rows = Lists.newArrayList();
            for (Map.Entry<String, Long> entry : stats.entrySet()) {
                if (Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ctx, catalog, database, entry.getKey(), PrivPredicate.SHOW)) {
                    List<String> row = Lists.newArrayList();
                    row.add(entry.getKey());
                    row.add(String.valueOf(entry.getValue()));
                    rows.add(row);
                }
            }
            return new ShowResultSet(SHOW_QUERY_STATS_DATABASE_META_DATA, rows);
        }

        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ctx, catalog, database, table, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW QUERY STATS",
                    ctx.getQualifiedUser(), ctx.getRemoteIP(), database + "." + table);
        }

        List<List<String>> rows = Lists.newArrayList();
        if (isAll) {
            if (isVerbose) {
                Map<String, Map<String, Pair<Long, Long>>> stats =
                        QueryStatsUtil.getMergedTableAllVerboseStats(catalog, database, table);
                for (Map.Entry<String, Map<String, Pair<Long, Long>>> indexEntry : stats.entrySet()) {
                    for (Map.Entry<String, Pair<Long, Long>> columnEntry : indexEntry.getValue().entrySet()) {
                        List<String> row = Lists.newArrayList();
                        row.add(indexEntry.getKey());
                        row.add(columnEntry.getKey());
                        row.add(String.valueOf(columnEntry.getValue().first));
                        row.add(String.valueOf(columnEntry.getValue().second));
                        rows.add(row);
                    }
                }
                return new ShowResultSet(SHOW_QUERY_STATS_TABLE_ALL_VERBOSE_META_DATA, rows);
            } else {
                Map<String, Long> stats = QueryStatsUtil.getMergedTableAllStats(catalog, database, table);
                for (Map.Entry<String, Long> entry : stats.entrySet()) {
                    List<String> row = Lists.newArrayList();
                    row.add(entry.getKey());
                    row.add(String.valueOf(entry.getValue()));
                    rows.add(row);
                }
                return new ShowResultSet(SHOW_QUERY_STATS_TABLE_ALL_META_DATA, rows);
            }
        } else {
            Map<String, Pair<Long, Long>> stats = QueryStatsUtil.getMergedTableStats(catalog, database, table);
            for (Map.Entry<String, Pair<Long, Long>> entry : stats.entrySet()) {
                List<String> row = Lists.newArrayList();
                row.add(entry.getKey());
                row.add(String.valueOf(entry.getValue().first));
                row.add(String.valueOf(entry.getValue().second));
                rows.add(row);
            }
            return new ShowResultSet(SHOW_QUERY_STATS_TABLE_META_DATA, rows);
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowQueryStatsCommand(this, context);
    }
}
