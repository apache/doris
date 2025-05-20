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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.query.QueryStatsUtil;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The command for show query stats
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

    TableNameInfo tableNameInfo;
    String dbName;
    boolean all;
    boolean verbose;
    List<List<String>> totalRows = new ArrayList<>();
    ShowQueryStatsType type;

    public ShowQueryStatsCommand(String dbName, TableNameInfo tableNameInfo, boolean all, boolean verbose) {
        super(PlanType.SHOW_QUERY_STATS_COMMAND);
        this.dbName = dbName;
        this.tableNameInfo = tableNameInfo;
        this.all = all;
        this.verbose = verbose;
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws UserException {
        if (StringUtils.isEmpty(dbName)) {
            dbName = ctx.getDatabase();
            type = ShowQueryStatsType.DATABASE;
        }
        String catalog = Env.getCurrentEnv().getCurrentCatalog().getName();
        if (tableNameInfo == null && StringUtils.isEmpty(dbName)) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "SHOW QUERY STATS");
            }
            Map<String, Long> result = QueryStatsUtil.getMergedCatalogStats(catalog);
            result.forEach((dbName, queryHit) -> {
                totalRows.add(Arrays.asList(dbName, String.valueOf(queryHit)));
            });
            type = ShowQueryStatsType.CATALOG;
            return;
        }
        if (tableNameInfo != null) {
            tableNameInfo.analyze(ctx);
            dbName = tableNameInfo.getDb();
        }
        DatabaseIf db = Env.getCurrentEnv().getCurrentCatalog().getDbOrDdlException(dbName);
        String ctlName = db.getCatalog().getName();
        if (tableNameInfo != null) {
            db.getTableOrDdlException(tableNameInfo.getTbl());
        }
        if (tableNameInfo == null) {
            Map<String, Long> stats = QueryStatsUtil.getMergedDatabaseStats(catalog, dbName);
            stats.forEach((tableName, queryHit) -> {
                if (Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), ctlName, dbName, tableName, PrivPredicate.SHOW)) {
                    if (Util.isTempTable(tableName)) {
                        if (Util.isTempTableInCurrentSession(tableName)) {
                            totalRows.add(Arrays.asList(Util.getTempTableDisplayName(tableName),
                                    String.valueOf(queryHit)));
                        }
                    } else {
                        totalRows.add(Arrays.asList(tableName, String.valueOf(queryHit)));
                    }
                }
            });
        } else {
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(ConnectContext.get(), tableNameInfo, PrivPredicate.SHOW)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW QUERY STATS",
                        ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                        dbName + ": " + tableNameInfo);
            }
            if (all && verbose) {
                type = ShowQueryStatsType.TABLE_ALL_VERBOSE;
                QueryStatsUtil.getMergedTableAllVerboseStats(catalog, dbName, tableNameInfo.getTbl())
                        .forEach((indexName, stat) -> {
                            final boolean[] firstRow = new boolean[] {true};
                            stat.forEach((col, statCount) -> {
                                totalRows.add(Arrays.asList(firstRow[0] ? indexName : "", col,
                                        String.valueOf(statCount.first), String.valueOf(statCount.second)));
                                firstRow[0] = false;
                            });
                        });
            } else if (all) {
                type = ShowQueryStatsType.TABLE_ALL;

                Map<String, Long> stats = QueryStatsUtil.getMergedTableAllStats(catalog,
                        dbName, tableNameInfo.getTbl());
                stats.forEach((indexName, queryHit) -> {
                    totalRows.add(Arrays.asList(indexName, String.valueOf(queryHit)));
                });
            } else if (verbose) {
                Preconditions.checkState(false, "verbose is not supported if all is not set");
            } else {
                type = ShowQueryStatsType.TABLE;
                QueryStatsUtil.getMergedTableStats(catalog, dbName, tableNameInfo.getTbl())
                        .forEach((col, statCount) -> {
                            totalRows.add(
                                    Arrays.asList(col, String.valueOf(statCount.first),
                                            String.valueOf(statCount.second)));
                        });
            }
        }
    }

    /**
     * MetaData
     */
    @Override
    public ShowResultSetMetaData getMetaData() {
        switch (type) {
            case CATALOG:
                return SHOW_QUERY_STATS_CATALOG_META_DATA;
            case DATABASE:
                return SHOW_QUERY_STATS_DATABASE_META_DATA;
            case TABLE:
                return SHOW_QUERY_STATS_TABLE_META_DATA;
            case TABLE_ALL:
                return SHOW_QUERY_STATS_TABLE_ALL_META_DATA;
            case TABLE_ALL_VERBOSE:
                return SHOW_QUERY_STATS_TABLE_ALL_VERBOSE_META_DATA;
            default:
                Preconditions.checkState(false);
                return null;
        }
    }

    /**
     * Show query statistics type
     */
    public enum ShowQueryStatsType {
        CATALOG, DATABASE, TABLE, TABLE_ALL, TABLE_ALL_VERBOSE
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        return new ShowResultSet(getMetaData(), totalRows);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowQueryStatsCommand(this, context);
    }
}
