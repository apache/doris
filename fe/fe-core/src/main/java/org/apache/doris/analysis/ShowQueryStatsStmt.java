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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.statistics.query.QueryStatsUtil;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ShowQueryStatsStmt extends ShowStmt {

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

    TableName tableName;
    String dbName;
    boolean all;
    boolean verbose;
    List<List<String>> totalRows;
    ShowQueryStatsType type;

    /**
     * for SHOW QUERY STATS FROM TABLE
     */
    public ShowQueryStatsStmt(TableName tableName, boolean all, boolean verbose) {
        this.tableName = tableName;
        this.all = all;
        this.verbose = verbose;
        this.totalRows = Lists.newArrayList();
    }

    /**
     * for SHOW QUERY STATS FROM DATABASE
     */
    public ShowQueryStatsStmt(String dbName) {
        this.tableName = null;
        this.all = false;
        this.verbose = false;
        this.dbName = dbName;
        this.totalRows = Lists.newArrayList();
        this.type = ShowQueryStatsType.DATABASE;
    }

    /**
     * for SHOW QUERY STATS
     */
    public ShowQueryStatsStmt() {
        this.tableName = null;
        this.all = false;
        this.verbose = false;
        this.dbName = null;
        this.totalRows = Lists.newArrayList();
        this.type = ShowQueryStatsType.CATALOG;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (StringUtils.isEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            type = ShowQueryStatsType.DATABASE;
        }
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
        }
        String catalog = Env.getCurrentEnv().getCurrentCatalog().getName();
        if (tableName == null && StringUtils.isEmpty(dbName)) {
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
        if (tableName != null) {
            tableName.analyze(analyzer);
            dbName = tableName.getDb();
        }
        Database db = (Database) Env.getCurrentEnv().getCurrentCatalog().getDbOrDdlException(dbName);
        String ctlName = db.getCatalog().getName();
        if (tableName != null) {
            db.getTableOrDdlException(tableName.getTbl());
        }
        if (tableName == null) {
            Map<String, Long> stats = QueryStatsUtil.getMergedDatabaseStats(catalog, dbName);
            stats.forEach((tableName, queryHit) -> {
                if (Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), ctlName, dbName, tableName, PrivPredicate.SHOW)) {
                    totalRows.add(Arrays.asList(tableName, String.valueOf(queryHit)));
                }
            });
        } else {
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(ConnectContext.get(), tableName, PrivPredicate.SHOW)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW QUERY STATS",
                        ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                        dbName + ": " + tableName);
            }
            if (all && verbose) {
                type = ShowQueryStatsType.TABLE_ALL_VERBOSE;
                QueryStatsUtil.getMergedTableAllVerboseStats(catalog, dbName, tableName.getTbl())
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

                Map<String, Long> stats = QueryStatsUtil.getMergedTableAllStats(catalog, dbName, tableName.getTbl());
                stats.forEach((indexName, queryHit) -> {
                    totalRows.add(Arrays.asList(indexName, String.valueOf(queryHit)));
                });
            } else if (verbose) {
                Preconditions.checkState(false, "verbose is not supported if all is not set");
            } else {
                type = ShowQueryStatsType.TABLE;
                QueryStatsUtil.getMergedTableStats(catalog, dbName, tableName.getTbl()).forEach((col, statCount) -> {
                    totalRows.add(
                            Arrays.asList(col, String.valueOf(statCount.first), String.valueOf(statCount.second)));
                });
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder builder = new StringBuilder();
        builder.append("SHOW QUERY STATS");
        if (tableName != null) {
            builder.append(" FROM ").append(tableName.toSql());
        } else if (StringUtils.isNotEmpty(dbName)) {
            builder.append(" FROM ").append("`").append(dbName).append("`");
        }
        if (all) {
            builder.append(" ALL");
        }
        if (verbose) {
            builder.append(" VERBOSE");
        }
        return builder.toString();
    }

    public List<List<String>> getResultRows() throws AnalysisException {
        return totalRows;
    }

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

    @Override
    public String toString() {
        return toSql();
    }

    /**
     * Show query statistics type
     */
    public enum ShowQueryStatsType {
        CATALOG, DATABASE, TABLE, TABLE_ALL, TABLE_ALL_VERBOSE
    }
}
