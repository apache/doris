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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.TableValuedFunctionRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.IndexSchemaProcNode;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.common.proc.TableProcDir;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.systable.SysTable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.tablefunction.BackendsTableValuedFunction;
import org.apache.doris.tablefunction.LocalTableValuedFunction;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Describe command, support
 *   describe tbl
 *   describe tbl all
 *   describe tbl partition p1
 *   describe tbl partition (p1, p2)
 *   describe function tvf
 */
public class DescribeCommand extends ShowCommand {
    private static final Logger LOG = LogManager.getLogger(DescribeCommand.class);

    private TableNameInfo dbTableName;
    private boolean isAllTables = false;
    private boolean isOlapTable = false;
    private boolean showComment = false;

    private PartitionNamesInfo partitionNames;

    private TableValuedFunctionRef tableValuedFunctionRef;
    private boolean isTableValuedFunction;

    private List<List<String>> rows = new LinkedList<List<String>>();

    public DescribeCommand(TableNameInfo dbTableName, boolean isAllTables, PartitionNamesInfo partitionNames) {
        super(PlanType.DESCRIBE);
        this.dbTableName = dbTableName;
        this.isAllTables = isAllTables;
        this.partitionNames = partitionNames;
    }

    public DescribeCommand(TableValuedFunctionRef tableValuedFunctionRef) {
        super(PlanType.DESCRIBE);
        this.tableValuedFunctionRef = tableValuedFunctionRef;
        this.isTableValuedFunction = true;
        this.isAllTables = false;
    }

    /**
     * getAllMetaData
     */
    private static ShowResultSetMetaData getAllMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("IndexName", ScalarType.createVarchar(20)));
        builder.addColumn(new Column("IndexKeysType", ScalarType.createVarchar(20)));
        builder.addColumn(new Column("Field", ScalarType.createVarchar(20)));
        builder.addColumn(new Column("Type", ScalarType.createVarchar(20)));
        builder.addColumn(new Column("InternalType", ScalarType.createVarchar(20)));
        builder.addColumn(new Column("Null", ScalarType.createVarchar(10)));
        builder.addColumn(new Column("Key", ScalarType.createVarchar(10)));
        builder.addColumn(new Column("Default", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("Extra", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("Visible", ScalarType.createVarchar(10)));
        builder.addColumn(new Column("DefineExpr", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("WhereClause", ScalarType.createVarchar(30)));
        return builder.build();
    }

    /**
     * getMysqlMetaData
     */
    private static ShowResultSetMetaData getMysqlMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("Host", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("Port", ScalarType.createVarchar(10)));
        builder.addColumn(new Column("User", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("Password", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("Database", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("Table", ScalarType.createVarchar(30)));
        return builder.build();
    }

    /**
     * initEmptyRow
     */
    private static List<String> initEmptyRow() {
        List<String> emptyRow = new ArrayList<>(getAllMetaData().getColumns().size());
        for (int i = 0; i < getAllMetaData().getColumns().size(); i++) {
            emptyRow.add("");
        }
        return emptyRow;
    }

    /**
     * getMetaData
     */
    @Override
    public ShowResultSetMetaData getMetaData() {
        if (!isAllTables) {
            ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
            for (String col : IndexSchemaProcNode.TITLE_NAMES) {
                builder.addColumn(new Column(col, ScalarType.createVarchar(30)));
            }
            if (showComment) {
                builder.addColumn(new Column(IndexSchemaProcNode.COMMENT_COLUMN_TITLE, ScalarType.createStringType()));
            }
            return builder.build();
        } else {
            if (isOlapTable) {
                return getAllMetaData();
            } else {
                return getMysqlMetaData();
            }
        }
    }

    /**
     * validateTableValuedFunction
     */
    private void validateTableValuedFunction(ConnectContext ctx, String funcName) throws AnalysisException {
        // check privilege for backends/local tvf
        if (funcName.equalsIgnoreCase(BackendsTableValuedFunction.NAME)
                || funcName.equalsIgnoreCase(LocalTableValuedFunction.NAME)) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN)
                    && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx,
                    PrivPredicate.OPERATOR)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN/OPERATOR");
            }
        }
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (dbTableName != null) {
            dbTableName.analyze(ctx);
            CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(dbTableName.getCtl());
            DatabaseIf db = catalog.getDbOrAnalysisException(dbTableName.getDb());
            Pair<String, String> tableNameWithSysTableName
                    = SysTable.getTableNameWithSysTableName(dbTableName.getTbl());
            if (!Strings.isNullOrEmpty(tableNameWithSysTableName.second)) {
                TableIf table = db.getTableOrDdlException(tableNameWithSysTableName.first);
                isTableValuedFunction = true;
                Optional<TableValuedFunctionRef> optTvfRef = table.getSysTableFunctionRef(
                        dbTableName.getCtl(), dbTableName.getDb(), dbTableName.getTbl());
                if (!optTvfRef.isPresent()) {
                    throw new AnalysisException("sys table not found: " + tableNameWithSysTableName.second);
                }
                tableValuedFunctionRef = optTvfRef.get();
            }
        }

        if (!isAllTables && isTableValuedFunction) {
            validateTableValuedFunction(ctx, tableValuedFunctionRef.getTableFunction().getTableName());
            List<Column> columns = tableValuedFunctionRef.getTableFunction().getTableColumns();
            for (Column column : columns) {
                List<String> row = Arrays.asList(
                        column.getName(),
                        column.getOriginType().hideVersionForVersionColumn(true),
                        column.isAllowNull() ? "Yes" : "No",
                        ((Boolean) column.isKey()).toString(),
                        column.getDefaultValue() == null
                            ? FeConstants.null_string : column.getDefaultValue(),
                        "NONE");
                rows.add(row);
            }
            return new ShowResultSet(getMetaData(), rows);
        }

        if (partitionNames != null) {
            partitionNames.validate();
            if (partitionNames.isTemp()) {
                throw new AnalysisException("Do not support temp partitions");
            }
        }

        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(),
                        dbTableName.getCtl(),
                        dbTableName.getDb(),
                        dbTableName.getTbl(),
                        PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "DESCRIBE",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    dbTableName.toString());
        }

        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(dbTableName.getCtl());
        DatabaseIf db = catalog.getDbOrAnalysisException(dbTableName.getDb());
        TableIf table = db.getTableOrDdlException(dbTableName.getTbl());
        table.readLock();
        try {
            if (!isAllTables) {
                // show base table schema only
                String procString = "/catalogs/" + catalog.getId() + "/" + db.getId() + "/" + table.getId() + "/"
                        + TableProcDir.INDEX_SCHEMA + "/";
                if (table instanceof OlapTable) {
                    procString += ((OlapTable) table).getBaseIndexId();
                } else {
                    if (partitionNames != null) {
                        throw new AnalysisException(dbTableName.getTbl()
                            + " is not a OLAP table, describe table failed");
                    }
                    procString += table.getId();
                }
                if (partitionNames != null) {
                    procString += "/";
                    StringBuilder builder = new StringBuilder();
                    for (String str : partitionNames.getPartitionNames()) {
                        // TODO: describe tbl partition p1 can not execute, should fix it.
                        builder.append(str);
                        builder.append(",");
                    }
                    builder.deleteCharAt(builder.length() - 1);
                    procString += builder.toString();
                }
                ProcNodeInterface node = ProcService.getInstance().open(procString);
                if (node == null) {
                    throw new AnalysisException("Describe table[" + dbTableName.getTbl() + "] failed");
                }
                rows.addAll(getResultRows(node));
            } else {
                Util.prohibitExternalCatalog(dbTableName.getCtl(), this.getClass().getSimpleName() + " ALL");
                if (table instanceof OlapTable) {
                    isOlapTable = true;
                    OlapTable olapTable = (OlapTable) table;
                    Set<String> bfColumns = olapTable.getCopiedBfColumns();
                    Map<Long, List<Column>> indexIdToSchema = olapTable.getIndexIdToSchema();

                    // indices order
                    List<Long> indices = Lists.newArrayList();
                    indices.add(olapTable.getBaseIndexId());
                    for (Long indexId : indexIdToSchema.keySet()) {
                        if (indexId != olapTable.getBaseIndexId()) {
                            indices.add(indexId);
                        }
                    }

                    // add all indices
                    for (int i = 0; i < indices.size(); ++i) {
                        long indexId = indices.get(i);
                        List<Column> columns = indexIdToSchema.get(indexId);
                        String indexName = olapTable.getIndexNameById(indexId);
                        MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(indexId);
                        for (int j = 0; j < columns.size(); ++j) {
                            Column column = columns.get(j);

                            // Extra string (aggregation and bloom filter)
                            List<String> extras = Lists.newArrayList();
                            if (column.getAggregationType() != null) {
                                extras.add(column.getAggregationString());
                            }
                            if (bfColumns != null && bfColumns.contains(column.getName())) {
                                extras.add("BLOOM_FILTER");
                            }
                            String extraStr = StringUtils.join(extras, ",");

                            String defineExprStr = "";
                            Expr defineExpr = column.getDefineExpr();
                            if (defineExpr != null) {
                                column.getDefineExpr().disableTableName();
                                defineExprStr = defineExpr.toSqlWithoutTbl();
                            }

                            List<String> row = Arrays.asList(
                                    "",
                                    "",
                                    column.getName(),
                                    column.getOriginType().hideVersionForVersionColumn(true),
                                    column.getOriginType().toString(),
                                    column.isAllowNull() ? "Yes" : "No",
                                    ((Boolean) column.isKey()).toString(),
                                    column.getDefaultValue() == null
                                            ? FeConstants.null_string
                                            : column.getDefaultValue(),
                                    extraStr,
                                    ((Boolean) column.isVisible()).toString(),
                                    defineExprStr,
                                    "");

                            if (j == 0) {
                                row.set(0, indexName);
                                row.set(1, indexMeta.getKeysType().name());
                                Expr where = indexMeta.getWhereClause();
                                row.set(getMetaData().getColumns().size() - 1,
                                        where == null ? "" : where.toSqlWithoutTbl());
                            }

                            rows.add(row);
                        } // end for columns

                        if (i != indices.size() - 1) {
                            rows.add(initEmptyRow());
                        }
                    } // end for indices
                } else if (table.getType() == TableIf.TableType.ODBC) {
                    isOlapTable = false;
                    OdbcTable odbcTable = (OdbcTable) table;
                    List<String> row = Arrays.asList(odbcTable.getHost(),
                            odbcTable.getPort(),
                            odbcTable.getUserName(),
                            odbcTable.getPasswd(),
                            odbcTable.getOdbcDatabaseName(),
                            odbcTable.getOdbcTableName(),
                            odbcTable.getOdbcDriver(),
                            odbcTable.getOdbcTableTypeName());
                    rows.add(row);
                } else if (table.getType() == TableIf.TableType.JDBC) {
                    isOlapTable = false;
                    JdbcTable jdbcTable = (JdbcTable) table;
                    List<String> row = Arrays.asList(jdbcTable.getJdbcUrl(),
                            jdbcTable.getJdbcUser(),
                            jdbcTable.getJdbcPasswd(),
                            jdbcTable.getDriverClass(),
                            jdbcTable.getDriverUrl(),
                            jdbcTable.getExternalTableName(),
                            jdbcTable.getResourceName(),
                            jdbcTable.getJdbcTypeName());
                    rows.add(row);
                } else if (table.getType() == TableIf.TableType.MYSQL) {
                    isOlapTable = false;
                    MysqlTable mysqlTable = (MysqlTable) table;
                    List<String> row = Arrays.asList(mysqlTable.getHost(),
                            mysqlTable.getPort(),
                            mysqlTable.getUserName(),
                            mysqlTable.getPasswd(),
                            mysqlTable.getMysqlDatabaseName(),
                            mysqlTable.getMysqlTableName(),
                            mysqlTable.getCharset());
                    rows.add(row);
                } else {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_STORAGE_ENGINE, table.getType());
                }
            }
        } finally {
            table.readUnlock();
        }

        return new ShowResultSet(getMetaData(), rows);
    }

    /**
     * getResultRows
     */
    private List<List<String>> getResultRows(ProcNodeInterface node) throws AnalysisException {
        showComment = ConnectContext.get().getSessionVariable().showColumnCommentInDescribe;
        Preconditions.checkNotNull(node);
        List<List<String>> rows = node.fetchResult().getRows();
        List<List<String>> res = new ArrayList<>();
        for (List<String> row : rows) {
            try {
                Env.getCurrentEnv().getAccessManager()
                        .checkColumnsPriv(ConnectContext.get().getCurrentUserIdentity(), dbTableName.getCtl(),
                                dbTableName.getDb(), dbTableName.getTbl(), Sets.newHashSet(row.get(0)),
                                PrivPredicate.SHOW);
                res.add(row);
            } catch (UserException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(e.getMessage());
                }
            }
        }
        return res;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDescribeCommand(this, context);
    }
}
