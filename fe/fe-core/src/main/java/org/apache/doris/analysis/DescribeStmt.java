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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.IndexSchemaProcNode;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.common.proc.TableProcDir;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Preconditions;
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
import java.util.Set;

public class DescribeStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(DescribeStmt.class);
    private static final ShowResultSetMetaData DESC_OLAP_TABLE_ALL_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("IndexName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("IndexKeysType", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("InternalType", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Key", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Extra", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Visible", ScalarType.createVarchar(10)))
                    .addColumn(new Column("DefineExpr", ScalarType.createVarchar(30)))
                    .addColumn(new Column("WhereClause", ScalarType.createVarchar(30)))
                    .build();

    private static final ShowResultSetMetaData DESC_MYSQL_TABLE_ALL_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Host", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Port", ScalarType.createVarchar(10)))
                    .addColumn(new Column("User", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Password", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Database", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Table", ScalarType.createVarchar(30)))
                    .build();

    // empty col num equals to DESC_OLAP_TABLE_ALL_META_DATA.size()
    private static final List<String> EMPTY_ROW = initEmptyRow();

    private TableName dbTableName;
    private ProcNodeInterface node;
    private PartitionNames partitionNames;

    List<List<String>> totalRows = new LinkedList<List<String>>();

    private boolean isAllTables;
    private boolean isOlapTable = false;

    TableValuedFunctionRef tableValuedFunctionRef;
    boolean isTableValuedFunction;

    public DescribeStmt(TableName dbTableName, boolean isAllTables) {
        this.dbTableName = dbTableName;
        this.isAllTables = isAllTables;
    }

    public DescribeStmt(TableName dbTableName, boolean isAllTables, PartitionNames partitionNames) {
        this.dbTableName = dbTableName;
        this.isAllTables = isAllTables;
        this.partitionNames = partitionNames;
    }

    public DescribeStmt(TableValuedFunctionRef tableValuedFunctionRef) {
        this.tableValuedFunctionRef = tableValuedFunctionRef;
        this.isTableValuedFunction = true;
        this.isAllTables = false;
    }

    public boolean isAllTables() {
        return isAllTables;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (!isAllTables && isTableValuedFunction) {
            tableValuedFunctionRef.analyze(analyzer);
            List<Column> columns = tableValuedFunctionRef.getTable().getBaseSchema();
            for (Column column : columns) {
                List<String> row = Arrays.asList(
                        column.getName(),
                        column.getOriginType().toString(),
                        column.isAllowNull() ? "Yes" : "No",
                        ((Boolean) column.isKey()).toString(),
                        column.getDefaultValue() == null
                                ? FeConstants.null_string : column.getDefaultValue(),
                        "NONE"
                );
                if (column.getOriginType().isDatetimeV2()) {
                    StringBuilder typeStr = new StringBuilder("DATETIME");
                    if (((ScalarType) column.getOriginType()).getScalarScale() > 0) {
                        typeStr.append("(").append(((ScalarType) column.getOriginType()).getScalarScale()).append(")");
                    }
                    row.set(1, typeStr.toString());
                } else if (column.getOriginType().isDateV2()) {
                    row.set(1, "DATE");
                } else if (column.getOriginType().isDecimalV3()) {
                    StringBuilder typeStr = new StringBuilder("DECIMAL");
                    ScalarType sType = (ScalarType) column.getOriginType();
                    int scale = sType.getScalarScale();
                    int precision = sType.getScalarPrecision();
                    // not default
                    if (scale > 0 && precision != 9) {
                        typeStr.append("(").append(precision).append(", ").append(scale)
                                .append(")");
                    }
                    row.set(1, typeStr.toString());
                }
                totalRows.add(row);
            }
            return;
        }

        if (partitionNames != null) {
            partitionNames.analyze(analyzer);
            if (partitionNames.isTemp()) {
                throw new AnalysisException("Do not support temp partitions");
            }
        }

        dbTableName.analyze(analyzer);

        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), dbTableName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "DESCRIBE",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    dbTableName.toString());
        }

        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(dbTableName.getCtl());
        DatabaseIf db = catalog.getDbOrAnalysisException(dbTableName.getDb());
        TableIf table = db.getTableOrAnalysisException(dbTableName.getTbl());

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
                        builder.append(str);
                        builder.append(",");
                    }
                    builder.deleteCharAt(builder.length() - 1);
                    procString += builder.toString();
                }
                node = ProcService.getInstance().open(procString);
                if (node == null) {
                    throw new AnalysisException("Describe table[" + dbTableName.getTbl() + "] failed");
                }
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

                            List<String> row = Arrays.asList(
                                    "",
                                    "",
                                    column.getName(),
                                    column.getOriginType().toString(),
                                    column.getOriginType().toString(),
                                    column.isAllowNull() ? "Yes" : "No",
                                    ((Boolean) column.isKey()).toString(),
                                    column.getDefaultValue() == null
                                            ? FeConstants.null_string
                                            : column.getDefaultValue(),
                                    extraStr,
                                    ((Boolean) column.isVisible()).toString(),
                                    column.getDefineExpr() == null ? "" : column.getDefineExpr().toSql(),
                                    "");

                            if (column.getOriginType().isDatetimeV2()) {
                                StringBuilder typeStr = new StringBuilder("DATETIME");
                                if (((ScalarType) column.getOriginType()).getScalarScale() > 0) {
                                    typeStr.append("(").append(((ScalarType) column.getOriginType()).getScalarScale())
                                            .append(")");
                                }
                                row.set(3, typeStr.toString());
                            } else if (column.getOriginType().isDateV2()) {
                                row.set(3, "DATE");
                            } else if (column.getOriginType().isDecimalV3()) {
                                StringBuilder typeStr = new StringBuilder("DECIMAL");
                                ScalarType sType = (ScalarType) column.getOriginType();
                                int scale = sType.getScalarScale();
                                int precision = sType.getScalarPrecision();
                                // not default
                                if (scale > 0 && precision != 9) {
                                    typeStr.append("(").append(precision).append(", ").append(scale)
                                            .append(")");
                                }
                                row.set(3, typeStr.toString());
                            }

                            if (j == 0) {
                                row.set(0, indexName);
                                row.set(1, indexMeta.getKeysType().name());
                                Expr where = indexMeta.getWhereClause();
                                row.set(DESC_OLAP_TABLE_ALL_META_DATA.getColumns().size() - 1,
                                        where == null ? "" : where.toSqlWithoutTbl());
                            }

                            totalRows.add(row);
                        } // end for columns

                        if (i != indices.size() - 1) {
                            totalRows.add(EMPTY_ROW);
                        }
                    } // end for indices
                } else if (table.getType() == TableType.ODBC) {
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
                    totalRows.add(row);
                } else if (table.getType() == TableType.JDBC) {
                    isOlapTable = false;
                    JdbcTable jdbcTable = (JdbcTable) table;
                    List<String> row = Arrays.asList(jdbcTable.getJdbcUrl(), jdbcTable.getJdbcUser(),
                            jdbcTable.getJdbcPasswd(), jdbcTable.getDriverClass(), jdbcTable.getDriverUrl(),
                            jdbcTable.getExternalTableName(), jdbcTable.getResourceName(), jdbcTable.getJdbcTypeName());
                    totalRows.add(row);
                } else if (table.getType() == TableType.MYSQL) {
                    isOlapTable = false;
                    MysqlTable mysqlTable = (MysqlTable) table;
                    List<String> row = Arrays.asList(mysqlTable.getHost(),
                                                     mysqlTable.getPort(),
                                                     mysqlTable.getUserName(),
                                                     mysqlTable.getPasswd(),
                                                     mysqlTable.getMysqlDatabaseName(),
                                                     mysqlTable.getMysqlTableName(),
                                                     mysqlTable.getCharset());
                    totalRows.add(row);
                } else {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_UNKNOWN_STORAGE_ENGINE, table.getType());
                }
            }
        } finally {
            table.readUnlock();
        }
    }

    public String getTableName() {
        return dbTableName.getTbl();
    }

    public String getDb() {
        return dbTableName.getDb();
    }

    public List<List<String>> getResultRows() throws AnalysisException {
        if (isAllTables) {
            return totalRows;
        } else {
            if (isTableValuedFunction) {
                return totalRows;
            }
            Preconditions.checkNotNull(node);
            List<List<String>> rows = node.fetchResult().getRows();
            List<List<String>> res = new ArrayList<>();
            for (List<String> row : rows) {
                try {
                    Env.getCurrentEnv().getAccessManager()
                            .checkColumnsPriv(ConnectContext.get().getCurrentUserIdentity(), dbTableName.getCtl(),
                                    getDb(), getTableName(), Sets.newHashSet(row.get(0)), PrivPredicate.SHOW);
                    res.add(row);
                } catch (UserException e) {
                    LOG.debug(e.getMessage());
                }
            }
            return res;
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        if (!isAllTables) {
            ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
            for (String col : IndexSchemaProcNode.TITLE_NAMES) {
                builder.addColumn(new Column(col, ScalarType.createVarchar(30)));
            }
            return builder.build();
        } else {
            if (isOlapTable) {
                return DESC_OLAP_TABLE_ALL_META_DATA;
            } else {
                return DESC_MYSQL_TABLE_ALL_META_DATA;
            }
        }
    }

    @Override
    public String toSql() {
        return "DESCRIBE `" + dbTableName + "`" + (isAllTables ? " ALL" : "");
    }

    @Override
    public String toString() {
        return toSql();
    }

    private static List<String> initEmptyRow() {
        List<String> emptyRow = new ArrayList<>(DESC_OLAP_TABLE_ALL_META_DATA.getColumns().size());
        for (int i = 0; i < DESC_OLAP_TABLE_ALL_META_DATA.getColumns().size(); i++) {
            emptyRow.add("");
        }
        return emptyRow;
    }
}
