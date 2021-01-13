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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.MysqlTable;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.common.proc.TableProcDir;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DescribeStmt extends ShowStmt {
    private static final ShowResultSetMetaData DESC_OLAP_TABLE_ALL_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("IndexName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("IndexKeysType", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Field", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Key", ScalarType.createVarchar(10)))
                    .addColumn(new Column("Default", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Extra", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Visible", ScalarType.createVarchar(10)))
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
    
    List<List<String>> totalRows;

    private boolean isAllTables;
    private boolean isOlapTable;

    public DescribeStmt(TableName dbTableName, boolean isAllTables) {
        this.dbTableName = dbTableName;
        this.totalRows = new LinkedList<List<String>>();
        this.isAllTables = isAllTables;
    }

    public boolean isAllTables() {
        return isAllTables;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        dbTableName.analyze(analyzer);
        
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbTableName.getDb(),
                                                                dbTableName.getTbl(), PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "DESCRIBE",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                dbTableName.getTbl());
        }

        Database db = Catalog.getCurrentCatalog().getDb(dbTableName.getDb());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbTableName.getDb());
        }

        Table table = db.getTable(dbTableName.getTbl());
        if (table == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, dbTableName.getTbl());
        }

        table.readLock();
        try {
            if (!isAllTables) {
                // show base table schema only
                String procString = "/dbs/" + db.getId() + "/" + table.getId() + "/" + TableProcDir.INDEX_SCHEMA
                        + "/";
                if (table.getType() == TableType.OLAP) {
                    procString += ((OlapTable) table).getBaseIndexId();
                } else {
                    procString += table.getId();
                }

                node = ProcService.getInstance().open(procString);
                if (node == null) {
                    throw new AnalysisException("Describe table[" + dbTableName.getTbl() + "] failed");
                }
            } else {
                if (table.getType() == TableType.OLAP) {
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
                                extras.add(column.getAggregationType().name());
                            }
                            if (bfColumns != null && bfColumns.contains(column.getName())) {
                                extras.add("BLOOM_FILTER");
                            }
                            String extraStr = StringUtils.join(extras, ",");

                            List<String> row = Arrays.asList(
                                    "",
                                    "",
                                    column.getDisplayName(),
                                    column.getOriginType().toString(),
                                    column.isAllowNull() ? "Yes" : "No",
                                    ((Boolean) column.isKey()).toString(),
                                    column.getDefaultValue() == null ? FeConstants.null_string : column.getDefaultValue(),
                                    extraStr,
                                    ((Boolean) column.isVisible()).toString()
                            );

                            if (j == 0) {
                                row.set(0, indexName);
                                row.set(1, indexMeta.getKeysType().name());
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
                } else if (table.getType() == TableType.MYSQL) {
                    isOlapTable = false;
                    MysqlTable mysqlTable = (MysqlTable) table;
                    List<String> row = Arrays.asList(mysqlTable.getHost(),
                                                     mysqlTable.getPort(),
                                                     mysqlTable.getUserName(),
                                                     mysqlTable.getPasswd(),
                                                     mysqlTable.getMysqlDatabaseName(),
                                                     mysqlTable.getMysqlTableName());
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
            Preconditions.checkNotNull(node);
            return node.fetchResult().getRows();
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        if (!isAllTables) {
            ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

            ProcResult result = null;
            try {
                result = node.fetchResult();
            } catch (AnalysisException e) {
                return builder.build();
            }

            for (String col : result.getColumnNames()) {
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
