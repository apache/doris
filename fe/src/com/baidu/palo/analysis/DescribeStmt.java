// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.analysis;

import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.ColumnType;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.MysqlTable;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.catalog.Table.TableType;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.proc.ProcNodeInterface;
import com.baidu.palo.common.proc.ProcResult;
import com.baidu.palo.common.proc.ProcService;
import com.baidu.palo.common.proc.TableProcDir;
import com.baidu.palo.qe.ShowResultSetMetaData;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DescribeStmt extends ShowStmt {
    private static final ShowResultSetMetaData DESC_OLAP_TABLE_ALL_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("IndexName", ColumnType.createVarchar(20)))
                    .addColumn(new Column("Field", ColumnType.createVarchar(20)))
                    .addColumn(new Column("Type", ColumnType.createVarchar(20)))
                    .addColumn(new Column("Null", ColumnType.createVarchar(10)))
                    .addColumn(new Column("Key", ColumnType.createVarchar(10)))
                    .addColumn(new Column("Default", ColumnType.createVarchar(30)))
                    .addColumn(new Column("Extra", ColumnType.createVarchar(30)))
                    .build();

    private static final ShowResultSetMetaData DESC_MYSQL_TABLE_ALL_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Host", ColumnType.createVarchar(30)))
                    .addColumn(new Column("Port", ColumnType.createVarchar(10)))
                    .addColumn(new Column("User", ColumnType.createVarchar(30)))
                    .addColumn(new Column("Password", ColumnType.createVarchar(30)))
                    .addColumn(new Column("Database", ColumnType.createVarchar(30)))
                    .addColumn(new Column("Table", ColumnType.createVarchar(30)))
                    .build();

    // empty col num equals to DESC_OLAP_TABLE_ALL_META_DATA.size()
    private static final List<String> EMPTY_ROW = Arrays.asList("", "", "", "", "", "", "");

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
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        dbTableName.analyze(analyzer);
        if (!analyzer.getCatalog().getUserMgr()
                .checkAccess(analyzer.getUser(), dbTableName.getDb(), AccessPrivilege.READ_ONLY)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED,
                    analyzer.getUser(), dbTableName.getDb());
        }

        Database db = Catalog.getInstance().getDb(dbTableName.getDb());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbTableName.getDb());
        }
        db.readLock();
        try {
            Table table = db.getTable(dbTableName.getTbl());
            if (table == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, dbTableName.getTbl());
            }

            if (!isAllTables) {
                // show base table schema only
                String procString = "/dbs/" + db.getId() + "/" + table.getId() + "/" + TableProcDir.INDEX_SCHEMA
                        + "/" + table.getName();

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
                    indices.add(olapTable.getId());
                    for (Long indexId : indexIdToSchema.keySet()) {
                        if (indexId != olapTable.getId()) {
                            indices.add(indexId);
                        }
                    }

                    // add all indices
                    for (int i = 0; i < indices.size(); ++i) {
                        long indexId = indices.get(i);
                        List<Column> columns = indexIdToSchema.get(indexId);
                        String indexName = olapTable.getIndexNameById(indexId);
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

                            List<String> row = Arrays.asList("",
                                                             column.getName(),
                                                             column.getColumnType().toString(),
                                                             column.isAllowNull() ? "Yes" : "No",
                                                             ((Boolean) column.isKey()).toString(),
                                                             column.getDefaultValue() == null
                                                                     ? "N/A" : column.getDefaultValue(),
                                                             extraStr);

                            if (j == 0) {
                                row.set(0, indexName);
                            }

                            totalRows.add(row);
                        } // end for columns

                        if (i != indices.size() - 1) {
                            totalRows.add(EMPTY_ROW);
                        }
                    } // end for indices
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
            db.readUnlock();
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
                builder.addColumn(new Column(col, ColumnType.createVarchar(30)));
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

}
