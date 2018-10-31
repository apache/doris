// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.ColumnType;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Replica.ReplicaState;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.catalog.Table.TableType;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.UserException;
import com.baidu.palo.common.Pair;
import com.baidu.palo.common.util.DebugUtil;
import com.baidu.palo.mysql.privilege.PrivPredicate;
import com.baidu.palo.qe.ConnectContext;
import com.baidu.palo.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class ShowDataStmt extends ShowStmt {
    private static final ShowResultSetMetaData SHOW_TABLE_DATA_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ColumnType.createVarchar(20)))
                    .addColumn(new Column("Size", ColumnType.createVarchar(30)))
                    .build();

    private static final ShowResultSetMetaData SHOW_INDEX_DATA_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ColumnType.createVarchar(20)))
                    .addColumn(new Column("IndexName", ColumnType.createVarchar(20)))
                    .addColumn(new Column("Size", ColumnType.createVarchar(30)))
                    .build();

    private String dbName;
    private String tableName;

    List<List<String>> totalRows;

    public ShowDataStmt(String dbName, String tableName) {
        this.dbName = dbName;
        this.tableName = tableName;

        this.totalRows = new LinkedList<List<String>>();
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            dbName = ClusterNamespace.getFullName(getClusterName(), dbName);
        }
        
        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        db.readLock();
        try {
            if (tableName == null) {
                long totalSize = 0;

                // sort by table name
                List<Table> tables = db.getTables();
                SortedSet<Table> sortedTables = new TreeSet<>(new Comparator<Table>() {
                    @Override
                    public int compare(Table t1, Table t2) {
                        return t1.getName().compareTo(t2.getName());
                    }
                });

                for (Table table : tables) {
                    if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbName,
                                                                            table.getName(),
                                                                            PrivPredicate.SHOW)) {
                        continue;
                    }
                    sortedTables.add(table);
                }

                for (Table table : sortedTables) {
                    if (table.getType() != TableType.OLAP) {
                        continue;
                    }

                    OlapTable olapTable = (OlapTable) table;
                    long tableSize = 0;
                    for (Partition partition : olapTable.getPartitions()) {
                        for (MaterializedIndex mIndex : partition.getMaterializedIndices()) {
                            for (Tablet tablet : mIndex.getTablets()) {
                                for (Replica replica : tablet.getReplicas()) {
                                    if (replica.getState() == ReplicaState.NORMAL
                                            || replica.getState() == ReplicaState.SCHEMA_CHANGE) {
                                        tableSize += replica.getDataSize();
                                    }
                                } // end for replicas
                            } // end for tablets
                        } // end for tables
                    } // end for partitions

                    Pair<Double, String> tableSizePair = DebugUtil.getByteUint(tableSize);
                    String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(tableSizePair.first) + " "
                            + tableSizePair.second;

                    List<String> row = Arrays.asList(table.getName(), readableSize);
                    totalRows.add(row);

                    totalSize += tableSize;
                } // end for tables

                Pair<Double, String> totalSizePair = DebugUtil.getByteUint(totalSize);
                String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalSizePair.first) + " "
                        + totalSizePair.second;
                List<String> total = Arrays.asList("Total", readableSize);
                totalRows.add(total);

                // quota
                long quota = db.getDataQuota();
                Pair<Double, String> quotaPair = DebugUtil.getByteUint(quota);
                String readableQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(quotaPair.first) + " "
                        + quotaPair.second;
                List<String> quotaRow = Arrays.asList("Quota", readableQuota);
                totalRows.add(quotaRow);

                // left
                long left = Math.max(0, quota - totalSize);
                Pair<Double, String> leftPair = DebugUtil.getByteUint(left);
                String readableLeft = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(leftPair.first) + " "
                        + leftPair.second;
                List<String> leftRow = Arrays.asList("Left", readableLeft);
                totalRows.add(leftRow);
            } else {
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbName,
                                                                        tableName,
                                                                        PrivPredicate.SHOW)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW DATA",
                                                        ConnectContext.get().getQualifiedUser(),
                                                        ConnectContext.get().getRemoteIP(),
                                                        tableName);
                }

                Table table = db.getTable(tableName);
                if (table == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                }

                if (table.getType() != TableType.OLAP) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NOT_OLAP_TABLE, tableName);
                }

                OlapTable olapTable = (OlapTable) table;
                int i = 0;
                long totalSize = 0;

                // sort by index name
                Map<String, Long> indexNames = olapTable.getIndexNameToId();
                Map<String, Long> sortedIndexNames = new TreeMap<String, Long>();
                for (Map.Entry<String, Long> entry : indexNames.entrySet()) {
                    sortedIndexNames.put(entry.getKey(), entry.getValue());
                }

                for (Long indexId : sortedIndexNames.values()) {
                    long indexSize = 0;
                    for (Partition partition : olapTable.getPartitions()) {
                        MaterializedIndex mIndex = partition.getIndex(indexId);
                        for (Tablet tablet : mIndex.getTablets()) {
                            for (Replica replica : tablet.getReplicas()) {
                                if (replica.getState() == ReplicaState.NORMAL
                                        || replica.getState() == ReplicaState.SCHEMA_CHANGE) {
                                    indexSize += replica.getDataSize();
                                }
                            } // end for replicas
                        } // end for tablets
                    } // end for partitions

                    Pair<Double, String> indexSizePair = DebugUtil.getByteUint(indexSize);
                    String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(indexSizePair.first) + " "
                            + indexSizePair.second;

                    List<String> row = null;
                    if (i == 0) {
                        row = Arrays.asList(tableName,
                                            olapTable.getIndexNameById(indexId),
                                            readableSize);
                    } else {
                        row = Arrays.asList("",
                                            olapTable.getIndexNameById(indexId),
                                            readableSize);
                    }

                    totalSize += indexSize;
                    totalRows.add(row);

                    i++;
                } // end for indices

                Pair<Double, String> totalSizePair = DebugUtil.getByteUint(totalSize);
                String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalSizePair.first) + " "
                        + totalSizePair.second;
                List<String> row = Arrays.asList("", "Total", readableSize);
                totalRows.add(row);
            }
        } finally {
            db.readUnlock();
        }
    }

    public boolean hasTable() {
        return this.tableName != null;
    }

    public List<List<String>> getResultRows() throws AnalysisException {
        return totalRows;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        if (tableName != null) {
            return SHOW_INDEX_DATA_META_DATA;
        } else {
            return SHOW_TABLE_DATA_META_DATA;
        }
    }

    @Override
    public String toSql() {
        StringBuilder builder = new StringBuilder();
        builder.append("SHOW DATA");
        if (dbName == null) {
            return builder.toString();
        }

        builder.append(" FROM `").append(dbName).append("`");
        if (tableName == null) {
            return builder.toString();
        }
        builder.append(".`").append(tableName).append("`");
        return builder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}

