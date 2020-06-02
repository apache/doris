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
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

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
                    .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Size", ScalarType.createVarchar(30)))
                    .addColumn(new Column("ReplicaCount", ScalarType.createVarchar(20)))
                    .build();

    private static final ShowResultSetMetaData SHOW_INDEX_DATA_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("IndexName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Size", ScalarType.createVarchar(30)))
                    .addColumn(new Column("ReplicaCount", ScalarType.createVarchar(20)))
                    .addColumn(new Column("RowCount", ScalarType.createVarchar(20)))
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
        
        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        if (tableName == null) {
            db.readLock();
            try {
                long totalSize = 0;
                long totalReplicaCount = 0;

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
                    long replicaCount = 0;
                    olapTable.readLock();
                    try {
                        tableSize = olapTable.getDataSize();
                        replicaCount = olapTable.getReplicaCount();
                    } finally {
                        olapTable.readUnlock();
                    }
                    Pair<Double, String> tableSizePair = DebugUtil.getByteUint(tableSize);
                    String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(tableSizePair.first) + " "
                            + tableSizePair.second;

                    List<String> row = Arrays.asList(table.getName(), readableSize, String.valueOf(replicaCount));
                    totalRows.add(row);

                    totalSize += tableSize;
                    totalReplicaCount += replicaCount;
                } // end for tables

                Pair<Double, String> totalSizePair = DebugUtil.getByteUint(totalSize);
                String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalSizePair.first) + " "
                        + totalSizePair.second;
                List<String> total = Arrays.asList("Total", readableSize, String.valueOf(totalReplicaCount));
                totalRows.add(total);

                // quota
                long quota = db.getDataQuota();
                long replicaQuota = db.getReplicaQuota();
                Pair<Double, String> quotaPair = DebugUtil.getByteUint(quota);
                String readableQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(quotaPair.first) + " "
                        + quotaPair.second;

                List<String> quotaRow = Arrays.asList("Quota", readableQuota, String.valueOf(replicaQuota));
                totalRows.add(quotaRow);

                // left
                long left = Math.max(0, quota - totalSize);
                long replicaCountLeft = Math.max(0, replicaQuota - totalReplicaCount);
                Pair<Double, String> leftPair = DebugUtil.getByteUint(left);
                String readableLeft = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(leftPair.first) + " "
                        + leftPair.second;
                List<String> leftRow = Arrays.asList("Left", readableLeft, String.valueOf(replicaCountLeft));
                totalRows.add(leftRow);
            } finally {
                db.readUnlock();
            }
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
            long totalReplicaCount = 0;

            olapTable.readLock();
            try {
                // sort by index name
                Map<String, Long> indexNames = olapTable.getIndexNameToId();
                Map<String, Long> sortedIndexNames = new TreeMap<String, Long>();
                for (Map.Entry<String, Long> entry : indexNames.entrySet()) {
                    sortedIndexNames.put(entry.getKey(), entry.getValue());
                }

                for (Long indexId : sortedIndexNames.values()) {
                    long indexSize = 0;
                    long indexReplicaCount = 0;
                    long indexRowCount = 0;
                    for (Partition partition : olapTable.getAllPartitions()) {
                        MaterializedIndex mIndex = partition.getIndex(indexId);
                        indexSize += mIndex.getDataSize();
                        indexReplicaCount += mIndex.getReplicaCount();
                        indexRowCount += mIndex.getRowCount();
                    }

                    Pair<Double, String> indexSizePair = DebugUtil.getByteUint(indexSize);
                    String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(indexSizePair.first) + " "
                            + indexSizePair.second;

                    List<String> row = null;
                    if (i == 0) {
                        row = Arrays.asList(tableName,
                                olapTable.getIndexNameById(indexId),
                                readableSize, String.valueOf(indexReplicaCount),
                                String.valueOf(indexRowCount));
                    } else {
                        row = Arrays.asList("",
                                olapTable.getIndexNameById(indexId),
                                readableSize, String.valueOf(indexReplicaCount),
                                String.valueOf(indexRowCount));
                    }

                    totalSize += indexSize;
                    totalReplicaCount += indexReplicaCount;
                    totalRows.add(row);

                    i++;
                } // end for indices

                Pair<Double, String> totalSizePair = DebugUtil.getByteUint(totalSize);
                String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalSizePair.first) + " "
                        + totalSizePair.second;
                List<String> row = Arrays.asList("", "Total", readableSize, String.valueOf(totalReplicaCount), "");
                totalRows.add(row);
            } finally {
                olapTable.readUnlock();
            }
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

