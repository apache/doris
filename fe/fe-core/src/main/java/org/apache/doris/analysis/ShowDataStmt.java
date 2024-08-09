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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class ShowDataStmt extends ShowStmt {
    private static final ShowResultSetMetaData SHOW_DATABASE_DATA_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("DbId", ScalarType.createVarchar(20)))
                    .addColumn(new Column("DbName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Size", ScalarType.createVarchar(30)))
                    .addColumn(new Column("RemoteSize", ScalarType.createVarchar(30)))
                    .addColumn(new Column("RecycleSize", ScalarType.createVarchar(30)))
                    .addColumn(new Column("RecycleRemoteSize", ScalarType.createVarchar(30)))
                    .build();

    private static final ShowResultSetMetaData SHOW_TABLE_DATA_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Size", ScalarType.createVarchar(30)))
                    .addColumn(new Column("ReplicaCount", ScalarType.createVarchar(20)))
                    .addColumn(new Column("RemoteSize", ScalarType.createVarchar(30)))
                    .build();

    private static final ShowResultSetMetaData SHOW_WAREHOUSE_DATA_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("DBName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("DataSize", ScalarType.createVarchar(20)))
                    .addColumn(new Column("RecycleSize", ScalarType.createVarchar(20)))
                    .build();

    private static final ShowResultSetMetaData SHOW_INDEX_DATA_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TableName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("IndexName", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Size", ScalarType.createVarchar(30)))
                    .addColumn(new Column("ReplicaCount", ScalarType.createVarchar(20)))
                    .addColumn(new Column("RowCount", ScalarType.createVarchar(20)))
                    .addColumn(new Column("RemoteSize", ScalarType.createVarchar(30)))
                    .build();

    public static final ImmutableList<String> SHOW_TABLE_DATA_META_DATA_ORIGIN =
            new ImmutableList.Builder<String>().add("TableName").add("Size").add("ReplicaCount")
            .add("RemoteSize").build();

    public static final ImmutableList<String> SHOW_INDEX_DATA_META_DATA_ORIGIN =
            new ImmutableList.Builder<String>().add("TableName").add("IndexName").add("Size").add("ReplicaCount")
                    .add("RowCount").add("RemoteSize").build();

    TableName tableName;
    String dbName;
    List<List<String>> totalRows;
    List<List<Object>> totalRowsObject = Lists.newArrayList();

    private List<OrderByElement> orderByElements;
    private List<OrderByPair> orderByPairs;

    private final Map<String, String> properties;

    private static final String WAREHOUSE = "entire_warehouse";
    private static final String DB_LIST = "db_names";

    public ShowDataStmt(TableName tableName, List<OrderByElement> orderByElements, Map<String, String> properties) {
        this.tableName = tableName;
        this.totalRows = Lists.newArrayList();
        this.orderByElements = orderByElements;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (getDbStatsByProperties()) {
            return;
        }
        dbName = analyzer.getDefaultDb();
        if (Strings.isNullOrEmpty(dbName)) {
            getAllDbStats();
            return;
        }
        if (tableName != null) {
            tableName.analyze(analyzer);
            // disallow external catalog
            Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());
            dbName = tableName.getDb();
        } else {
            Util.prohibitExternalCatalog(analyzer.getDefaultCatalog(), this.getClass().getSimpleName());
        }

        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);

        // order by
        if (orderByElements != null && !orderByElements.isEmpty()) {
            orderByPairs = new ArrayList<>();
            for (OrderByElement orderByElement : orderByElements) {
                if (!(orderByElement.getExpr() instanceof SlotRef)) {
                    throw new AnalysisException("Should order by column");
                }
                SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                int index = analyzeColumn(slotRef.getColumnName(), tableName == null ? null : tableName.getTbl());
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                orderByPairs.add(orderByPair);
            }
        }

        if (tableName == null) {
            db.readLock();
            try {
                long totalSize = 0;
                long totalReplicaCount = 0;
                long totalRemoteSize = 0;
                // sort by table name
                List<Table> tables = db.getTables();
                SortedSet<Table> sortedTables = new TreeSet<>(new Comparator<Table>() {
                    @Override
                    public int compare(Table t1, Table t2) {
                        return t1.getName().compareTo(t2.getName());
                    }
                });

                for (Table table : tables) {
                    if (!Env.getCurrentEnv().getAccessManager()
                            .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName,
                                    table.getName(),
                                    PrivPredicate.SHOW)) {
                        continue;
                    }
                    sortedTables.add(table);
                }

                for (Table table : sortedTables) {
                    if (!table.isManagedTable()) {
                        continue;
                    }

                    OlapTable olapTable = (OlapTable) table;
                    long tableSize = 0;
                    long replicaCount = 0;
                    long remoteSize = 0;

                    tableSize = olapTable.getDataSize();
                    replicaCount = olapTable.getReplicaCount();
                    remoteSize = olapTable.getRemoteDataSize();

                    //|TableName|Size|ReplicaCount|RemoteSize
                    List<Object> row = Arrays.asList(table.getName(), tableSize, replicaCount, remoteSize);
                    totalRowsObject.add(row);

                    totalSize += tableSize;
                    totalReplicaCount += replicaCount;
                    totalRemoteSize += remoteSize;
                } // end for tables

                // sort by
                if (orderByPairs != null && !orderByPairs.isEmpty()) {
                    // k-> index, v-> isDesc
                    Map<Integer, Boolean> sortMap = Maps.newLinkedHashMap();
                    for (OrderByPair orderByPair : orderByPairs) {
                        sortMap.put(orderByPair.getIndex(), orderByPair.isDesc());

                    }
                    totalRowsObject.sort(sortRows(sortMap));
                }

                // for output
                for (List<Object> row : totalRowsObject) {
                    //|TableName|Size|ReplicaCount|RemoteSize
                    Pair<Double, String> tableSizePair = DebugUtil.getByteUint((long) row.get(1));
                    String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(tableSizePair.first) + " "
                            + tableSizePair.second;
                    Pair<Double, String> remoteSizePair = DebugUtil.getByteUint((long) row.get(3));
                    String remoteReadableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(remoteSizePair.first) + " "
                            + remoteSizePair.second;
                    List<String> result = Arrays.asList(String.valueOf(row.get(0)),
                            readableSize, String.valueOf(row.get(2)), remoteReadableSize);
                    totalRows.add(result);
                }

                Pair<Double, String> totalSizePair = DebugUtil.getByteUint(totalSize);
                String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalSizePair.first) + " "
                        + totalSizePair.second;
                Pair<Double, String> totalRemoteSizePair = DebugUtil.getByteUint(totalRemoteSize);
                String remoteReadableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalRemoteSizePair.first) + " "
                        + totalRemoteSizePair.second;
                List<String> total = Arrays.asList("Total", readableSize, String.valueOf(totalReplicaCount),
                         remoteReadableSize);
                totalRows.add(total);

                // quota
                long quota = db.getDataQuota();
                long replicaQuota = db.getReplicaQuota();
                Pair<Double, String> quotaPair = DebugUtil.getByteUint(quota);
                String readableQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(quotaPair.first) + " "
                        + quotaPair.second;

                List<String> quotaRow = Arrays.asList("Quota", readableQuota, String.valueOf(replicaQuota), "");
                totalRows.add(quotaRow);

                // left
                long left = Math.max(0, quota - totalSize);
                long replicaCountLeft = Math.max(0, replicaQuota - totalReplicaCount);
                Pair<Double, String> leftPair = DebugUtil.getByteUint(left);
                String readableLeft = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(leftPair.first) + " "
                        + leftPair.second;
                List<String> leftRow = Arrays.asList("Left", readableLeft, String.valueOf(replicaCountLeft), "");
                totalRows.add(leftRow);
            } finally {
                db.readUnlock();
            }
        } else {
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), tableName,
                    PrivPredicate.SHOW)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW DATA",
                        ConnectContext.get().getQualifiedUser(),
                        ConnectContext.get().getRemoteIP(),
                        dbName + ": " + tableName);
            }

            OlapTable olapTable = (OlapTable) db
                    .getTableOrMetaException(tableName.getTbl(), TableType.OLAP);
            long totalSize = 0;
            long totalReplicaCount = 0;
            long totalRemoteSize = 0;
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
                    long indexRemoteSize = 0;
                    for (Partition partition : olapTable.getAllPartitions()) {
                        MaterializedIndex mIndex = partition.getIndex(indexId);
                        indexSize += mIndex.getDataSize(false);
                        indexReplicaCount += mIndex.getReplicaCount();
                        indexRowCount += mIndex.getRowCount() == -1 ? 0 : mIndex.getRowCount();
                        indexRemoteSize += mIndex.getRemoteDataSize();
                    }

                    String indexName = olapTable.getIndexNameById(indexId);
                    // .add("TableName").add("IndexName").add("Size").add("ReplicaCount").add("RowCount")
                    //      .add("RemoteSize")
                    List<Object> row = Arrays.asList(tableName, indexName, indexSize, indexReplicaCount,
                             indexRowCount, indexRemoteSize);
                    totalRowsObject.add(row);

                    totalSize += indexSize;
                    totalReplicaCount += indexReplicaCount;
                    totalRemoteSize += indexRemoteSize;
                } // end for indices

                // sort by
                if (orderByPairs != null && !orderByPairs.isEmpty()) {
                    // k-> index, v-> isDesc
                    Map<Integer, Boolean> sortMap = Maps.newLinkedHashMap();
                    for (OrderByPair orderByPair : orderByPairs) {
                        sortMap.put(orderByPair.getIndex(), orderByPair.isDesc());

                    }
                    totalRowsObject.sort(sortRows(sortMap));
                }

                // for output
                for (int index = 0; index <= totalRowsObject.size() - 1; index++) {
                    //| TableName| IndexName | Size | ReplicaCount | RowCount | RemoteSize
                    List<Object> row = totalRowsObject.get(index);
                    List<String> result;
                    Pair<Double, String> tableSizePair = DebugUtil.getByteUint((long) row.get(2));
                    String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(tableSizePair.first)
                            + " " + tableSizePair.second;
                    Pair<Double, String> remoteSizePair = DebugUtil.getByteUint((long) row.get(5));
                    String remoteReadableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(remoteSizePair.first) + " "
                            + remoteSizePair.second;
                    if (index == 0) {
                        result = Arrays.asList(tableName.getTbl(), String.valueOf(row.get(1)),
                                readableSize, String.valueOf(row.get(3)),
                                String.valueOf(row.get(4)), remoteReadableSize);
                    } else {
                        result = Arrays.asList("", String.valueOf(row.get(1)),
                                readableSize, String.valueOf(row.get(3)),
                                String.valueOf(row.get(4)), remoteReadableSize);
                    }
                    totalRows.add(result);
                }

                Pair<Double, String> totalSizePair = DebugUtil.getByteUint(totalSize);
                String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalSizePair.first) + " "
                        + totalSizePair.second;
                Pair<Double, String> totalRemoteSizePair = DebugUtil.getByteUint(totalRemoteSize);
                String remoteReadableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalRemoteSizePair.first) + " "
                        + totalRemoteSizePair.second;
                List<String> row = Arrays.asList("", "Total", readableSize, String.valueOf(totalReplicaCount), "",
                         remoteReadableSize);
                totalRows.add(row);
            } finally {
                olapTable.readUnlock();
            }
        }
    }

    public static int analyzeColumn(String columnName, String tableName) throws AnalysisException {
        ImmutableList<String> titles = SHOW_TABLE_DATA_META_DATA_ORIGIN;
        if (tableName != null) {
            titles = SHOW_INDEX_DATA_META_DATA_ORIGIN;
        }
        for (String title : titles) {
            if (title.equalsIgnoreCase(columnName)) {
                return titles.indexOf(title);
            }
        }

        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }

    private static Comparator<List<Object>> sortRows(Map<Integer, Boolean> sortMap) {
        Ordering ordering = Ordering.natural();

        return new Comparator<List<Object>>() {
            @Override
            public int compare(final List<Object> o1, final List<Object> o2) {
                ComparisonChain comparisonChain = ComparisonChain.start();

                for (Map.Entry<Integer, Boolean> sort : sortMap.entrySet()) {
                    int index = sort.getKey();
                    boolean isDesc = sort.getValue();
                    comparisonChain = comparisonChain.compare(o1.get(index), o2.get(index),
                        isDesc ? ordering.reverse() : ordering);
                }
                return comparisonChain.result();
            }
        };
    }

    public boolean hasTable() {
        return this.tableName != null;
    }

    public List<List<String>> getResultRows() throws AnalysisException {
        return totalRows;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        String value = null;
        if (properties != null) {
            value = properties.get(WAREHOUSE);
        }
        if (value != null && value.equals("true")) {
            return SHOW_WAREHOUSE_DATA_META_DATA;
        }

        if (Strings.isNullOrEmpty(dbName)) {
            return SHOW_DATABASE_DATA_META_DATA;
        }
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

        if (tableName != null) {
            builder.append(" FROM ");
            builder.append(tableName.toSql());
        }

        // Order By clause
        if (orderByElements != null) {
            builder.append(" ORDER BY ");
            for (int i = 0; i < orderByElements.size(); ++i) {
                builder.append(orderByElements.get(i).getExpr().toSql());
                builder.append((orderByElements.get(i).getIsAsc()) ? " ASC" : " DESC");
                builder.append((i + 1 != orderByElements.size()) ? ", " : "");
            }
        }
        return builder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    private boolean getDbStatsByProperties() {
        if (properties == null) {
            return false;
        }
        String value = properties.get(WAREHOUSE);
        if (value != null && value.equals("true")) {
            List<String> dbList = null;
            String dbNames = properties.get(DB_LIST);
            if (dbNames != null) {
                dbList = Arrays.asList(dbNames.split(","));
            }
            Map<String, Long> dbToDataSize = Env.getCurrentInternalCatalog().getUsedDataQuota();
            Map<Long, Pair<Long, Long>> dbToRecycleSize = Env.getCurrentRecycleBin().getDbToRecycleSize();
            Long total = 0L;
            Long totalRecycleSize = 0L;
            if (dbList == null) {
                for (Map.Entry<String, Long> pair : dbToDataSize.entrySet()) {
                    Database db = Env.getCurrentInternalCatalog().getDbNullable(pair.getKey());
                    if (db == null) {
                        continue;
                    }
                    Long recycleSize = dbToRecycleSize.getOrDefault(db.getId(), Pair.of(0L, 0L)).first;
                    List<String> result = Arrays.asList(db.getName(),
                            String.valueOf(pair.getValue()), String.valueOf(recycleSize));
                    totalRows.add(result);
                    total += pair.getValue();
                    totalRecycleSize += recycleSize;
                    dbToRecycleSize.remove(db.getId());
                }

                // Append left database in recycle bin
                for (Map.Entry<Long, Pair<Long, Long>> entry : dbToRecycleSize.entrySet()) {
                    List<String> result = Arrays.asList("NULL:" + entry.getKey(),
                            "0", String.valueOf(entry.getValue().first));
                    totalRows.add(result);
                    totalRecycleSize += entry.getValue().first;
                }
            } else {
                for (String databaseName : Env.getCurrentInternalCatalog().getDbNames()) {
                    Database db = Env.getCurrentInternalCatalog().getDbNullable(databaseName);
                    if (db == null) {
                        continue;
                    }
                    if (!dbList.contains(db.getName())) {
                        continue;
                    }
                    Long recycleSize = dbToRecycleSize.getOrDefault(db.getId(), Pair.of(0L, 0L)).first;
                    Long dataSize = dbToDataSize.getOrDefault(databaseName, 0L);
                    List<String> result =
                            Arrays.asList(db.getName(), String.valueOf(dataSize), String.valueOf(recycleSize));
                    totalRows.add(result);
                    total += dataSize;
                    totalRecycleSize += recycleSize;
                }
            }
            List<String> result = Arrays.asList("total", String.valueOf(total), String.valueOf(totalRecycleSize));
            totalRows.add(result);
            return true;
        }
        return false;
    }

    private void getAllDbStats() throws AnalysisException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }
        List<String> dbNames = Env.getCurrentInternalCatalog().getDbNames();
        if (dbNames == null || dbNames.isEmpty()) {
            return;
        }
        long totalSize = 0;
        long totalRemoteSize = 0;
        long totalRecycleSize = 0;
        long totalRecycleRemoteSize = 0;
        Map<Long, Pair<Long, Long>> dbToRecycleSize = Env.getCurrentRecycleBin().getDbToRecycleSize();
        // show all database datasize
        for (String dbName : dbNames) {
            DatabaseIf db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
            if (db == null) {
                continue;
            }
            List<String> dbInfo = new ArrayList<>();
            db.readLock();
            try {
                dbInfo.add(String.valueOf(db.getId()));
                dbInfo.add(dbName);
                Pair<Long, Long> usedSize =  ((Database) db).getUsedDataSize();
                dbInfo.add(String.valueOf(usedSize.first));
                dbInfo.add(String.valueOf(usedSize.second));
                totalSize += usedSize.first;
                totalRemoteSize += usedSize.second;
            } finally {
                db.readUnlock();
            }

            Pair<Long, Long> recycleSize = dbToRecycleSize.getOrDefault(db.getId(), Pair.of(0L, 0L));
            dbInfo.add(String.valueOf(recycleSize.first));
            dbInfo.add(String.valueOf(recycleSize.second));
            totalRecycleSize += recycleSize.first;
            totalRecycleRemoteSize += recycleSize.second;
            dbToRecycleSize.remove(db.getId());
            totalRows.add(dbInfo);
        }

        // Append left database in recycle bin
        for (Map.Entry<Long, Pair<Long, Long>> entry : dbToRecycleSize.entrySet()) {
            List<String> dbInfo = new ArrayList<>();
            dbInfo.add(String.valueOf(entry.getKey()));
            dbInfo.add("NULL");
            dbInfo.add("0");
            dbInfo.add("0");
            dbInfo.add(String.valueOf(entry.getValue().first));
            dbInfo.add(String.valueOf(entry.getValue().second));
            totalRecycleSize += entry.getValue().first;
            totalRecycleRemoteSize += entry.getValue().second;
            totalRows.add(dbInfo);
        }

        // calc total size
        List<String> dbInfo = new ArrayList<>();
        dbInfo.add("Total");
        dbInfo.add("NULL");
        dbInfo.add(String.valueOf(totalSize));
        dbInfo.add(String.valueOf(totalRemoteSize));
        dbInfo.add(String.valueOf(totalRecycleSize));
        dbInfo.add(String.valueOf(totalRecycleRemoteSize));
        totalRows.add(dbInfo);
    }
}
