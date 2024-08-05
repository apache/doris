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

package org.apache.doris.datasource;

import org.apache.doris.analysis.TableName;
import org.apache.doris.common.info.SimpleTableInfo;
import org.apache.doris.datasource.hive.HMSCachedClient;
import org.apache.doris.datasource.hive.HiveDatabaseMetadata;
import org.apache.doris.datasource.hive.HivePartitionStatistics;
import org.apache.doris.datasource.hive.HivePartitionWithStatistics;
import org.apache.doris.datasource.hive.HiveTableMetadata;
import org.apache.doris.datasource.hive.HiveUtil;
import org.apache.doris.datasource.hive.event.MetastoreNotificationFetchException;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TestHMSCachedClient implements HMSCachedClient {

    public Map<SimpleTableInfo, List<Partition>> partitions = new ConcurrentHashMap<>();
    public Map<String, List<Table>> tables = new HashMap<>();
    public List<Database> dbs = new ArrayList<>();

    @Override
    public void close() {
    }

    @Override
    public Database getDatabase(String dbName) {
        for (Database db : this.dbs) {
            if (db.getName().equals(dbName)) {
                return db;
            }
        }
        throw new RuntimeException("can't found database: " + dbName);
    }

    @Override
    public List<String> getAllDatabases() {
        return null;
    }

    @Override
    public List<String> getAllTables(String dbName) {
        return null;
    }

    @Override
    public boolean tableExists(String dbName, String tblName) {
        List<Table> tablesList = getTableList(dbName);
        for (Table table : tablesList) {
            if (table.getTableName().equals(tblName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName) {
        List<Partition> partitionList = getPartitionList(dbName, tblName);
        ArrayList<String> ret = new ArrayList<>();
        for (Partition partition : partitionList) {
            StringBuilder names = new StringBuilder();
            List<String> values = partition.getValues();
            for (int i = 0; i < values.size(); i++) {
                names.append(values.get(i));
                if (i < values.size() - 1) {
                    names.append("/");
                }
            }
            ret.add(names.toString());
        }
        return ret;
    }

    @Override
    public List<Partition> listPartitions(String dbName, String tblName) {
        return getPartitionList(dbName, tblName);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, long maxListPartitionNum) {
        return listPartitionNames(dbName, tblName);
    }

    @Override
    public Partition getPartition(String dbName, String tblName, List<String> partitionValues) {
        synchronized (this) {
            List<Partition> partitionList = getPartitionList(dbName, tblName);
            for (Partition partition : partitionList) {
                if (partition.getValues().equals(partitionValues)) {
                    return partition;
                }
            }
            throw new RuntimeException("can't found partition");
        }
    }

    @Override
    public List<Partition> getPartitions(String dbName, String tblName, List<String> partitionNames) {
        synchronized (this) {
            List<Partition> partitionList = getPartitionList(dbName, tblName);
            ArrayList<Partition> ret = new ArrayList<>();
            List<List<String>> partitionValuesList =
                    partitionNames
                            .stream()
                            .map(HiveUtil::toPartitionValues)
                            .collect(Collectors.toList());
            partitionValuesList.forEach(values -> {
                for (Partition partition : partitionList) {
                    if (partition.getValues().equals(values)) {
                        ret.add(partition);
                        break;
                    }
                }
            });
            return ret;
        }
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        List<Table> tablesList = getTableList(dbName);
        for (Table table : tablesList) {
            if (table.getTableName().equals(tblName)) {
                return table;
            }
        }
        throw new RuntimeException("can't found table: " + tblName);
    }

    @Override
    public List<FieldSchema> getSchema(String dbName, String tblName) {
        return null;
    }

    @Override
    public Map<String, String> getDefaultColumnValues(String dbName, String tblName) {
        return new HashMap<>();
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tblName, List<String> columns) {
        return null;
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tblName, List<String> partNames, List<String> columns) {
        return null;
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() {
        return null;
    }

    @Override
    public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents, IMetaStoreClient.NotificationFilter filter) throws MetastoreNotificationFetchException {
        return null;
    }

    @Override
    public long openTxn(String user) {
        return 0;
    }

    @Override
    public void commitTxn(long txnId) {

    }

    @Override
    public ValidWriteIdList getValidWriteIds(String fullTableName, long currentTransactionId) {
        return null;
    }

    @Override
    public void acquireSharedLock(String queryId, long txnId, String user, TableName tblName, List<String> partitionNames, long timeoutMs) {

    }

    @Override
    public String getCatalogLocation(String catalogName) {
        return null;
    }

    @Override
    public void createDatabase(DatabaseMetadata db) {
        dbs.add(HiveUtil.toHiveDatabase((HiveDatabaseMetadata) db));
        tables.put(db.getDbName(), new ArrayList<>());
    }

    @Override
    public void dropDatabase(String dbName) {
        Database db = getDatabase(dbName);
        this.dbs.remove(db);
    }

    @Override
    public void dropTable(String dbName, String tableName) {
        Table table = getTable(dbName, tableName);
        this.tables.get(dbName).remove(table);
        this.partitions.remove(new SimpleTableInfo(dbName, tableName));
    }

    @Override
    public void truncateTable(String dbName, String tblName, List<String> partitions) {}

    @Override
    public void createTable(TableMetadata tbl, boolean ignoreIfExists) {
        String dbName = tbl.getDbName();
        String tbName = tbl.getTableName();
        if (tableExists(dbName, tbName)) {
            throw new RuntimeException("Table '" + tbName + "' has existed in '" + dbName + "'.");
        }

        List<Table> tableList = getTableList(tbl.getDbName());
        tableList.add(HiveUtil.toHiveTable((HiveTableMetadata) tbl));
        SimpleTableInfo key = new SimpleTableInfo(dbName, tbName);
        partitions.put(key, new ArrayList<>());
    }

    @Override
    public void updateTableStatistics(String dbName, String tableName, Function<HivePartitionStatistics, HivePartitionStatistics> update) {
        synchronized (this) {
            Table originTable = getTable(dbName, tableName);
            Map<String, String> originParams = originTable.getParameters();
            HivePartitionStatistics updatedStats = update.apply(HiveUtil.toHivePartitionStatistics(originParams));

            Table newTable = originTable.deepCopy();
            Map<String, String> newParams =
                    HiveUtil.updateStatisticsParameters(originParams, updatedStats.getCommonStatistics());
            newParams.put("transient_lastDdlTime", String.valueOf(System.currentTimeMillis() / 1000));
            newTable.setParameters(newParams);
            List<Table> tableList = getTableList(dbName);
            tableList.remove(originTable);
            tableList.add(newTable);
        }
    }

    @Override
    public void updatePartitionStatistics(String dbName, String tableName, String partitionName, Function<HivePartitionStatistics, HivePartitionStatistics> update) {

        synchronized (this) {
            List<Partition> partitions = getPartitions(dbName, tableName, ImmutableList.of(partitionName));
            if (partitions.size() != 1) {
                throw new RuntimeException("Metastore returned multiple partitions for name: " + partitionName);
            }

            Partition originPartition = partitions.get(0);
            Map<String, String> originParams = originPartition.getParameters();
            HivePartitionStatistics updatedStats = update.apply(HiveUtil.toHivePartitionStatistics(originParams));

            Partition modifiedPartition = originPartition.deepCopy();
            Map<String, String> newParams =
                    HiveUtil.updateStatisticsParameters(originParams, updatedStats.getCommonStatistics());
            newParams.put("transient_lastDdlTime", String.valueOf(System.currentTimeMillis() / 1000));
            modifiedPartition.setParameters(newParams);

            List<Partition> partitionList = getPartitionList(dbName, tableName);
            partitionList.remove(originPartition);
            partitionList.add(modifiedPartition);
        }
    }

    @Override
    public void addPartitions(String dbName, String tableName, List<HivePartitionWithStatistics> partitions) {
        synchronized (this) {
            List<Partition> partitionList = getPartitionList(dbName, tableName);
            List<Partition> hivePartitions = partitions.stream()
                    .map(HiveUtil::toMetastoreApiPartition)
                    .collect(Collectors.toList());
            partitionList.addAll(hivePartitions);
        }
    }

    @Override
    public void dropPartition(String dbName, String tableName, List<String> partitionValues, boolean deleteData) {
        synchronized (this) {
            List<Partition> partitionList = getPartitionList(dbName, tableName);
            for (int j = 0; j < partitionList.size(); j++) {
                Partition partition = partitionList.get(j);
                if (partition.getValues().equals(partitionValues)) {
                    partitionList.remove(partition);
                    return;
                }
            }
            throw new RuntimeException("can't found the partition");
        }
    }

    public List<Partition> getPartitionList(String dbName, String tableName) {
        SimpleTableInfo key = new SimpleTableInfo(dbName, tableName);
        List<Partition> partitionList = this.partitions.get(key);
        if (partitionList == null) {
            throw new RuntimeException("can't found table: " + key);
        }
        return partitionList;
    }

    public List<Table> getTableList(String dbName) {
        List<Table> tablesList = this.tables.get(dbName);
        if (tablesList == null) {
            throw new RuntimeException("can't found database: " + dbName);
        }
        return tablesList;
    }
}
