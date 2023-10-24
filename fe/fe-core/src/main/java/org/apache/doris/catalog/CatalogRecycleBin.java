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

package org.apache.doris.catalog;

import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.RangeUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.RecoverInfo;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.collect.Table.Cell;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CatalogRecycleBin extends MasterDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(CatalogRecycleBin.class);
    // erase meta at least after minEraseLatency milliseconds
    // to avoid erase log ahead of drop log
    private static final long minEraseLatency = 10 * 60 * 1000;  // 10 min

    private Map<Long, RecycleDatabaseInfo> idToDatabase;
    private Map<Long, RecycleTableInfo> idToTable;
    private Map<Long, RecyclePartitionInfo> idToPartition;

    private Map<Long, Long> idToRecycleTime;

    public CatalogRecycleBin() {
        super("recycle bin");
        idToDatabase = Maps.newHashMap();
        idToTable = Maps.newHashMap();
        idToPartition = Maps.newHashMap();
        idToRecycleTime = Maps.newHashMap();
    }

    public synchronized boolean allTabletsInRecycledStatus(List<Long> backendTabletIds) {
        Set<Long> recycledTabletSet = Sets.newHashSet();

        Iterator<Map.Entry<Long, RecyclePartitionInfo>> iterator = idToPartition.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecyclePartitionInfo> entry = iterator.next();
            RecyclePartitionInfo partitionInfo = entry.getValue();
            Partition partition = partitionInfo.getPartition();
            addRecycledTabletsForPartition(recycledTabletSet, partition);
        }

        Iterator<Map.Entry<Long, RecycleTableInfo>> tableIter = idToTable.entrySet().iterator();
        while (tableIter.hasNext()) {
            Map.Entry<Long, RecycleTableInfo> entry = tableIter.next();
            RecycleTableInfo tableInfo = entry.getValue();
            Table table = tableInfo.getTable();
            addRecycledTabletsForTable(recycledTabletSet, table);
        }

        Iterator<Map.Entry<Long, RecycleDatabaseInfo>> dbIterator = idToDatabase.entrySet().iterator();
        while (dbIterator.hasNext()) {
            Map.Entry<Long, RecycleDatabaseInfo> entry = dbIterator.next();
            RecycleDatabaseInfo dbInfo = entry.getValue();
            Database db = dbInfo.getDb();
            for (Table table : db.getTables()) {
                addRecycledTabletsForTable(recycledTabletSet, table);
            }
        }

        return recycledTabletSet.size() >= backendTabletIds.size() && recycledTabletSet.containsAll(backendTabletIds);
    }

    private void addRecycledTabletsForTable(Set<Long> recycledTabletSet, Table table) {
        if (table.getType() == TableType.OLAP) {
            OlapTable olapTable = (OlapTable) table;
            Collection<Partition> allPartitions = olapTable.getAllPartitions();
            for (Partition partition : allPartitions) {
                addRecycledTabletsForPartition(recycledTabletSet, partition);
            }
        }
    }

    private void addRecycledTabletsForPartition(Set<Long> recycledTabletSet, Partition partition) {
        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
            for (Tablet tablet : index.getTablets()) {
                recycledTabletSet.add(tablet.getId());
            }
        }
    }

    public synchronized boolean recycleDatabase(Database db, Set<String> tableNames, Set<Long> tableIds,
                                                boolean isReplay, long replayRecycleTime) {
        long recycleTime = 0;
        if (idToDatabase.containsKey(db.getId())) {
            LOG.error("db[{}] already in recycle bin.", db.getId());
            return false;
        }

        // db should be empty. all tables are recycled before
        Preconditions.checkState(db.getTables().isEmpty());

        // recycle db
        RecycleDatabaseInfo databaseInfo = new RecycleDatabaseInfo(db, tableNames, tableIds);
        idToDatabase.put(db.getId(), databaseInfo);
        if (!isReplay || replayRecycleTime == 0) {
            recycleTime = System.currentTimeMillis();
        } else {
            recycleTime = replayRecycleTime;
        }
        idToRecycleTime.put(db.getId(), recycleTime);
        LOG.info("recycle db[{}-{}]", db.getId(), db.getFullName());
        return true;
    }

    public synchronized boolean recycleTable(long dbId, Table table, boolean isReplay, long replayRecycleTime) {
        long recycleTime = 0;
        if (idToTable.containsKey(table.getId())) {
            LOG.error("table[{}] already in recycle bin.", table.getId());
            return false;
        }

        // recycle table
        RecycleTableInfo tableInfo = new RecycleTableInfo(dbId, table);
        if (!isReplay || replayRecycleTime == 0) {
            recycleTime = System.currentTimeMillis();
        } else {
            recycleTime = replayRecycleTime;
        }
        idToRecycleTime.put(table.getId(), recycleTime);
        idToTable.put(table.getId(), tableInfo);
        LOG.info("recycle table[{}-{}]", table.getId(), table.getName());
        return true;
    }

    public synchronized boolean recyclePartition(long dbId, long tableId, String tableName, Partition partition,
                                                 Range<PartitionKey> range, PartitionItem listPartitionItem,
                                                 DataProperty dataProperty, ReplicaAllocation replicaAlloc,
                                                 boolean isInMemory, boolean isMutable) {
        if (idToPartition.containsKey(partition.getId())) {
            LOG.error("partition[{}] already in recycle bin.", partition.getId());
            return false;
        }

        // recycle partition
        RecyclePartitionInfo partitionInfo = new RecyclePartitionInfo(dbId, tableId, partition,
                range, listPartitionItem, dataProperty, replicaAlloc, isInMemory, isMutable);
        idToRecycleTime.put(partition.getId(), System.currentTimeMillis());
        idToPartition.put(partition.getId(), partitionInfo);
        LOG.info("recycle partition[{}-{}] of table [{}-{}]", partition.getId(), partition.getName(),
                tableId, tableName);
        return true;
    }

    public synchronized Long getRecycleTimeById(long id) {
        return idToRecycleTime.get(id);
    }

    public synchronized void setRecycleTimeByIdForReplay(long id, Long recycleTime) {
        idToRecycleTime.put(id, recycleTime);
    }

    public synchronized boolean isRecyclePartition(long dbId, long tableId, long partitionId) {
        return idToDatabase.containsKey(dbId) || idToTable.containsKey(tableId)
                || idToPartition.containsKey(partitionId);
    }

    public synchronized void getRecycleIds(Set<Long> dbIds, Set<Long> tableIds, Set<Long> partitionIds) {
        dbIds.addAll(idToDatabase.keySet());
        tableIds.addAll(idToTable.keySet());
        partitionIds.addAll(idToPartition.keySet());
    }

    private synchronized boolean isExpire(long id, long currentTimeMs) {
        long latency = currentTimeMs - idToRecycleTime.get(id);
        return latency > minEraseLatency && latency > Config.catalog_trash_expire_second * 1000L;
    }

    private synchronized void eraseDatabase(long currentTimeMs, int keepNum) {
        // 1. erase expired database
        Iterator<Map.Entry<Long, RecycleDatabaseInfo>> dbIter = idToDatabase.entrySet().iterator();
        while (dbIter.hasNext()) {
            Map.Entry<Long, RecycleDatabaseInfo> entry = dbIter.next();
            RecycleDatabaseInfo dbInfo = entry.getValue();
            Database db = dbInfo.getDb();
            if (isExpire(db.getId(), currentTimeMs)) {
                // erase db
                dbIter.remove();
                idToRecycleTime.remove(entry.getKey());
                Env.getCurrentEnv().eraseDatabase(db.getId(), true);
                LOG.info("erase db[{}]", db.getId());
            }
        }
        // 2. erase exceed number
        if (keepNum < 0) {
            return;
        }
        Set<String> dbNames = idToDatabase.values().stream().map(d -> d.getDb().getFullName())
                .collect(Collectors.toSet());
        for (String dbName : dbNames) {
            eraseDatabaseWithSameName(dbName, currentTimeMs, keepNum);
        }
    }

    private synchronized List<Long> getSameNameDbIdListToErase(String dbName, int maxSameNameTrashNum) {
        Iterator<Map.Entry<Long, RecycleDatabaseInfo>> iterator = idToDatabase.entrySet().iterator();
        List<List<Long>> dbRecycleTimeLists = Lists.newArrayList();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecycleDatabaseInfo> entry = iterator.next();
            RecycleDatabaseInfo dbInfo = entry.getValue();
            Database db = dbInfo.getDb();
            if (db.getFullName().equals(dbName)) {
                List<Long> dbRecycleTimeInfo = Lists.newArrayList();
                dbRecycleTimeInfo.add(entry.getKey());
                dbRecycleTimeInfo.add(idToRecycleTime.get(entry.getKey()));

                dbRecycleTimeLists.add(dbRecycleTimeInfo);
            }
        }
        List<Long> dbIdToErase = Lists.newArrayList();
        if (dbRecycleTimeLists.size() <= maxSameNameTrashNum) {
            return dbIdToErase;
        }
        // order by recycle time desc
        dbRecycleTimeLists.sort((x, y) ->
                (x.get(1).longValue() < y.get(1).longValue()) ? 1 : ((x.get(1).equals(y.get(1))) ? 0 : -1));

        for (int i = maxSameNameTrashNum; i < dbRecycleTimeLists.size(); i++) {
            dbIdToErase.add(dbRecycleTimeLists.get(i).get(0));
        }
        return dbIdToErase;
    }

    private synchronized void eraseDatabaseWithSameName(String dbName, long currentTimeMs, int maxSameNameTrashNum) {
        List<Long> dbIdToErase = getSameNameDbIdListToErase(dbName, maxSameNameTrashNum);
        for (Long dbId : dbIdToErase) {
            RecycleDatabaseInfo dbInfo = idToDatabase.get(dbId);
            if (!isExpireMinLatency(dbId, currentTimeMs)) {
                continue;
            }
            eraseAllTables(dbInfo);
            idToDatabase.remove(dbId);
            idToRecycleTime.remove(dbId);
            Env.getCurrentEnv().eraseDatabase(dbId, true);
            LOG.info("erase database[{}] name: {}", dbId, dbName);
        }
    }

    private synchronized boolean isExpireMinLatency(long id, long currentTimeMs) {
        return (currentTimeMs - idToRecycleTime.get(id)) > minEraseLatency;
    }

    private void eraseAllTables(RecycleDatabaseInfo dbInfo) {
        Database db = dbInfo.getDb();
        Set<String> tableNames = Sets.newHashSet(dbInfo.getTableNames());
        Set<Long> tableIds = Sets.newHashSet(dbInfo.getTableIds());
        long dbId = db.getId();
        Iterator<Map.Entry<Long, RecycleTableInfo>> iterator = idToTable.entrySet().iterator();
        while (iterator.hasNext() && !tableNames.isEmpty()) {
            Map.Entry<Long, RecycleTableInfo> entry = iterator.next();
            RecycleTableInfo tableInfo = entry.getValue();
            if (tableInfo.getDbId() != dbId || !tableNames.contains(tableInfo.getTable().getName())
                    || !tableIds.contains(tableInfo.getTable().getId())) {
                continue;
            }

            Table table = tableInfo.getTable();
            if (table.getType() == TableType.OLAP) {
                Env.getCurrentEnv().onEraseOlapTable((OlapTable) table, false);
            }
            iterator.remove();
            idToRecycleTime.remove(table.getId());
            tableNames.remove(table.getName());
            Env.getCurrentEnv().getEditLog().logEraseTable(table.getId());
            LOG.info("erase db[{}] with table[{}]: {}", dbId, table.getId(), table.getName());
        }
    }

    public synchronized void replayEraseDatabase(long dbId) {
        idToDatabase.remove(dbId);
        idToRecycleTime.remove(dbId);
        Env.getCurrentEnv().eraseDatabase(dbId, false);
        LOG.info("replay erase db[{}]", dbId);
    }

    private synchronized void eraseTable(long currentTimeMs, int keepNum) {
        // 1. erase expired tables
        Iterator<Map.Entry<Long, RecycleTableInfo>> tableIter = idToTable.entrySet().iterator();
        while (tableIter.hasNext()) {
            Map.Entry<Long, RecycleTableInfo> entry = tableIter.next();
            RecycleTableInfo tableInfo = entry.getValue();
            Table table = tableInfo.getTable();
            long tableId = table.getId();

            if (isExpire(tableId, currentTimeMs)) {
                if (table.getType() == TableType.OLAP) {
                    Env.getCurrentEnv().onEraseOlapTable((OlapTable) table, false);
                }

                // erase table
                tableIter.remove();
                idToRecycleTime.remove(tableId);

                // log
                Env.getCurrentEnv().getEditLog().logEraseTable(tableId);
                LOG.info("erase table[{}]", tableId);
            }
        } // end for tables

        // 2. erase exceed num
        if (keepNum < 0) {
            return;
        }
        Map<Long, Set<String>> dbId2TableNames = Maps.newHashMap();
        for (RecycleTableInfo tableInfo : idToTable.values()) {
            Set<String> tblNames = dbId2TableNames.get(tableInfo.dbId);
            if (tblNames == null) {
                tblNames = Sets.newHashSet();
                dbId2TableNames.put(tableInfo.dbId, tblNames);
            }
            tblNames.add(tableInfo.getTable().getName());
        }
        for (Map.Entry<Long, Set<String>> entry : dbId2TableNames.entrySet()) {
            for (String tblName : entry.getValue()) {
                eraseTableWithSameName(entry.getKey(), tblName, currentTimeMs, keepNum);
            }
        }
    }

    private synchronized List<Long> getSameNameTableIdListToErase(long dbId, String tableName,
                                                                  int maxSameNameTrashNum) {
        Iterator<Map.Entry<Long, RecycleTableInfo>> iterator = idToTable.entrySet().iterator();
        List<List<Long>> tableRecycleTimeLists = Lists.newArrayList();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecycleTableInfo> entry = iterator.next();
            RecycleTableInfo tableInfo = entry.getValue();
            if (tableInfo.getDbId() != dbId) {
                continue;
            }

            Table table = tableInfo.getTable();
            if (table.getName().equals(tableName)) {
                List<Long> tableRecycleTimeInfo = Lists.newArrayList();
                tableRecycleTimeInfo.add(entry.getKey());
                tableRecycleTimeInfo.add(idToRecycleTime.get(entry.getKey()));

                tableRecycleTimeLists.add(tableRecycleTimeInfo);
            }
        }
        List<Long> tableIdToErase = Lists.newArrayList();
        if (tableRecycleTimeLists.size() <= maxSameNameTrashNum) {
            return tableIdToErase;
        }
        // order by recycle time desc
        tableRecycleTimeLists.sort((x, y) ->
                (x.get(1).longValue() < y.get(1).longValue()) ? 1 : ((x.get(1).equals(y.get(1))) ? 0 : -1));

        for (int i = maxSameNameTrashNum; i < tableRecycleTimeLists.size(); i++) {
            tableIdToErase.add(tableRecycleTimeLists.get(i).get(0));
        }
        return tableIdToErase;
    }

    private synchronized void eraseTableWithSameName(long dbId, String tableName, long currentTimeMs,
            int maxSameNameTrashNum) {
        List<Long> tableIdToErase = getSameNameTableIdListToErase(dbId, tableName, maxSameNameTrashNum);
        for (Long tableId : tableIdToErase) {
            RecycleTableInfo tableInfo = idToTable.get(tableId);
            if (!isExpireMinLatency(tableId, currentTimeMs)) {
                continue;
            }
            Table table = tableInfo.getTable();
            if (table.getType() == TableType.OLAP) {
                Env.getCurrentEnv().onEraseOlapTable((OlapTable) table, false);
            }

            idToTable.remove(tableId);
            idToRecycleTime.remove(tableId);
            Env.getCurrentEnv().getEditLog().logEraseTable(tableId);
            LOG.info("erase table[{}] name: {} from db[{}]", tableId, tableName, dbId);
        }
    }

    public synchronized void replayEraseTable(long tableId) {
        LOG.info("before replay erase table[{}]", tableId);
        RecycleTableInfo tableInfo = idToTable.remove(tableId);
        idToRecycleTime.remove(tableId);
        Table table = tableInfo.getTable();
        if (table.getType() == TableType.OLAP) {
            Env.getCurrentEnv().onEraseOlapTable((OlapTable) table, true);
        }
        LOG.info("replay erase table[{}]", tableId);
    }

    private synchronized void erasePartition(long currentTimeMs, int keepNum) {
        // 1. erase expired partitions
        Iterator<Map.Entry<Long, RecyclePartitionInfo>> iterator = idToPartition.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecyclePartitionInfo> entry = iterator.next();
            RecyclePartitionInfo partitionInfo = entry.getValue();
            Partition partition = partitionInfo.getPartition();

            long partitionId = entry.getKey();
            if (isExpire(partitionId, currentTimeMs)) {
                Env.getCurrentEnv().onErasePartition(partition);
                // erase partition
                iterator.remove();
                idToRecycleTime.remove(partitionId);
                // log
                Env.getCurrentEnv().getEditLog().logErasePartition(partitionId);
                LOG.info("erase partition[{}]. reason: expired", partitionId);
            }
        } // end for partitions

        // 2. erase exceed number
        if (keepNum < 0) {
            return;
        }
        com.google.common.collect.Table<Long, Long, Set<String>> dbTblId2PartitionNames = HashBasedTable.create();
        for (RecyclePartitionInfo partitionInfo : idToPartition.values()) {
            Set<String> partitionNames = dbTblId2PartitionNames.get(partitionInfo.dbId, partitionInfo.tableId);
            if (partitionNames == null) {
                partitionNames = Sets.newHashSet();
                dbTblId2PartitionNames.put(partitionInfo.dbId, partitionInfo.tableId, partitionNames);
            }
            partitionNames.add(partitionInfo.getPartition().getName());
        }
        for (Cell<Long, Long, Set<String>> cell : dbTblId2PartitionNames.cellSet()) {
            for (String partitionName : cell.getValue()) {
                erasePartitionWithSameName(cell.getRowKey(), cell.getColumnKey(), partitionName, currentTimeMs,
                        keepNum);
            }
        }
    }

    private synchronized List<Long> getSameNamePartitionIdListToErase(long dbId, long tableId, String partitionName,
                                                                      int maxSameNameTrashNum) {
        Iterator<Map.Entry<Long, RecyclePartitionInfo>> iterator = idToPartition.entrySet().iterator();
        List<List<Long>> partitionRecycleTimeLists = Lists.newArrayList();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecyclePartitionInfo> entry = iterator.next();
            RecyclePartitionInfo partitionInfo = entry.getValue();
            if (partitionInfo.getDbId() != dbId || partitionInfo.getTableId() != tableId) {
                continue;
            }

            Partition partition = partitionInfo.getPartition();
            if (partition.getName().equals(partitionName)) {
                List<Long> partitionRecycleTimeInfo = Lists.newArrayList();
                partitionRecycleTimeInfo.add(entry.getKey());
                partitionRecycleTimeInfo.add(idToRecycleTime.get(entry.getKey()));

                partitionRecycleTimeLists.add(partitionRecycleTimeInfo);
            }
        }
        List<Long> partitionIdToErase = Lists.newArrayList();
        if (partitionRecycleTimeLists.size() <= maxSameNameTrashNum) {
            return partitionIdToErase;
        }
        // order by recycle time desc
        partitionRecycleTimeLists.sort((x, y) ->
                (x.get(1).longValue() < y.get(1).longValue()) ? 1 : ((x.get(1).equals(y.get(1))) ? 0 : -1));

        for (int i = maxSameNameTrashNum; i < partitionRecycleTimeLists.size(); i++) {
            partitionIdToErase.add(partitionRecycleTimeLists.get(i).get(0));
        }
        return partitionIdToErase;
    }

    private synchronized void erasePartitionWithSameName(long dbId, long tableId, String partitionName,
            long currentTimeMs, int maxSameNameTrashNum) {
        List<Long> partitionIdToErase = getSameNamePartitionIdListToErase(dbId, tableId, partitionName,
                maxSameNameTrashNum);
        for (Long partitionId : partitionIdToErase) {
            RecyclePartitionInfo partitionInfo = idToPartition.get(partitionId);
            if (!isExpireMinLatency(partitionId, currentTimeMs)) {
                continue;
            }
            Partition partition = partitionInfo.getPartition();

            Env.getCurrentEnv().onErasePartition(partition);
            idToPartition.remove(partitionId);
            idToRecycleTime.remove(partitionId);
            Env.getCurrentEnv().getEditLog().logErasePartition(partitionId);
            LOG.info("erase partition[{}] name: {} from table[{}] from db[{}]", partitionId, partitionName, tableId,
                    dbId);
        }
    }

    public synchronized void replayErasePartition(long partitionId) {
        RecyclePartitionInfo partitionInfo = idToPartition.remove(partitionId);
        idToRecycleTime.remove(partitionId);

        if (partitionInfo == null) {
            LOG.error("replayErasePartition: partitionInfo is null for partitionId[{}]", partitionId);
        }

        Partition partition = partitionInfo.getPartition();
        Env.getCurrentEnv().onErasePartition(partition);

        LOG.info("replay erase partition[{}]", partitionId);
    }

    public synchronized Database recoverDatabase(String dbName, long dbId) throws DdlException {
        RecycleDatabaseInfo dbInfo = null;
        long recycleTime = -1;
        Iterator<Map.Entry<Long, RecycleDatabaseInfo>> iterator = idToDatabase.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecycleDatabaseInfo> entry = iterator.next();
            if (dbName.equals(entry.getValue().getDb().getFullName())) {
                if (dbId == -1) {
                    if (recycleTime <= idToRecycleTime.get(entry.getKey())) {
                        recycleTime = idToRecycleTime.get(entry.getKey());
                        dbInfo = entry.getValue();
                    }
                } else if (entry.getKey() == dbId) {
                    dbInfo = entry.getValue();
                    break;
                }
            }
        }

        if (dbInfo == null) {
            throw new DdlException("Unknown database '" + dbName + "' or database id '" + dbId + "'");
        }

        // 1. recover all tables in this db
        recoverAllTables(dbInfo);

        Database db = dbInfo.getDb();
        // 2. remove db from idToDatabase and idToRecycleTime
        idToDatabase.remove(db.getId());
        idToRecycleTime.remove(db.getId());

        return db;
    }

    public synchronized Database replayRecoverDatabase(long dbId) {
        RecycleDatabaseInfo dbInfo = idToDatabase.get(dbId);

        try {
            recoverAllTables(dbInfo);
        } catch (DdlException e) {
            // should not happened
            LOG.error("failed replay recover database: {}", dbId, e);
        }

        idToDatabase.remove(dbId);
        idToRecycleTime.remove(dbId);

        return dbInfo.getDb();
    }

    private void recoverAllTables(RecycleDatabaseInfo dbInfo) throws DdlException {
        Database db = dbInfo.getDb();
        Set<String> tableNames = Sets.newHashSet(dbInfo.getTableNames());
        Set<Long> tableIds = Sets.newHashSet(dbInfo.getTableIds());
        long dbId = db.getId();
        Iterator<Map.Entry<Long, RecycleTableInfo>> iterator = idToTable.entrySet().iterator();
        while (iterator.hasNext() && !tableNames.isEmpty()) {
            Map.Entry<Long, RecycleTableInfo> entry = iterator.next();
            RecycleTableInfo tableInfo = entry.getValue();
            if (tableInfo.getDbId() != dbId || !tableNames.contains(tableInfo.getTable().getName())
                    || !tableIds.contains(tableInfo.getTable().getId())) {
                continue;
            }

            Table table = tableInfo.getTable();
            db.createTable(table);
            LOG.info("recover db[{}] with table[{}]: {}", dbId, table.getId(), table.getName());
            iterator.remove();
            idToRecycleTime.remove(table.getId());
            tableNames.remove(table.getName());
        }

        if (!tableNames.isEmpty()) {
            throw new DdlException("Tables[" + tableNames + "] is missing. Can not recover db");
        }
    }

    public synchronized boolean recoverTable(Database db, String tableName, long tableId,
                                             String newTableName) throws DdlException {
        // make sure to get db lock
        Table table = null;
        long recycleTime = -1;
        long dbId = db.getId();
        Iterator<Map.Entry<Long, RecycleTableInfo>> iterator = idToTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecycleTableInfo> entry = iterator.next();
            RecycleTableInfo tableInfo = entry.getValue();
            if (tableInfo.getDbId() != dbId) {
                continue;
            }

            if (!tableInfo.getTable().getName().equals(tableName)) {
                continue;
            }

            if (tableId == -1) {
                if (recycleTime <= idToRecycleTime.get(entry.getKey())) {
                    recycleTime = idToRecycleTime.get(entry.getKey());
                    table = tableInfo.getTable();
                }
            } else if (entry.getKey() == tableId) {
                table = tableInfo.getTable();
                break;
            }
        }

        if (table == null) {
            throw new DdlException("Unknown table '" + tableName + "' or table id '" + tableId + "' in "
                + db.getFullName());
        }

        innerRecoverTable(db, table, tableName, newTableName, null, false);
        LOG.info("recover db[{}] with table[{}]: {}", dbId, table.getId(), table.getName());
        return true;
    }

    public synchronized void replayRecoverTable(Database db, long tableId, String newTableName) throws DdlException {
        // make sure to get db write lock
        Iterator<Map.Entry<Long, RecycleTableInfo>> iterator = idToTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecycleTableInfo> entry = iterator.next();
            RecycleTableInfo tableInfo = entry.getValue();
            if (tableInfo.getTable().getId() != tableId) {
                continue;
            }
            Preconditions.checkState(tableInfo.getDbId() == db.getId());
            Table table = tableInfo.getTable();
            String tableName = table.getName();
            if (innerRecoverTable(db, table, tableName, newTableName, iterator, true)) {
                break;
            }
        }
    }

    private synchronized boolean innerRecoverTable(Database db, Table table, String tableName, String newTableName,
                                                Iterator<Map.Entry<Long, RecycleTableInfo>> iterator,
                                                boolean isReplay) throws DdlException {
        table.writeLock();
        try {
            if (!Strings.isNullOrEmpty(newTableName)) {
                if (Env.isStoredTableNamesLowerCase()) {
                    newTableName = newTableName.toLowerCase();
                }
                if (!tableName.equals(newTableName)) {
                    // check if name is already used
                    if (db.getTable(newTableName).isPresent()) {
                        throw new DdlException("Table name[" + newTableName + "] is already used");
                    }

                    if (table.getType() == TableType.OLAP) {
                        // olap table should also check if any rollup has same name as "newTableName"
                        ((OlapTable) table).checkAndSetName(newTableName, false);
                    } else {
                        table.setName(newTableName);
                    }
                }
            }

            db.createTable(table);
            if (isReplay) {
                iterator.remove();
            } else {
                idToTable.remove(table.getId());
            }
            idToRecycleTime.remove(table.getId());
            if (isReplay) {
                LOG.info("replay recover table[{}]", table.getId());
            } else {
                // log
                RecoverInfo recoverInfo = new RecoverInfo(db.getId(), table.getId(), -1L, "", newTableName, "");
                Env.getCurrentEnv().getEditLog().logRecoverTable(recoverInfo);
            }
            // Only olap table need recover dynamic partition, other table like jdbc odbc view.. do not need it
            if (table.getType() == TableType.OLAP) {
                DynamicPartitionUtil.registerOrRemoveDynamicPartitionTable(db.getId(), (OlapTable) table, isReplay);
            }
        } finally {
            table.writeUnlock();
        }
        return true;
    }

    public synchronized void recoverPartition(long dbId, OlapTable table, String partitionName,
            long partitionIdToRecover, String newPartitionName) throws DdlException {
        long recycleTime = -1;
        // make sure to get db write lock
        RecyclePartitionInfo recoverPartitionInfo = null;

        Iterator<Map.Entry<Long, RecyclePartitionInfo>> iterator = idToPartition.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecyclePartitionInfo> entry = iterator.next();
            RecyclePartitionInfo partitionInfo = entry.getValue();

            if (partitionInfo.getTableId() != table.getId()) {
                continue;
            }

            if (!partitionInfo.getPartition().getName().equalsIgnoreCase(partitionName)) {
                continue;
            }

            if (partitionIdToRecover == -1) {
                if (recycleTime <= idToRecycleTime.get(entry.getKey())) {
                    recycleTime = idToRecycleTime.get(entry.getKey());
                    recoverPartitionInfo = partitionInfo;
                }
            } else if (entry.getKey() == partitionIdToRecover) {
                recoverPartitionInfo = partitionInfo;
                break;
            }
        }

        if (recoverPartitionInfo == null) {
            throw new DdlException("No partition named '" + partitionName + "' or partition id '" + partitionIdToRecover
                    + "' in table " + table.getName());
        }

        PartitionInfo partitionInfo = table.getPartitionInfo();
        Range<PartitionKey> recoverRange = recoverPartitionInfo.getRange();
        PartitionItem recoverItem = null;
        if (partitionInfo.getType() == PartitionType.RANGE) {
            recoverItem = new RangePartitionItem(recoverRange);
        } else if (partitionInfo.getType() == PartitionType.LIST) {
            recoverItem = recoverPartitionInfo.getListPartitionItem();
        }
        // check if partition item is invalid
        if (partitionInfo.getAnyIntersectItem(recoverItem, false) != null) {
            throw new DdlException("Can not recover partition[" + partitionName + "]. Partition item conflict.");
        }

        // recover partition
        Partition recoverPartition = recoverPartitionInfo.getPartition();
        Preconditions.checkState(recoverPartition.getName().equalsIgnoreCase(partitionName));
        if (!Strings.isNullOrEmpty(newPartitionName)) {
            if (table.checkPartitionNameExist(newPartitionName)) {
                throw new DdlException("Partition name[" + newPartitionName + "] is already used");
            }
        }
        table.addPartition(recoverPartition);
        if (!Strings.isNullOrEmpty(newPartitionName)) {
            table.renamePartition(partitionName, newPartitionName);
        }

        // recover partition info
        long partitionId = recoverPartition.getId();
        partitionInfo.setItem(partitionId, false, recoverItem);
        partitionInfo.setDataProperty(partitionId, recoverPartitionInfo.getDataProperty());
        partitionInfo.setReplicaAllocation(partitionId, recoverPartitionInfo.getReplicaAlloc());
        partitionInfo.setIsInMemory(partitionId, recoverPartitionInfo.isInMemory());
        partitionInfo.setIsMutable(partitionId, recoverPartitionInfo.isMutable());

        // remove from recycle bin
        idToPartition.remove(partitionId);
        idToRecycleTime.remove(partitionId);

        // log
        RecoverInfo recoverInfo = new RecoverInfo(dbId, table.getId(), partitionId, "", "", newPartitionName);
        Env.getCurrentEnv().getEditLog().logRecoverPartition(recoverInfo);
        LOG.info("recover partition[{}]", partitionId);
    }

    // The caller should keep table write lock
    public synchronized void replayRecoverPartition(OlapTable table, long partitionId,
                                                    String newPartitionName) throws DdlException {
        Iterator<Map.Entry<Long, RecyclePartitionInfo>> iterator = idToPartition.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecyclePartitionInfo> entry = iterator.next();
            RecyclePartitionInfo recyclePartitionInfo = entry.getValue();
            if (recyclePartitionInfo.getPartition().getId() != partitionId) {
                continue;
            }

            Preconditions.checkState(recyclePartitionInfo.getTableId() == table.getId());
            if (!Strings.isNullOrEmpty(newPartitionName)) {
                if (table.checkPartitionNameExist(newPartitionName)) {
                    throw new DdlException("Partition name[" + newPartitionName + "] is already used");
                }
            }
            table.addPartition(recyclePartitionInfo.getPartition());
            if (!Strings.isNullOrEmpty(newPartitionName)) {
                table.renamePartition(recyclePartitionInfo.getPartition().getName(), newPartitionName);
            }
            PartitionInfo partitionInfo = table.getPartitionInfo();
            PartitionItem recoverItem = null;
            if (partitionInfo.getType() == PartitionType.RANGE) {
                recoverItem = new RangePartitionItem(recyclePartitionInfo.getRange());
            } else if (partitionInfo.getType() == PartitionType.LIST) {
                recoverItem = recyclePartitionInfo.getListPartitionItem();
            }
            partitionInfo.setItem(partitionId, false, recoverItem);
            partitionInfo.setDataProperty(partitionId, recyclePartitionInfo.getDataProperty());
            partitionInfo.setReplicaAllocation(partitionId, recyclePartitionInfo.getReplicaAlloc());
            partitionInfo.setIsInMemory(partitionId, recyclePartitionInfo.isInMemory());
            partitionInfo.setIsMutable(partitionId, recyclePartitionInfo.isMutable());

            iterator.remove();
            idToRecycleTime.remove(partitionId);

            LOG.info("replay recover partition[{}]", partitionId);
            break;
        }
    }

    // no need to use synchronized.
    // only called when loading image
    public void addTabletToInvertedIndex() {
        // no need to handle idToDatabase. Database is already empty before being put here

        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        // idToTable
        for (RecycleTableInfo tableInfo : idToTable.values()) {
            Table table = tableInfo.getTable();
            if (table.getType() != TableType.OLAP) {
                continue;
            }

            long dbId = tableInfo.getDbId();
            OlapTable olapTable = (OlapTable) table;
            long tableId = olapTable.getId();
            for (Partition partition : olapTable.getAllPartitions()) {
                long partitionId = partition.getId();
                TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    long indexId = index.getId();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                    for (Tablet tablet : index.getTablets()) {
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash, medium);
                        long tabletId = tablet.getId();
                        invertedIndex.addTablet(tabletId, tabletMeta);
                        for (Replica replica : tablet.getReplicas()) {
                            invertedIndex.addReplica(tabletId, replica);
                        }
                    }
                } // end for indices
            } // end for partitions
        }

        // idToPartition
        for (RecyclePartitionInfo partitionInfo : idToPartition.values()) {
            long dbId = partitionInfo.getDbId();
            long tableId = partitionInfo.getTableId();
            Partition partition = partitionInfo.getPartition();
            long partitionId = partition.getId();

            // we need to get olap table to get schema hash info
            // first find it in catalog. if not found, it should be in recycle bin
            OlapTable olapTable = null;
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                // just log. db should be in recycle bin
                if (!idToDatabase.containsKey(dbId)) {
                    LOG.error("db[{}] is neither in catalog nor in recycle bin"
                            + " when rebuilding inverted index from recycle bin, partition[{}]",
                            dbId, partitionId);
                    continue;
                }
            } else {
                olapTable = (OlapTable) db.getTableNullable(tableId);
            }

            if (olapTable == null) {
                if (!idToTable.containsKey(tableId)) {
                    LOG.error("table[{}] is neither in catalog nor in recycle bin"
                            + " when rebuilding inverted index from recycle bin, partition[{}]",
                            tableId, partitionId);
                    continue;
                }
                RecycleTableInfo tableInfo = idToTable.get(tableId);
                olapTable = (OlapTable) tableInfo.getTable();
            }
            Preconditions.checkNotNull(olapTable);
            // storage medium should be got from RecyclePartitionInfo, not from olap table. because olap table
            // does not have this partition any more
            TStorageMedium medium = partitionInfo.getDataProperty().getStorageMedium();
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                long indexId = index.getId();
                int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                for (Tablet tablet : index.getTablets()) {
                    TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash, medium);
                    long tabletId = tablet.getId();
                    invertedIndex.addTablet(tabletId, tabletMeta);
                    for (Replica replica : tablet.getReplicas()) {
                        invertedIndex.addReplica(tabletId, replica);
                    }
                }
            } // end for indices
        }

    }

    @Override
    protected void runAfterCatalogReady() {
        long currentTimeMs = System.currentTimeMillis();
        // should follow the partition/table/db order
        // in case of partition(table) is still in recycle bin but table(db) is missing
        int keepNum = Config.max_same_name_catalog_trash_num;
        erasePartition(currentTimeMs, keepNum);
        eraseTable(currentTimeMs, keepNum);
        eraseDatabase(currentTimeMs, keepNum);
    }

    public List<List<String>> getInfo() {
        List<List<String>> dbInfos = Lists.newArrayList();
        for (Map.Entry<Long, RecycleDatabaseInfo> entry : idToDatabase.entrySet()) {
            List<String> info = Lists.newArrayList();
            info.add("Database");
            RecycleDatabaseInfo dbInfo = entry.getValue();
            Database db = dbInfo.getDb();
            info.add(db.getFullName());
            info.add(String.valueOf(entry.getKey()));
            info.add("");
            info.add("");
            //info.add(String.valueOf(idToRecycleTime.get(entry.getKey())));
            info.add(TimeUtils.longToTimeString(idToRecycleTime.get(entry.getKey())));

            dbInfos.add(info);
        }
        // sort by Name, DropTime
        dbInfos.sort((x, y) -> {
            int nameRet = x.get(1).compareTo(y.get(1));
            if (nameRet == 0) {
                return x.get(5).compareTo(y.get(5));
            } else {
                return nameRet;
            }
        });

        List<List<String>> tableInfos = Lists.newArrayList();
        for (Map.Entry<Long, RecycleTableInfo> entry : idToTable.entrySet()) {
            List<String> info = Lists.newArrayList();
            info.add("Table");
            RecycleTableInfo tableInfo = entry.getValue();
            Table table = tableInfo.getTable();
            info.add(table.getName());
            info.add(String.valueOf(tableInfo.getDbId()));
            info.add(String.valueOf(entry.getKey()));
            info.add("");
            //info.add(String.valueOf(idToRecycleTime.get(entry.getKey())));
            info.add(TimeUtils.longToTimeString(idToRecycleTime.get(entry.getKey())));

            tableInfos.add(info);
        }
        // sort by Name, DropTime
        tableInfos.sort((x, y) -> {
            int nameRet = x.get(1).compareTo(y.get(1));
            if (nameRet == 0) {
                return x.get(5).compareTo(y.get(5));
            } else {
                return nameRet;
            }
        });

        List<List<String>> partitionInfos = Lists.newArrayList();
        for (Map.Entry<Long, RecyclePartitionInfo> entry : idToPartition.entrySet()) {
            List<String> info = Lists.newArrayList();
            info.add("Partition");
            RecyclePartitionInfo partitionInfo = entry.getValue();
            Partition partition = partitionInfo.getPartition();
            info.add(partition.getName());
            info.add(String.valueOf(partitionInfo.getDbId()));
            info.add(String.valueOf(partitionInfo.getTableId()));
            info.add(String.valueOf(entry.getKey()));
            //info.add(String.valueOf(idToRecycleTime.get(entry.getKey())));
            info.add(TimeUtils.longToTimeString(idToRecycleTime.get(entry.getKey())));

            partitionInfos.add(info);
        }
        // sort by Name, DropTime
        partitionInfos.sort((x, y) -> {
            int nameRet = x.get(1).compareTo(y.get(1));
            if (nameRet == 0) {
                return x.get(5).compareTo(y.get(5));
            } else {
                return nameRet;
            }
        });

        return Stream.of(dbInfos, tableInfos, partitionInfos).flatMap(Collection::stream).collect(Collectors.toList());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int count = idToDatabase.size();
        out.writeInt(count);
        for (Map.Entry<Long, RecycleDatabaseInfo> entry : idToDatabase.entrySet()) {
            out.writeLong(entry.getKey());
            entry.getValue().write(out);
        }

        count = idToTable.size();
        out.writeInt(count);
        for (Map.Entry<Long, RecycleTableInfo> entry : idToTable.entrySet()) {
            out.writeLong(entry.getKey());
            entry.getValue().write(out);
        }

        count = idToPartition.size();
        out.writeInt(count);
        for (Map.Entry<Long, RecyclePartitionInfo> entry : idToPartition.entrySet()) {
            out.writeLong(entry.getKey());
            entry.getValue().write(out);
        }

        count = idToRecycleTime.size();
        out.writeInt(count);
        for (Map.Entry<Long, Long> entry : idToRecycleTime.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeLong(entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            long id = in.readLong();
            RecycleDatabaseInfo dbInfo = new RecycleDatabaseInfo();
            dbInfo.readFields(in);
            idToDatabase.put(id, dbInfo);
        }

        count = in.readInt();
        for (int i = 0; i < count; i++) {
            long id = in.readLong();
            RecycleTableInfo tableInfo = new RecycleTableInfo();
            tableInfo.readFields(in);
            idToTable.put(id, tableInfo);
        }

        count = in.readInt();
        for (int i = 0; i < count; i++) {
            long id = in.readLong();
            RecyclePartitionInfo partitionInfo = new RecyclePartitionInfo();
            partitionInfo.readFields(in);
            idToPartition.put(id, partitionInfo);
        }

        count = in.readInt();
        for (int i = 0; i < count; i++) {
            long id = in.readLong();
            long time = in.readLong();
            idToRecycleTime.put(id, time);
        }
        updateDbInfoForLowerVersion();
    }

    private void updateDbInfoForLowerVersion() {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_114) {
            Iterator<Map.Entry<Long, RecycleDatabaseInfo>> dbIterator = idToDatabase.entrySet().iterator();
            while (dbIterator.hasNext()) {
                Map.Entry<Long, RecycleDatabaseInfo> dbEntry = dbIterator.next();
                RecycleDatabaseInfo dbInfo = dbEntry.getValue();
                Set<String> tableNames = Sets.newHashSet(dbInfo.getTableNames());
                Set<Long> tableIds = Sets.newHashSet();
                Iterator<Map.Entry<Long, RecycleTableInfo>> iterator = idToTable.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Long, RecycleTableInfo> entry = iterator.next();
                    RecycleTableInfo tableInfo = entry.getValue();
                    if (tableInfo.getDbId() != dbEntry.getKey()
                            || !tableNames.contains(tableInfo.getTable().getName())) {
                        continue;
                    }

                    tableIds.add(entry.getKey());
                }
                dbInfo.setTableIds(tableIds);
            }
        }
    }

    public class RecycleDatabaseInfo implements Writable {
        private Database db;
        private Set<String> tableNames;
        private Set<Long> tableIds;

        public RecycleDatabaseInfo() {
            tableNames = Sets.newHashSet();
            tableIds = Sets.newHashSet();
        }

        public RecycleDatabaseInfo(Database db, Set<String> tableNames, Set<Long> tableIds) {
            this.db = db;
            this.tableNames = tableNames;
            this.tableIds = tableIds;
        }

        public Database getDb() {
            return db;
        }

        public Set<String> getTableNames() {
            return tableNames;
        }

        public Set<Long> getTableIds() {
            return tableIds;
        }

        public Set<Long> setTableIds(Set<Long> tableIds) {
            return this.tableIds = tableIds;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            db.write(out);

            int count = tableNames.size();
            out.writeInt(count);
            for (String tableName : tableNames) {
                Text.writeString(out, tableName);
            }

            count = tableIds.size();
            out.writeInt(count);
            for (long tableId : tableIds) {
                out.writeLong(tableId);
            }
        }

        public void readFields(DataInput in) throws IOException {
            db = Database.read(in);

            int count  = in.readInt();
            for (int i = 0; i < count; i++) {
                String tableName = Text.readString(in);
                tableNames.add(tableName);
            }
            if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_114) {
                count = in.readInt();
                for (int i = 0; i < count; i++) {
                    long tableId = in.readLong();
                    tableIds.add(tableId);
                }
            }
        }
    }

    public class RecycleTableInfo implements Writable {
        private long dbId;
        private Table table;

        public RecycleTableInfo() {
            // for persist
        }

        public RecycleTableInfo(long dbId, Table table) {
            this.dbId = dbId;
            this.table = table;
        }

        public long getDbId() {
            return dbId;
        }

        public Table getTable() {
            return table;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(dbId);
            table.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            dbId = in.readLong();
            table = Table.read(in);
        }
    }

    public class RecyclePartitionInfo implements Writable {
        private long dbId;
        private long tableId;
        private Partition partition;
        private Range<PartitionKey> range;
        private PartitionItem listPartitionItem;
        private DataProperty dataProperty;
        private ReplicaAllocation replicaAlloc;
        private boolean isInMemory;
        private boolean isMutable = true;

        public RecyclePartitionInfo() {
            // for persist
        }

        public RecyclePartitionInfo(long dbId, long tableId, Partition partition,
                                    Range<PartitionKey> range, PartitionItem listPartitionItem,
                                    DataProperty dataProperty, ReplicaAllocation replicaAlloc,
                                    boolean isInMemory, boolean isMutable) {
            this.dbId = dbId;
            this.tableId = tableId;
            this.partition = partition;
            this.range = range;
            this.listPartitionItem = listPartitionItem;
            this.dataProperty = dataProperty;
            this.replicaAlloc = replicaAlloc;
            this.isInMemory = isInMemory;
            this.isMutable = isMutable;
        }

        public long getDbId() {
            return dbId;
        }

        public long getTableId() {
            return tableId;
        }

        public Partition getPartition() {
            return partition;
        }

        public Range<PartitionKey> getRange() {
            return range;
        }

        public PartitionItem getListPartitionItem() {
            return listPartitionItem;
        }

        public DataProperty getDataProperty() {
            return dataProperty;
        }

        public ReplicaAllocation getReplicaAlloc() {
            return replicaAlloc;
        }

        public boolean isInMemory() {
            return isInMemory;
        }

        public boolean isMutable() {
            return isMutable;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(dbId);
            out.writeLong(tableId);
            partition.write(out);
            RangeUtils.writeRange(out, range);
            listPartitionItem.write(out);
            dataProperty.write(out);
            replicaAlloc.write(out);
            out.writeBoolean(isInMemory);
            out.writeBoolean(isMutable);
        }

        public void readFields(DataInput in) throws IOException {
            dbId = in.readLong();
            tableId = in.readLong();
            partition = Partition.read(in);
            range = RangeUtils.readRange(in);
            listPartitionItem = ListPartitionItem.read(in);
            dataProperty = DataProperty.read(in);
            if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_105) {
                short replicationNum = in.readShort();
                replicaAlloc = new ReplicaAllocation(replicationNum);
            } else {
                replicaAlloc = ReplicaAllocation.read(in);
            }
            isInMemory = in.readBoolean();
            if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_115) {
                isMutable = in.readBoolean();
            }
        }
    }

    // currently only used when loading image. So no synchronized protected.
    public List<Long> getAllDbIds() {
        return Lists.newArrayList(idToDatabase.keySet());
    }
}
