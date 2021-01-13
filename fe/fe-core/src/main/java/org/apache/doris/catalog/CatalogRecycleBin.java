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
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.RangeUtils;
import org.apache.doris.persist.ColocatePersistInfo;
import org.apache.doris.persist.RecoverInfo;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.DropReplicaTask;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

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

public class CatalogRecycleBin extends MasterDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(CatalogRecycleBin.class);

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

    public synchronized boolean recycleDatabase(Database db, Set<String> tableNames) {
        if (idToDatabase.containsKey(db.getId())) {
            LOG.error("db[{}] already in recycle bin.", db.getId());
            return false;
        }
        
        // db should be empty. all tables are recycled before
        Preconditions.checkState(db.getTables().isEmpty());

        // erase db with same name
        eraseDatabaseWithSameName(db.getFullName());

        // recycle db
        RecycleDatabaseInfo databaseInfo = new RecycleDatabaseInfo(db, tableNames);
        idToDatabase.put(db.getId(), databaseInfo);
        idToRecycleTime.put(db.getId(), System.currentTimeMillis());
        LOG.info("recycle db[{}-{}]", db.getId(), db.getFullName());
        return true;
    }

    public synchronized boolean recycleTable(long dbId, Table table) {
        if (idToTable.containsKey(table.getId())) {
            LOG.error("table[{}] already in recycle bin.", table.getId());
            return false;
        }

        // erase table with same name
        eraseTableWithSameName(dbId, table.getName());

        // recycle table
        RecycleTableInfo tableInfo = new RecycleTableInfo(dbId, table);
        idToRecycleTime.put(table.getId(), System.currentTimeMillis());
        idToTable.put(table.getId(), tableInfo);
        LOG.info("recycle table[{}-{}]", table.getId(), table.getName());
        return true;
    }

    public synchronized boolean recyclePartition(long dbId, long tableId, Partition partition,
                                                 Range<PartitionKey> range, DataProperty dataProperty,
                                                 short replicationNum,
                                                 boolean isInMemory) {
        if (idToPartition.containsKey(partition.getId())) {
            LOG.error("partition[{}] already in recycle bin.", partition.getId());
            return false;
        }

        // erase partition with same name
        erasePartitionWithSameName(dbId, tableId, partition.getName());

        // recycle partition
        RecyclePartitionInfo partitionInfo = new RecyclePartitionInfo(dbId, tableId, partition,
                                                                      range, dataProperty, replicationNum,
                                                                      isInMemory);
        idToRecycleTime.put(partition.getId(), System.currentTimeMillis());
        idToPartition.put(partition.getId(), partitionInfo);
        LOG.info("recycle partition[{}-{}]", partition.getId(), partition.getName());
        return true;
    }

    private synchronized boolean isExpire(long id, long currentTimeMs) {
        if (currentTimeMs - idToRecycleTime.get(id) > Config.catalog_trash_expire_second * 1000L) {
            return true;
        }
        return false;
    }

    private synchronized void eraseDatabase(long currentTimeMs) {
        Iterator<Map.Entry<Long, RecycleDatabaseInfo>> dbIter = idToDatabase.entrySet().iterator();
        while (dbIter.hasNext()) {
            Map.Entry<Long, RecycleDatabaseInfo> entry = dbIter.next();
            RecycleDatabaseInfo dbInfo = entry.getValue();
            Database db = dbInfo.getDb();
            if (isExpire(db.getId(), currentTimeMs)) {
                // erase db
                dbIter.remove();
                idToRecycleTime.remove(entry.getKey());

                // remove jobs
                Catalog.getCurrentCatalog().getLoadInstance().removeDbLoadJob(db.getId());
                Catalog.getCurrentCatalog().getLoadInstance().removeDbDeleteJob(db.getId());
                Catalog.getCurrentCatalog().getSchemaChangeHandler().removeDbAlterJob(db.getId());
                Catalog.getCurrentCatalog().getRollupHandler().removeDbAlterJob(db.getId());

                // remove database transaction manager
                Catalog.getCurrentCatalog().getGlobalTransactionMgr().removeDatabaseTransactionMgr(db.getId());

                // log
                Catalog.getCurrentCatalog().getEditLog().logEraseDb(entry.getKey());
                LOG.info("erase db[{}]", entry.getKey());
            }
        }
    }

    private synchronized void eraseDatabaseWithSameName(String dbName) {
        Iterator<Map.Entry<Long, RecycleDatabaseInfo>> iterator = idToDatabase.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecycleDatabaseInfo> entry = iterator.next();
            RecycleDatabaseInfo dbInfo = entry.getValue();
            Database db = dbInfo.getDb();
            if (db.getFullName().equals(dbName)) {
                iterator.remove();
                idToRecycleTime.remove(entry.getKey());

                // remove database transaction manager
                Catalog.getCurrentCatalog().getGlobalTransactionMgr().removeDatabaseTransactionMgr(db.getId());

                LOG.info("erase database[{}] name: {}", db.getId(), dbName);
            }
        }
    }

    public synchronized void replayEraseDatabase(long dbId) {
        idToDatabase.remove(dbId);
        idToRecycleTime.remove(dbId);

        // remove jobs
        Catalog.getCurrentCatalog().getLoadInstance().removeDbLoadJob(dbId);
        Catalog.getCurrentCatalog().getLoadInstance().removeDbDeleteJob(dbId);
        Catalog.getCurrentCatalog().getSchemaChangeHandler().removeDbAlterJob(dbId);
        Catalog.getCurrentCatalog().getRollupHandler().removeDbAlterJob(dbId);

        // remove database transaction manager
        Catalog.getCurrentCatalog().getGlobalTransactionMgr().removeDatabaseTransactionMgr(dbId);
        LOG.info("replay erase db[{}]", dbId);
    }

    private synchronized void eraseTable(long currentTimeMs) {
        Iterator<Map.Entry<Long, RecycleTableInfo>> tableIter = idToTable.entrySet().iterator();
        while (tableIter.hasNext()) {
            Map.Entry<Long, RecycleTableInfo> entry = tableIter.next();
            RecycleTableInfo tableInfo = entry.getValue();
            Table table = tableInfo.getTable();
            long tableId = table.getId();

            if (isExpire(tableId, currentTimeMs)) {
                if (table.getType() == TableType.OLAP) {
                    onEraseOlapTable((OlapTable) table);
                }

                // erase table
                tableIter.remove();
                idToRecycleTime.remove(tableId);

                // log
                Catalog.getCurrentCatalog().getEditLog().logEraseTable(tableId);
                LOG.info("erase table[{}]", tableId);
            }
        } // end for tables
    }

    private void onEraseOlapTable(OlapTable olapTable) {
        // inverted index
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        Collection<Partition> allPartitions = olapTable.getAllPartitions();
        for (Partition partition : allPartitions) {
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    invertedIndex.deleteTablet(tablet.getId());
                }
            }
        }

        // drop all replicas
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Partition partition : olapTable.getAllPartitions()) {
            List<MaterializedIndex> allIndices = partition.getMaterializedIndices(IndexExtState.ALL);
            for (MaterializedIndex materializedIndex : allIndices) {
                long indexId = materializedIndex.getId();
                int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                for (Tablet tablet : materializedIndex.getTablets()) {
                    long tabletId = tablet.getId();
                    List<Replica> replicas = tablet.getReplicas();
                    for (Replica replica : replicas) {
                        long backendId = replica.getBackendId();
                        DropReplicaTask dropTask = new DropReplicaTask(backendId, tabletId, schemaHash);
                        batchTask.addTask(dropTask);
                    } // end for replicas
                } // end for tablets
            } // end for indices
        } // end for partitions
        AgentTaskExecutor.submit(batchTask);

        // colocation
        if (Catalog.getCurrentColocateIndex().removeTable(olapTable.getId())) {
            Catalog.getCurrentCatalog().getEditLog().logColocateRemoveTable(
                    ColocatePersistInfo.createForRemoveTable(olapTable.getId()));
        }
    }

    private synchronized void eraseTableWithSameName(long dbId, String tableName) {
        Iterator<Map.Entry<Long, RecycleTableInfo>> iterator = idToTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecycleTableInfo> entry = iterator.next();
            RecycleTableInfo tableInfo = entry.getValue();
            if (tableInfo.getDbId() != dbId) {
                continue;
            }

            Table table = tableInfo.getTable();
            if (table.getName().equals(tableName)) {
                if (table.getType() == TableType.OLAP) {
                    onEraseOlapTable((OlapTable) table);
                }

                iterator.remove();
                idToRecycleTime.remove(table.getId());
                LOG.info("erase table[{}] name: {}", table.getId(), tableName);
            }
        }
    }

    public synchronized void replayEraseTable(long tableId) {
        RecycleTableInfo tableInfo = idToTable.remove(tableId);
        idToRecycleTime.remove(tableId);

        Table table = tableInfo.getTable();
        if (table.getType() == TableType.OLAP && !Catalog.isCheckpointThread()) {
            OlapTable olapTable = (OlapTable) table;

            // remove tablet from inverted index
            TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
            for (Partition partition : olapTable.getAllPartitions()) {
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    for (Tablet tablet : index.getTablets()) {
                        invertedIndex.deleteTablet(tablet.getId());
                    }
                }
            }
        }

        // colocation
        Catalog.getCurrentColocateIndex().removeTable(tableId);

        LOG.info("replay erase table[{}]", tableId);
    }

    private synchronized void erasePartition(long currentTimeMs) {
        Iterator<Map.Entry<Long, RecyclePartitionInfo>> iterator = idToPartition.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecyclePartitionInfo> entry = iterator.next();
            RecyclePartitionInfo partitionInfo = entry.getValue();
            Partition partition = partitionInfo.getPartition();

            long partitionId = entry.getKey();
            if (isExpire(partitionId, currentTimeMs)) {
                // remove tablet in inverted index
                TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    for (Tablet tablet : index.getTablets()) {
                        invertedIndex.deleteTablet(tablet.getId());
                    }
                }

                // erase partition
                iterator.remove();
                idToRecycleTime.remove(partitionId);

                // log
                Catalog.getCurrentCatalog().getEditLog().logErasePartition(partitionId);
                LOG.info("erase partition[{}]", partitionId);
            }
        } // end for partitions
    }

    private synchronized void erasePartitionWithSameName(long dbId, long tableId, String partitionName) {
        Iterator<Map.Entry<Long, RecyclePartitionInfo>> iterator = idToPartition.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecyclePartitionInfo> entry = iterator.next();
            RecyclePartitionInfo partitionInfo = entry.getValue();
            if (partitionInfo.getDbId() != dbId || partitionInfo.getTableId() != tableId) {
                continue;
            }

            Partition partition = partitionInfo.getPartition();
            if (partition.getName().equals(partitionName)) {
                // remove tablet in inverted index
                TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                    for (Tablet tablet : index.getTablets()) {
                        invertedIndex.deleteTablet(tablet.getId());
                    }
                }

                iterator.remove();
                idToRecycleTime.remove(entry.getKey());

                LOG.info("erase partition[{}] name: {}", partition.getId(), partitionName);
            }
        }
    }

    public synchronized void replayErasePartition(long partitionId) {
        RecyclePartitionInfo partitionInfo = idToPartition.remove(partitionId);
        idToRecycleTime.remove(partitionId);

        Partition partition = partitionInfo.getPartition();
        if (!Catalog.isCheckpointThread()) {
            // remove tablet from inverted index
            TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    invertedIndex.deleteTablet(tablet.getId());
                }
            }
        }

        LOG.info("replay erase partition[{}]", partitionId);
    }

    public synchronized Database recoverDatabase(String dbName) throws DdlException {
        RecycleDatabaseInfo dbInfo = null;
        Iterator<Map.Entry<Long, RecycleDatabaseInfo>> iterator = idToDatabase.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecycleDatabaseInfo> entry = iterator.next();
            if (dbName.equals(entry.getValue().getDb().getFullName())) {
                dbInfo = entry.getValue();
                break;
            }
        }
        
        if (dbInfo == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
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
        long dbId = db.getId();
        Iterator<Map.Entry<Long, RecycleTableInfo>> iterator = idToTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecycleTableInfo> entry = iterator.next();
            RecycleTableInfo tableInfo = entry.getValue();
            if (tableInfo.getDbId() != dbId || !tableNames.contains(tableInfo.getTable().getName())) {
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

    public synchronized boolean recoverTable(Database db, String tableName) {
        // make sure to get db lock
        long dbId = db.getId();
        Iterator<Map.Entry<Long, RecycleTableInfo>> iterator = idToTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecycleTableInfo> entry = iterator.next();
            RecycleTableInfo tableInfo = entry.getValue();
            if (tableInfo.getDbId() != dbId) {
                continue;
            }

            Table table = tableInfo.getTable();
            if (!table.getName().equals(tableName)) {
                continue;
            }

            db.createTable(table);

            iterator.remove();
            idToRecycleTime.remove(table.getId());

            // log
            RecoverInfo recoverInfo = new RecoverInfo(dbId, table.getId(), -1L);
            Catalog.getCurrentCatalog().getEditLog().logRecoverTable(recoverInfo);
            LOG.info("recover db[{}] with table[{}]: {}", dbId, table.getId(), table.getName());
            return true;
        }

        return false;
    }

    public synchronized void replayRecoverTable(Database db, long tableId) {
        // make sure to get db write lock
        Iterator<Map.Entry<Long, RecycleTableInfo>> iterator = idToTable.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecycleTableInfo> entry = iterator.next();
            RecycleTableInfo tableInfo = entry.getValue();
            if (tableInfo.getTable().getId() != tableId) {
                continue;
            }

            Preconditions.checkState(tableInfo.getDbId() == db.getId());

            db.createTable(tableInfo.getTable());
            iterator.remove();
            idToRecycleTime.remove(tableInfo.getTable().getId());
            LOG.info("replay recover table[{}]", tableId);
            break;
        }
    }

    public synchronized void recoverPartition(long dbId, OlapTable table, String partitionName) throws DdlException {
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

            recoverPartitionInfo = partitionInfo;
            break;
        }

        if (recoverPartitionInfo == null) {
            throw new DdlException("No partition named " + partitionName + " in table " + table.getName());
        }
        
        // check if range is invalid
        Range<PartitionKey> recoverRange = recoverPartitionInfo.getRange();
        RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        if (partitionInfo.getAnyIntersectRange(recoverRange, false) != null) {
            throw new DdlException("Can not recover partition[" + partitionName + "]. Range conflict.");
        }

        // recover partition
        Partition recoverPartition = recoverPartitionInfo.getPartition();
        Preconditions.checkState(recoverPartition.getName().equalsIgnoreCase(partitionName));
        table.addPartition(recoverPartition);
        
        // recover partition info
        long partitionId = recoverPartition.getId();
        partitionInfo.setRange(partitionId, false, recoverRange);
        partitionInfo.setDataProperty(partitionId, recoverPartitionInfo.getDataProperty());
        partitionInfo.setReplicationNum(partitionId, recoverPartitionInfo.getReplicationNum());
        partitionInfo.setIsInMemory(partitionId, recoverPartitionInfo.isInMemory());

        // remove from recycle bin
        idToPartition.remove(partitionId);
        idToRecycleTime.remove(partitionId);

        // log
        RecoverInfo recoverInfo = new RecoverInfo(dbId, table.getId(), partitionId);
        Catalog.getCurrentCatalog().getEditLog().logRecoverPartition(recoverInfo);
        LOG.info("recover partition[{}]", partitionId);
    }

    // The caller should keep table write lock
    public synchronized void replayRecoverPartition(OlapTable table, long partitionId) {
        Iterator<Map.Entry<Long, RecyclePartitionInfo>> iterator = idToPartition.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecyclePartitionInfo> entry = iterator.next();
            RecyclePartitionInfo partitionInfo = entry.getValue();
            if (partitionInfo.getPartition().getId() != partitionId) {
                continue;
            }

            Preconditions.checkState(partitionInfo.getTableId() == table.getId());

            table.addPartition(partitionInfo.getPartition());
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
            rangePartitionInfo.setRange(partitionId, false, partitionInfo.getRange());
            rangePartitionInfo.setDataProperty(partitionId, partitionInfo.getDataProperty());
            rangePartitionInfo.setReplicationNum(partitionId, partitionInfo.getReplicationNum());
            rangePartitionInfo.setIsInMemory(partitionId, partitionInfo.isInMemory());

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

        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
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
                    TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash, medium);
                    for (Tablet tablet : index.getTablets()) {
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
            Database db = Catalog.getCurrentCatalog().getDb(dbId);
            if (db == null) {
                // just log. db should be in recycle bin
                if (!idToDatabase.containsKey(dbId)) {
                    LOG.error("db[{}] is neither in catalog nor in recycle bin"
                            + " when rebuilding inverted index from recycle bin, partition[{}]",
                              dbId, partitionId);
                    continue;
                }
            } else {
                olapTable = (OlapTable) db.getTable(tableId);
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
                TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash, medium);
                for (Tablet tablet : index.getTablets()) {
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
        erasePartition(currentTimeMs);
        eraseTable(currentTimeMs);
        eraseDatabase(currentTimeMs);
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
    }

    public class RecycleDatabaseInfo implements Writable {
        private Database db;
        private Set<String> tableNames;

        public RecycleDatabaseInfo() {
            tableNames = Sets.newHashSet();
        }

        public RecycleDatabaseInfo(Database db, Set<String> tableNames) {
            this.db = db;
            this.tableNames = tableNames;
        }

        public Database getDb() {
            return db;
        }

        public Set<String> getTableNames() {
            return tableNames;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            db.write(out);

            int count = tableNames.size();
            out.writeInt(count);
            for (String tableName : tableNames) {
                Text.writeString(out, tableName);
            }
        }

        public void readFields(DataInput in) throws IOException {
            db = Database.read(in);
            
            int count  = in.readInt();
            for (int i = 0; i < count; i++) {
                String tableName = Text.readString(in);
                tableNames.add(tableName);
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
        private DataProperty dataProperty;
        private short replicationNum;
        private boolean isInMemory;

        public RecyclePartitionInfo() {
            // for persist
        }

        public RecyclePartitionInfo(long dbId, long tableId, Partition partition,
                                    Range<PartitionKey> range, DataProperty dataProperty, short replicationNum,
                                    boolean isInMemory) {
            this.dbId = dbId;
            this.tableId = tableId;
            this.partition = partition;
            this.range = range;
            this.dataProperty = dataProperty;
            this.replicationNum = replicationNum;
            this.isInMemory = isInMemory;
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

        public DataProperty getDataProperty() {
            return dataProperty;
        }

        public short getReplicationNum() {
            return replicationNum;
        }

        public boolean isInMemory() {
            return isInMemory;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(dbId);
            out.writeLong(tableId);
            partition.write(out);
            RangeUtils.writeRange(out, range);
            dataProperty.write(out);
            out.writeShort(replicationNum);
            out.writeBoolean(isInMemory);
        }

        public void readFields(DataInput in) throws IOException {
            dbId = in.readLong();
            tableId = in.readLong();
            partition = Partition.read(in);
            range = RangeUtils.readRange(in);
            dataProperty = DataProperty.read(in);
            replicationNum = in.readShort();
            if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_72) {
                isInMemory = in.readBoolean();
            }
        }
    }
    
    // currently only used when loading image. So no synchronized protected.
    public List<Long> getAllDbIds() {
        return Lists.newArrayList(idToDatabase.keySet());
    }
}
