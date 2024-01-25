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

package org.apache.doris.binlog;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.persist.AlterDatabasePropertyInfo;
import org.apache.doris.persist.BarrierLog;
import org.apache.doris.persist.BatchModifyPartitionsInfo;
import org.apache.doris.persist.BinlogGcInfo;
import org.apache.doris.persist.DropPartitionInfo;
import org.apache.doris.persist.ModifyTablePropertyOperationLog;
import org.apache.doris.persist.ReplacePartitionOperationLog;
import org.apache.doris.persist.TableAddOrDropColumnsInfo;
import org.apache.doris.persist.TruncateTableInfo;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TBinlogType;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BinlogManager {
    private static final int BUFFER_SIZE = 16 * 1024;

    private static final Logger LOG = LogManager.getLogger(BinlogManager.class);

    private ReentrantReadWriteLock lock;
    private Map<Long, DBBinlog> dbBinlogMap;
    private BinlogConfigCache binlogConfigCache;

    public BinlogManager() {
        lock = new ReentrantReadWriteLock();
        dbBinlogMap = Maps.newHashMap();
        binlogConfigCache = new BinlogConfigCache();
    }

    private void afterAddBinlog(TBinlog binlog) {
        if (!binlog.isSetRemoveEnableCache()) {
            return;
        }
        if (!binlog.isRemoveEnableCache()) {
            return;
        }

        long dbId = binlog.getDbId();
        boolean onlyDb = true;
        if (binlog.isSetTableIds()) {
            for (long tableId : binlog.getTableIds()) {
                binlogConfigCache.remove(tableId);
                onlyDb = false;
            }
        }
        if (onlyDb) {
            binlogConfigCache.remove(dbId);
        }
    }

    private void addBinlog(TBinlog binlog) {
        if (!Config.enable_feature_binlog) {
            return;
        }

        DBBinlog dbBinlog;
        lock.writeLock().lock();
        try {
            long dbId = binlog.getDbId();
            dbBinlog = dbBinlogMap.get(dbId);

            if (dbBinlog == null) {
                dbBinlog = new DBBinlog(binlogConfigCache, binlog);
                dbBinlogMap.put(dbId, dbBinlog);
            }
        } finally {
            lock.writeLock().unlock();
        }

        dbBinlog.addBinlog(binlog);
    }

    private void addBinlog(long dbId, List<Long> tableIds, long commitSeq, long timestamp, TBinlogType type,
                           String data, boolean removeEnableCache) {
        if (!Config.enable_feature_binlog) {
            return;
        }

        TBinlog binlog = new TBinlog();
        // set commitSeq, timestamp, type, dbId, data
        binlog.setCommitSeq(commitSeq);
        binlog.setTimestamp(timestamp);
        binlog.setType(type);
        binlog.setDbId(dbId);
        binlog.setData(data);
        if (tableIds != null && !tableIds.isEmpty()) {
            binlog.setTableIds(tableIds);
        }
        binlog.setTableRef(0);
        binlog.setRemoveEnableCache(removeEnableCache);

        // Check if all db or table binlog is disable, return
        boolean dbBinlogEnable = binlogConfigCache.isEnableDB(dbId);
        boolean anyEnable = dbBinlogEnable;
        if (tableIds != null) {
            for (long tableId : tableIds) {
                boolean tableBinlogEnable = binlogConfigCache.isEnableTable(dbId, tableId);
                anyEnable = anyEnable || tableBinlogEnable;
                if (anyEnable) {
                    break;
                }
            }
        }

        if (anyEnable) {
            addBinlog(binlog);
        }

        afterAddBinlog(binlog);
    }

    public void addUpsertRecord(UpsertRecord upsertRecord) {
        long dbId = upsertRecord.getDbId();
        List<Long> tableIds = upsertRecord.getAllReleatedTableIds();
        long commitSeq = upsertRecord.getCommitSeq();
        long timestamp = upsertRecord.getTimestamp();
        TBinlogType type = TBinlogType.UPSERT;
        String data = upsertRecord.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data, false);
    }

    public void addAddPartitionRecord(AddPartitionRecord addPartitionRecord) {
        long dbId = addPartitionRecord.getDbId();
        List<Long> tableIds = Lists.newArrayList();
        tableIds.add(addPartitionRecord.getTableId());
        long commitSeq = addPartitionRecord.getCommitSeq();
        long timestamp = -1;
        TBinlogType type = TBinlogType.ADD_PARTITION;
        String data = addPartitionRecord.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data, false);
    }

    public void addCreateTableRecord(CreateTableRecord createTableRecord) {
        long dbId = createTableRecord.getDbId();
        List<Long> tableIds = Lists.newArrayList();
        tableIds.add(createTableRecord.getTableId());
        long commitSeq = createTableRecord.getCommitSeq();
        long timestamp = -1;
        TBinlogType type = TBinlogType.CREATE_TABLE;
        String data = createTableRecord.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data, false);
    }

    public void addDropPartitionRecord(DropPartitionInfo dropPartitionInfo, long commitSeq) {
        long dbId = dropPartitionInfo.getDbId();
        List<Long> tableIds = Lists.newArrayList();
        tableIds.add(dropPartitionInfo.getTableId());
        long timestamp = -1;
        TBinlogType type = TBinlogType.DROP_PARTITION;
        String data = dropPartitionInfo.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data, false);
    }

    public void addDropTableRecord(DropTableRecord record) {
        long dbId = record.getDbId();
        List<Long> tableIds = Lists.newArrayList();
        tableIds.add(record.getTableId());
        long commitSeq = record.getCommitSeq();
        long timestamp = -1;
        TBinlogType type = TBinlogType.DROP_TABLE;
        String data = record.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data, false);
    }

    public void addAlterJobV2(AlterJobV2 alterJob, long commitSeq) {
        long dbId = alterJob.getDbId();
        List<Long> tableIds = Lists.newArrayList();
        tableIds.add(alterJob.getTableId());
        long timestamp = -1;
        TBinlogType type = TBinlogType.ALTER_JOB;
        String data = alterJob.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data, false);
    }

    public void addModifyTableAddOrDropColumns(TableAddOrDropColumnsInfo info, long commitSeq) {
        long dbId = info.getDbId();
        List<Long> tableIds = Lists.newArrayList();
        tableIds.add(info.getTableId());
        long timestamp = -1;
        TBinlogType type = TBinlogType.MODIFY_TABLE_ADD_OR_DROP_COLUMNS;
        String data = info.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data, false);
    }

    public void addAlterDatabaseProperty(AlterDatabasePropertyInfo info, long commitSeq) {
        long dbId = info.getDbId();
        List<Long> tableIds = Lists.newArrayList();

        long timestamp = -1;
        TBinlogType type = TBinlogType.ALTER_DATABASE_PROPERTY;
        String data = info.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data, true);
    }

    public void addModifyTableProperty(ModifyTablePropertyOperationLog info, long commitSeq) {
        long dbId = info.getDbId();
        List<Long> tableIds = Lists.newArrayList();
        tableIds.add(info.getTableId());
        long timestamp = -1;
        TBinlogType type = TBinlogType.MODIFY_TABLE_PROPERTY;
        String data = info.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data, true);
    }

    // add Barrier log
    public void addBarrierLog(BarrierLog barrierLog, long commitSeq) {
        if (barrierLog == null) {
            return;
        }

        long dbId = barrierLog.getDbId();
        long tableId = barrierLog.getTableId();
        if (dbId == 0 || tableId == 0) {
            return;
        }

        List<Long> tableIds = Lists.newArrayList();
        tableIds.add(tableId);
        long timestamp = -1;
        TBinlogType type = TBinlogType.BARRIER;
        String data = barrierLog.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data, false);
    }

    // add Modify partitions
    public void addModifyPartitions(BatchModifyPartitionsInfo info, long commitSeq) {
        long dbId = info.getDbId();
        List<Long> tableIds = Lists.newArrayList();
        tableIds.add(info.getTableId());
        long timestamp = -1;
        TBinlogType type = TBinlogType.MODIFY_PARTITIONS;
        String data = info.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data, false);
    }

    // add Replace partition
    public void addReplacePartitions(ReplacePartitionOperationLog info, long commitSeq) {
        long dbId = info.getDbId();
        List<Long> tableIds = Lists.newArrayList();
        tableIds.add(info.getTblId());
        long timestamp = -1;
        TBinlogType type = TBinlogType.REPLACE_PARTITIONS;
        String data = info.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data, false);
    }

    // add Truncate Table
    public void addTruncateTable(TruncateTableInfo info, long commitSeq) {
        long dbId = info.getDbId();
        List<Long> tableIds = Lists.newArrayList();
        tableIds.add(info.getTblId());
        long timestamp = -1;
        TBinlogType type = TBinlogType.TRUNCATE_TABLE;
        String data = info.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data, false);
    }

    // get binlog by dbId, return first binlog.version > version
    public Pair<TStatus, TBinlog> getBinlog(long dbId, long tableId, long prevCommitSeq) {
        TStatus status = new TStatus(TStatusCode.OK);
        lock.readLock().lock();
        try {
            DBBinlog dbBinlog = dbBinlogMap.get(dbId);
            if (dbBinlog == null) {
                status.setStatusCode(TStatusCode.BINLOG_NOT_FOUND_DB);
                LOG.warn("dbBinlog not found. dbId: {}", dbId);
                return Pair.of(status, null);
            }

            return dbBinlog.getBinlog(tableId, prevCommitSeq);
        } finally {
            lock.readLock().unlock();
        }
    }

    // get binlog by dbId, return first binlog.version > version
    public Pair<TStatus, Long> getBinlogLag(long dbId, long tableId, long prevCommitSeq) {
        TStatus status = new TStatus(TStatusCode.OK);
        lock.readLock().lock();
        try {
            DBBinlog dbBinlog = dbBinlogMap.get(dbId);
            if (dbBinlog == null) {
                status.setStatusCode(TStatusCode.BINLOG_NOT_FOUND_DB);
                LOG.warn("dbBinlog not found. dbId: {}", dbId);
                return Pair.of(status, null);
            }

            return dbBinlog.getBinlogLag(tableId, prevCommitSeq);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<BinlogTombstone> gc() {
        LOG.info("begin gc binlog");

        lock.writeLock().lock();
        Map<Long, DBBinlog> gcDbBinlogMap;
        try {
            gcDbBinlogMap = Maps.newHashMap(dbBinlogMap);
        } finally {
            lock.writeLock().unlock();
        }

        if (gcDbBinlogMap.isEmpty()) {
            LOG.info("gc binlog, dbBinlogMap is null");
            return null;
        }

        List<BinlogTombstone> tombstones = Lists.newArrayList();
        for (DBBinlog dbBinlog : gcDbBinlogMap.values()) {
            BinlogTombstone dbTombstones = dbBinlog.gc();
            if (dbTombstones != null) {
                tombstones.add(dbTombstones);
            }
        }
        return tombstones;
    }

    public void replayGc(BinlogGcInfo binlogGcInfo) {
        lock.writeLock().lock();
        Map<Long, DBBinlog> gcDbBinlogMap;
        try {
            gcDbBinlogMap = Maps.newHashMap(dbBinlogMap);
        } finally {
            lock.writeLock().unlock();
        }

        if (gcDbBinlogMap.isEmpty()) {
            LOG.info("replay gc binlog, dbBinlogMap is null");
            return;
        }

        for (BinlogTombstone tombstone : binlogGcInfo.getTombstones()) {
            long dbId = tombstone.getDbId();
            DBBinlog dbBinlog = gcDbBinlogMap.get(dbId);
            dbBinlog.replayGc(tombstone);
        }
    }

    public void removeDB(long dbId) {
        lock.writeLock().lock();
        try {
            dbBinlogMap.remove(dbId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeTable(long dbId, long tableId) {
        lock.writeLock().lock();
        try {
            DBBinlog dbBinlog = dbBinlogMap.get(dbId);
            if (dbBinlog != null) {
                dbBinlog.removeTable(tableId);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }


    private static void writeTBinlogToStream(DataOutputStream dos, TBinlog binlog) throws TException, IOException {
        TMemoryBuffer buffer = new TMemoryBuffer(BUFFER_SIZE);
        TBinaryProtocol protocol = new TBinaryProtocol(buffer);
        binlog.write(protocol);
        byte[] data = buffer.getArray();
        dos.writeInt(data.length);
        dos.write(data);
    }


    // not thread safety, do this without lock
    public long write(DataOutputStream dos, long checksum) throws IOException {
        if (!Config.enable_feature_binlog) {
            return checksum;
        }

        List<TBinlog> binlogs = Lists.newArrayList();
        // Step 1: get all binlogs
        for (DBBinlog dbBinlog : dbBinlogMap.values()) {
            dbBinlog.getAllBinlogs(binlogs);
        }

        // Step 2: write binlogs length
        dos.writeInt(binlogs.size());
        LOG.info("write binlogs length: {}", binlogs.size());

        // Step 3: write all binlogs to dos
        // binlog is a thrift type TBinlog
        for (TBinlog binlog : binlogs) {
            try {
                writeTBinlogToStream(dos, binlog);
            } catch (TException e) {
                throw new IOException("failed to write binlog to TMemoryBuffer");
            }
        }

        return checksum;
    }

    public TBinlog readTBinlogFromStream(DataInputStream dis) throws TException, IOException {
        // We assume that the first int is the length of the serialized data.
        int length = dis.readInt();
        byte[] data = new byte[length];
        dis.readFully(data);
        TMemoryInputTransport transport = new TMemoryInputTransport(data);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        TBinlog binlog = new TBinlog();
        binlog.read(protocol);
        return binlog;
    }

    // db TBinlogs in file struct:
    // (tableDummy)TBinlog.belong == tableId, (dbDummy)TBinlog.belong == -1
    // +---------------------------+------------------+-----------------------------------+
    // | (tableDummy)TBinlog | ... | (dbDummy)TBinlog | TBinlog | TBinlog | TBinlog | ... |
    // +---------------------------+------------------+-----------------------------------+
    // |        Unnecessary        |     Necessary    |             Unnecessary           |
    // +---------------------------+------------------+-----------------------------------+
    public long read(DataInputStream dis, long checksum) throws IOException {
        // Step 1: read binlogs length
        int size = dis.readInt();
        LOG.info("read binlogs length: {}", size);

        // Step 2: read all binlogs from dis
        long currentDbId = -1;
        boolean currentDbBinlogEnable = false;
        List<TBinlog> tableDummies = Lists.newArrayList();
        try {
            for (int i = 0; i < size; i++) {
                // Step 2.1: read a binlog
                TBinlog binlog = readTBinlogFromStream(dis);

                if (!Config.enable_feature_binlog) {
                    continue;
                }

                // Step 2.2: check if there is in next db Binlogs region
                long dbId = binlog.getDbId();
                if (dbId != currentDbId) {
                    // if there is in next db Binlogs region, check and update metadata
                    Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
                    if (db == null) {
                        LOG.warn("db not found. dbId: {}", dbId);
                        continue;
                    }
                    currentDbId = dbId;
                    currentDbBinlogEnable = db.getBinlogConfig().isEnable();
                    tableDummies = Lists.newArrayList();
                }

                // step 2.3: recover binlog
                if (binlog.getType() == TBinlogType.DUMMY) {
                    // collect tableDummyBinlogs and dbDummyBinlog to recover DBBinlog and TableBinlog
                    if (binlog.getBelong() == -1) {
                        DBBinlog dbBinlog = DBBinlog.recoverDbBinlog(binlogConfigCache, binlog, tableDummies,
                                currentDbBinlogEnable);
                        dbBinlogMap.put(dbId, dbBinlog);
                    } else {
                        tableDummies.add(binlog);
                    }
                } else {
                    // recover common binlogs
                    DBBinlog dbBinlog = dbBinlogMap.get(dbId);
                    if (dbBinlog == null) {
                        LOG.warn("dbBinlog recover fail! binlog {} is before dummy. dbId: {}", binlog, dbId);
                        continue;
                    }
                    binlog.setTableRef(0);
                    dbBinlog.recoverBinlog(binlog, currentDbBinlogEnable);
                }
            }
        } catch (TException e) {
            throw new IOException("failed to read binlog from TMemoryBuffer", e);
        }

        return checksum;
    }

    // remove DB
    // remove Table
}
