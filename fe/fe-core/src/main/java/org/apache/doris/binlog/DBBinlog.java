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

import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TBinlogType;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DBBinlog {
    private static final Logger LOG = LogManager.getLogger(BinlogManager.class);

    private long dbId;
    // guard for allBinlogs && tableBinlogMap
    private ReentrantReadWriteLock lock;
    // all binlogs contain table binlogs && create table binlog etc ...
    private TreeSet<TBinlog> allBinlogs;
    // table binlogs
    private Map<Long, TableBinlog> tableBinlogMap;

    // Pair(commitSeq, timestamp), used for gc
    // need UpsertRecord to add timestamps for gc
    private List<Pair<Long, Long>> timestamps;

    private List<TBinlog> tableDummyBinlogs;

    private BinlogConfigCache binlogConfigCache;

    public DBBinlog(BinlogConfigCache binlogConfigCache, TBinlog binlog) {
        lock = new ReentrantReadWriteLock();
        this.dbId = binlog.getDbId();
        this.binlogConfigCache = binlogConfigCache;

        // allBinlogs treeset order by commitSeq
        allBinlogs = Sets.newTreeSet(Comparator.comparingLong(TBinlog::getCommitSeq));
        tableDummyBinlogs = Lists.newArrayList();
        tableBinlogMap = Maps.newHashMap();
        timestamps = Lists.newArrayList();

        TBinlog dummy;
        if (binlog.getType() == TBinlogType.DUMMY) {
            dummy = binlog;
        } else {
            dummy = BinlogUtils.newDummyBinlog(dbId, -1);
        }
        allBinlogs.add(dummy);
    }

    public static DBBinlog recoverDbBinlog(BinlogConfigCache binlogConfigCache, TBinlog dbDummy,
                                           List<TBinlog> tableDummies, boolean dbBinlogEnable) {
        DBBinlog dbBinlog = new DBBinlog(binlogConfigCache, dbDummy);
        long dbId = dbDummy.getDbId();
        for (TBinlog tableDummy : tableDummies) {
            long tableId = tableDummy.getBelong();
            if (!dbBinlogEnable && !binlogConfigCache.isEnableTable(dbId, tableId)) {
                continue;
            }
            dbBinlog.tableBinlogMap.put(tableId, new TableBinlog(binlogConfigCache, tableDummy, dbId, tableId));
            dbBinlog.tableDummyBinlogs.add(tableDummy);
        }

        return dbBinlog;
    }

    // not thread safety, do this without lock
    public void recoverBinlog(TBinlog binlog, boolean dbBinlogEnable) {
        List<Long> tableIds = binlog.getTableIds();

        if (binlog.getTimestamp() > 0 && dbBinlogEnable) {
            timestamps.add(Pair.of(binlog.getCommitSeq(), binlog.getTimestamp()));
        }

        allBinlogs.add(binlog);

        if (tableIds == null) {
            return;
        }

        for (long tableId : tableIds) {
            TableBinlog tableBinlog = getTableBinlog(binlog, tableId, dbBinlogEnable);
            if (tableBinlog == null) {
                continue;
            }
            tableBinlog.recoverBinlog(binlog);
        }
    }

    // TODO(Drogon): remove TableBinlog after DropTable, think table drop && recovery
    private TableBinlog getTableBinlog(TBinlog binlog, long tableId, boolean dbBinlogEnable) {
        TableBinlog tableBinlog = tableBinlogMap.get(tableId);
        if (tableBinlog == null) {
            if (dbBinlogEnable || binlogConfigCache.isEnableTable(dbId, tableId)) {
                tableBinlog = new TableBinlog(binlogConfigCache, binlog, dbId,  tableId);
                tableBinlogMap.put(tableId, tableBinlog);
                tableDummyBinlogs.add(tableBinlog.getDummyBinlog());
            }
        }
        return tableBinlog;
    }

    // guard by BinlogManager, if addBinlog called, more than one(db/tables) enable binlog
    public void addBinlog(TBinlog binlog) {
        boolean dbBinlogEnable = binlogConfigCache.isEnableDB(dbId);
        List<Long> tableIds = binlog.getTableIds();

        lock.writeLock().lock();
        try {
            allBinlogs.add(binlog);

            if (binlog.getTimestamp() > 0 && dbBinlogEnable) {
                timestamps.add(Pair.of(binlog.getCommitSeq(), binlog.getTimestamp()));
            }

            if (tableIds == null) {
                return;
            }

            // HACK: for metadata fix
            // we should not add binlog for create table and drop table in table binlog
            if (!binlog.isSetType()) {
                return;
            }
            switch (binlog.getType()) {
                case CREATE_TABLE:
                    return;
                case DROP_TABLE:
                    return;
                default:
                    break;
            }

            for (long tableId : tableIds) {
                TableBinlog tableBinlog = getTableBinlog(binlog, tableId, dbBinlogEnable);
                if (tableBinlog != null) {
                    tableBinlog.addBinlog(binlog);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long getDbId() {
        return dbId;
    }

    public Pair<TStatus, TBinlog> getBinlog(long tableId, long prevCommitSeq) {
        TStatus status = new TStatus(TStatusCode.OK);
        lock.readLock().lock();
        try {
            if (tableId >= 0) {
                TableBinlog tableBinlog = tableBinlogMap.get(tableId);
                if (tableBinlog == null) {
                    LOG.warn("table binlog not found. tableId: {}", tableId);
                    status.setStatusCode(TStatusCode.BINLOG_NOT_FOUND_TABLE);
                    return Pair.of(status, null);
                }
                return tableBinlog.getBinlog(prevCommitSeq);
            }

            return BinlogUtils.getBinlog(allBinlogs, prevCommitSeq);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Pair<TStatus, Long> getBinlogLag(long tableId, long prevCommitSeq) {
        TStatus status = new TStatus(TStatusCode.OK);
        lock.readLock().lock();
        try {
            if (tableId >= 0) {
                TableBinlog tableBinlog = tableBinlogMap.get(tableId);
                if (tableBinlog == null) {
                    LOG.warn("table binlog not found. tableId: {}", tableId);
                    status.setStatusCode(TStatusCode.BINLOG_NOT_FOUND_TABLE);
                    return Pair.of(status, null);
                }
                return tableBinlog.getBinlogLag(prevCommitSeq);
            }

            return BinlogUtils.getBinlogLag(allBinlogs, prevCommitSeq);
        } finally {
            lock.readLock().unlock();
        }
    }

    public BinlogTombstone gc() {
        // check db
        BinlogConfig dbBinlogConfig = binlogConfigCache.getDBBinlogConfig(dbId);
        if (dbBinlogConfig == null) {
            LOG.error("db not found. dbId: {}", dbId);
            return null;
        }

        boolean dbBinlogEnable = dbBinlogConfig.isEnable();
        BinlogTombstone tombstone;
        if (dbBinlogEnable) {
            // db binlog is enabled, only one binlogTombstones
            long ttlSeconds = dbBinlogConfig.getTtlSeconds();
            long expiredMs = BinlogUtils.getExpiredMs(ttlSeconds);

            tombstone = dbBinlogEnableGc(expiredMs);
        } else {
            tombstone = dbBinlogDisableGc();
        }

        return tombstone;
    }

    private BinlogTombstone collectTableTombstone(List<BinlogTombstone> tableTombstones, boolean isDbGc) {
        if (tableTombstones.isEmpty()) {
            return null;
        }

        BinlogTombstone dbTombstone = new BinlogTombstone(dbId, isDbGc);
        for (BinlogTombstone tableTombstone : tableTombstones) {
            // collect tableCommitSeq
            dbTombstone.mergeTableTombstone(tableTombstone);

            // collect tableVersionMap
            Map<Long, UpsertRecord.TableRecord> tableVersionMap = tableTombstone.getTableVersionMap();
            if (tableVersionMap.size() > 1) {
                LOG.warn("tableVersionMap size is greater than 1. tableVersionMap: {}", tableVersionMap);
            }
            dbTombstone.addTableRecord(tableVersionMap);
        }

        LOG.info("After GC, dbId: {}, dbExpiredBinlog: {}, tableExpiredBinlogs: {}",
                dbId, dbTombstone.getCommitSeq(), dbTombstone.getTableCommitSeqMap());

        return dbTombstone;
    }

    private BinlogTombstone dbBinlogDisableGc() {
        List<BinlogTombstone> tombstones = Lists.newArrayList();
        List<TableBinlog> tableBinlogs;

        lock.readLock().lock();
        try {
            tableBinlogs = Lists.newArrayList(tableBinlogMap.values());
        } finally {
            lock.readLock().unlock();
        }

        for (TableBinlog tableBinlog : tableBinlogs) {
            BinlogTombstone tombstone = tableBinlog.ttlGc();
            if (tombstone != null) {
                tombstones.add(tombstone);
            }
        }
        BinlogTombstone tombstone = collectTableTombstone(tombstones, false);
        if (tombstone != null) {
            removeExpiredMetaData(tombstone.getCommitSeq());
        }

        return tombstone;
    }

    private void removeExpiredMetaData(long largestExpiredCommitSeq) {
        lock.writeLock().lock();
        try {
            Iterator<TBinlog> binlogIter = allBinlogs.iterator();
            TBinlog dummy = binlogIter.next();
            boolean foundFirstUsingBinlog = false;
            long lastCommitSeq = -1;

            while (binlogIter.hasNext()) {
                TBinlog binlog = binlogIter.next();
                long commitSeq = binlog.getCommitSeq();
                if (commitSeq <= largestExpiredCommitSeq) {
                    if (binlog.table_ref <= 0) {
                        binlogIter.remove();
                        if (!foundFirstUsingBinlog) {
                            lastCommitSeq = commitSeq;
                        }
                    } else {
                        foundFirstUsingBinlog = true;
                    }
                } else {
                    break;
                }
            }

            if (lastCommitSeq != -1) {
                dummy.setCommitSeq(lastCommitSeq);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private BinlogTombstone dbBinlogEnableGc(long expiredMs) {
        // step 1: get current tableBinlog info and expiredCommitSeq
        long expiredCommitSeq = -1;
        lock.writeLock().lock();
        try {
            Iterator<Pair<Long, Long>> timeIter = timestamps.iterator();
            while (timeIter.hasNext()) {
                Pair<Long, Long> pair = timeIter.next();
                if (pair.second <= expiredMs) {
                    expiredCommitSeq = pair.first;
                    timeIter.remove();
                } else {
                    break;
                }
            }

            Iterator<TBinlog> binlogIter = allBinlogs.iterator();
            TBinlog dummy = binlogIter.next();
            dummy.setCommitSeq(expiredCommitSeq);

            while (binlogIter.hasNext()) {
                TBinlog binlog = binlogIter.next();
                if (binlog.getCommitSeq() <= expiredCommitSeq) {
                    binlogIter.remove();
                } else {
                    break;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

        if (expiredCommitSeq == -1) {
            return null;
        }

        // step 2: gc every tableBinlog in dbBinlog, get table tombstone to complete db tombstone
        List<BinlogTombstone> tableTombstones = Lists.newArrayList();
        for (TableBinlog tableBinlog : tableBinlogMap.values()) {
            // step 2.1: gc tableBinlog，and get table tombstone
            BinlogTombstone tableTombstone = tableBinlog.commitSeqGc(expiredCommitSeq);
            if (tableTombstone != null) {
                tableTombstones.add(tableTombstone);
            }
        }

        return collectTableTombstone(tableTombstones, true);
    }

    public void replayGc(BinlogTombstone tombstone) {
        if (tombstone.isDbBinlogTomstone()) {
            dbBinlogEnableReplayGc(tombstone);
        } else {
            dbBinlogDisableReplayGc(tombstone);
            removeExpiredMetaData(tombstone.getCommitSeq());
        }
    }

    public void dbBinlogEnableReplayGc(BinlogTombstone tombstone) {
        long largestExpiredCommitSeq = tombstone.getCommitSeq();

        lock.writeLock().lock();
        try {
            Iterator<Pair<Long, Long>> timeIter = timestamps.iterator();
            while (timeIter.hasNext()) {
                long commitSeq = timeIter.next().first;
                if (commitSeq <= largestExpiredCommitSeq) {
                    timeIter.remove();
                } else {
                    break;
                }
            }

            Iterator<TBinlog> binlogIter = allBinlogs.iterator();
            TBinlog dummy = binlogIter.next();
            dummy.setCommitSeq(largestExpiredCommitSeq);

            while (binlogIter.hasNext()) {
                TBinlog binlog = binlogIter.next();
                if (binlog.getCommitSeq() <= largestExpiredCommitSeq) {
                    binlogIter.remove();
                } else {
                    break;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

        dbBinlogDisableReplayGc(tombstone);
    }

    public void dbBinlogDisableReplayGc(BinlogTombstone tombstone) {
        List<TableBinlog> tableBinlogs;

        lock.readLock().lock();
        try {
            tableBinlogs = Lists.newArrayList(tableBinlogMap.values());
        } finally {
            lock.readLock().unlock();
        }

        if (tableBinlogs.isEmpty()) {
            return;
        }

        Map<Long, Long> tableCommitSeqMap = tombstone.getTableCommitSeqMap();
        for (TableBinlog tableBinlog : tableBinlogs) {
            long tableId = tableBinlog.getTableId();
            if (tableCommitSeqMap.containsKey(tableId)) {
                tableBinlog.replayGc(tableCommitSeqMap.get(tableId));
            }
        }
    }

    // not thread safety, do this without lock
    public void getAllBinlogs(List<TBinlog> binlogs) {
        binlogs.addAll(tableDummyBinlogs);
        binlogs.addAll(allBinlogs);
    }

    public void removeTable(long tableId) {
        lock.writeLock().lock();
        try {
            tableBinlogMap.remove(tableId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void getBinlogInfo(BaseProcResult result) {
        BinlogConfig binlogConfig = binlogConfigCache.getDBBinlogConfig(dbId);

        String dbName = "(dropped)";
        String dropped = "true";
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        if (db != null) {
            dbName = db.getFullName();
            dropped = "false";
        }

        lock.readLock().lock();
        try {
            boolean dbBinlogEnable = binlogConfigCache.isEnableDB(dbId);
            if (dbBinlogEnable) {
                List<String> info = new ArrayList<>();

                info.add(dbName);
                String type = "db";
                info.add(type);
                String id = String.valueOf(dbId);
                info.add(id);
                info.add(dropped);
                String binlogLength = String.valueOf(allBinlogs.size());
                info.add(binlogLength);
                String firstBinlogCommittedTime = null;
                String readableFirstBinlogCommittedTime = null;
                if (!timestamps.isEmpty()) {
                    long timestamp = timestamps.get(0).second;
                    firstBinlogCommittedTime = String.valueOf(timestamp);
                    readableFirstBinlogCommittedTime = BinlogUtils.convertTimeToReadable(timestamp);
                }
                info.add(firstBinlogCommittedTime);
                info.add(readableFirstBinlogCommittedTime);
                String lastBinlogCommittedTime = null;
                String readableLastBinlogCommittedTime = null;
                if (!timestamps.isEmpty()) {
                    long timestamp = timestamps.get(timestamps.size() - 1).second;
                    lastBinlogCommittedTime = String.valueOf(timestamp);
                    readableLastBinlogCommittedTime = BinlogUtils.convertTimeToReadable(timestamp);
                }
                info.add(lastBinlogCommittedTime);
                info.add(readableLastBinlogCommittedTime);
                String binlogTtlSeconds = null;
                if (binlogConfig != null) {
                    binlogTtlSeconds = String.valueOf(binlogConfig.getTtlSeconds());
                }
                info.add(binlogTtlSeconds);

                result.addRow(info);
            } else {
                for (TableBinlog tableBinlog : tableBinlogMap.values()) {
                    tableBinlog.getBinlogInfo(db, result);
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }
}
