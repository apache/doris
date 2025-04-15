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
import org.apache.doris.persist.BarrierLog;
import org.apache.doris.persist.DropInfo;
import org.apache.doris.persist.DropPartitionInfo;
import org.apache.doris.persist.RecoverInfo;
import org.apache.doris.persist.ReplacePartitionOperationLog;
import org.apache.doris.persist.ReplaceTableOperationLog;
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
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DBBinlog {
    private static final Logger LOG = LogManager.getLogger(BinlogManager.class);

    private long dbId;
    // The size of all binlogs.
    private long binlogSize;
    // guard for allBinlogs && tableBinlogMap
    private ReentrantReadWriteLock lock;
    // all binlogs contain table binlogs && create table binlog etc ...
    private TreeSet<TBinlog> allBinlogs;
    // table binlogs
    private Map<Long, TableBinlog> tableBinlogMap;

    // Pair(commitSeq, timestamp), used for gc
    // need UpsertRecord to add timestamps for gc
    private List<Pair<Long, Long>> timestamps;

    // The commit seq of the dropped partitions
    private List<Pair<Long, Long>> droppedPartitions;
    // The commit seq of the dropped tables
    private List<Pair<Long, Long>> droppedTables;
    // The commit seq of the dropped indexes
    private List<Pair<Long, Long>> droppedIndexes;

    private List<TBinlog> tableDummyBinlogs;

    private BinlogConfigCache binlogConfigCache;

    // The binlogs that are locked by the syncer.
    // syncer id => commit seq
    private Map<String, Long> lockedBinlogs;

    public DBBinlog(BinlogConfigCache binlogConfigCache, TBinlog binlog) {
        lock = new ReentrantReadWriteLock();
        this.dbId = binlog.getDbId();
        this.binlogConfigCache = binlogConfigCache;
        this.binlogSize = 0;

        // allBinlogs treeset order by commitSeq
        allBinlogs = Sets.newTreeSet(Comparator.comparingLong(TBinlog::getCommitSeq));
        tableDummyBinlogs = Lists.newArrayList();
        tableBinlogMap = Maps.newHashMap();
        timestamps = Lists.newArrayList();
        droppedPartitions = Lists.newArrayList();
        droppedTables = Lists.newArrayList();
        droppedIndexes = Lists.newArrayList();
        lockedBinlogs = Maps.newHashMap();

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
        binlogSize += BinlogUtils.getApproximateMemoryUsage(binlog);
        recordDroppedOrRecoveredResources(binlog);

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

    // TODO(Drogon): remove TableBinlog after DropTable, think table drop &&
    // recovery
    private TableBinlog getTableBinlog(TBinlog binlog, long tableId, boolean dbBinlogEnable) {
        TableBinlog tableBinlog = tableBinlogMap.get(tableId);
        if (tableBinlog == null) {
            if (dbBinlogEnable || binlogConfigCache.isEnableTable(dbId, tableId)) {
                tableBinlog = new TableBinlog(binlogConfigCache, binlog, dbId, tableId);
                tableBinlogMap.put(tableId, tableBinlog);
                tableDummyBinlogs.add(tableBinlog.getDummyBinlog());
            }
        }
        return tableBinlog;
    }

    // guard by BinlogManager, if addBinlog called, more than one(db/tables) enable
    // binlog
    public void addBinlog(TBinlog binlog, Object raw) {
        boolean dbBinlogEnable = binlogConfigCache.isEnableDB(dbId);
        List<Long> tableIds = binlog.getTableIds();

        lock.writeLock().lock();
        try {
            allBinlogs.add(binlog);
            binlogSize += BinlogUtils.getApproximateMemoryUsage(binlog);

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

            recordDroppedOrRecoveredResources(binlog, raw);

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

    public Pair<TStatus, List<TBinlog>> getBinlog(long tableId, long prevCommitSeq, long numAcquired) {
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
                return tableBinlog.getBinlog(prevCommitSeq, numAcquired);
            }

            return BinlogUtils.getBinlog(allBinlogs, prevCommitSeq, numAcquired);
        } finally {
            lock.readLock().unlock();
        }
    }

    // Get the dropped partitions of the db.
    public List<Pair<Long, Long>> getDroppedPartitions() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(droppedPartitions);
        } finally {
            lock.readLock().unlock();
        }
    }

    // Get the dropped tables of the db.
    public List<Pair<Long, Long>> getDroppedTables() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(droppedTables);
        } finally {
            lock.readLock().unlock();
        }
    }

    // Get the dropped indexes of the db.
    public List<Pair<Long, Long>> getDroppedIndexes() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(droppedIndexes);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Pair<TStatus, BinlogLagInfo> getBinlogLag(long tableId, long prevCommitSeq) {
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

    public Pair<TStatus, Long> lockBinlog(long tableId, String jobUniqueId, long lockCommitSeq) {
        TableBinlog tableBinlog = null;
        lock.writeLock().lock();
        try {
            if (tableId < 0) {
                return lockDbBinlog(jobUniqueId, lockCommitSeq);
            }

            tableBinlog = tableBinlogMap.get(tableId);
        } finally {
            lock.writeLock().unlock();
        }

        if (tableBinlog == null) {
            LOG.warn("table binlog not found. dbId: {}, tableId: {}", dbId, tableId);
            return Pair.of(new TStatus(TStatusCode.BINLOG_NOT_FOUND_TABLE), -1L);
        }
        return tableBinlog.lockBinlog(jobUniqueId, lockCommitSeq);
    }

    // Require: the write lock is held by the caller.
    private Pair<TStatus, Long> lockDbBinlog(String jobUniqueId, long lockCommitSeq) {
        TBinlog firstBinlog = allBinlogs.first();
        TBinlog lastBinlog = allBinlogs.last();

        if (lockCommitSeq < 0) {
            // lock the latest binlog
            lockCommitSeq = lastBinlog.getCommitSeq();
        } else if (lockCommitSeq < firstBinlog.getCommitSeq()) {
            // lock the first binlog
            lockCommitSeq = firstBinlog.getCommitSeq();
        } else if (lastBinlog.getCommitSeq() < lockCommitSeq) {
            LOG.warn("try lock future binlogs, dbId: {}, lockCommitSeq: {}, lastCommitSeq: {}, jobId: {}",
                    dbId, lockCommitSeq, lastBinlog.getCommitSeq(), jobUniqueId);
            return Pair.of(new TStatus(TStatusCode.BINLOG_TOO_NEW_COMMIT_SEQ), -1L);
        }

        // keep idempotent
        Long commitSeq = lockedBinlogs.get(jobUniqueId);
        if (commitSeq != null && lockCommitSeq <= commitSeq) {
            LOG.debug("binlog is locked, commitSeq: {}, jobId: {}, dbId: {}", commitSeq, jobUniqueId, dbId);
            return Pair.of(new TStatus(TStatusCode.OK), commitSeq);
        }

        lockedBinlogs.put(jobUniqueId, lockCommitSeq);
        return Pair.of(new TStatus(TStatusCode.OK), lockCommitSeq);
    }

    public BinlogTombstone gc() {
        // check db
        BinlogConfig dbBinlogConfig = binlogConfigCache.getDBBinlogConfig(dbId);
        if (dbBinlogConfig == null) {
            LOG.error("db not found. dbId: {}", dbId);
            return null;
        } else if (!dbBinlogConfig.isEnable()) {
            return dbBinlogDisableGc();
        } else {
            return dbBinlogEnableGc(dbBinlogConfig);
        }
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
            BinlogTombstone tombstone = tableBinlog.gc();
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

    // remove expired binlogs and dropped partitions, used in disable db binlog gc.
    private void removeExpiredMetaData(long largestExpiredCommitSeq) {
        lock.writeLock().lock();
        try {
            Iterator<TBinlog> binlogIter = allBinlogs.iterator();
            TBinlog dummy = binlogIter.next();
            boolean foundFirstUsingBinlog = false;
            long lastCommitSeq = -1;
            long removed = 0;

            while (binlogIter.hasNext()) {
                TBinlog binlog = binlogIter.next();
                long commitSeq = binlog.getCommitSeq();
                if (commitSeq <= largestExpiredCommitSeq) {
                    if (binlog.table_ref <= 0) {
                        binlogIter.remove();
                        binlogSize -= BinlogUtils.getApproximateMemoryUsage(binlog);
                        ++removed;
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

            gcDroppedResources(largestExpiredCommitSeq);
            if (lastCommitSeq != -1) {
                dummy.setCommitSeq(lastCommitSeq);
            }

            LOG.info("remove {} expired binlogs, dbId: {}, left: {}", removed, dbId, allBinlogs.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    // Get last expired binlog, and gc expired binlogs/timestamps/dropped
    // partitions, used in enable db binlog gc.
    private TBinlog getLastExpiredBinlog(BinlogComparator checker) {
        TBinlog lastExpiredBinlog = null;

        Iterator<TBinlog> binlogIter = allBinlogs.iterator();
        TBinlog dummy = binlogIter.next();
        while (binlogIter.hasNext()) {
            TBinlog binlog = binlogIter.next();
            if (checker.isExpired(binlog)) {
                binlogIter.remove();
                binlogSize -= BinlogUtils.getApproximateMemoryUsage(binlog);
                lastExpiredBinlog = binlog;
            } else {
                break;
            }
        }

        if (lastExpiredBinlog != null) {
            final long expiredCommitSeq = lastExpiredBinlog.getCommitSeq();
            dummy.setCommitSeq(expiredCommitSeq);
            dummy.setTimestamp(lastExpiredBinlog.getTimestamp());

            // release expired timestamps by commit seq.
            Iterator<Pair<Long, Long>> timeIter = timestamps.iterator();
            while (timeIter.hasNext() && timeIter.next().first <= expiredCommitSeq) {
                timeIter.remove();
            }

            lockedBinlogs.entrySet().removeIf(ent -> ent.getValue() <= expiredCommitSeq);
            gcDroppedResources(expiredCommitSeq);
        }

        return lastExpiredBinlog;
    }

    private Optional<Long> getMinLockedCommitSeq() {
        lock.readLock().lock();
        try {
            Optional<Long> minLockedCommitSeq = lockedBinlogs.values().stream().min(Long::compareTo);
            for (TableBinlog tableBinlog : tableBinlogMap.values()) {
                Optional<Long> tableMinLockedCommitSeq = tableBinlog.getMinLockedCommitSeq();
                if (!tableMinLockedCommitSeq.isPresent()) {
                    continue;
                }
                if (minLockedCommitSeq.isPresent()) {
                    minLockedCommitSeq = Optional.of(Math.min(minLockedCommitSeq.get(), tableMinLockedCommitSeq.get()));
                } else {
                    minLockedCommitSeq = tableMinLockedCommitSeq;
                }
            }
            return minLockedCommitSeq;
        } finally {
            lock.readLock().unlock();
        }
    }

    private BinlogTombstone dbBinlogEnableGc(BinlogConfig dbBinlogConfig) {
        long ttlSeconds = dbBinlogConfig.getTtlSeconds();
        long maxBytes = dbBinlogConfig.getMaxBytes();
        long maxHistoryNums = dbBinlogConfig.getMaxHistoryNums();
        long expiredMs = BinlogUtils.getExpiredMs(ttlSeconds);

        LOG.info("gc db binlog. dbId: {}, expiredMs: {}, ttlSecond: {}, maxBytes: {}, maxHistoryNums: {}",
                dbId, expiredMs, ttlSeconds, maxBytes, maxHistoryNums);

        // step 1: get current tableBinlog info and expiredCommitSeq
        Optional<Long> minLockedCommitSeq = getMinLockedCommitSeq();
        TBinlog lastExpiredBinlog = null;
        List<TableBinlog> tableBinlogs = Lists.newArrayList();
        lock.writeLock().lock();
        try {
            long expiredCommitSeq = -1L;
            Iterator<Pair<Long, Long>> timeIter = timestamps.iterator();
            while (timeIter.hasNext()) {
                Pair<Long, Long> pair = timeIter.next();
                if (pair.second > expiredMs) {
                    break;
                }
                expiredCommitSeq = pair.first;
            }

            // Speed up gc by recycling binlogs that are not locked by syncer.
            // To keep compatible with the old version, if no binlog is locked here, fallthrough to the
            // previous behavior (keep the entire binlogs until it is expired).
            if (minLockedCommitSeq.isPresent() && expiredCommitSeq + 1L < minLockedCommitSeq.get()) {
                expiredCommitSeq = minLockedCommitSeq.get() - 1L;
            }

            final long lastExpiredCommitSeq = expiredCommitSeq;
            BinlogComparator checker = (binlog) -> {
                // NOTE: TreeSet read size during iterator remove is valid.
                //
                // The expired conditions in order:
                // 1. expired time
                // 2. the max bytes
                // 3. the max history num
                return binlog.getCommitSeq() <= lastExpiredCommitSeq
                        || maxBytes < binlogSize
                        || maxHistoryNums < allBinlogs.size();
            };
            lastExpiredBinlog = getLastExpiredBinlog(checker);
            tableBinlogs.addAll(tableBinlogMap.values());
        } finally {
            lock.writeLock().unlock();
        }

        if (lastExpiredBinlog == null) {
            return null;
        }

        // step 2: gc every tableBinlog in dbBinlog, get table tombstone to complete db
        // tombstone
        List<BinlogTombstone> tableTombstones = Lists.newArrayList();
        for (TableBinlog tableBinlog : tableBinlogs) {
            // step 2.1: gc tableBinlog，and get table tombstone
            BinlogTombstone tableTombstone = tableBinlog.commitSeqGc(lastExpiredBinlog.getCommitSeq());
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
            BinlogComparator checker = (binlog) -> binlog.getCommitSeq() <= largestExpiredCommitSeq;
            getLastExpiredBinlog(checker);
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

    private void gcDroppedResources(long commitSeq) {
        Iterator<Pair<Long, Long>> iter = droppedPartitions.iterator();
        while (iter.hasNext() && iter.next().second < commitSeq) {
            iter.remove();
        }
        iter = droppedTables.iterator();
        while (iter.hasNext() && iter.next().second < commitSeq) {
            iter.remove();
        }
        iter = droppedIndexes.iterator();
        while (iter.hasNext() && iter.next().second < commitSeq) {
            iter.remove();
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
                String binlogSize = String.valueOf(this.binlogSize);
                info.add(binlogSize);
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
                String binlogMaxBytes = null;
                String binlogMaxHistoryNums = null;
                if (binlogConfig != null) {
                    binlogTtlSeconds = String.valueOf(binlogConfig.getTtlSeconds());
                    binlogMaxBytes = String.valueOf(binlogConfig.getMaxBytes());
                    binlogMaxHistoryNums = String.valueOf(binlogConfig.getMaxHistoryNums());
                }
                info.add(binlogTtlSeconds);
                info.add(binlogMaxBytes);
                info.add(binlogMaxHistoryNums);

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

    private void recordDroppedOrRecoveredResources(TBinlog binlog) {
        recordDroppedOrRecoveredResources(binlog, null);
    }

    // A method to record the dropped tables, indexes, and partitions.
    private void recordDroppedOrRecoveredResources(TBinlog binlog, Object raw) {
        recordDroppedOrRecoveredResources(binlog.getType(), binlog.getCommitSeq(), binlog.getData(), raw);
    }

    private void recordDroppedOrRecoveredResources(TBinlogType binlogType, long commitSeq, String data, Object raw) {
        if (raw == null) {
            switch (binlogType) {
                case DROP_PARTITION:
                    raw = DropPartitionInfo.fromJson(data);
                    break;
                case DROP_TABLE:
                    raw = DropTableRecord.fromJson(data);
                    break;
                case ALTER_JOB:
                    raw = AlterJobRecord.fromJson(data);
                    break;
                case TRUNCATE_TABLE:
                    raw = TruncateTableRecord.fromJson(data);
                    break;
                case REPLACE_TABLE:
                    raw = ReplaceTableOperationLog.fromJson(data);
                    break;
                case DROP_ROLLUP:
                    raw = DropInfo.fromJson(data);
                    break;
                case BARRIER:
                    raw = BarrierLog.fromJson(data);
                    break;
                case RECOVER_INFO:
                    raw = RecoverInfo.fromJson(data);
                    break;
                case REPLACE_PARTITIONS:
                    raw = ReplacePartitionOperationLog.fromJson(data);
                    break;
                default:
                    break;
            }
            if (raw == null) {
                return;
            }
        }

        recordDroppedOrRecoveredResources(binlogType, commitSeq, raw);
    }

    private void recordDroppedOrRecoveredResources(TBinlogType binlogType, long commitSeq, Object raw) {
        if (binlogType == TBinlogType.DROP_PARTITION && raw instanceof DropPartitionInfo) {
            long partitionId = ((DropPartitionInfo) raw).getPartitionId();
            if (partitionId > 0) {
                droppedPartitions.add(Pair.of(partitionId, commitSeq));
            }
        } else if (binlogType == TBinlogType.DROP_TABLE && raw instanceof DropTableRecord) {
            long tableId = ((DropTableRecord) raw).getTableId();
            if (tableId > 0) {
                droppedTables.add(Pair.of(tableId, commitSeq));
            }
        } else if (binlogType == TBinlogType.ALTER_JOB && raw instanceof AlterJobRecord) {
            AlterJobRecord alterJobRecord = (AlterJobRecord) raw;
            if (alterJobRecord.isJobFinished() && alterJobRecord.isSchemaChangeJob()) {
                for (Long indexId : alterJobRecord.getOriginIndexIdList()) {
                    if (indexId != null && indexId > 0) {
                        droppedIndexes.add(Pair.of(indexId, commitSeq));
                    }
                }
            }
        } else if (binlogType == TBinlogType.TRUNCATE_TABLE && raw instanceof TruncateTableRecord) {
            TruncateTableRecord truncateTableRecord = (TruncateTableRecord) raw;
            for (long partitionId : truncateTableRecord.getOldPartitionIds()) {
                droppedPartitions.add(Pair.of(partitionId, commitSeq));
            }
        } else if (binlogType == TBinlogType.REPLACE_TABLE && raw instanceof ReplaceTableOperationLog) {
            ReplaceTableOperationLog record = (ReplaceTableOperationLog) raw;
            if (!record.isSwapTable()) {
                droppedTables.add(Pair.of(record.getOrigTblId(), commitSeq));
            }
        } else if (binlogType == TBinlogType.DROP_ROLLUP && raw instanceof DropInfo) {
            long indexId = ((DropInfo) raw).getIndexId();
            if (indexId > 0) {
                droppedIndexes.add(Pair.of(indexId, commitSeq));
            }
        } else if (binlogType == TBinlogType.BARRIER && raw instanceof BarrierLog) {
            BarrierLog log = (BarrierLog) raw;
            // keep compatible with doris 2.0/2.1
            if (log.hasBinlog()) {
                recordDroppedOrRecoveredResources(log.getBinlogType(), commitSeq, log.getBinlog(), null);
            }
        } else if ((binlogType == TBinlogType.RECOVER_INFO) && (raw instanceof RecoverInfo)) {
            RecoverInfo recoverInfo = (RecoverInfo) raw;
            long partitionId = recoverInfo.getPartitionId();
            long tableId = recoverInfo.getTableId();
            if (partitionId > 0) {
                droppedPartitions.removeIf(entry -> (entry.first == partitionId));
            } else if (tableId > 0) {
                droppedTables.removeIf(entry -> (entry.first == tableId));
            }
        } else if ((binlogType == TBinlogType.REPLACE_PARTITIONS) && (raw instanceof ReplacePartitionOperationLog)) {
            ReplacePartitionOperationLog replacePartitionOperationLog = (ReplacePartitionOperationLog) raw;
            for (Long partitionId : replacePartitionOperationLog.getReplacedPartitionIds()) {
                droppedPartitions.add(Pair.of(partitionId, commitSeq));
            }
        }
    }

}
