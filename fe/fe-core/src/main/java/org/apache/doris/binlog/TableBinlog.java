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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Pair;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TBinlogType;
import org.apache.doris.thrift.TStatus;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TableBinlog {
    private static final Logger LOG = LogManager.getLogger(TableBinlog.class);

    private long dbId;
    private long tableId;
    private long binlogSize;
    private ReentrantReadWriteLock lock;
    private TreeSet<TBinlog> binlogs;

    // Pair(commitSeq, timestamp), used for gc
    // need UpsertRecord to add timestamps for gc
    private List<Pair<Long, Long>> timestamps;

    private BinlogConfigCache binlogConfigCache;

    public TableBinlog(BinlogConfigCache binlogConfigCache, TBinlog binlog, long dbId, long tableId) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.binlogSize = 0;
        lock = new ReentrantReadWriteLock();
        binlogs = Sets.newTreeSet(Comparator.comparingLong(TBinlog::getCommitSeq));
        timestamps = Lists.newArrayList();

        TBinlog dummy;
        if (binlog.getType() == TBinlogType.DUMMY) {
            dummy = binlog;
        } else {
            dummy = BinlogUtils.newDummyBinlog(binlog.getDbId(), tableId);
        }
        binlogs.add(dummy);
        this.binlogConfigCache = binlogConfigCache;
    }

    public TBinlog getDummyBinlog() {
        return binlogs.first();
    }

    public long getTableId() {
        return tableId;
    }

    // not thread safety, do this without lock
    public void recoverBinlog(TBinlog binlog) {
        TBinlog dummy = getDummyBinlog();
        if (binlog.getCommitSeq() > dummy.getCommitSeq()) {
            addBinlogWithoutCheck(binlog);
        }
    }

    public void addBinlog(TBinlog binlog) {
        lock.writeLock().lock();
        try {
            addBinlogWithoutCheck(binlog);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void addBinlogWithoutCheck(TBinlog binlog) {
        binlogs.add(binlog);
        ++binlog.table_ref;
        binlogSize += BinlogUtils.getApproximateMemoryUsage(binlog);
        if (binlog.getTimestamp() > 0) {
            timestamps.add(Pair.of(binlog.getCommitSeq(), binlog.getTimestamp()));
        }
    }

    public Pair<TStatus, TBinlog> getBinlog(long prevCommitSeq) {
        lock.readLock().lock();
        try {
            return BinlogUtils.getBinlog(binlogs, prevCommitSeq);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Pair<TStatus, Long> getBinlogLag(long prevCommitSeq) {
        lock.readLock().lock();
        try {
            return BinlogUtils.getBinlogLag(binlogs, prevCommitSeq);
        } finally {
            lock.readLock().unlock();
        }
    }

    private Pair<TBinlog, Long> getLastUpsertAndLargestCommitSeq(BinlogComparator checker) {
        if (binlogs.size() <= 1) {
            return null;
        }

        Iterator<TBinlog> iter = binlogs.iterator();
        TBinlog dummyBinlog = iter.next();
        TBinlog tombstoneUpsert = null;
        TBinlog lastExpiredBinlog = null;
        while (iter.hasNext()) {
            TBinlog binlog = iter.next();
            if (checker.isExpired(binlog)) {
                lastExpiredBinlog = binlog;
                --binlog.table_ref;
                binlogSize -= BinlogUtils.getApproximateMemoryUsage(binlog);
                if (binlog.getType() == TBinlogType.UPSERT) {
                    tombstoneUpsert = binlog;
                }
                iter.remove();
            } else {
                break;
            }
        }

        if (lastExpiredBinlog == null) {
            return null;
        }

        long expiredCommitSeq = lastExpiredBinlog.getCommitSeq();
        dummyBinlog.setCommitSeq(expiredCommitSeq);

        Iterator<Pair<Long, Long>> timeIterator = timestamps.iterator();
        while (timeIterator.hasNext() && timeIterator.next().first <= expiredCommitSeq) {
            timeIterator.remove();
        }

        return Pair.of(tombstoneUpsert, expiredCommitSeq);
    }

    // this method call when db binlog enable
    public BinlogTombstone commitSeqGc(long expiredCommitSeq) {
        Pair<TBinlog, Long> tombstoneInfo;

        // step 1: get tombstoneUpsertBinlog and dummyBinlog
        lock.writeLock().lock();
        try {
            BinlogComparator check = (binlog) -> binlog.getCommitSeq() <= expiredCommitSeq;
            tombstoneInfo = getLastUpsertAndLargestCommitSeq(check);
        } finally {
            lock.writeLock().unlock();
        }

        // step 2: set tombstone by tombstoneInfo
        // if there have expired Binlogs, tombstoneInfo != null
        if (tombstoneInfo == null) {
            return null;
        }

        TBinlog lastUpsertBinlog = tombstoneInfo.first;
        long largestCommitSeq = tombstoneInfo.second;
        BinlogTombstone tombstone = new BinlogTombstone(tableId, largestCommitSeq);
        if (lastUpsertBinlog != null) {
            UpsertRecord upsertRecord = UpsertRecord.fromJson(lastUpsertBinlog.getData());
            tombstone.addTableRecord(tableId, upsertRecord);
        }

        return tombstone;
    }

    // this method call when db binlog disable
    public BinlogTombstone gc() {
        // step 1: get expire time
        BinlogConfig tableBinlogConfig = binlogConfigCache.getTableBinlogConfig(dbId, tableId);
        if (tableBinlogConfig == null) {
            return null;
        }

        long ttlSeconds = tableBinlogConfig.getTtlSeconds();
        long maxBytes = tableBinlogConfig.getMaxBytes();
        long maxHistoryNums = tableBinlogConfig.getMaxHistoryNums();
        long expiredMs = BinlogUtils.getExpiredMs(ttlSeconds);

        LOG.info(
                "gc table binlog. dbId: {}, tableId: {}, expiredMs: {}, ttlSecond: {}, maxBytes: {}, "
                        + "maxHistoryNums: {}, now: {}",
                dbId, tableId, expiredMs, ttlSeconds, maxBytes, maxHistoryNums, System.currentTimeMillis());

        // step 2: get tombstoneUpsertBinlog and dummyBinlog
        Pair<TBinlog, Long> tombstoneInfo;
        lock.writeLock().lock();
        try {
            // find the last expired commit seq.
            long expiredCommitSeq = -1;
            Iterator<Pair<Long, Long>> timeIterator = timestamps.iterator();
            while (timeIterator.hasNext()) {
                Pair<Long, Long> entry = timeIterator.next();
                if (expiredMs < entry.second) {
                    break;
                }
                expiredCommitSeq = entry.first;
            }

            final long lastExpiredCommitSeq = expiredCommitSeq;
            BinlogComparator check = (binlog) -> {
                // NOTE: TreeSet read size during iterator remove is valid.
                //
                // The expired conditions in order:
                // 1. expired time
                // 2. the max bytes
                // 3. the max history num
                return binlog.getCommitSeq() <= lastExpiredCommitSeq
                        || maxBytes < binlogSize
                        || maxHistoryNums < binlogs.size();
            };
            tombstoneInfo = getLastUpsertAndLargestCommitSeq(check);
        } finally {
            lock.writeLock().unlock();
        }

        // step 3: set tombstone by tombstoneInfo
        // if have expired Binlogs, tombstoneInfo != null
        if (tombstoneInfo == null) {
            return null;
        }

        TBinlog lastUpsertBinlog = tombstoneInfo.first;
        long largestCommitSeq = tombstoneInfo.second;
        BinlogTombstone tombstone = new BinlogTombstone(tableId, largestCommitSeq);
        if (lastUpsertBinlog != null) {
            UpsertRecord upsertRecord = UpsertRecord.fromJson(lastUpsertBinlog.getData());
            tombstone.addTableRecord(tableId, upsertRecord);
        }

        return tombstone;
    }

    public void replayGc(long largestExpiredCommitSeq) {
        lock.writeLock().lock();
        try {
            BinlogComparator checker = (binlog) -> binlog.getCommitSeq() <= largestExpiredCommitSeq;
            getLastUpsertAndLargestCommitSeq(checker);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void getBinlogInfo(Database db, BaseProcResult result) {
        BinlogConfig binlogConfig = binlogConfigCache.getTableBinlogConfig(dbId, tableId);

        String tableName = null;
        String dropped = null;
        if (db == null) {
            tableName = "(dropped).(unknown)";
            dropped = "true";
        } else {
            String dbName = db.getFullName();
            Table table = db.getTableNullable(tableId);
            if (table == null) {
                dropped = "true";
                tableName = dbName + ".(dropped)";
            }

            dropped = "false";
            if (table instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) table;
                tableName = dbName + "." + olapTable.getName();
            } else {
                tableName = dbName + ".(not_olaptable)";
            }
        }

        lock.readLock().lock();
        try {
            List<String> info = new ArrayList<>();

            info.add(tableName);
            String type = "table";
            info.add(type);

            String id = String.valueOf(tableId);
            info.add(id);
            info.add(dropped);
            String binlogLength = String.valueOf(binlogs.size());
            info.add(binlogLength);
            String binlogSize = String.valueOf(this.binlogSize);
            info.add(binlogSize);
            String firstBinlogCommittedTime = null;
            String readableFirstBinlogCommittedTime = null;
            for (TBinlog binlog : binlogs) {
                long timestamp = binlog.getTimestamp();
                if (timestamp != -1) {
                    firstBinlogCommittedTime = String.valueOf(timestamp);
                    readableFirstBinlogCommittedTime = BinlogUtils.convertTimeToReadable(timestamp);
                    break;
                }
            }
            info.add(firstBinlogCommittedTime);
            info.add(readableFirstBinlogCommittedTime);
            String lastBinlogCommittedTime = null;
            String readableLastBinlogCommittedTime = null;
            Iterator<TBinlog> iterator = binlogs.descendingIterator();
            while (iterator.hasNext()) {
                TBinlog binlog = iterator.next();
                long timestamp = binlog.getTimestamp();
                if (timestamp != -1) {
                    lastBinlogCommittedTime = String.valueOf(timestamp);
                    readableLastBinlogCommittedTime = BinlogUtils.convertTimeToReadable(timestamp);
                    break;
                }
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
        } finally {
            lock.readLock().unlock();
        }
    }
}
