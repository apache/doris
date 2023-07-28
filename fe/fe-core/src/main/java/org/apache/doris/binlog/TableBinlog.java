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
import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TBinlogType;
import org.apache.doris.thrift.TStatus;

import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TableBinlog {
    private static final Logger LOG = LogManager.getLogger(TableBinlog.class);

    private long dbId;
    private long tableId;
    private ReentrantReadWriteLock lock;
    private TreeSet<TBinlog> binlogs;
    private BinlogConfigCache binlogConfigCache;

    public TableBinlog(BinlogConfigCache binlogConfigCache, TBinlog binlog, long dbId, long tableId) {
        this.dbId = dbId;
        this.tableId = tableId;
        lock = new ReentrantReadWriteLock();
        binlogs = Sets.newTreeSet(Comparator.comparingLong(TBinlog::getCommitSeq));

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
            binlogs.add(binlog);
            ++binlog.table_ref;
        }
    }

    public void addBinlog(TBinlog binlog) {
        lock.writeLock().lock();
        try {
            binlogs.add(binlog);
            ++binlog.table_ref;
        } finally {
            lock.writeLock().unlock();
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

    private Pair<TBinlog, Long> getLastUpsertAndLargestCommitSeq(long expired, BinlogComparator checker) {
        if (binlogs.size() <= 1) {
            return null;
        }

        Iterator<TBinlog> iter = binlogs.iterator();
        TBinlog dummyBinlog = iter.next();
        TBinlog tombstoneUpsert = null;
        TBinlog lastExpiredBinlog = null;
        while (iter.hasNext()) {
            TBinlog binlog = iter.next();
            if (checker.isExpired(binlog, expired)) {
                lastExpiredBinlog = binlog;
                --binlog.table_ref;
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

        dummyBinlog.setCommitSeq(lastExpiredBinlog.getCommitSeq());

        return Pair.of(tombstoneUpsert, lastExpiredBinlog.getCommitSeq());
    }

    // this method call when db binlog enable
    public BinlogTombstone commitSeqGc(long expiredCommitSeq) {
        Pair<TBinlog, Long> tombstoneInfo;

        // step 1: get tombstoneUpsertBinlog and dummyBinlog
        lock.writeLock().lock();
        try {
            BinlogComparator check = (binlog, expire) -> binlog.getCommitSeq() <= expire;
            tombstoneInfo = getLastUpsertAndLargestCommitSeq(expiredCommitSeq, check);
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
    public BinlogTombstone ttlGc() {
        // step 1: get expire time
        BinlogConfig tableBinlogConfig = binlogConfigCache.getTableBinlogConfig(dbId, tableId);
        if (tableBinlogConfig == null) {
            return null;
        }

        long ttlSeconds = tableBinlogConfig.getTtlSeconds();
        long expiredMs = BinlogUtils.getExpiredMs(ttlSeconds);

        if (expiredMs < 0) {
            return null;
        }
        LOG.info("ttl gc. dbId: {}, tableId: {}, expiredMs: {}", dbId, tableId, expiredMs);

        // step 2: get tombstoneUpsertBinlog and dummyBinlog
        Pair<TBinlog, Long> tombstoneInfo;
        lock.writeLock().lock();
        try {
            BinlogComparator check = (binlog, expire) -> binlog.getTimestamp() <= expire;
            tombstoneInfo = getLastUpsertAndLargestCommitSeq(expiredMs, check);
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
            long lastSeq = -1;
            Iterator<TBinlog> iter = binlogs.iterator();
            TBinlog dummyBinlog = iter.next();

            while (iter.hasNext()) {
                TBinlog binlog = iter.next();
                long commitSeq = binlog.getCommitSeq();
                if (commitSeq <= largestExpiredCommitSeq) {
                    lastSeq = commitSeq;
                    --binlog.table_ref;
                    iter.remove();
                } else {
                    break;
                }
            }

            if (lastSeq != -1) {
                dummyBinlog.setCommitSeq(lastSeq);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
