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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    public DBBinlog(long dbId) {
        lock = new ReentrantReadWriteLock();
        this.dbId = dbId;
        // allBinlogs treeset order by commitSeq
        allBinlogs = new TreeSet<TBinlog>((o1, o2) -> {
            if (o1.getCommitSeq() < o2.getCommitSeq()) {
                return -1;
            } else if (o1.getCommitSeq() > o2.getCommitSeq()) {
                return 1;
            } else {
                return 0;
            }
        });
        tableBinlogMap = new HashMap<Long, TableBinlog>();
        timestamps = new ArrayList<Pair<Long, Long>>();
    }

    public void addBinlog(TBinlog binlog, boolean dbBinlogEnable) {
        List<Long> tableIds = binlog.getTableIds();
        lock.writeLock().lock();
        try {
            if (binlog.getTimestamp() > 0 && dbBinlogEnable) {
                timestamps.add(Pair.of(binlog.getCommitSeq(), binlog.getTimestamp()));
            }

            allBinlogs.add(binlog);

            if (tableIds == null) {
                return;
            }

            for (long tableId : tableIds) {
                TableBinlog tableBinlog = tableBinlogMap.get(tableId);
                if (tableBinlog == null) {
                    tableBinlog = new TableBinlog(tableId);
                    tableBinlogMap.put(tableId, tableBinlog);
                }
                tableBinlog.addBinlog(binlog);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long getDbId() {
        return dbId;
    }

    public Pair<TStatus, TBinlog> getBinlog(long tableId, long commitSeq) {
        TStatus status = new TStatus(TStatusCode.OK);
        lock.readLock().lock();
        try {
            if (tableId >= 0) {
                TableBinlog tableBinlog = tableBinlogMap.get(tableId);
                if (tableBinlog == null) {
                    status.setStatusCode(TStatusCode.BINLOG_NOT_FOUND_TABLE);
                    return Pair.of(status, null);
                }
                return tableBinlog.getBinlog(commitSeq);
            }

            return BinlogUtils.getBinlog(allBinlogs, commitSeq);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<BinlogTombstone> gc() {
        // check db
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            LOG.error("db not found. dbId: {}", dbId);
            return null;
        }

        boolean dbBinlogEnable = db.getBinlogConfig().isEnable();
        if (dbBinlogEnable) {
            // db binlog is enable, only one binlogTombstones
            long ttlSeconds = db.getBinlogConfig().getTtlSeconds();
            long currentSeconds = System.currentTimeMillis() / 1000;
            long expireSeconds = currentSeconds - ttlSeconds;
            long expireMs = expireSeconds * 1000;

            BinlogTombstone tombstone = dbBinlogEnableGc(expireMs);
            List<BinlogTombstone> tombstones = new ArrayList<BinlogTombstone>();
            if (tombstone != null) {
                tombstones.add(tombstone);
            }
            return tombstones;
        } else {
            return dbBinlogDisableGc(db);
        }
    }

    private List<BinlogTombstone> dbBinlogDisableGc(Database db) {
        List<BinlogTombstone> tombstones = new ArrayList<BinlogTombstone>();
        List<TableBinlog> tableBinlogs = null;

        lock.writeLock().lock();
        try {
            tableBinlogs = new ArrayList<TableBinlog>(tableBinlogMap.values());
        } finally {
            lock.writeLock().unlock();
        }

        for (TableBinlog tableBinlog : tableBinlogs) {
            BinlogTombstone tombstone = tableBinlog.gc(db);
            if (tombstone != null) {
                tombstones.add(tombstone);
            }
        }
        return tombstones;
    }

    private BinlogTombstone dbBinlogEnableGc(long expireMs) {
        // find commitSeq from timestamps, if commitSeq's timestamp is less than expireSeconds, then remove it
        long largestExpiredCommitSeq = -1;
        TBinlog tombstoneBinlog = null;
        List<Long> tableIds = null;
        List<TableBinlog> tableBinlogs = null;

        lock.writeLock().lock();
        try {
            Iterator<Pair<Long, Long>> iterator = timestamps.iterator();
            while (iterator.hasNext()) {
                Pair<Long, Long> pair = iterator.next();
                if (pair.second < expireMs) {
                    largestExpiredCommitSeq = pair.first;
                    iterator.remove();
                } else {
                    break;
                }
            }

            Iterator<TBinlog> binlogIterator = allBinlogs.iterator();
            while (binlogIterator.hasNext()) {
                TBinlog binlog = binlogIterator.next();
                if (binlog.getCommitSeq() <= largestExpiredCommitSeq) {
                    tombstoneBinlog = binlog;
                    binlogIterator.remove();
                } else {
                    break;
                }
            }

            tableIds = new ArrayList<Long>(tableBinlogMap.keySet());
            tableBinlogs = new ArrayList<TableBinlog>(tableBinlogMap.values());
        } finally {
            lock.writeLock().unlock();
        }
        LOG.info("gc binlog. dbId: {}, expireMs: {}, largestExpiredCommitSeq: {}",
                dbId, expireMs, largestExpiredCommitSeq);
        if (tombstoneBinlog == null) {
            return null;
        }

        BinlogTombstone tombstone = new BinlogTombstone(dbId, tableIds, tombstoneBinlog.getCommitSeq());
        for (TableBinlog tableBinlog : tableBinlogs) {
            BinlogTombstone binlogTombstone = tableBinlog.gc(largestExpiredCommitSeq);
            if (binlogTombstone == null) {
                continue;
            }

            Map<Long, UpsertRecord.TableRecord> tableVersionMap = binlogTombstone.getTableVersionMap();
            if (tableVersionMap.size() > 1) {
                LOG.warn("tableVersionMap size is greater than 1. tableVersionMap: {}", tableVersionMap);
            }
            for (Map.Entry<Long, UpsertRecord.TableRecord> entry : tableVersionMap.entrySet()) {
                long tableId = entry.getKey();
                UpsertRecord.TableRecord record = entry.getValue();
                tombstone.addTableRecord(tableId, record);
            }
        }

        return tombstone;
    }

    public void replayGc(BinlogTombstone tombstone) {
        if (tombstone.isDbBinlogTomstone()) {
            dbBinlogEnableReplayGc(tombstone);
        } else {
            dbBinlogDisableReplayGc(tombstone);
        }
    }

    public void dbBinlogEnableReplayGc(BinlogTombstone tombstone) {
        long largestExpiredCommitSeq = tombstone.getCommitSeq();

        lock.writeLock().lock();
        try {
            Iterator<Pair<Long, Long>> iterator = timestamps.iterator();
            while (iterator.hasNext()) {
                Pair<Long, Long> pair = iterator.next();
                if (pair.first <= largestExpiredCommitSeq) {
                    iterator.remove();
                } else {
                    break;
                }
            }

            Iterator<TBinlog> binlogIterator = allBinlogs.iterator();
            while (binlogIterator.hasNext()) {
                TBinlog binlog = binlogIterator.next();
                if (binlog.getCommitSeq() <= largestExpiredCommitSeq) {
                    binlogIterator.remove();
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
        List<TableBinlog> tableBinlogs = null;

        lock.writeLock().lock();
        try {
            tableBinlogs = new ArrayList<TableBinlog>(tableBinlogMap.values());
        } finally {
            lock.writeLock().unlock();
        }

        if (tableBinlogs.isEmpty()) {
            return;
        }

        Set<Long> tableIds = new HashSet<Long>(tombstone.getTableIds());
        long largestExpiredCommitSeq = tombstone.getCommitSeq();
        for (TableBinlog tableBinlog : tableBinlogs) {
            if (tableIds.contains(tableBinlog.getTableId())) {
                tableBinlog.replayGc(largestExpiredCommitSeq);
            }
        }
    }

    // not thread safety, do this without lock
    public void getAllBinlogs(List<TBinlog> binlogs) {
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
}
