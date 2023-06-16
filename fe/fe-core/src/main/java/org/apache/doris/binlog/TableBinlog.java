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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TBinlogType;
import org.apache.doris.thrift.TStatus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TableBinlog {
    private static final Logger LOG = LogManager.getLogger(TableBinlog.class);

    private long tableId;
    private ReentrantReadWriteLock lock;
    private TreeSet<TBinlog> binlogs;

    public TableBinlog(long tableId) {
        this.tableId = tableId;
        lock = new ReentrantReadWriteLock();
        // binlogs treeset order by commitSeq
        binlogs = new TreeSet<TBinlog>((o1, o2) -> {
            if (o1.getCommitSeq() < o2.getCommitSeq()) {
                return -1;
            } else if (o1.getCommitSeq() > o2.getCommitSeq()) {
                return 1;
            } else {
                return 0;
            }
        });
    }

    public long getTableId() {
        return tableId;
    }

    public void addBinlog(TBinlog binlog) {
        lock.writeLock().lock();
        try {
            binlogs.add(binlog);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Pair<TStatus, TBinlog> getBinlog(long commitSeq) {
        lock.readLock().lock();
        try {
            return BinlogUtils.getBinlog(binlogs, commitSeq);
        } finally {
            lock.readLock().unlock();
        }
    }

    // this method call when db binlog enable
    public BinlogTombstone gc(long largestExpiredCommitSeq) {
        TBinlog tombstoneUpsert = null;

        lock.writeLock().lock();
        try {
            Iterator<TBinlog> iter = binlogs.iterator();
            while (iter.hasNext()) {
                TBinlog binlog = iter.next();
                if (binlog.getCommitSeq() <= largestExpiredCommitSeq) {
                    if (binlog.getType() == TBinlogType.UPSERT) {
                        tombstoneUpsert = binlog;
                    }
                    iter.remove();
                } else {
                    break;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

        if (tombstoneUpsert == null) {
            return null;
        }

        BinlogTombstone tombstone = new BinlogTombstone(-1, largestExpiredCommitSeq);
        UpsertRecord upsertRecord = UpsertRecord.fromJson(tombstoneUpsert.getData());
        tombstone.addTableRecord(tableId, upsertRecord);
        return tombstone;
    }

    // this method call when db binlog disable
    public BinlogTombstone gc(Database db) {
        OlapTable table = null;
        try {
            Table tbl = db.getTableOrMetaException(tableId);
            if (tbl == null) {
                LOG.warn("fail to get table. db: {}, table id: {}", db.getFullName(), tableId);
                return null;
            }
            if (!(tbl instanceof OlapTable)) {
                LOG.warn("table is not olap table. db: {}, table id: {}", db.getFullName(), tableId);
                return null;
            }
            table = (OlapTable) tbl;
        } catch (Exception e) {
            LOG.warn("fail to get table. db: {}, table id: {}", db.getFullName(), tableId);
            return null;
        }

        long dbId = db.getId();
        long ttlSeconds = table.getBinlogConfig().getTtlSeconds();
        long currentSeconds = System.currentTimeMillis() / 1000;
        long expireSeconds = currentSeconds - ttlSeconds;
        long expireMs = expireSeconds * 1000;

        TBinlog tombstoneUpsert = null;
        long largestExpiredCommitSeq = 0;
        lock.writeLock().lock();
        try {
            Iterator<TBinlog> iter = binlogs.iterator();
            while (iter.hasNext()) {
                TBinlog binlog = iter.next();
                if (binlog.getTimestamp() <= expireMs) {
                    if (binlog.getType() == TBinlogType.UPSERT) {
                        tombstoneUpsert = binlog;
                    }
                    largestExpiredCommitSeq = binlog.getCommitSeq();
                    iter.remove();
                } else {
                    break;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

        BinlogTombstone tombstone = new BinlogTombstone(dbId, largestExpiredCommitSeq);
        if (tombstoneUpsert != null) {
            UpsertRecord upsertRecord = UpsertRecord.fromJson(tombstoneUpsert.getData());
            tombstone.addTableRecord(tableId, upsertRecord);
        }

        return tombstone;
    }

    public void replayGc(long largestExpiredCommitSeq) {
        lock.writeLock().lock();
        try {
            Iterator<TBinlog> iter = binlogs.iterator();
            while (iter.hasNext()) {
                TBinlog binlog = iter.next();
                if (binlog.getCommitSeq() <= largestExpiredCommitSeq) {
                    iter.remove();
                } else {
                    break;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
