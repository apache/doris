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

import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DBBinlog {
    private long dbId;
    // guard for allBinlogs && tableBinlogMap
    private ReentrantReadWriteLock lock;
    // all binlogs contain table binlogs && create table binlog etc ...
    private TreeSet<TBinlog> allBinlogs;
    // table binlogs
    private Map<Long, TableBinlog> tableBinlogMap;

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
    }

    public void addBinlog(TBinlog binlog) {
        List<Long> tableIds = binlog.getTableIds();
        lock.writeLock().lock();
        try {
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

    // not thread safety, do this without lock
    public void getAllBinlogs(List<TBinlog> binlogs) {
        binlogs.addAll(allBinlogs);
    }
}
