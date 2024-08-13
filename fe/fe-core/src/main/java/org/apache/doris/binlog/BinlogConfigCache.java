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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BinlogConfigCache {
    private static final Logger LOG = LogManager.getLogger(BinlogConfigCache.class);

    private Map<Long, BinlogConfig> dbTableBinlogEnableMap; // db or table all use id
    private ReentrantReadWriteLock lock;

    public BinlogConfigCache() {
        dbTableBinlogEnableMap = new HashMap<Long, BinlogConfig>();
        lock = new ReentrantReadWriteLock();
    }

    // Get the binlog config of the specified db, return null if no such database
    // exists.
    public BinlogConfig getDBBinlogConfig(long dbId) {
        lock.readLock().lock();
        BinlogConfig binlogConfig = dbTableBinlogEnableMap.get(dbId);
        lock.readLock().unlock();
        if (binlogConfig != null) {
            return binlogConfig;
        }

        lock.writeLock().lock();
        try {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                LOG.warn("db not found. dbId: {}", dbId);
                return null;
            }

            binlogConfig = db.getBinlogConfig();
            dbTableBinlogEnableMap.put(dbId, binlogConfig);
        } finally {
            lock.writeLock().unlock();
        }
        return binlogConfig;
    }

    public boolean isEnableDB(long dbId) {
        BinlogConfig dBinlogConfig = getDBBinlogConfig(dbId);
        if (dBinlogConfig == null) {
            return false;
        }
        return dBinlogConfig.isEnable();
    }

    public long getDBTtlSeconds(long dbId) {
        BinlogConfig dBinlogConfig = getDBBinlogConfig(dbId);
        if (dBinlogConfig == null) {
            return BinlogConfig.TTL_SECONDS;
        }
        return dBinlogConfig.getTtlSeconds();
    }

    public BinlogConfig getTableBinlogConfig(long dbId, long tableId) {
        lock.readLock().lock();
        BinlogConfig tableBinlogConfig = dbTableBinlogEnableMap.get(tableId);
        lock.readLock().unlock();
        if (tableBinlogConfig != null) {
            return tableBinlogConfig;
        }

        lock.writeLock().lock();
        try {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                LOG.warn("db not found. dbId: {}", dbId);
                return null;
            }

            Table table = db.getTableNullable(tableId);
            if (table == null) {
                LOG.warn("fail to get table. db: {}, table id: {}", db.getFullName(), tableId);
                return null;
            }
            if (!(table instanceof OlapTable)) {
                LOG.warn("table is not olap table. db: {}, table id: {}", db.getFullName(), tableId);
                return null;
            }

            OlapTable olapTable = (OlapTable) table;
            tableBinlogConfig = olapTable.getBinlogConfig();
            // get table binlog config, when table modify binlogConfig
            // it create a new binlog, not update inplace, so we don't need to clone
            // binlogConfig
            dbTableBinlogEnableMap.put(tableId, tableBinlogConfig);
            return tableBinlogConfig;
        } catch (Exception e) {
            LOG.warn("fail to get table. db: {}, table id: {}", dbId, tableId);
            return null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean isEnableTable(long dbId, long tableId) {
        BinlogConfig tableBinlogConfig = getTableBinlogConfig(dbId, tableId);
        if (tableBinlogConfig == null) {
            return false;
        }
        return tableBinlogConfig.isEnable();
    }

    public long getTableTtlSeconds(long dbId, long tableId) {
        BinlogConfig tableBinlogConfig = getTableBinlogConfig(dbId, tableId);
        if (tableBinlogConfig == null) {
            return BinlogConfig.TTL_SECONDS;
        }
        return tableBinlogConfig.getTtlSeconds();
    }

    public void remove(long id) {
        lock.writeLock().lock();
        try {
            dbTableBinlogEnableMap.remove(id);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
