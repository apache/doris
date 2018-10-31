// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.catalog;

import com.baidu.palo.catalog.MaterializedIndex.IndexState;
import com.baidu.palo.catalog.Replica.ReplicaState;
import com.baidu.palo.catalog.Table.TableType;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.FeConstants;
import com.baidu.palo.common.FeMetaVersion;
import com.baidu.palo.common.Pair;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;
import com.baidu.palo.common.util.DebugUtil;
import com.baidu.palo.persist.CreateTableInfo;
import com.baidu.palo.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.Adler32;

/**
 * Internal representation of db-related metadata. Owned by Catalog instance.
 * Not thread safe.
 * <p/>
 * The static initialisation method loadDb is the only way to construct a Db
 * object.
 * <p/>
 * Tables are stored in a map from the table name to the table object. They may
 * be loaded 'eagerly' at construction or 'lazily' on first reference. Tables
 * are accessed via getTable which may trigger a metadata read in two cases: *
 * if the table has never been loaded * if the table loading failed on the
 * previous attempt
 */
public class Database extends MetaObject implements Writable {
    private static final Logger LOG = LogManager.getLogger(Database.class);

    // empirical value.
    // assume that the time a lock is held by thread is less then 100ms
    public static final long TRY_LOCK_TIMEOUT_MS = 100L;

    private long id;
    private String fullQualifiedName;
    private String clusterName;
    private ReentrantReadWriteLock rwLock;
    
    // temp for trace
    private Map<String, Throwable> allLocks = Maps.newConcurrentMap();

    // table family group map
    private Map<Long, Table> idToTable;
    private Map<String, Table> nameToTable;

    private long dataQuotaBytes;

    public enum DbState {
        NORMAL, LINK, MOVE
    }

    private String attachDbName;
    private DbState dbState;

    public Database() {
        this(0, null);
    }

    public Database(long id, String name) {
        this.id = id;
        this.fullQualifiedName = name;
        if (this.fullQualifiedName == null) {
            this.fullQualifiedName = "";
        }
        this.rwLock = new ReentrantReadWriteLock(true);
        this.idToTable = new HashMap<Long, Table>();
        this.nameToTable = new HashMap<String, Table>();
        this.dataQuotaBytes = FeConstants.default_db_data_quota_bytes;
        this.dbState = DbState.NORMAL;
        this.attachDbName = "";
        this.clusterName = "";
    }

    public void readLock() {
        this.rwLock.readLock().lock();
        allLocks.put(Long.toString(Thread.currentThread().getId()), new Exception());
    }

    public void printLocks() {
        for (Throwable exception: allLocks.values()) {
            LOG.debug("a lock in db [{}]", fullQualifiedName, exception);
        }
    }

    public void readUnlock() {
        this.rwLock.readLock().unlock();
        allLocks.remove(Long.toString(Thread.currentThread().getId()));
    }

    public void writeLock() {
        this.rwLock.writeLock().lock();
        allLocks.put(Long.toString(Thread.currentThread().getId()), new Exception());
    }

    public boolean tryWriteLock(long timeout, TimeUnit unit) {
        try {
            boolean result =  this.rwLock.writeLock().tryLock(timeout, unit);
            if (result) {
                allLocks.put(Long.toString(Thread.currentThread().getId()), new Exception());
            }
            return result;
        } catch (InterruptedException e) {
            LOG.warn("failed to try write lock at db[" + id + "]", e);
            return false;
        }
    }

    public void writeUnlock() {
        this.rwLock.writeLock().unlock();
        allLocks.remove(Long.toString(Thread.currentThread().getId()));
    }

    public boolean isWriteLockHeldByCurrentThread() {
        return this.rwLock.writeLock().isHeldByCurrentThread();
    }

    public long getId() {
        return id;
    }

    public String getFullName() {
        return fullQualifiedName;
    }

    public void setNameWithLock(String newName) {
        writeLock();
        try {
            this.fullQualifiedName = newName;
        } finally {
            writeUnlock();
        }
    }

    public void setDataQuotaWithLock(long newQuota) {
        Preconditions.checkArgument(newQuota >= 0L);
        LOG.info("database[{}] set quota from {} to {}", fullQualifiedName, dataQuotaBytes, newQuota);
        writeLock();
        try {
            this.dataQuotaBytes = newQuota;
        } finally {
            writeUnlock();
        }
    }

    public long getDataQuota() {
        return dataQuotaBytes;
    }

    public long getDataQuotaLeftWithLock() {
        long usedDataQuota = 0;
        readLock();
        try {
            for (Table table : this.idToTable.values()) {
                if (table.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                for (Partition partition : olapTable.getPartitions()) {
                    for (MaterializedIndex mIndex : partition.getMaterializedIndices()) {
                        // skip ROLLUP index
                        if (mIndex.getState() == IndexState.ROLLUP) {
                            continue;
                        }

                        for (Tablet tablet : mIndex.getTablets()) {
                            for (Replica replica : tablet.getReplicas()) {
                                if (replica.getState() == ReplicaState.NORMAL
                                        || replica.getState() == ReplicaState.SCHEMA_CHANGE) {
                                    usedDataQuota += replica.getDataSize();
                                }
                            } // end for replicas
                        } // end for tablets
                    } // end for tables
                } // end for table families
            } // end for groups

            long leftDataQuota = dataQuotaBytes - usedDataQuota;
            return leftDataQuota > 0L ? leftDataQuota : 0L;
        } finally {
            readUnlock();
        }
    }

    public void checkQuota() throws DdlException {
        Pair<Double, String> quotaUnitPair = DebugUtil.getByteUint(dataQuotaBytes);
        String readableQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(quotaUnitPair.first) + " "
                + quotaUnitPair.second;
        long leftQuota = getDataQuotaLeftWithLock();

        Pair<Double, String> leftQuotaUnitPair = DebugUtil.getByteUint(leftQuota);
        String readableLeftQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(leftQuotaUnitPair.first) + " "
                + leftQuotaUnitPair.second;

        LOG.info("database[{}] data quota: left bytes: {} / total: {}",
                 fullQualifiedName, readableLeftQuota, readableQuota);

        if (leftQuota <= 0L) {
            throw new DdlException("Database[" + fullQualifiedName
                    + "] data size exceeds quota[" + readableQuota + "]");
        }
    }

    public boolean createTableWithLock(Table table, boolean isReplay, boolean setIfNotExist) {
        boolean result = true;
        writeLock();
        try {
            String tableName = table.getName();
            if (nameToTable.containsKey(tableName)) {
                result = setIfNotExist;
            } else {
                idToTable.put(table.getId(), table);
                nameToTable.put(table.getName(), table);

                if (!isReplay) {
                    // Write edit log
                    CreateTableInfo info = new CreateTableInfo(fullQualifiedName, table);
                    Catalog.getInstance().getEditLog().logCreateTable(info);
                }
                if (table.getType() == TableType.ELASTICSEARCH) {
                    Catalog.getCurrentCatalog().getEsStateStore().registerTable((EsTable)table);
                }
            }
            return result;
        } finally {
            writeUnlock();
        }
    }

    public boolean createTable(Table table) {
        boolean result = true;
        String tableName = table.getName();
        if (nameToTable.containsKey(tableName)) {
            result = false;
        } else {
            idToTable.put(table.getId(), table);
            nameToTable.put(table.getName(), table);
        }
        return result;
    }

    public void dropTableWithLock(String tableName) {
        writeLock();
        try {
            Table table = this.nameToTable.get(tableName);
            if (table != null) {
                this.nameToTable.remove(tableName);
                this.idToTable.remove(table.getId());
            }
        } finally {
            writeUnlock();
        }
    }

    public void dropTable(String tableName) {
        Table table = this.nameToTable.get(tableName);
        if (table != null) {
            this.nameToTable.remove(tableName);
            this.idToTable.remove(table.getId());
        }
    }

    public List<Table> getTables() {
        List<Table> tables = new ArrayList<Table>(idToTable.values());
        return tables;
    }

    public Set<String> getTableNamesWithLock() {
        readLock();
        try {
            Set<String> tableNames = new HashSet<String>();
            for (String name : this.nameToTable.keySet()) {
                tableNames.add(name);
            }
            return tableNames;
        } finally {
            readUnlock();
        }
    }

    public Table getTable(String tableName) {
        if (nameToTable.containsKey(tableName)) {
            return nameToTable.get(tableName);
        }
        return null;
    }

    public Table getTable(long tableId) {
        return idToTable.get(tableId);
    }

    public int getMaxReplicationNum() {
        int ret = 0;
        readLock();
        try {
            for (Table table : idToTable.values()) {
                if (table.getType() != TableType.OLAP) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                for (Partition partition : olapTable.getPartitions()) {
                    short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
                    if (ret < replicationNum) {
                        ret = replicationNum;
                    }
                }
            }
        } finally {
            readUnlock();
        }
        return ret;
    }

    public static Database read(DataInput in) throws IOException {
        Database db = new Database();
        db.readFields(in);
        return db;
    }

    @Override
    public int getSignature(int signatureVersion) {
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);
        String charsetName = "UTF-8";
        try {
            adler32.update(this.fullQualifiedName.getBytes(charsetName));
        } catch (UnsupportedEncodingException e) {
            LOG.error("encoding error", e);
            return -1;
        }
        return Math.abs((int) adler32.getValue());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        out.writeLong(id);
        Text.writeString(out, fullQualifiedName);
        // write tables
        int numTables = nameToTable.size();
        out.writeInt(numTables);
        for (Map.Entry<String, Table> entry : nameToTable.entrySet()) {
            entry.getValue().write(out);
        }

        out.writeLong(dataQuotaBytes);
        Text.writeString(out, clusterName);
        Text.writeString(out, dbState.name());
        Text.writeString(out, attachDbName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        id = in.readLong();
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_30) {
            fullQualifiedName = ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, Text.readString(in));
        } else {
            fullQualifiedName = Text.readString(in);
        }
        // read groups
        int numTables = in.readInt();
        for (int i = 0; i < numTables; ++i) {
            Table table = Table.read(in);
            nameToTable.put(table.getName(), table);
            idToTable.put(table.getId(), table);
        }

        // read quota
        dataQuotaBytes = in.readLong();
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_30) {
            clusterName = SystemInfoService.DEFAULT_CLUSTER;
        } else {
            clusterName = Text.readString(in);
            dbState = DbState.valueOf(Text.readString(in));
            attachDbName = Text.readString(in);
        }
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Database)) {
            return false;
        }

        Database database = (Database) obj;

        if (idToTable != database.idToTable) {
            if (idToTable.size() != database.idToTable.size()) {
                return false;
            }
            for (Entry<Long, Table> entry : idToTable.entrySet()) {
                long key = entry.getKey();
                if (!database.idToTable.containsKey(key)) {
                    return false;
                }
                if (!entry.getValue().equals(database.idToTable.get(key))) {
                    return false;
                }
            }
        }

        return (id == database.id) && (fullQualifiedName.equals(database.fullQualifiedName)
                && dataQuotaBytes == database.dataQuotaBytes);
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public DbState getDbState() {
        return dbState;
    }

    public void setDbState(DbState dbState) {
        if (dbState == null) {
            return;
        }
        this.dbState = dbState;
    }

    public void setAttachDb(String name) {
        this.attachDbName = name;
    }

    public String getAttachDb() {
        return this.attachDbName;
    }

    public void setName(String name) {
        this.fullQualifiedName = name;
    }
}
