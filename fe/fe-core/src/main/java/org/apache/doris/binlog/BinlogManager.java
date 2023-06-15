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
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.persist.BinlogGcInfo;
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
import org.apache.thrift.transport.TTransportException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BinlogManager {
    private static final int BUFFER_SIZE = 16 * 1024;

    private static final Logger LOG = LogManager.getLogger(BinlogManager.class);

    private ReentrantReadWriteLock lock;
    private Map<Long, DBBinlog> dbBinlogMap;

    public BinlogManager() {
        lock = new ReentrantReadWriteLock();
        dbBinlogMap = Maps.newHashMap();
    }

    private void addBinlog(TBinlog binlog) {
        if (!Config.enable_feature_binlog) {
            return;
        }

        // find db BinlogConfig
        long dbId = binlog.getDbId();
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            LOG.warn("db not found. dbId: {}", dbId);
            return;
        }
        boolean dbBinlogEnable = db.getBinlogConfig().isEnable();

        DBBinlog dbBinlog;
        lock.writeLock().lock();
        try {
            dbBinlog = dbBinlogMap.get(dbId);
            if (dbBinlog == null) {
                dbBinlog = new DBBinlog(dbId);
                dbBinlogMap.put(dbId, dbBinlog);
            }
        } finally {
            lock.writeLock().unlock();
        }

        dbBinlog.addBinlog(binlog, dbBinlogEnable);
    }

    private void addBinlog(long dbId, List<Long> tableIds, long commitSeq, long timestamp, TBinlogType type,
                           String data) {
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
        addBinlog(binlog);
    }

    public void addUpsertRecord(UpsertRecord upsertRecord) {
        long dbId = upsertRecord.getDbId();
        List<Long> tableIds = upsertRecord.getAllReleatedTableIds();
        long commitSeq = upsertRecord.getCommitSeq();
        long timestamp = upsertRecord.getTimestamp();
        TBinlogType type = TBinlogType.UPSERT;
        String data = upsertRecord.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data);
    }

    public void addAddPartitionRecord(AddPartitionRecord addPartitionRecord) {
        long dbId = addPartitionRecord.getDbId();
        List<Long> tableIds = new ArrayList<Long>();
        tableIds.add(addPartitionRecord.getTableId());
        long commitSeq = addPartitionRecord.getCommitSeq();
        long timestamp = -1;
        TBinlogType type = TBinlogType.ADD_PARTITION;
        String data = addPartitionRecord.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data);
    }

    // get binlog by dbId, return first binlog.version > version
    public Pair<TStatus, TBinlog> getBinlog(long dbId, long tableId, long commitSeq) {
        TStatus status = new TStatus(TStatusCode.OK);
        lock.readLock().lock();
        try {
            DBBinlog dbBinlog = dbBinlogMap.get(dbId);
            if (dbBinlog == null) {
                status.setStatusCode(TStatusCode.BINLOG_NOT_FOUND_DB);
                LOG.warn("dbBinlog not found. dbId: {}", dbId);
                return Pair.of(status, null);
            }

            return dbBinlog.getBinlog(tableId, commitSeq);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<BinlogTombstone> gc() {
        LOG.info("begin gc binlog");

        lock.writeLock().lock();
        Map<Long, DBBinlog> gcDbBinlogMap = null;
        try {
            gcDbBinlogMap = new HashMap<Long, DBBinlog>(dbBinlogMap);
        } finally {
            lock.writeLock().unlock();
        }

        if (gcDbBinlogMap.isEmpty()) {
            LOG.info("gc binlog, dbBinlogMap is null");
            return null;
        }

        List<BinlogTombstone> tombstones = Lists.newArrayList();
        for (DBBinlog dbBinlog : gcDbBinlogMap.values()) {
            List<BinlogTombstone> dbTombstones = dbBinlog.gc();
            if (dbTombstones != null) {
                tombstones.addAll(dbTombstones);
            }
        }
        return tombstones;
    }

    public void replayGc(BinlogGcInfo binlogGcInfo) {
        lock.writeLock().lock();
        Map<Long, DBBinlog> gcDbBinlogMap = null;
        try {
            gcDbBinlogMap = new HashMap<Long, DBBinlog>(dbBinlogMap);
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

        List<TBinlog> binlogs = new ArrayList<TBinlog>();
        // Step 1: get all binlogs
        for (DBBinlog dbBinlog : dbBinlogMap.values()) {
            dbBinlog.getAllBinlogs(binlogs);
        }
        // sort binlogs by commitSeq
        Collections.sort(binlogs, new Comparator<TBinlog>() {
            @Override
            public int compare(TBinlog o1, TBinlog o2) {
                return Long.compare(o1.getCommitSeq(), o2.getCommitSeq());
            }
        });

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

    public void read(DataInputStream dis) throws IOException {
        // Step 1: read binlogs length
        int length = dis.readInt();

        // Step 2: read all binlogs from dis && add binlog
        TMemoryBuffer buffer;
        TBinaryProtocol protocol;
        try {
            buffer = new TMemoryBuffer(BUFFER_SIZE);
            protocol = new TBinaryProtocol(buffer);
        } catch (TTransportException e) {
            throw new IOException("failed to create TMemoryBuffer");
        }

        for (int i = 0; i < length; i++) {
            TBinlog binlog = new TBinlog();
            try {
                binlog.read(protocol);
            } catch (TException e) {
                throw new IOException("failed to read binlog from TMemoryBuffer");
            }
            addBinlog(binlog);
        }
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

    public long read(DataInputStream dis, long checksum) throws IOException {
        if (!Config.enable_feature_binlog) {
            return checksum;
        }

        // Step 1: read binlogs length
        int size = dis.readInt();
        LOG.info("read binlogs length: {}", size);

        // Step 2: read all binlogs from dis
        for (int i = 0; i < size; i++) {
            try {
                TBinlog binlog = readTBinlogFromStream(dis);
                addBinlog(binlog);
            } catch (TException e) {
                throw new IOException("failed to read binlog from TMemoryBuffer", e);
            }
        }

        return checksum;
    }

    // remove DB
    // remove Table
}
