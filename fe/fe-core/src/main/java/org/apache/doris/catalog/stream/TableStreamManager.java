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

package org.apache.doris.catalog.stream;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.persist.PruneTableStreamPartitionOffsetInfo;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TableStreamManager extends MasterDaemon implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(TableStreamManager.class);
    @SerializedName(value = "dbStreamMap")
    private Map<Long, Set<Long>> dbStreamMap;
    protected MonitoredReentrantReadWriteLock rwLock;

    public TableStreamManager() {
        super("table-stream-cleanup", Config.table_stream_partition_offset_cleanup_interval_second * 1000L);
        this.rwLock = new MonitoredReentrantReadWriteLock(true);
        this.dbStreamMap = new HashMap<>();
    }

    @Override
    protected void runAfterCatalogReady() {
        if (Config.enable_table_stream) {
            cleanupStalePartitionOffsets();
        }
    }

    public void addTableStream(BaseTableStream stream) {
        rwLock.writeLock().lock();
        try {
            dbStreamMap.computeIfAbsent(stream.getDatabase().getId(), k -> new HashSet<>()).add(stream.getId());
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void removeStaleDbAndStream(BaseTableStream stream) {
        rwLock.writeLock().lock();
        try {
            Optional.ofNullable(dbStreamMap.get(stream.getDatabase().getId()))
                    .ifPresent(set -> set.remove(stream.getId()));
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void removeStaleDbAndStream(List<Long> staleDbIds, List<Pair<Long, Long>> staleStreamIds) {
        if (staleStreamIds.isEmpty() && staleDbIds.isEmpty()) {
            return;
        }
        rwLock.writeLock().lock();
        try {
            staleDbIds.forEach(dbId -> dbStreamMap.remove(dbId));
            staleStreamIds.forEach(
                    pair -> Optional.ofNullable(dbStreamMap.get(pair.first))
                            .ifPresent(set -> set.remove(pair.second))
            );
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static TableStreamManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, TableStreamManager.class);
    }

    public Set<Long> getTableStreamIds(DatabaseIf db) {
        Set<Long> result = new HashSet<>();
        rwLock.readLock().lock();
        try {
            result.addAll(dbStreamMap.getOrDefault(db.getId(), new HashSet<>()));
        } finally {
            rwLock.readLock().unlock();
        }
        return result;
    }

    public void cleanupStalePartitionOffsets() {
        List<Long> staleDbIds = new ArrayList<>();
        List<Pair<Long, Long>> staleStreamIds = new ArrayList<>();
        List<PruneTableStreamPartitionOffsetInfo.Entry> pruneEntries = new ArrayList<>();
        for (Map.Entry<Long, Set<Long>> entry : copyDbStreamMap().entrySet()) {
            Optional<Database> db = Env.getCurrentInternalCatalog().getDb(entry.getKey());
            if (!db.isPresent()) {
                staleDbIds.add(entry.getKey());
                continue;
            }
            for (Long tableId : entry.getValue()) {
                Optional<Table> table = db.get().getTable(tableId);
                if (!table.isPresent()) {
                    staleStreamIds.add(Pair.of(db.get().getId(), tableId));
                    continue;
                }
                if (!(table.get() instanceof OlapTableStream)) {
                    staleStreamIds.add(Pair.of(db.get().getId(), tableId));
                    continue;
                }
                cleanupStalePartitionOffsets((OlapTableStream) table.get()).ifPresent(pruneEntries::add);
            }
        }
        removeStaleDbAndStream(staleDbIds, staleStreamIds);
        if (!pruneEntries.isEmpty()) {
            Env.getCurrentEnv().getEditLog().logPruneTableStreamPartitionOffsets(
                    new PruneTableStreamPartitionOffsetInfo(pruneEntries));
        }
    }

    private Optional<PruneTableStreamPartitionOffsetInfo.Entry> cleanupStalePartitionOffsets(OlapTableStream stream) {
        if (stream.isDisabled() || stream.isStale()) {
            return Optional.empty();
        }
        OlapTable baseTable = stream.getBaseTableNullable();
        if (baseTable == null) {
            return Optional.empty();
        }
        Set<Long> validPartitionIds;
        if (!baseTable.tryReadLock(Table.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("skip cleaning stream {} because base table {} read lock is busy",
                        stream.getName(), baseTable.getName());
            }
            return Optional.empty();
        }
        try {
            if (baseTable.isDropped) {
                return Optional.empty();
            }
            validPartitionIds = new HashSet<>(baseTable.getPartitionIds());
        } finally {
            baseTable.readUnlock();
        }
        if (!stream.tryWriteLockIfExist(Table.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("skip cleaning stream {} because stream write lock is busy", stream.getName());
            }
            return Optional.empty();
        }
        try {
            if (stream.isDisabled() || stream.isStale()) {
                return Optional.empty();
            }
            Set<Long> stalePartitionIds = stream.unprotectedCollectStalePartitionOffsetIds(validPartitionIds);
            if (stalePartitionIds.isEmpty()) {
                return Optional.empty();
            }
            int removedPartitionCount = stream.unprotectedPrunePartitionOffsets(stalePartitionIds);
            if (removedPartitionCount > 0) {
                LOG.info("cleaned {} stale partition offset entries from stream {}.{} ({})",
                        removedPartitionCount, stream.getDatabase().getFullName(), stream.getName(), stream.getId());
            }
            return Optional.of(new PruneTableStreamPartitionOffsetInfo.Entry(
                    stream.getDatabase().getId(), stream.getId(), stalePartitionIds));
        } finally {
            stream.writeUnlock();
        }
    }

    public void replayPruneTableStreamPartitionOffsets(PruneTableStreamPartitionOffsetInfo info) {
        if (info == null || info.getEntries() == null || info.getEntries().isEmpty()) {
            return;
        }
        for (PruneTableStreamPartitionOffsetInfo.Entry entry : info.getEntries()) {
            replayPruneTableStreamPartitionOffsets(entry);
        }
    }

    private void replayPruneTableStreamPartitionOffsets(PruneTableStreamPartitionOffsetInfo.Entry entry) {
        if (entry == null || entry.getPartitionIds() == null || entry.getPartitionIds().isEmpty()) {
            return;
        }
        Optional<Database> db = Env.getCurrentInternalCatalog().getDb(entry.getDbId());
        if (!db.isPresent()) {
            LOG.info("skip replay pruning partition offsets because db {} does not exist", entry.getDbId());
            return;
        }
        Optional<Table> table = db.get().getTable(entry.getStreamId());
        if (!table.isPresent()) {
            LOG.info("skip replay pruning partition offsets because stream {}.{} does not exist",
                    entry.getDbId(), entry.getStreamId());
            return;
        }
        if (!(table.get() instanceof OlapTableStream)) {
            LOG.info("skip replay pruning partition offsets because table {}.{} is not an olap table stream",
                    entry.getDbId(), entry.getStreamId());
            return;
        }
        OlapTableStream stream = (OlapTableStream) table.get();
        if (!stream.tryWriteLockIfExist(Table.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            LOG.warn("skip replay pruning partition offsets because stream {}.{} write lock is busy",
                    db.get().getFullName(), stream.getName());
            return;
        }
        try {
            stream.unprotectedPrunePartitionOffsets(entry.getPartitionIds());
        } finally {
            stream.writeUnlock();
        }
    }

    private Map<Long, Set<Long>> copyDbStreamMap() {
        Map<Long, Set<Long>> copiedMap = new HashMap<>();
        rwLock.readLock().lock();
        try {
            for (Map.Entry<Long, Set<Long>> e : dbStreamMap.entrySet()) {
                copiedMap.put(e.getKey(), new HashSet<>(e.getValue()));
            }
        } finally {
            rwLock.readLock().unlock();
        }
        return copiedMap;
    }

    public void fillTableStreamValuesMetadataResult(List<TRow> dataBatch) {
        for (Map.Entry<Long, Set<Long>> entry : copyDbStreamMap().entrySet()) {
            Optional<Database> db = Env.getCurrentInternalCatalog().getDb(entry.getKey());
            if (db.isPresent()) {
                for (Long tableId : entry.getValue()) {
                    Optional<Table> table = db.get().getTable(tableId);
                    if (!table.isPresent()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.warn("invalid stream id: {}, db: {}", tableId, db.get().getFullName());
                        }
                        continue;
                    }
                    if (!(table.get() instanceof BaseTableStream)) {
                        LOG.warn("invalid not table stream type table: {}", table.get().getName());
                        continue;
                    }
                    BaseTableStream stream = (BaseTableStream) table.get();
                    if (stream.readLockIfExist()) {
                        try {
                            TRow trow = new TRow();
                            // DB_NAME
                            trow.addToColumnValue(new TCell().setStringVal(stream.getDatabase().getFullName()));
                            // STREAM_NAME
                            trow.addToColumnValue(new TCell().setStringVal(stream.getName()));
                            // STREAM_ID
                            trow.addToColumnValue(new TCell().setLongVal(stream.getId()));
                            // STREAM_TYPE
                            trow.addToColumnValue(new TCell().setStringVal(stream.getTableStreamType()));
                            // CONSUME_TYPE
                            trow.addToColumnValue(new TCell().setStringVal(stream.getConsumeTypeString()));
                            // STREAM_COMMENT
                            trow.addToColumnValue(new TCell().setStringVal(stream.getComment()));
                            TableIf baseTable = stream.getBaseTableNullable();
                            if (baseTable == null) {
                                // BASE_TABLE_NAME
                                trow.addToColumnValue(new TCell().setStringVal("N/A"));
                                // BASE_TABLE_DB
                                trow.addToColumnValue(new TCell().setStringVal("N/A"));
                                // BASE_TABLE_CTL
                                trow.addToColumnValue(new TCell().setStringVal("N/A"));
                                // BASE_TABLE_TYPE
                                trow.addToColumnValue(new TCell().setStringVal("N/A"));
                            } else {
                                List<String> baseTableQualifiers = baseTable.getFullQualifiers();
                                // BASE_TABLE_NAME
                                trow.addToColumnValue(new TCell().setStringVal(baseTableQualifiers.get(2)));
                                // BASE_TABLE_DB
                                trow.addToColumnValue(new TCell().setStringVal(baseTableQualifiers.get(1)));
                                // BASE_TABLE_CTL
                                trow.addToColumnValue(new TCell().setStringVal(baseTableQualifiers.get(0)));
                                // BASE_TABLE_TYPE
                                trow.addToColumnValue(new TCell().setStringVal(baseTable.getType().name()));
                            }
                            // ENABLED
                            trow.addToColumnValue(new TCell().setBoolVal(!stream.isDisabled()));
                            // IS_STALE
                            trow.addToColumnValue(new TCell().setBoolVal(stream.isStale()));
                            // STALE_REASON
                            trow.addToColumnValue(new TCell().setStringVal(stream.getStaleReason()));
                            dataBatch.add(trow);
                        } finally {
                            stream.readUnlock();
                        }
                    }
                }
            }
        }
    }

    public void fillStreamConsumptionValuesMetadataResult(List<TRow> dataBatch) {
        for (Map.Entry<Long, Set<Long>> entry : copyDbStreamMap().entrySet()) {
            Optional<Database> db = Env.getCurrentInternalCatalog().getDb(entry.getKey());
            if (db.isPresent()) {
                for (Long tableId : entry.getValue()) {
                    Optional<Table> table = db.get().getTable(tableId);
                    if (!table.isPresent()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.warn("invalid stream id: {}, db: {}", tableId, db.get().getFullName());
                        }
                        continue;
                    }
                    Preconditions.checkArgument(table.get() instanceof BaseTableStream);
                    BaseTableStream stream = (BaseTableStream) table.get();
                    if (stream.readLockIfExist()) {
                        try {
                            stream.fillTableStreamConsumptionInfo(dataBatch);
                        } finally {
                            stream.readUnlock();
                        }
                    }
                }
            }
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        this.rwLock = new MonitoredReentrantReadWriteLock(true);
        this.intervalMs = Config.table_stream_partition_offset_cleanup_interval_second * 1000L;
    }
}
