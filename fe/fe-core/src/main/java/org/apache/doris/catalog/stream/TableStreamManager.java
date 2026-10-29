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
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.CloudTableStreamReadStateHelper;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.persist.EditLog.EditLogItem;
import org.apache.doris.persist.TableStreamCleanupInfo;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.tso.TSOService;
import org.apache.doris.tso.TSOTimestamp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class TableStreamManager extends MasterDaemon implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(TableStreamManager.class);
    private static final String BASE_TABLE_NOT_FOUND_STALE_REASON = "Base table does not exist";
    // The most partitions one MetaService read of a keep-alive round asks for; one INSERT may consume as many
    // (cloud_table_stream_max_partitions_per_insert).
    private static final int CLOUD_KEEP_ALIVE_BATCH_PARTITIONS = 10000;

    @FunctionalInterface
    public interface StreamConsumptionSelector {
        // unit is null while selecting a stream, and is set after its base-table partitions are known.
        boolean test(String dbName, String streamName, long streamId, String unit);

        // Callers override this to skip partition snapshots when no UNIT predicate exists.
        default boolean hasUnitFilter() {
            return true;
        }
    }

    @SerializedName(value = "dbStreamMap")
    private Map<Long, Set<Long>> dbStreamMap;
    protected MonitoredReentrantReadWriteLock rwLock;

    public TableStreamManager() {
        super("table-stream-manager", Config.table_stream_partition_offset_cleanup_interval_second * 1000L);
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

    public void removeTableStream(BaseTableStream stream) {
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

    public List<Cloud.TableStreamIdentityPB> getCloudTableStreamsForBaseTable(
            long baseDbId, long baseTableId) {
        List<Cloud.TableStreamIdentityPB> identities = new ArrayList<>();
        for (Map.Entry<Long, Set<Long>> entry : copyDbStreamMap().entrySet()) {
            Optional<Database> streamDb = Env.getCurrentInternalCatalog().getDb(entry.getKey());
            if (!streamDb.isPresent()) {
                continue;
            }
            for (Long streamId : entry.getValue()) {
                Optional<Table> table = streamDb.get().getTable(streamId);
                if (!table.isPresent() || !(table.get() instanceof OlapTableStream)) {
                    continue;
                }
                OlapTableStream stream = (OlapTableStream) table.get();
                if (stream.getBaseTableInfo().getDbId() == baseDbId
                        && stream.getBaseTableInfo().getTableId() == baseTableId) {
                    identities.add(Cloud.TableStreamIdentityPB.newBuilder()
                            .setBaseDbId(baseDbId)
                            .setBaseTableId(baseTableId)
                            .setStreamDbId(entry.getKey())
                            .setStreamId(streamId)
                            .build());
                }
            }
        }
        identities.sort((left, right) -> Long.compare(left.getStreamId(), right.getStreamId()));
        return identities;
    }

    public void cleanupStalePartitionOffsets() {
        List<Long> staleDbIds = new ArrayList<>();
        List<Pair<Long, Long>> staleStreamIds = new ArrayList<>();
        List<EditLogItem> editLogItems = new ArrayList<>();
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
                cleanupStalePartitionOffsets((OlapTableStream) table.get()).ifPresent(editLogItems::add);
            }
        }
        removeStaleDbAndStream(staleDbIds, staleStreamIds);
        if (!staleDbIds.isEmpty() || !staleStreamIds.isEmpty()) {
            editLogItems.add(Env.getCurrentEnv().getEditLog().logTableStreamCleanup(
                    new TableStreamCleanupInfo(Collections.emptyList(), staleDbIds, staleStreamIds)));
        }
        editLogItems.forEach(EditLogItem::await);
    }

    private Optional<EditLogItem> cleanupStalePartitionOffsets(OlapTableStream stream) {
        if (!stream.tryReadLock(Table.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("skip cleaning stream {} because stream read lock is busy", stream.getName());
            }
            return Optional.empty();
        }
        // stream read lock is held
        OlapTable baseTable;
        try {
            if (stream.isDisabled() || stream.isStale()) {
                return Optional.empty();
            }
            baseTable = stream.getBaseTableNullable();
            if (baseTable == null) {
                return Optional.empty();
            }
        } finally {
            stream.readUnlock();
        }
        // stream read lock is released
        if (!baseTable.tryReadLock(Table.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("skip cleaning stream {} because base table {} read lock is busy",
                        stream.getName(), baseTable.getName());
            }
            return Optional.empty();
        }
        Set<Long> stalePartitionIds;
        EditLogItem editLogItem;
        try {
            if (baseTable.isDropped) {
                return Optional.empty();
            }
            Set<Long> validPartitionIds = new HashSet<>(baseTable.getPartitionIds());
            while (DebugPointUtil.getDebugParamOrDefault(
                    "TableStreamManager.cleanupStalePartitionOffsets.blockAfterPartitionSnapshot", -1L)
                    == stream.getId()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            }
            if (!stream.tryWriteLockIfExist(Table.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("skip cleaning stream {} because it is busy or dropped", stream.getName());
                }
                return Optional.empty();
            }
            try {
                if (stream.isDisabled() || stream.isStale()) {
                    return Optional.empty();
                }
                stalePartitionIds = stream.unprotectedCollectStalePartitionOffsetIds(validPartitionIds);
                if (stalePartitionIds.isEmpty()) {
                    return Optional.empty();
                }
                stream.unprotectedPrunePartitionOffsets(stalePartitionIds);
                TableStreamCleanupInfo.PartitionOffsetPruneEntry pruneEntry =
                        new TableStreamCleanupInfo.PartitionOffsetPruneEntry(
                                stream.getDatabase().getId(), stream.getId(), stalePartitionIds);
                editLogItem = Env.getCurrentEnv().getEditLog().logTableStreamCleanup(
                        new TableStreamCleanupInfo(Collections.singletonList(pruneEntry)));
            } finally {
                stream.writeUnlock();
            }
        } finally {
            baseTable.readUnlock();
        }
        LOG.info("cleaned {} stale partition offset entries from stream {}.{} ({})",
                stalePartitionIds.size(), stream.getDatabase().getFullName(), stream.getName(), stream.getId());
        return Optional.of(editLogItem);
    }

    /**
     * Records a keep-alive TSO for the partitions of an IVM stream whose offsets stay where the partitions were
     * last consumed while the partitions are not written, so that a read of such a partition does not take its
     * offset for expired once the partition is written again (F-2). A partition qualifies when the base table
     * applies changelog.ttl_seconds, the stream has a real offset for it past the initial snapshot, nothing is
     * committed to the partition after that offset, and the newer of the offset and its keep-alive TSO is older
     * than half the TTL.
     *
     * <p>The keep-alive TSO is taken from the TSO service while the base table read lock is held. A commit takes
     * its TSO inside the base table write lock, so every commit the partition has not had yet gets a larger one,
     * and the partition holds no change in (offset, keep-alive TSO]. Locks are taken in the order
     * {@link #cleanupStalePartitionOffsets(OlapTableStream)} takes them.
     */
    public Optional<EditLogItem> keepAliveIdlePartitions(OlapTableStream stream) {
        if (!stream.tryReadLock(Table.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("skip keeping stream {} alive because stream read lock is busy", stream.getName());
            }
            return Optional.empty();
        }
        // stream read lock is held
        OlapTable baseTable;
        Map<Long, Long> offsets;
        Map<Long, Long> effectiveOffsets = new HashMap<>();
        try {
            if (stream.isDisabled() || stream.isStale()) {
                return Optional.empty();
            }
            baseTable = stream.getBaseTableNullable();
            if (baseTable == null) {
                return Optional.empty();
            }
            offsets = stream.getConsumedPartitionOffsets();
            offsets.forEach((partitionId, offset) -> effectiveOffsets.put(partitionId,
                    OlapTableStream.effectiveOffset(offset, stream.getKeepAlive(partitionId))));
        } finally {
            stream.readUnlock();
        }
        // stream read lock is released
        if (offsets.isEmpty()) {
            return Optional.empty();
        }
        while (DebugPointUtil.getDebugParamOrDefault(
                "TableStreamManager.keepAliveIdlePartitions.blockBeforeBaseTableLock", -1L) == stream.getId()) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        if (!baseTable.tryReadLock(Table.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("skip keeping stream {} alive because base table {} read lock is busy",
                        stream.getName(), baseTable.getName());
            }
            return Optional.empty();
        }
        Map<Long, Long> keptAlive = new HashMap<>();
        long keepAliveTso;
        EditLogItem editLogItem;
        try {
            if (baseTable.isDropped || !baseTable.hasChangelogTtl()) {
                return Optional.empty();
            }
            long renewCutoffTso = keepAliveRenewCutoffTso(baseTable.getChangelogConfig().getTtlSeconds());
            Set<Long> idle = new HashSet<>();
            for (Map.Entry<Long, Long> entry : offsets.entrySet()) {
                Partition partition = baseTable.getPartition(entry.getKey());
                // A commit after the offset shows in the partition TSO once it is visible, and in the next
                // version from the moment it is committed.
                if (partition != null && effectiveOffsets.get(entry.getKey()) <= renewCutoffTso
                        && partition.getTso() <= entry.getValue()
                        && partition.getNextVersion() == partition.getVisibleVersion() + 1) {
                    idle.add(entry.getKey());
                }
            }
            if (idle.isEmpty()) {
                return Optional.empty();
            }
            keepAliveTso = Env.getCurrentEnv().getTSOService().getTSO();
            if (!stream.tryWriteLockIfExist(Table.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("skip keeping stream {} alive because it is busy or dropped", stream.getName());
                }
                return Optional.empty();
            }
            try {
                if (stream.isDisabled() || stream.isStale()) {
                    return Optional.empty();
                }
                for (Long partitionId : idle) {
                    // A consumption between the two stream locks moved the offset, and the partition waits for
                    // the next round.
                    if (offsets.get(partitionId).equals(stream.getPartitionOffset(partitionId))) {
                        keptAlive.put(partitionId, offsets.get(partitionId));
                    }
                }
                if (keptAlive.isEmpty()) {
                    return Optional.empty();
                }
                stream.unprotectedKeepAlive(keptAlive, keepAliveTso);
                editLogItem = Env.getCurrentEnv().getEditLog().logTableStreamCleanup(
                        TableStreamCleanupInfo.ofKeepAlive(new TableStreamCleanupInfo.PartitionKeepAliveEntry(
                                stream.getDatabase().getId(), stream.getId(), keptAlive, keepAliveTso)));
            } finally {
                stream.writeUnlock();
            }
        } finally {
            baseTable.readUnlock();
        }
        LOG.info("kept {} idle partition offsets of stream {}.{} ({}) alive up to tso {}",
                keptAlive.size(), stream.getDatabase().getFullName(), stream.getName(), stream.getId(),
                keepAliveTso);
        return Optional.of(editLogItem);
    }

    // Renewed once less than half the TTL is left: the newer of the offset and the keep-alive TSO would have
    // expired under half the TTL. A TTL longer than the clock has run never expires anything, and this cutoff is
    // 0 then.
    private static long keepAliveRenewCutoffTso(long ttlSeconds) {
        return TSOTimestamp.calculateCutoff(TSOTimestamp.composePhysicalTimestamp(System.currentTimeMillis()),
                ttlSeconds / 2);
    }

    /**
     * The cloud mode counterpart of {@link #keepAliveIdlePartitions(OlapTableStream)}, for the IVM streams of one
     * round, whose offsets live in MetaService and are read from it in batches.
     *
     * <p>The keep-alive TSO is the committed TSO watermark, taken before any of the offsets is read. Every commit
     * at or below the watermark has finished by then, so a read made afterwards shows all of them, and a commit
     * that lands later gets a larger TSO. A partition whose offset is at its end in that read thus holds no change
     * in (offset, watermark], for good. A partition qualifies on what shared-nothing mode checks, as MetaService
     * reports it -- a real offset past the initial snapshot, at the end of the partition, whose newer of it and its
     * keep-alive TSO is older than half the TTL -- when the watermark is past that newer one. The keep-alive TSO is
     * recorded in FE metadata with the offset it was taken against, as in shared-nothing mode, and counts only for
     * a read from that offset: an offset MetaService no longer reports, after a consumption or a restore, leaves it
     * out.
     */
    public List<EditLogItem> keepAliveIdleCloudPartitions(List<OlapTableStream> streams) {
        return keepAliveIdleCloudPartitions(streams, CLOUD_KEEP_ALIVE_BATCH_PARTITIONS);
    }

    @VisibleForTesting
    List<EditLogItem> keepAliveIdleCloudPartitions(List<OlapTableStream> streams, int batchPartitions) {
        List<CloudKeepAliveSnapshot> snapshots = new ArrayList<>();
        for (OlapTableStream stream : streams) {
            snapshotForCloudKeepAlive(stream).ifPresent(snapshots::add);
        }
        if (snapshots.isEmpty()) {
            return Collections.emptyList();
        }
        TSOService.TSOStatusSnapshot tsoStatus = Env.getCurrentEnv().getTSOService().getStatusSnapshot();
        if (!tsoStatus.isInitialized() || tsoStatus.getCommittedTso() <= 0) {
            LOG.info("skip keeping IVM stream offsets alive because the committed TSO is not available");
            return Collections.emptyList();
        }
        long keepAliveTso = tsoStatus.getCommittedTso();
        List<EditLogItem> editLogItems = new ArrayList<>();
        // One MetaService read covers the offsets and versions it is asked for in one transaction, so a round is
        // read in batches of about as many partitions as one INSERT may consume.
        List<CloudKeepAliveSnapshot> batch = new ArrayList<>();
        int partitionsInBatch = 0;
        for (CloudKeepAliveSnapshot snapshot : snapshots) {
            if (!batch.isEmpty() && partitionsInBatch + snapshot.partitionIds.size() > batchPartitions) {
                keepAliveCloudBatch(batch, keepAliveTso, editLogItems);
                batch = new ArrayList<>();
                partitionsInBatch = 0;
            }
            batch.add(snapshot);
            partitionsInBatch += snapshot.partitionIds.size();
        }
        if (!batch.isEmpty()) {
            keepAliveCloudBatch(batch, keepAliveTso, editLogItems);
        }
        return editLogItems;
    }

    private Optional<CloudKeepAliveSnapshot> snapshotForCloudKeepAlive(OlapTableStream stream) {
        if (!stream.tryReadLock(Table.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("skip keeping stream {} alive because stream read lock is busy", stream.getName());
            }
            return Optional.empty();
        }
        // stream read lock is held
        OlapTable baseTable;
        Map<Long, OlapTableStream.PartitionKeepAlive> keepAlives;
        try {
            if (stream.isDisabled() || stream.isStale()) {
                return Optional.empty();
            }
            baseTable = stream.getBaseTableNullable();
            if (baseTable == null) {
                return Optional.empty();
            }
            keepAlives = stream.getKeepAlives();
        } finally {
            stream.readUnlock();
        }
        // stream read lock is released
        if (!baseTable.tryReadLock(Table.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("skip keeping stream {} alive because base table {} read lock is busy",
                        stream.getName(), baseTable.getName());
            }
            return Optional.empty();
        }
        try {
            if (baseTable.isDropped || !baseTable.hasChangelogTtl() || baseTable.getPartitionIds().isEmpty()) {
                return Optional.empty();
            }
            Cloud.TableStreamIdentityPB identity = Cloud.TableStreamIdentityPB.newBuilder()
                    .setBaseDbId(stream.getBaseTableInfo().getDbId())
                    .setBaseTableId(stream.getBaseTableInfo().getTableId())
                    .setStreamDbId(stream.getDatabase().getId())
                    .setStreamId(stream.getId())
                    .build();
            return Optional.of(new CloudKeepAliveSnapshot(stream, identity,
                    new LinkedHashSet<>(baseTable.getPartitionIds()), baseTable.getChangelogConfig().getTtlSeconds(),
                    keepAlives));
        } finally {
            baseTable.readUnlock();
        }
    }

    private void keepAliveCloudBatch(List<CloudKeepAliveSnapshot> batch, long keepAliveTso,
            List<EditLogItem> editLogItems) {
        while (batch.stream().anyMatch(snapshot -> snapshot.stream.getId() == DebugPointUtil.getDebugParamOrDefault(
                "TableStreamManager.keepAliveIdleCloudPartitions.blockBeforeReadStates", -1L))) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        Map<Cloud.TableStreamIdentityPB, Set<Long>> requestedPartitions = new LinkedHashMap<>();
        batch.forEach(snapshot -> requestedPartitions.put(snapshot.identity, snapshot.partitionIds));
        Map<Cloud.TableStreamIdentityPB, Map<Long, Cloud.TableStreamPartitionReadStatePB>> readStates;
        try {
            readStates = CloudTableStreamReadStateHelper.getReadStates(requestedPartitions);
        } catch (UserException e) {
            LOG.warn("skip keeping {} IVM streams alive because their Cloud read states are unavailable",
                    batch.size(), e);
            return;
        }
        for (CloudKeepAliveSnapshot snapshot : batch) {
            keepAliveCloudPartitions(snapshot, readStates.get(snapshot.identity), keepAliveTso)
                    .ifPresent(editLogItems::add);
        }
    }

    private Optional<EditLogItem> keepAliveCloudPartitions(CloudKeepAliveSnapshot snapshot,
            Map<Long, Cloud.TableStreamPartitionReadStatePB> readStates, long keepAliveTso) {
        // As of now rather than of the snapshot: the read may have waited.
        long renewCutoffTso = keepAliveRenewCutoffTso(snapshot.ttlSeconds);
        Map<Long, Long> idle = new HashMap<>();
        readStates.forEach((partitionId, state) -> {
            Preconditions.checkState(state.hasOffsetState() && state.hasEndTso(),
                    "Incomplete Cloud Table Stream read state for partition %s", partitionId);
            // The sentinel offset of a partition that was empty when the stream was created is not a real one.
            if (state.getOffsetState() != Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED
                    || !state.hasOffsetTso() || state.getOffsetTso() <= 0
                    || state.getOffsetTso() != state.getEndTso()) {
                return;
            }
            long effectiveOffset = OlapTableStream.effectiveOffset(state.getOffsetTso(),
                    snapshot.keepAlives.get(partitionId));
            if (effectiveOffset <= renewCutoffTso && effectiveOffset < keepAliveTso) {
                idle.put(partitionId, state.getOffsetTso());
            }
        });
        if (idle.isEmpty()) {
            return Optional.empty();
        }
        OlapTableStream stream = snapshot.stream;
        if (!stream.tryWriteLockIfExist(Table.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("skip keeping stream {} alive because it is busy or dropped", stream.getName());
            }
            return Optional.empty();
        }
        EditLogItem editLogItem;
        try {
            if (stream.isDisabled() || stream.isStale()) {
                return Optional.empty();
            }
            stream.unprotectedKeepAlive(idle, keepAliveTso);
            editLogItem = Env.getCurrentEnv().getEditLog().logTableStreamCleanup(
                    TableStreamCleanupInfo.ofKeepAlive(new TableStreamCleanupInfo.PartitionKeepAliveEntry(
                            stream.getDatabase().getId(), stream.getId(), idle, keepAliveTso)));
        } finally {
            stream.writeUnlock();
        }
        LOG.info("kept {} idle partition offsets of stream {}.{} ({}) alive up to tso {}",
                idle.size(), stream.getDatabase().getFullName(), stream.getName(), stream.getId(), keepAliveTso);
        return Optional.of(editLogItem);
    }

    private static class CloudKeepAliveSnapshot {
        private final OlapTableStream stream;
        private final Cloud.TableStreamIdentityPB identity;
        private final Set<Long> partitionIds;
        private final long ttlSeconds;
        private final Map<Long, OlapTableStream.PartitionKeepAlive> keepAlives;

        private CloudKeepAliveSnapshot(OlapTableStream stream, Cloud.TableStreamIdentityPB identity,
                Set<Long> partitionIds, long ttlSeconds, Map<Long, OlapTableStream.PartitionKeepAlive> keepAlives) {
            this.stream = stream;
            this.identity = identity;
            this.partitionIds = partitionIds;
            this.ttlSeconds = ttlSeconds;
            this.keepAlives = keepAlives;
        }
    }

    public void replayTableStreamCleanup(TableStreamCleanupInfo info) {
        if (info == null) {
            return;
        }
        for (TableStreamCleanupInfo.PartitionOffsetPruneEntry entry : info.getEntries()) {
            replayPrunePartitionOffset(entry);
        }
        for (TableStreamCleanupInfo.PartitionKeepAliveEntry entry : info.getKeepAliveEntries()) {
            replayKeepAlive(entry);
        }
        removeStaleDbAndStream(info.getStaleDbIds(), info.getStaleStreamIds());
    }

    private void replayKeepAlive(TableStreamCleanupInfo.PartitionKeepAliveEntry entry) {
        Optional<Database> db = Env.getCurrentInternalCatalog().getDb(entry.getDbId());
        if (!db.isPresent()) {
            LOG.info("skip replay keeping partition offsets alive because db {} does not exist", entry.getDbId());
            return;
        }
        Optional<Table> table = db.get().getTable(entry.getStreamId());
        if (!table.isPresent() || !(table.get() instanceof OlapTableStream)) {
            LOG.info("skip replay keeping partition offsets alive because stream {}.{} does not exist",
                    entry.getDbId(), entry.getStreamId());
            return;
        }
        OlapTableStream stream = (OlapTableStream) table.get();
        if (!stream.writeLockIfExist()) {
            LOG.warn("stream {}.{} write lock acquire failed", db.get().getFullName(), stream.getName());
            return;
        }
        try {
            stream.unprotectedKeepAlive(entry.getPartitionOffsets(), entry.getKeepAliveTso());
        } finally {
            stream.writeUnlock();
        }
    }

    private void replayPrunePartitionOffset(TableStreamCleanupInfo.PartitionOffsetPruneEntry entry) {
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
        if (!stream.writeLockIfExist()) {
            LOG.warn("stream {}.{} write lock acquire failed", db.get().getFullName(), stream.getName());
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
                            trow.addToColumnValue(new TCell().setStringVal(stream.getScanTypeString()));
                            // STREAM_COMMENT
                            trow.addToColumnValue(new TCell().setStringVal(stream.getComment()));
                            TableIf baseTable = stream.getBaseTableNullable();
                            boolean baseTableExists = baseTable != null;
                            if (!baseTableExists) {
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
                            trow.addToColumnValue(
                                    new TCell().setBoolVal(baseTableExists && !stream.isDisabled()));
                            // IS_STALE
                            trow.addToColumnValue(new TCell().setBoolVal(!baseTableExists || stream.isStale()));
                            // STALE_REASON
                            trow.addToColumnValue(new TCell().setStringVal(baseTableExists
                                    ? stream.getStaleReason() : BASE_TABLE_NOT_FOUND_STALE_REASON));
                            dataBatch.add(trow);
                        } finally {
                            stream.readUnlock();
                        }
                    }
                }
            }
        }
    }

    public void fillStreamConsumptionValuesMetadataResult(List<TRow> dataBatch) throws UserException {
        if (Config.isCloudMode()) {
            fillCloudStreamConsumptionValuesMetadataResult(dataBatch, null);
            return;
        }
        fillLocalStreamConsumptionValuesMetadataResult(dataBatch, null);
    }

    public void fillStreamConsumptionValuesMetadataResult(List<TRow> dataBatch,
            StreamConsumptionSelector selector) throws UserException {
        if (Config.isCloudMode()) {
            fillCloudStreamConsumptionValuesMetadataResult(dataBatch, selector);
            return;
        }
        fillLocalStreamConsumptionValuesMetadataResult(dataBatch, selector);
    }

    private void fillLocalStreamConsumptionValuesMetadataResult(List<TRow> dataBatch,
            StreamConsumptionSelector selector) {
        // Resolve registered streams and apply stream-level predicates before taking metadata locks.
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
                    Preconditions.checkArgument(table.get() instanceof OlapTableStream);
                    OlapTableStream stream = (OlapTableStream) table.get();
                    String dbName = db.get().getFullName();
                    String streamName = stream.getName();
                    long streamId = stream.getId();
                    if (selector != null && !selector.test(dbName, streamName, streamId, null)) {
                        continue;
                    }
                    List<OlapTableStream.StreamConsumptionUnitSnapshot> snapshots = Collections.emptyList();
                    if (stream.readLockIfExist()) {
                        try {
                            // Build rows directly unless UNIT evaluation requires a lock-protected metadata snapshot.
                            if (selector == null || !selector.hasUnitFilter()) {
                                stream.fillTableStreamConsumptionInfo(dataBatch);
                            } else {
                                // UNIT evaluation rewrites and folds expressions, so defer it until after unlocking.
                                snapshots = stream.snapshotTableStreamConsumptionInfo();
                            }
                        } finally {
                            stream.readUnlock();
                        }
                    }
                    // Both metadata locks are released here; materialize only units accepted by the selector.
                    for (OlapTableStream.StreamConsumptionUnitSnapshot snapshot : snapshots) {
                        if (selector.test(dbName, streamName, streamId, snapshot.getUnit())) {
                            snapshot.appendRow(dataBatch, dbName, streamName, streamId);
                        }
                    }
                }
            }
        }
    }

    private void fillCloudStreamConsumptionValuesMetadataResult(List<TRow> dataBatch,
            StreamConsumptionSelector selector) throws UserException {
        Map<Cloud.TableStreamIdentityPB, CloudStreamConsumptionSnapshot> snapshots = new LinkedHashMap<>();
        // Resolve registered streams and apply stream-level predicates before taking metadata locks.
        for (Map.Entry<Long, Set<Long>> entry : copyDbStreamMap().entrySet()) {
            Optional<Database> db = Env.getCurrentInternalCatalog().getDb(entry.getKey());
            if (!db.isPresent()) {
                continue;
            }
            for (Long tableId : entry.getValue()) {
                Optional<Table> table = db.get().getTable(tableId);
                if (!table.isPresent()) {
                    continue;
                }
                Preconditions.checkArgument(table.get() instanceof OlapTableStream);
                OlapTableStream stream = (OlapTableStream) table.get();
                if (selector != null
                        && !selector.test(db.get().getFullName(), stream.getName(), stream.getId(), null)) {
                    continue;
                }
                if (!stream.readLockIfExist()) {
                    continue;
                }
                Cloud.TableStreamIdentityPB identity;
                CloudStreamConsumptionSnapshot snapshot;
                try {
                    // Snapshot partition identities under lock; authoritative consumption state lives in MetaService.
                    OlapTable baseTable = stream.getBaseTableNullable();
                    if (baseTable == null || !baseTable.readLockIfExist()) {
                        continue;
                    }
                    try {
                        Map<Long, String> partitionNames = new LinkedHashMap<>();
                        baseTable.getPartitions().stream()
                                .forEach(partition -> partitionNames.put(partition.getId(), partition.getName()));
                        if (partitionNames.isEmpty()) {
                            continue;
                        }
                        identity = Cloud.TableStreamIdentityPB.newBuilder()
                                .setBaseDbId(stream.getBaseTableInfo().getDbId())
                                .setBaseTableId(stream.getBaseTableInfo().getTableId())
                                .setStreamDbId(entry.getKey())
                                .setStreamId(stream.getId())
                                .build();
                        snapshot = new CloudStreamConsumptionSnapshot(db.get().getFullName(), stream.getName(),
                                stream.getId(), partitionNames);
                    } finally {
                        baseTable.readUnlock();
                    }
                } finally {
                    stream.readUnlock();
                }
                // Evaluate UNIT expressions after unlocking, then retain only selected units in the global map.
                if (selector != null && selector.hasUnitFilter()) {
                    snapshot = snapshot.selectUnits(selector);
                    if (snapshot == null) {
                        continue;
                    }
                }
                CloudStreamConsumptionSnapshot previous = snapshots.put(identity, snapshot);
                Preconditions.checkState(previous == null,
                        "Duplicate Cloud Table Stream identity %s", identity);
            }
        }
        if (snapshots.isEmpty()) {
            return;
        }

        // Fetch authoritative states for selected units in one batch, then materialize the result rows.
        Map<Cloud.TableStreamIdentityPB, Set<Long>> requestedPartitions = new LinkedHashMap<>();
        snapshots.forEach((identity, snapshot) ->
                requestedPartitions.put(identity, snapshot.partitionNames.keySet()));
        Map<Cloud.TableStreamIdentityPB, Map<Long, Cloud.TableStreamPartitionReadStatePB>> readStates =
                CloudTableStreamReadStateHelper.getReadStates(requestedPartitions);
        for (Map.Entry<Cloud.TableStreamIdentityPB, CloudStreamConsumptionSnapshot> entry
                : snapshots.entrySet()) {
            entry.getValue().fillRows(readStates.get(entry.getKey()), dataBatch);
        }
    }

    private static class CloudStreamConsumptionSnapshot {
        private final String dbName;
        private final String streamName;
        private final long streamId;
        private final Map<Long, String> partitionNames;

        private CloudStreamConsumptionSnapshot(String dbName, String streamName, long streamId,
                Map<Long, String> partitionNames) {
            this.dbName = dbName;
            this.streamName = streamName;
            this.streamId = streamId;
            this.partitionNames = Collections.unmodifiableMap(partitionNames);
        }

        private CloudStreamConsumptionSnapshot selectUnits(StreamConsumptionSelector selector) {
            Map<Long, String> selectedPartitions = new LinkedHashMap<>();
            for (Map.Entry<Long, String> entry : partitionNames.entrySet()) {
                if (selector.test(dbName, streamName, streamId, entry.getValue())) {
                    selectedPartitions.put(entry.getKey(), entry.getValue());
                }
            }
            return selectedPartitions.isEmpty()
                    ? null
                    : new CloudStreamConsumptionSnapshot(dbName, streamName, streamId, selectedPartitions);
        }

        private void fillRows(Map<Long, Cloud.TableStreamPartitionReadStatePB> partitionStates,
                List<TRow> dataBatch) throws UserException {
            for (Map.Entry<Long, String> entry : partitionNames.entrySet()) {
                Cloud.TableStreamPartitionReadStatePB state = partitionStates.get(entry.getKey());
                if (!state.hasOffsetState() || !state.hasEndTso() || !state.hasVisibleVersion()) {
                    throw new UserException("MetaService returned incomplete Cloud Table Stream partition state");
                }
                TRow row = new TRow();
                row.addToColumnValue(new TCell().setStringVal(dbName));
                row.addToColumnValue(new TCell().setStringVal(streamName));
                row.addToColumnValue(new TCell().setLongVal(streamId));
                row.addToColumnValue(new TCell().setStringVal(entry.getValue()));
                if (state.getOffsetState()
                        == Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_UNKNOWN) {
                    row.addToColumnValue(new TCell().setStringVal("N/A"));
                    row.addToColumnValue(new TCell().setStringVal(
                            state.getVisibleVersion() > Partition.PARTITION_INIT_VERSION
                                    ? "N/A" : "0"));
                    row.addToColumnValue(new TCell().setLongVal(-1));
                } else {
                    if (!state.hasOffsetTso()) {
                        throw new UserException("MetaService returned a Cloud Table Stream state without Offset TSO");
                    }
                    row.addToColumnValue(new TCell().setStringVal(String.valueOf(state.getOffsetTso())));
                    row.addToColumnValue(new TCell().setStringVal(
                            String.valueOf(state.getEndTso() - state.getOffsetTso())));
                    row.addToColumnValue(new TCell().setLongVal(
                            state.hasLastConsumptionTimeMs() ? state.getLastConsumptionTimeMs() : -1));
                }
                dataBatch.add(row);
            }
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        this.rwLock = new MonitoredReentrantReadWriteLock(true);
        this.intervalMs = Config.table_stream_partition_offset_cleanup_interval_second * 1000L;
    }
}
