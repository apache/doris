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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Util;
import org.apache.doris.statistics.analysis.TableStatsMeta;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.tso.TSOTimestamp;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

// runtime-only class for unified query/insert experience, created when bind relation with OlapTableStream
public class OlapTableStreamWrapper extends OlapTable {
    private final OlapTableStream stream;
    private final OlapTable baseTable;
    protected final Map<Long, Pair<Long, Long>> outputUpdateMap;
    // The row counts the stream had recorded when the offsets above were read. The two have to
    // describe the same moment: an offset advance between them would pair the range of the old
    // offset with the counters of the new one, and the estimate of the range would read as empty.
    private Map<Long, PartitionStreamState> partitionStreamStates;
    private final KeysType keysType;
    private Map<Long, Cloud.TableStreamPartitionReadStatePB> cloudReadStates = ImmutableMap.of();
    private boolean cloudReadStatesInstalled;

    public OlapTableStreamWrapper(OlapTableStream stream, OlapTable baseTable, List<Long> selectedPartitionIds) {
        super(stream.getId(), stream.getName(), stream.getFullSchema(), baseTable.getKeysType(),
                baseTable.getPartitionInfo(), baseTable.getDefaultDistributionInfo());
        // Inherit base table's qualifiedDbName so that wrapper.getDatabase() can resolve the
        // owning Database via Env.getCurrentInternalCatalog().getDbNullable(qualifiedDbName).
        // Otherwise downstream consumers (e.g. QueryPartitionCollector, partition routing,
        // MV partition compensation) treat the wrapper as having no database and silently
        // fall back to empty results when scanning the stream.
        setQualifiedDbName(baseTable.getQualifiedDbName());
        this.stream = stream;
        this.baseTable = baseTable;
        this.keysType = baseTable.getKeysType();
        stream.readLock();
        try {
            this.outputUpdateMap = buildOutputUpdateMap(selectedPartitionIds);
            this.partitionStreamStates = snapshotPartitionStreamStates(selectedPartitionIds);
        } finally {
            stream.readUnlock();
        }
        this.getOrCreatTableProperty().setEnableUniqueKeyMergeOnWrite(baseTable.getEnableUniqueKeyMergeOnWrite());
    }

    /**
     * The row counts the stream recorded for the given partitions, read under the stream lock so
     * that they describe the same moment as the offsets of the same call.
     */
    private Map<Long, PartitionStreamState> snapshotPartitionStreamStates(Collection<Long> partitionIds) {
        ImmutableMap.Builder<Long, PartitionStreamState> states = ImmutableMap.builder();
        for (Long partitionId : partitionIds) {
            PartitionStreamState state = stream.getPartitionStreamState(partitionId);
            if (state != null) {
                states.put(partitionId, state);
            }
        }
        return states.build();
    }

    public Map<Long, Pair<Long, Long>> buildOutputUpdateMap(List<Long> selectedPartitionIds) {
        Map<Long, Pair<Long, Long>> outputUpdateMap = Maps.newHashMapWithExpectedSize(selectedPartitionIds.size());
        for (Long partitionId : selectedPartitionIds) {
            if (!baseTable.getPartition(partitionId).hasData()) {
                continue;
            }
            outputUpdateMap.put(partitionId, stream.getStreamUpdate(partitionId));
        }
        return outputUpdateMap;
    }

    public void installCloudReadStates(Map<Long, Cloud.TableStreamPartitionReadStatePB> readStates) {
        ImmutableMap<Long, Cloud.TableStreamPartitionReadStatePB> immutableReadStates =
                ImmutableMap.copyOf(readStates);
        if (cloudReadStatesInstalled) {
            Preconditions.checkState(cloudReadStates.equals(immutableReadStates),
                    "Cloud Table Stream read state changed within one statement");
            return;
        }
        for (Map.Entry<Long, Cloud.TableStreamPartitionReadStatePB> entry : immutableReadStates.entrySet()) {
            Cloud.TableStreamPartitionReadStatePB state = entry.getValue();
            Preconditions.checkArgument(state.hasPartitionId() && state.getPartitionId() == entry.getKey(),
                    "Cloud Table Stream partition state does not match partition %s", entry.getKey());
            Preconditions.checkArgument(state.hasOffsetState() && state.hasEndTso() && state.hasVisibleVersion(),
                    "Incomplete Cloud Table Stream read state for partition %s", entry.getKey());
            if (state.getOffsetState() == Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_UNKNOWN) {
                Preconditions.checkArgument(!state.hasOffsetTso(),
                        "UNKNOWN Cloud Table Stream state must not contain an offset TSO");
            } else {
                Preconditions.checkArgument(state.hasOffsetTso(),
                        "Cloud Table Stream state must contain an offset TSO");
                Preconditions.checkArgument(state.getOffsetTso() <= state.getEndTso(),
                        "Cloud Table Stream offset exceeds the statement end TSO");
            }
        }
        stream.readLock();
        try {
            cloudReadStates = immutableReadStates;
            cloudReadStatesInstalled = true;
            outputUpdateMap.clear();
            cloudReadStates.forEach((partitionId, state) -> {
                if (state.getVisibleVersion() > Partition.PARTITION_INIT_VERSION) {
                    Long startTso = state.hasOffsetTso() ? state.getOffsetTso() : null;
                    outputUpdateMap.put(partitionId, Pair.of(startTso, state.getEndTso()));
                }
            });
            partitionStreamStates = snapshotPartitionStreamStates(outputUpdateMap.keySet());
        } finally {
            stream.readUnlock();
        }
    }

    public boolean hasCloudReadStates() {
        return cloudReadStatesInstalled;
    }

    public Map<Long, Cloud.TableStreamPartitionReadStatePB> getCloudReadStates() {
        return cloudReadStates;
    }

    public Map<Long, Long> getCloudVisibleVersions(Collection<Long> partitionIds) {
        Preconditions.checkState(hasCloudReadStates(), "Cloud Table Stream read state is not installed");
        return partitionIds.stream().collect(Collectors.toMap(id -> id,
                id -> {
                    Cloud.TableStreamPartitionReadStatePB state = cloudReadStates.get(id);
                    Preconditions.checkNotNull(state,
                            "Cloud Table Stream read state is missing for partition %s", id);
                    return state.getVisibleVersion();
                }));
    }

    @Override
    public List<Column> getBaseSchema(boolean full) {
        return baseTable.getBaseSchema(full);
    }

    @Override
    public List<Column> getBaseSchema() {
        return baseTable.getBaseSchema();
    }

    // for display
    public String getIndexNameById(long indexId) {
        // always returns base index name
        return baseTable.getName();
    }

    // for olap table to thrift
    @Override
    public void getColumnDesc(long selectedIndexId, List<TColumn> columnsDesc, List<String> keyColumnNames,
                              List<TPrimitiveType> keyColumnTypes) {
        baseTable.getColumnDesc(selectedIndexId, columnsDesc, keyColumnNames, keyColumnTypes);
    }

    @Override
    public int getIndexSchemaVersion(long indexId) {
        return baseTable.getIndexSchemaVersion(indexId);
    }

    // no need for pre agg on olap table stream
    @Override
    public boolean isDupKeysOrMergeOnWrite() {
        return false;
    }

    @Override
    public long getBaseIndexId() {
        return baseTable.getBaseIndexId();
    }

    @Override
    public MaterializedIndexMeta getIndexMetaByIndexId(long indexId) {
        return baseTable.getIndexMetaByIndexId(indexId);
    }

    @Override
    public List<Column> getSchemaByIndexId(Long indexId) {
        // here is base table indexId, we can ignore it and use olap table stream schema
        return getBaseSchema(Util.showHiddenColumns());
    }

    // override all partition methods, olap table stream inherit all partitions from base table
    @Override
    public Partition getPartition(String partitionName) {
        return baseTable.getPartition(partitionName);
    }

    @Override
    public Partition getPartition(long partitionId) {
        return baseTable.getPartition(partitionId);
    }

    @Override
    public Partition getPartition(String partitionName, boolean isTempPartition) {
        return baseTable.getPartition(partitionName, isTempPartition);
    }

    @Override
    public List<Long> getPartitionIds() {
        return baseTable.getPartitionIds();
    }

    public Map<Long, Pair<Long, Long>> getOutputUpdateMap() {
        return outputUpdateMap;
    }

    public Long getStreamDbId() {
        return stream.getDatabase().getId();
    }

    public Long getStreamId() {
        return stream.getId();
    }

    public long getBaseDbId() {
        return stream.getBaseTableInfo().getDbId();
    }

    public long getBaseTableId() {
        return stream.getBaseTableInfo().getTableId();
    }

    public Cloud.TableStreamIdentityPB getCloudIdentity() {
        return Cloud.TableStreamIdentityPB.newBuilder()
                .setBaseDbId(getBaseDbId())
                .setBaseTableId(getBaseTableId())
                .setStreamDbId(getStreamDbId())
                .setStreamId(getStreamId())
                .build();
    }

    @Override
    public boolean hasDeleteSign() {
        return getDeleteSignColumn() != null;
    }

    @Override
    public boolean getEnableUniqueKeyMergeOnWrite() {
        return baseTable.getEnableUniqueKeyMergeOnWrite();
    }

    @Override
    public boolean isMorTable() {
        return baseTable.isMorTable();
    }

    @Override
    public Collection<Partition> getPartitions() {
        return baseTable.getPartitions();
    }

    @Override
    public List<Long> selectNonEmptyPartitionIds(Collection<Long> partitionIds,
            Optional<StreamReadMode> streamReadMode) {
        StreamReadMode readMode = streamReadMode.orElse(StreamReadMode.INCREMENTAL);
        if (hasCloudReadStates()) {
            return partitionIds.stream()
                    .filter(id -> {
                        Cloud.TableStreamPartitionReadStatePB state = cloudReadStates.get(id);
                        return state != null && state.getVisibleVersion() > Partition.PARTITION_INIT_VERSION;
                    })
                    .collect(Collectors.toList());
        }
        if (readMode == StreamReadMode.SNAPSHOT || readMode == StreamReadMode.RESET) {
            return baseTable.selectNonEmptyPartitionIds(partitionIds, Optional.of(readMode));
        }
        List<Long> nonEmptyIds = Lists.newArrayListWithCapacity(partitionIds.size());
        for (Long partitionId : partitionIds) {
            if (stream.hasData(getPartition(partitionId))) {
                nonEmptyIds.add(partitionId);
            }
        }
        return nonEmptyIds;
    }

    public List<Long> filterHistoryPartitionIds(List<Long> partitionIds) {
        if (hasCloudReadStates()) {
            return partitionIds.stream()
                    .filter(id -> cloudReadStates.get(id).getOffsetState()
                            == Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING)
                    .collect(ImmutableList.toImmutableList());
        }
        return partitionIds.stream()
                .filter(partitionId -> stream.hasHistoricalData(partitionId))
                .collect(ImmutableList.toImmutableList());
    }

    public List<Long> filterIncrementalPartitionIds(List<Long> partitionIds) {
        if (hasCloudReadStates()) {
            return partitionIds.stream()
                    .filter(id -> {
                        Cloud.TableStreamPartitionReadStatePB state = cloudReadStates.get(id);
                        return state.getOffsetState()
                                != Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING
                                && (!state.hasOffsetTso() || state.getOffsetTso() < state.getEndTso());
                    })
                    .collect(ImmutableList.toImmutableList());
        }
        return partitionIds.stream()
                .filter(partitionId -> !stream.hasHistoricalData(partitionId)
                        && stream.hasData(getPartition(partitionId)))
                .collect(ImmutableList.toImmutableList());
    }

    public List<Long> filterConsumedPartitionIds(List<Long> partitionIds) {
        if (hasCloudReadStates()) {
            return partitionIds.stream()
                    .filter(id -> {
                        Cloud.TableStreamPartitionReadStatePB state = cloudReadStates.get(id);
                        // A partition empty at stream creation is recorded as CONSUMED with the
                        // sentinel offset -1 (CloudInternalCatalog: emptyPartition -> commit_tso=-1).
                        // Such a partition has no real consumption baseline; exclude it so snapshot
                        // rebuild does not fall back to the live TSO and leak post-snapshot rows.
                        // Mirrors the non-cloud hasConsumedData() offset > 0 guard.
                        return state.getOffsetState()
                                == Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED
                                && state.hasOffsetTso() && state.getOffsetTso() > 0;
                    })
                    .collect(ImmutableList.toImmutableList());
        }
        return partitionIds.stream()
                .filter(partitionId -> stream.hasConsumedData(partitionId))
                .collect(ImmutableList.toImmutableList());
    }

    public OlapTable getBaseTable() {
        return baseTable;
    }

    public KeysType getStreamKeysType() {
        return keysType;
    }

    public BaseTableStream.StreamScanType getStreamScanType() {
        if (keysType == KeysType.DUP_KEYS) {
            return BaseTableStream.StreamScanType.APPEND_ONLY;
        }
        return stream.getStreamScanType();
    }

    public Map<Long, Pair<Long, Long>> getPartitionOffsets(List<Long> selectedPartitionIds) {
        return outputUpdateMap.entrySet().stream()
                .filter(s -> selectedPartitionIds.contains(s.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, s -> {
                    // Storage keeps the real committed TSO points (closed-interval semantics).
                    // BE scans a left-closed right-open range [startTso, endTso), so convert the
                    // bounds only in this scan-facing read view. outputUpdateMap and the offset
                    // commit path (toOlapTableStreamUpdate) stay on the real-TSO coordinate system.
                    Pair<Long, Long> v = s.getValue();
                    return Pair.of(TSOTimestamp.toExclusiveBound(v.first),
                            TSOTimestamp.toExclusiveBound(v.second));
                }));
    }

    // get history partition offsets partitionId -> (null, historicalTimestampOffset)
    public Map<Long, Pair<Long, Long>> getHistoryPartitionOffsets(List<Long> selectedPartitionIds) {
        return outputUpdateMap.entrySet().stream()
                .filter(s -> selectedPartitionIds.contains(s.getKey()))
                // historicalTso is an inclusive upper bound; shift to the half-open exclusive end.
                .collect(Collectors.toMap(Map.Entry::getKey,
                        s -> Pair.of(null, TSOTimestamp.toExclusiveBound(s.getValue().first))));
    }

    /**
     * Per-partition estimate of the rows a changelog read of {@code offsetMap} returns, which the
     * callers hand to the wrapper the read is planned over. See {@link PartitionStreamState} for
     * the rules and for why every answer errs on the large side.
     *
     * <p>{@code offsetMap} is the one the scan uses, after the changelog TTL adjustment, so the
     * estimate describes the range the scan actually reads.
     */
    public Map<Long, Long> estimateChangelogRead(List<Long> partitionIds,
            Map<Long, Pair<Long, Long>> offsetMap) {
        long tableRowCount = getBaseTableRowCount();
        Map<Long, Long> estimates = Maps.newHashMapWithExpectedSize(partitionIds.size());
        for (Long partitionId : partitionIds) {
            estimates.put(partitionId, estimatePartitionChangelogRead(partitionId, offsetMap, tableRowCount));
        }
        return estimates;
    }

    /**
     * Per-partition estimate of the rows a snapshot read answers with: the image at the stream
     * offset. A duplicate key image only grows, so the current rows bound it; a unique or
     * aggregating one also holds the rows deleted after the offset, which the change rows of the
     * range bound.
     */
    public Map<Long, Long> estimateSnapshotRead(List<Long> partitionIds) {
        long tableRowCount = getBaseTableRowCount();
        boolean appendOnly = keysType == KeysType.DUP_KEYS;
        Map<Long, Long> estimates = Maps.newHashMapWithExpectedSize(partitionIds.size());
        for (Long partitionId : partitionIds) {
            PartitionStreamState state = partitionStreamStates.get(partitionId);
            long storedBaseRowCount = state == null
                    ? PartitionStreamState.UNKNOWN_ROW_COUNT : state.getBaseRowCount();
            long delta = appendOnly ? PartitionStreamState.UNKNOWN_ROW_COUNT
                    : changelogDeltaOf(partitionId, state);
            estimates.put(partitionId, PartitionStreamState.estimateSnapshotRead(appendOnly, storedBaseRowCount,
                    getBaseRowCount(partitionId), delta, tableRowCount));
        }
        return estimates;
    }

    private long estimatePartitionChangelogRead(long partitionId, Map<Long, Pair<Long, Long>> offsetMap,
            long tableRowCount) {
        Partition basePartition = baseTable.getPartition(partitionId);
        if (basePartition == null || !basePartition.hasData()) {
            // Nothing was ever written, so the read returns nothing whatever the counters say.
            return 1;
        }
        PartitionStreamState state = partitionStreamStates.get(partitionId);
        long storedChangelogRowCount = state == null
                ? PartitionStreamState.UNKNOWN_ROW_COUNT : state.getChangelogRowCount();
        long startTso = PartitionStreamState.UNKNOWN_ROW_COUNT;
        long endTso = PartitionStreamState.UNKNOWN_ROW_COUNT;
        Pair<Long, Long> range = offsetMap.get(partitionId);
        if (range == null) {
            range = outputUpdateMap.get(partitionId);
        }
        if (range != null) {
            startTso = range.first == null ? PartitionStreamState.UNKNOWN_ROW_COUNT : range.first;
            endTso = range.second == null ? PartitionStreamState.UNKNOWN_ROW_COUNT : range.second;
        }
        return PartitionStreamState.estimateChangelogRead(startTso, endTso, storedChangelogRowCount,
                getChangelogRowCount(partitionId), getBaseRowCount(partitionId), tableRowCount);
    }

    private long changelogDeltaOf(long partitionId, PartitionStreamState state) {
        long storedChangelogRowCount = state == null
                ? PartitionStreamState.UNKNOWN_ROW_COUNT : state.getChangelogRowCount();
        Pair<Long, Long> range = outputUpdateMap.get(partitionId);
        long startTso = range == null || range.first == null
                ? PartitionStreamState.UNKNOWN_ROW_COUNT : range.first;
        long endTso = range == null || range.second == null
                ? PartitionStreamState.UNKNOWN_ROW_COUNT : range.second;
        return PartitionStreamState.changelogDelta(startTso, endTso, storedChangelogRowCount,
                getChangelogRowCount(partitionId));
    }

    private long getBaseTableRowCount() {
        long rowCount = baseTable.getRowCountForIndex(baseTable.getBaseIndexId(), false);
        if (rowCount > 0) {
            return rowCount;
        }
        // The backends have not reported the tablets yet. The statistics record of the table is
        // kept up to date by the loads themselves, so it answers for the table as a whole: that
        // over-estimates a partition of a partitioned table, which is the safe direction.
        TableStatsMeta tableStatsMeta = Env.getCurrentEnv().getAnalysisManager()
                .findTableStatsStatus(baseTable.getId());
        if (tableStatsMeta != null) {
            long recorded = tableStatsMeta.getRowCountWithDeltaRows(baseTable, baseTable.getBaseIndexId());
            if (recorded > 0) {
                return recorded;
            }
        }
        return rowCount;
    }

    private long getBaseRowCount(long partitionId) {
        long rowCount = baseTable.getRowCountForPartitionIndex(partitionId, baseTable.getBaseIndexId(), false);
        return rowCount > 0 ? rowCount : getBaseTableRowCount();
    }

    private long getChangelogRowCount(long partitionId) {
        if (!baseTable.hasChangelog()) {
            return PartitionStreamState.UNKNOWN_ROW_COUNT;
        }
        return baseTable.getRowCountForPartitionIndex(partitionId,
                baseTable.getChangelogIndexMeta().getIndexId(), false);
    }

    /**
     * Whether the snapshot read of these partitions answers with the table as it is now.
     *
     * <p>It does not when the read leaves out rows the table holds, and that happens in two ways. The read
     * drops the partitions with no consumption baseline -- no offset, or the sentinel one of a partition that
     * was empty when the stream was created -- because there is no offset to read them from; rows such a
     * partition holds by now are rows the answer does not have. And the partitions it does read it reads as
     * the table is only where the offset reached the end of the partition: what is behind that offset it
     * answers with the image at the offset, which is the table as it was then. This is the question
     * {@code NormalizeOlapTableStreamScan} answers when it binds the read, asked by the refresh that may not
     * record a partition it answered from an incomplete or older image as holding the table's current state.
     *
     * <p>Asked in one expression for both key types: a duplicate-key read is bounded by the offset for every
     * partition rather than split into the two kinds, and a partition whose offset reached the end is read as
     * the table is by either of them.
     */
    public boolean answersWithTheCurrentTable(List<Long> partitionIds) {
        List<Long> consumed = filterConsumedPartitionIds(partitionIds);
        Set<Long> consumedIds = ImmutableSet.copyOf(consumed);
        Set<Long> atTheEnd = ImmutableSet.copyOf(filterNormalSnapshotPartitionIds(consumed));
        for (Long partitionId : partitionIds) {
            if (!consumedIds.contains(partitionId)) {
                // Not read at all: rows it holds are missing from the answer.
                if (getBaseTable().getPartition(partitionId).hasData()) {
                    return false;
                }
                continue;
            }
            if (!atTheEnd.contains(partitionId)) {
                return false;
            }
        }
        return true;
    }

    public List<Long> filterNormalSnapshotPartitionIds(List<Long> partitionIds) {
        if (hasCloudReadStates()) {
            return partitionIds.stream()
                    .filter(id -> {
                        Cloud.TableStreamPartitionReadStatePB state = cloudReadStates.get(id);
                        return state.hasOffsetTso() && state.getOffsetTso() == state.getEndTso();
                    })
                    .collect(Collectors.toList());
        }
        return partitionIds.stream()
                .filter(partitionId -> !stream.hasData(getPartition(partitionId)))
                .collect(Collectors.toList());
    }

    public boolean isHistoryPartition(long partitionId) {
        if (hasCloudReadStates()) {
            return cloudReadStates.get(partitionId).getOffsetState()
                    == Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING;
        }
        return stream.hasHistoricalData(partitionId);
    }
}
