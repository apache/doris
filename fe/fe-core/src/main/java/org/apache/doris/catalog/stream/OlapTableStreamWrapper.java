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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Util;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TPrimitiveType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

// runtime-only class for unified query/insert experience, created when bind relation with OlapTableStream
public class OlapTableStreamWrapper extends OlapTable {
    private final OlapTableStream stream;
    private final OlapTable baseTable;
    protected final Map<Long, Pair<Long, Long>> outputUpdateMap;
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
        this.outputUpdateMap = buildOutputUpdateMap(selectedPartitionIds);
        this.getOrCreatTableProperty().setEnableUniqueKeyMergeOnWrite(baseTable.getEnableUniqueKeyMergeOnWrite());
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
        cloudReadStates = immutableReadStates;
        cloudReadStatesInstalled = true;
        outputUpdateMap.clear();
        cloudReadStates.forEach((partitionId, state) -> {
            if (state.getVisibleVersion() > Partition.PARTITION_INIT_VERSION) {
                Long startTso = state.hasOffsetTso() ? state.getOffsetTso() : null;
                outputUpdateMap.put(partitionId, Pair.of(startTso, state.getEndTso()));
            }
        });
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
                    .filter(id -> cloudReadStates.get(id).getOffsetState()
                            == Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED)
                    .collect(ImmutableList.toImmutableList());
        }
        return partitionIds.stream()
                .filter(partitionId -> stream.hasConsumedData(partitionId))
                .collect(ImmutableList.toImmutableList());
    }

    public OlapTable getBaseTable() {
        return baseTable;
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
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    // get history partition offsets partitionId -> (null, historicalTimestampOffset)
    public Map<Long, Pair<Long, Long>> getHistoryPartitionOffsets(List<Long> selectedPartitionIds) {
        return outputUpdateMap.entrySet().stream()
                .filter(s -> selectedPartitionIds.contains(s.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, s -> Pair.of(null, s.getValue().first)));
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
