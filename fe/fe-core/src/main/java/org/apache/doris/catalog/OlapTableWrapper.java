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

package org.apache.doris.catalog;

import org.apache.doris.catalog.stream.StreamReadMode;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A lightweight wrapper base class for {@link OlapTable}.
 *
 * <p>It delegates table locks and partition-related operations to the wrapped table to keep the
 * metadata view consistent.
 */
public class OlapTableWrapper extends OlapTable {

    protected final OlapTable originTable;
    // Fields are optional independently: SN has no fixed version, and ordinary wrappers have
    // no explicit estimate. Keep the union of their partition IDs in one immutable snapshot.
    private final Map<Long, PartitionScanState> partitionScanStates;
    private final int estimatedPartitionCount;
    private final boolean fixedVisibleVersions;

    private static class PartitionScanState {
        private final Pair<Long, Long> offsets;
        private final Long visibleVersion;
        private final Long rowCount;

        private PartitionScanState(Pair<Long, Long> offsets, Long visibleVersion, Long rowCount) {
            this.offsets = offsets;
            this.visibleVersion = visibleVersion;
            this.rowCount = rowCount;
        }
    }

    protected OlapTableWrapper(OlapTable originTable, String wrapperName, List<Column> baseSchema, KeysType keysType,
                               Map<Long, Pair<Long, Long>> partitionOffsetMap) {
        this(originTable, wrapperName, baseSchema, keysType, partitionOffsetMap, Collections.emptyMap(),
                Collections.emptyMap());
    }

    protected OlapTableWrapper(OlapTable originTable, String wrapperName, List<Column> baseSchema, KeysType keysType,
                               Map<Long, Pair<Long, Long>> partitionOffsetMap,
                               Map<Long, Long> partitionVisibleVersionMap) {
        this(originTable, wrapperName, baseSchema, keysType, partitionOffsetMap, partitionVisibleVersionMap,
                Collections.emptyMap());
    }

    protected OlapTableWrapper(OlapTable originTable, String wrapperName, List<Column> baseSchema, KeysType keysType,
                               Map<Long, Pair<Long, Long>> partitionOffsetMap,
                               Map<Long, Long> partitionVisibleVersionMap,
                               Map<Long, Long> partitionRowCounts) {
        super(originTable.getId(), wrapperName, baseSchema,
                keysType, originTable.getPartitionInfo(), originTable.getDefaultDistributionInfo());
        this.originTable = originTable;
        this.setBaseIndexId(originTable.getBaseIndexId());
        this.setQualifiedDbName(originTable.getQualifiedDbName());
        Set<Long> partitionIds = new HashSet<>(partitionOffsetMap.keySet());
        partitionIds.addAll(partitionVisibleVersionMap.keySet());
        partitionIds.addAll(partitionRowCounts.keySet());
        ImmutableMap.Builder<Long, PartitionScanState> states = ImmutableMap.builder();
        for (Long id : partitionIds) {
            states.put(id, new PartitionScanState(partitionOffsetMap.get(id),
                    partitionVisibleVersionMap.get(id), partitionRowCounts.get(id)));
        }
        this.partitionScanStates = states.build();
        this.estimatedPartitionCount = partitionRowCounts.size();
        this.fixedVisibleVersions = !partitionVisibleVersionMap.isEmpty();
    }

    public OlapTableWrapper(OlapTable originTable, Map<Long, Pair<Long, Long>> partitionOffsetMap) {
        this(originTable, partitionOffsetMap, Collections.emptyMap());
    }

    public OlapTableWrapper(OlapTable originTable, Map<Long, Pair<Long, Long>> partitionOffsetMap,
                            Map<Long, Long> partitionVisibleVersionMap) {
        this(originTable, partitionOffsetMap, partitionVisibleVersionMap, Collections.emptyMap());
    }

    public OlapTableWrapper(OlapTable originTable, Map<Long, Pair<Long, Long>> partitionOffsetMap,
                            Map<Long, Long> partitionVisibleVersionMap, Map<Long, Long> partitionRowCounts) {
        this(originTable, originTable.getName(), originTable.getBaseSchema(), originTable.getKeysType(),
                partitionOffsetMap, partitionVisibleVersionMap, partitionRowCounts);
    }

    protected OlapTableWrapper(OlapTable originTable) {
        this(originTable, Collections.emptyMap());
    }

    public OlapTable getOriginTable() {
        return originTable;
    }

    @Override
    public long getBaseIndexId() {
        return originTable.getBaseIndexId();
    }

    @Override
    public MaterializedIndexMeta getIndexMetaByIndexId(long indexId) {
        return originTable.getIndexMetaByIndexId(indexId);
    }

    @Override
    public String getIndexNameById(long indexId) {
        return originTable.getIndexNameById(indexId);
    }

    @Override
    public int getIndexSchemaVersion(long indexId) {
        return originTable.getIndexSchemaVersion(indexId);
    }

    @Override
    public List<Column> getSchemaByIndexId(Long indexId) {
        return originTable.getSchemaByIndexId(indexId);
    }

    @Override
    public List<Column> getSchemaByIndexId(Long indexId, boolean full) {
        return originTable.getSchemaByIndexId(indexId, full);
    }

    @Override
    public void readLock() {
        originTable.readLock();
    }

    @Override
    public boolean tryReadLock(long timeout, TimeUnit unit) {
        return originTable.tryReadLock(timeout, unit);
    }

    @Override
    public void readUnlock() {
        originTable.readUnlock();
    }

    @Override
    public PartitionInfo getPartitionInfo() {
        return originTable.getPartitionInfo();
    }

    @Override
    public Partition getPartition(String partitionName, boolean isTempPartition) {
        return originTable.getPartition(partitionName, isTempPartition);
    }

    @Override
    public Partition getPartition(String partitionName) {
        return originTable.getPartition(partitionName);
    }

    @Override
    public Partition getPartition(long partitionId) {
        return originTable.getPartition(partitionId);
    }

    @Override
    public PartitionItem getPartitionItemOrAnalysisException(String partitionName) throws AnalysisException {
        return originTable.getPartitionItemOrAnalysisException(partitionName);
    }

    @Override
    public Set<String> getPartitionNames() {
        return originTable.getPartitionNames();
    }

    @Override
    public List<Long> getPartitionIds() {
        return originTable.getPartitionIds();
    }

    @Override
    public Collection<Partition> getPartitions() {
        return originTable.getPartitions();
    }

    @Override
    public List<Long> selectNonEmptyPartitionIds(Collection<Long> partitionIds,
            Optional<StreamReadMode> streamReadMode) {
        return originTable.selectNonEmptyPartitionIds(partitionIds, streamReadMode);
    }

    @Override
    public Set<String> getDistributionColumnNames() {
        return originTable.getDistributionColumnNames();
    }

    @Override
    public boolean hasChangelog() {
        return originTable.hasChangelog();
    }

    @Override
    public MaterializedIndexMeta getBaseIndexMeta() {
        return originTable.getBaseIndexMeta();
    }

    @Override
    public MaterializedIndex getBaseIndex() {
        return originTable.getBaseIndex();
    }

    /**
     * Row counts are derived from the partitions of the table, and this wrapper holds none of its
     * own -- it only delegates partition lookups to the table it wraps. The inherited methods
     * iterate the wrapper's private partition map, so every scan over a wrapper would be estimated
     * at zero rows, which the statistics calculator raises to one; a reset read of a
     * four-billion-row table would then look like a one-row build side to the optimizer.
     *
     * <p>Delegating answers the read with the table it is over: a reset read, and a snapshot read
     * whose offsets already reached the end, see exactly that table's content, and a snapshot read
     * of an older offset is bounded by it (the table only grows for the duplicate-key tables this
     * wrapper serves in that way).
     *
     * <p>A caller which knows what the read returns records it per partition and overrides this:
     * a stream read answers with a range of change rows, or with the image at an offset, and
     * neither is the row count of the table it reads.
     */
    @Override
    public long getRowCountForIndex(long indexId, boolean strict) {
        if (!hasRowCountEstimates(indexId)) {
            return originTable.getRowCountForIndex(indexId, strict);
        }
        long total = 0;
        long unknown = 0;
        for (Map.Entry<Long, Long> entry : getPartitionRowCounts().entrySet()) {
            long rows = entry.getValue();
            if (rows < 0) {
                rows = originTable.getRowCountForPartitionIndex(entry.getKey(), indexId, strict);
            }
            if (rows < 0) {
                unknown++;
            } else {
                total += rows;
            }
        }
        if (unknown > 0) {
            long tableRows = originTable.getRowCountForIndex(indexId, strict);
            if (tableRows < 0 && unknown == estimatedPartitionCount) {
                return TableIf.UNKNOWN_ROW_COUNT;
            }
            total += Math.max(unknown, tableRows * unknown / originTable.getPartitionNum());
        }
        return total;
    }

    @Override
    public long getRowCountForPartitionIndex(long partitionId, long indexId, boolean strict) {
        PartitionScanState state = partitionScanStates.get(partitionId);
        Long rowCount = indexId == getBaseIndexId() && state != null ? state.rowCount : null;
        if (rowCount != null) {
            return rowCount;
        }
        return originTable.getRowCountForPartitionIndex(partitionId, indexId, strict);
    }

    /**
     * The row count the caller recorded for this scan, or {@code -1} when the table it wraps
     * answers instead.
     */
    public long getRowCountEstimate() {
        return estimatedPartitionCount == 0 ? TableIf.UNKNOWN_ROW_COUNT : getRowCountForIndex(getBaseIndexId(), true);
    }

    public boolean hasRowCountEstimates(long indexId) {
        return indexId == getBaseIndexId() && estimatedPartitionCount > 0;
    }

    @Override
    public int getPartitionNum() {
        return originTable.getPartitionNum();
    }

    public Pair<Long, Long> getPartitionOffset(long partitionId) {
        PartitionScanState state = partitionScanStates.get(partitionId);
        return state == null ? null : state.offsets;
    }

    public boolean hasFixedVisibleVersions() {
        return fixedVisibleVersions;
    }

    public Map<Long, Long> getPartitionVisibleVersionMap() {
        return Maps.transformValues(Maps.filterValues(partitionScanStates, state -> state.visibleVersion != null),
                state -> state.visibleVersion);
    }

    private Map<Long, Pair<Long, Long>> getPartitionOffsetMap() {
        return Maps.transformValues(Maps.filterValues(partitionScanStates, state -> state.offsets != null),
                state -> state.offsets);
    }

    private Map<Long, Long> getPartitionRowCounts() {
        return Maps.transformValues(Maps.filterValues(partitionScanStates, state -> state.rowCount != null),
                state -> state.rowCount);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        OlapTableWrapper other = (OlapTableWrapper) obj;
        return originTable.equals(other.originTable)
                && getPartitionOffsetMap().equals(other.getPartitionOffsetMap())
                && getPartitionVisibleVersionMap().equals(other.getPartitionVisibleVersionMap());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), originTable.getId(), getPartitionOffsetMap(),
                getPartitionVisibleVersionMap());
    }
}
