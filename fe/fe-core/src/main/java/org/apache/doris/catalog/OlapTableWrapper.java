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
    // no explicit estimate. Keep the participating partitions in one immutable snapshot.
    private final Map<Long, PartitionScanState> partitionScanStates;
    private final boolean hasRowCountEstimates;
    private final boolean fixedVisibleVersions;

    public static class PartitionScanState {
        private final Pair<Long, Long> offsets;
        private final Long visibleVersion;
        private final Long rowCount;

        public PartitionScanState(Pair<Long, Long> offsets, Long visibleVersion, Long rowCount) {
            this.offsets = offsets;
            this.visibleVersion = visibleVersion;
            this.rowCount = rowCount;
        }
    }

    protected OlapTableWrapper(OlapTable originTable, String wrapperName, List<Column> baseSchema, KeysType keysType,
            Map<Long, PartitionScanState> partitionScanStates) {
        super(originTable.getId(), wrapperName, baseSchema,
                keysType, originTable.getPartitionInfo(), originTable.getDefaultDistributionInfo());
        this.originTable = originTable;
        this.setBaseIndexId(originTable.getBaseIndexId());
        this.setQualifiedDbName(originTable.getQualifiedDbName());
        this.partitionScanStates = ImmutableMap.copyOf(partitionScanStates);
        boolean estimates = false;
        boolean fixedVersions = false;
        for (PartitionScanState state : this.partitionScanStates.values()) {
            estimates |= state.rowCount != null;
            fixedVersions |= state.visibleVersion != null;
        }
        this.hasRowCountEstimates = estimates;
        this.fixedVisibleVersions = fixedVersions;
    }

    public OlapTableWrapper(OlapTable originTable, Map<Long, PartitionScanState> partitionScanStates) {
        this(originTable, originTable.getName(), originTable.getBaseSchema(), originTable.getKeysType(),
                partitionScanStates);
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
     * Returns the wrapped table's whole-index row count. The optimizer aggregates scan row
     * counts over the selected partitions through {@link #getRowCountForPartitionIndex}.
     */
    @Override
    public long getRowCountForIndex(long indexId, boolean strict) {
        return originTable.getRowCountForIndex(indexId, strict);
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

    public boolean hasRowCountEstimates(long indexId) {
        return indexId == getBaseIndexId() && hasRowCountEstimates;
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
