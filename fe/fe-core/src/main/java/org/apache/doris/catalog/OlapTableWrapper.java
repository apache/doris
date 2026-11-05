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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
    private final Map<Long, Pair<Long, Long>> partitionOffsetMap; // partitionId -> (startOffset, endOffset)
    private final Map<Long, Long> partitionVisibleVersionMap;
    // Per-partition row counts the scan is estimated with, when the caller knows better than the
    // table it wraps can answer: a stream read returns the rows of a range, or the image at an
    // offset, which neither the changelog index nor the base table reports. Empty means that the
    // table it wraps answers, which is the case for every wrapper outside a stream read.
    private final Map<Long, Long> partitionRowCounts;

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
        this.partitionOffsetMap = partitionOffsetMap;
        this.partitionVisibleVersionMap = ImmutableMap.copyOf(partitionVisibleVersionMap);
        this.partitionRowCounts = ImmutableMap.copyOf(partitionRowCounts);
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
        super(originTable.getId(), originTable.getName(), originTable.getBaseSchema(),
                originTable.getKeysType(), originTable.getPartitionInfo(), originTable.getDefaultDistributionInfo());
        this.originTable = originTable;
        this.setBaseIndexId(originTable.getBaseIndexId());
        this.setQualifiedDbName(originTable.getQualifiedDbName());
        this.partitionOffsetMap = partitionOffsetMap;
        this.partitionVisibleVersionMap = ImmutableMap.copyOf(partitionVisibleVersionMap);
        this.partitionRowCounts = ImmutableMap.copyOf(partitionRowCounts);
    }

    protected OlapTableWrapper(OlapTable originTable) {
        this(originTable, new HashMap<>());
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
        long estimate = getRowCountEstimate();
        return estimate < 0 ? originTable.getRowCountForIndex(indexId, strict) : estimate;
    }

    @Override
    public long getRowCountForPartitionIndex(long partitionId, long indexId, boolean strict) {
        Long rowCount = partitionRowCounts.get(partitionId);
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
        if (partitionRowCounts.isEmpty()) {
            return TableIf.UNKNOWN_ROW_COUNT;
        }
        long total = 0;
        for (long rowCount : partitionRowCounts.values()) {
            total += rowCount;
        }
        return total;
    }

    @Override
    public int getPartitionNum() {
        return originTable.getPartitionNum();
    }

    public Pair<Long, Long> getPartitionOffset(long partitionId) {
        return partitionOffsetMap.get(partitionId);
    }

    public boolean hasFixedVisibleVersions() {
        return !partitionVisibleVersionMap.isEmpty();
    }

    public Map<Long, Long> getPartitionVisibleVersionMap() {
        return partitionVisibleVersionMap;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        OlapTableWrapper other = (OlapTableWrapper) obj;
        return originTable.equals(other.originTable)
                && partitionOffsetMap.equals(other.partitionOffsetMap)
                && partitionVisibleVersionMap.equals(other.partitionVisibleVersionMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), originTable.getId(), partitionOffsetMap,
                partitionVisibleVersionMap);
    }
}
