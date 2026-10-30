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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

public class OlapTableStream extends BaseTableStream {

    // partition id -> what the stream holds for the partition. A state is replaced as a whole under the stream write
    // lock, never changed in place, and it is also read without the lock: an IVM refresh reads the stream through a
    // scan it creates after the statement has locked its tables (OlapTableStreamWrapper). So it is a concurrent map,
    // and such a read sees one whole state of a partition.
    @SerializedName("ps")
    private ConcurrentMap<Long, PartitionStreamState> partitionStates;

    // for persist; a stream from an image written before "ps" reads back with no partition state
    public OlapTableStream() {
        super();
        this.partitionStates = Maps.newConcurrentMap();
    }

    public OlapTableStream(long id, String streamName, TableIf baseTable) {
        super(id, streamName, baseTable);
        Preconditions.checkArgument(baseTable instanceof OlapTable);
        this.partitionStates = Maps.newConcurrentMap();
    }

    public OlapTableStream(String streamName, TableIf baseTable) {
        this(-1, streamName, baseTable);
    }

    @Override
    public String getTableStreamType() {
        return "OLAP_TABLE_STREAM";
    }

    @Override
    public OlapTable getBaseTableNullable() {
        TableIf baseTable = super.getBaseTableNullable();
        if (baseTable == null || !(baseTable instanceof OlapTable)) {
            return null;
        }
        return (OlapTable) baseTable;
    }

    @Override
    protected List<Column> generateDynamicSchema() {
        OlapTable baseTable = getBaseTableNullable();
        if (baseTable == null) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        // inherit base table's visible columns
        for (Column column : baseTable.getBaseSchema()) {
            if (column.isVisible()) {
                builder.add(column);
            }
        }
        // extra stream columns
        Column sequenceColumn = new Column(Column.STREAM_SEQ_COL, Type.BIGINT);
        sequenceColumn.setIsVisible(false);
        builder.add(sequenceColumn);
        Column changeTypeColumn = new Column(Column.STREAM_CHANGE_TYPE_COL, Type.VARCHAR);
        changeTypeColumn.setIsVisible(false);
        builder.add(changeTypeColumn);
        // Only expose stream LSN when the base table stores row LSN, e.g. dup table with changelog.
        if (baseTable.hasRowLsnColumn()) {
            Column lsnColumn = new Column(Column.STREAM_LSN_COL, Type.BIGINT);
            lsnColumn.setIsVisible(false);
            builder.add(lsnColumn);
        }
        return builder.build();
    }

    // used for init, should inside base table read lock
    @Override
    public void setProperties(Map<String, String> properties) throws AnalysisException {
        setPropertiesWithoutOffsetInitialization(properties);
        initializeLocalOffsets();
    }

    public void setPropertiesWithoutOffsetInitialization(Map<String, String> properties)
            throws AnalysisException {
        super.setProperties(properties);
    }

    private void initializeLocalOffsets() {
        // set offset according to baseTable
        OlapTable baseTable = getBaseTableNullable();
        if (baseTable == null) {
            return;
        }
        if (!showInitialRows) {
            // set partition offset
            baseTable.getPartitions()
                    .forEach(p -> partitionStates.put(p.getId(), PartitionStreamState.ofOffset(p.getTso())));
        } else {
            baseTable.getPartitions()
                    .stream()
                    .filter(p -> p.getVisibleVersion() > Partition.PARTITION_INIT_VERSION)
                    .forEach(p -> partitionStates.put(p.getId(), PartitionStreamState.ofHistoricalTso(p.getTso())));
        }
    }

    public static OlapTableStream read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), OlapTableStream.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    void fillTableStreamConsumptionInfo(List<TRow> dataBatch) {
        OlapTable table = getBaseTableNullable();
        if (table == null) {
            return;
        }
        if (table.readLockIfExist()) {
            try {
                for (Partition partition : table.getPartitions()) {
                    PartitionStreamState state = stateOf(partition.getId());
                    boolean hasOffset = state.getOffset() != null;
                    appendConsumptionRow(dataBatch, qualifiedDbName, name, id, partition.getName(), hasOffset,
                            hasOffset ? state.getOffset() : 0,
                            hasOffset ? partition.getTso() : 0,
                            !hasOffset && partition.hasData(),
                            state.getConsumptionTime());
                }
            } finally {
                table.readUnlock();
            }
        }
    }

    @Override
    void fillTableStreamConsumptionInfo(List<TRow> dataBatch, Predicate<String> unitSelector) {
        for (StreamConsumptionUnitSnapshot snapshot : snapshotTableStreamConsumptionInfo()) {
            if (unitSelector.test(snapshot.unit)) {
                snapshot.appendRow(dataBatch, qualifiedDbName, name, id);
            }
        }
    }

    List<StreamConsumptionUnitSnapshot> snapshotTableStreamConsumptionInfo() {
        // Copy row inputs under lock so UNIT expression rewriting and folding can run after unlocking.
        OlapTable table = getBaseTableNullable();
        if (table == null) {
            return ImmutableList.of();
        }
        List<StreamConsumptionUnitSnapshot> snapshots = new ArrayList<>();
        if (table.readLockIfExist()) {
            try {
                for (Partition partition : table.getPartitions()) {
                    snapshots.add(snapshotPartition(partition));
                }
            } finally {
                table.readUnlock();
            }
        }
        return snapshots;
    }

    private StreamConsumptionUnitSnapshot snapshotPartition(Partition partition) {
        PartitionStreamState state = stateOf(partition.getId());
        boolean hasOffset = state.getOffset() != null;
        return new StreamConsumptionUnitSnapshot(
                partition.getName(), hasOffset,
                hasOffset ? state.getOffset() : 0,
                hasOffset ? partition.getTso() : 0,
                !hasOffset && partition.hasData(),
                state.getConsumptionTime());
    }

    private static void appendConsumptionRow(List<TRow> dataBatch, String dbName, String streamName,
            long streamId, String unit, boolean hasOffset, long offset, long endTso, boolean hasData,
            long lastConsumptionTime) {
        TRow row = new TRow();
        row.addToColumnValue(new TCell().setStringVal(dbName));
        row.addToColumnValue(new TCell().setStringVal(streamName));
        row.addToColumnValue(new TCell().setLongVal(streamId));
        row.addToColumnValue(new TCell().setStringVal(unit));
        if (hasOffset) {
            row.addToColumnValue(new TCell().setStringVal(String.valueOf(offset)));
            row.addToColumnValue(new TCell().setStringVal(String.valueOf(endTso - offset)));
            row.addToColumnValue(new TCell().setLongVal(lastConsumptionTime));
        } else {
            row.addToColumnValue(new TCell().setStringVal("N/A"));
            row.addToColumnValue(new TCell().setStringVal(hasData ? "N/A" : "0"));
            row.addToColumnValue(new TCell().setLongVal(-1));
        }
        dataBatch.add(row);
    }

    static class StreamConsumptionUnitSnapshot {
        private final String unit;
        private final boolean hasOffset;
        private final long offset;
        private final long endTso;
        private final boolean hasData;
        private final long lastConsumptionTime;

        private StreamConsumptionUnitSnapshot(String unit, boolean hasOffset, long offset, long endTso,
                boolean hasData, long lastConsumptionTime) {
            this.unit = unit;
            this.hasOffset = hasOffset;
            this.offset = offset;
            this.endTso = endTso;
            this.hasData = hasData;
            this.lastConsumptionTime = lastConsumptionTime;
        }

        String getUnit() {
            return unit;
        }

        void appendRow(List<TRow> dataBatch, String dbName, String streamName, long streamId) {
            appendConsumptionRow(dataBatch, dbName, streamName, streamId, unit, hasOffset, offset, endTso,
                    hasData, lastConsumptionTime);
        }
    }

    public boolean hasData(Partition partition) {
        // if all available visible data has been consumed, return false
        Long offset = stateOf(partition.getId()).getOffset();
        return (offset == null || !offset.equals(partition.getTso())) && partition.hasData();
    }

    public boolean hasHistoricalData(long partitionId) {
        return stateOf(partitionId).getHistoricalTso() != null;
    }

    public boolean hasConsumedData(long partitionId) {
        // A partition that was empty at stream creation is recorded with the sentinel offset -1
        // (see initializeLocalOffsets); a real committed TSO is always positive (its physical part
        // is non-zero). So only a positive recorded offset counts as a real consumption baseline.
        // This keeps empty partitions out of the snapshot scan instead of letting them fall back to
        // the live partition TSO and leak post-snapshot rows.
        return stateOf(partitionId).hasConsumedData();
    }

    /** The consumption offset of the partition, or null when the stream has none for it. */
    public Long getPartitionOffset(long partitionId) {
        return stateOf(partitionId).getOffset();
    }

    /** The partitions with a consumption baseline -- a real offset, past the initial snapshot -- and their offsets. */
    Map<Long, Long> getConsumedPartitionOffsets() {
        Map<Long, Long> offsets = new HashMap<>();
        partitionStates.forEach((partitionId, state) -> {
            if (state.hasConsumedData() && state.getHistoricalTso() == null) {
                offsets.put(partitionId, state.getOffset());
            }
        });
        return offsets;
    }

    /** The keep-alive TSO of the partition with the offset it was taken against, or null when it has none. */
    public PartitionKeepAlive getKeepAlive(long partitionId) {
        return stateOf(partitionId).getKeepAlive();
    }

    /** The keep-alive TSOs of the partitions that have one, with the offsets they were taken against. */
    Map<Long, PartitionKeepAlive> getKeepAlives() {
        Map<Long, PartitionKeepAlive> keepAlives = new HashMap<>();
        partitionStates.forEach((partitionId, state) -> {
            if (state.getKeepAlive() != null) {
                keepAlives.put(partitionId, state.getKeepAlive());
            }
        });
        return keepAlives;
    }

    /**
     * Records that the partitions hold no change after the given offsets up to {@code keepAliveTso}. One taken
     * against another offset is replaced, and one taken against the same offset only grows.
     */
    void unprotectedKeepAlive(Map<Long, Long> partitionOffsets, long keepAliveTso) {
        Preconditions.checkState(isWriteLockHeldByCurrentThread(),
                "unprotectedKeepAlive must be called with write lock held");
        partitionOffsets.forEach((partitionId, offset) -> {
            PartitionStreamState state = stateOf(partitionId);
            PartitionKeepAlive recorded = state.getKeepAlive();
            if (recorded == null || recorded.getOffset() != offset || recorded.getKeepAliveTso() < keepAliveTso) {
                partitionStates.put(partitionId, state.withKeepAlive(new PartitionKeepAlive(offset, keepAliveTso)));
            }
        });
    }

    /**
     * Where a read from {@code offset} needs the changelog from: after the keep-alive TSO taken against that
     * offset, when it is the newer of the two, and from the offset otherwise.
     */
    public static long effectiveOffset(long offset, PartitionKeepAlive keepAlive) {
        if (keepAlive == null || keepAlive.getOffset() != offset) {
            return offset;
        }
        return Math.max(offset, keepAlive.getKeepAliveTso());
    }

    public Pair<Long, Long> getStreamUpdate(Long partitionId) {
        // if partition has historical data, return <historical tso, current tso>
        // otherwise, return <current consumed tso, current tso>
        PartitionStreamState state = stateOf(partitionId);
        Long left = state.getHistoricalTso() != null ? state.getHistoricalTso() : state.getOffset();
        return Pair.of(left, getBaseTableNullable().getPartition(partitionId).getTso());
    }

    @Override
    public void unprotectedCheckStreamUpdate(AbstractTableStreamUpdate update)
            throws UserException {
        Preconditions.checkArgument(update instanceof OlapTableStreamUpdate);
        // check valid
        ((OlapTableStreamUpdate) update).checkPartitionOffset(getDBName(), getName(), partitionStates);
    }

    @Override
    public void unprotectedUpdateStreamUpdate(AbstractTableStreamUpdate update, Long ts) {
        Map<Long, Long> next = ((OlapTableStreamUpdate) update).getNext();
        for (Map.Entry<Long, Long> entry : next.entrySet()) {
            partitionStates.put(entry.getKey(), stateOf(entry.getKey()).withConsumedOffset(entry.getValue(), ts));
        }
    }

    Set<Long> unprotectedCollectStalePartitionOffsetIds(Set<Long> validPartitionIds) {
        Preconditions.checkState(isWriteLockHeldByCurrentThread(),
                "unprotectedCollectStalePartitionOffsetIds must be called with write lock held");
        Set<Long> stalePartitionIds = new HashSet<>();
        for (Long partitionId : partitionStates.keySet()) {
            if (!validPartitionIds.contains(partitionId)) {
                stalePartitionIds.add(partitionId);
            }
        }
        return stalePartitionIds;
    }

    void unprotectedPrunePartitionOffsets(Set<Long> partitionIds) {
        Preconditions.checkState(isWriteLockHeldByCurrentThread(),
                "unprotectedPrunePartitionOffsets must be called with write lock held");
        for (Long partitionId : partitionIds) {
            partitionStates.remove(partitionId);
        }
    }

    private PartitionStreamState stateOf(long partitionId) {
        return partitionStates.getOrDefault(partitionId, PartitionStreamState.NONE);
    }

    /**
     * What the stream holds for one partition of its base table. It is replaced rather than changed, so that a read
     * made without the stream lock sees all of one state.
     */
    public static class PartitionStreamState {
        // the state of a partition the stream holds nothing for
        static final PartitionStreamState NONE = new PartitionStreamState(null, -1L, null, null);

        // the TSO the partition is consumed up to, or null when the stream has no offset for it
        @SerializedName("o")
        private Long offset;

        // when a consumption last moved the offset, or -1 when none has since the stream was created
        @SerializedName("ct")
        private long consumptionTime;

        // the TSO of the initial rows the stream shows and has yet to return, or null when there are none
        @SerializedName("ht")
        private Long historicalTso;

        // The keep-alive TSO K with the offset C it was taken against, or null: the partition is known to hold no
        // change in (C, K], so the changelog a read from C needs is only the part after K. It counts only for a
        // read from C, so an offset that moved, or one that is not what it was taken against (a cloud offset lives
        // in MetaService, not in this state), leaves it out. Only the partitions of an IVM stream have one
        // (IvmStreamKeepAliveDaemon).
        @SerializedName("ka")
        private PartitionKeepAlive keepAlive;

        public PartitionStreamState(Long offset, long consumptionTime, Long historicalTso,
                PartitionKeepAlive keepAlive) {
            this.offset = offset;
            this.consumptionTime = consumptionTime;
            this.historicalTso = historicalTso;
            this.keepAlive = keepAlive;
        }

        static PartitionStreamState ofOffset(long offset) {
            return new PartitionStreamState(offset, -1L, null, null);
        }

        static PartitionStreamState ofHistoricalTso(long historicalTso) {
            return new PartitionStreamState(null, -1L, historicalTso, null);
        }

        public Long getOffset() {
            return offset;
        }

        public long getConsumptionTime() {
            return consumptionTime;
        }

        public Long getHistoricalTso() {
            return historicalTso;
        }

        public PartitionKeepAlive getKeepAlive() {
            return keepAlive;
        }

        // see OlapTableStream#hasConsumedData
        boolean hasConsumedData() {
            return offset != null && offset > 0;
        }

        // A consumption moves the offset and returns the initial rows, if the partition had any left.
        PartitionStreamState withConsumedOffset(long offset, long consumptionTime) {
            return new PartitionStreamState(offset, consumptionTime, null, keepAlive);
        }

        PartitionStreamState withKeepAlive(PartitionKeepAlive keepAlive) {
            return new PartitionStreamState(offset, consumptionTime, historicalTso, keepAlive);
        }
    }

    /** A keep-alive TSO K with the offset C it was taken against: the partition holds no change in (C, K]. */
    public static class PartitionKeepAlive {
        @SerializedName("o")
        private long offset;

        @SerializedName("k")
        private long keepAliveTso;

        public PartitionKeepAlive(long offset, long keepAliveTso) {
            this.offset = offset;
            this.keepAliveTso = keepAliveTso;
        }

        public long getOffset() {
            return offset;
        }

        public long getKeepAliveTso() {
            return keepAliveTso;
        }
    }
}
