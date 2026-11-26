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
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.cloud.proto.Cloud;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class OlapTableStream extends BaseTableStream {

    @SerializedName("ps")
    private Map<Long, PartitionStreamState> partitionStates = new HashMap<>();

    // This monitor protects only the state map. Never acquire a metadata lock or call MS while holding it.
    private final transient Object partitionStatesLock = new Object();

    /** One committed consumption state; offsets and counts are persisted and replaced together. */
    public static class PartitionStreamState {
        @SerializedName("o")
        private final Long offset;
        @SerializedName("ct")
        private final long consumptionTime;
        @SerializedName("ht")
        private final Long historicalTso;
        @SerializedName("brc")
        private final long baseRowCount;
        @SerializedName("crc")
        private final long changelogRowCount;

        private static final PartitionStreamState INITIAL_STATE = new PartitionStreamState(
                null, -1, null, StreamRowCountEstimator.UNKNOWN_ROW_COUNT,
                StreamRowCountEstimator.UNKNOWN_ROW_COUNT);

        public static PartitionStreamState initial() {
            return INITIAL_STATE;
        }

        public static PartitionStreamState restored(Long offset, long consumptionTime, Long historicalTso,
                long baseRowCount, long changelogRowCount) {
            return new PartitionStreamState(offset, consumptionTime, historicalTso,
                    baseRowCount, changelogRowCount);
        }

        private PartitionStreamState(Long offset, long consumptionTime, Long historicalTso,
                long baseRowCount, long changelogRowCount) {
            this.offset = offset;
            this.consumptionTime = consumptionTime;
            this.historicalTso = historicalTso;
            this.baseRowCount = baseRowCount;
            this.changelogRowCount = changelogRowCount;
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

        public Long getBaselineOffset() {
            return historicalTso != null ? historicalTso : offset;
        }

        public long getBaseRowCount() {
            return baseRowCount;
        }

        public long getChangelogRowCount() {
            return changelogRowCount;
        }

        static PartitionStreamState fromCloudReadState(Cloud.TableStreamPartitionReadStatePB state) {
            boolean history = state.getOffsetState()
                    == Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING;
            return new PartitionStreamState(history ? null : state.getOffsetTso(),
                    state.hasLastConsumptionTimeMs() ? state.getLastConsumptionTimeMs() : -1,
                    history ? state.getOffsetTso() : null,
                    state.hasBaseRowCount() ? state.getBaseRowCount() : StreamRowCountEstimator.UNKNOWN_ROW_COUNT,
                    state.hasChangelogRowCount() ? state.getChangelogRowCount()
                            : StreamRowCountEstimator.UNKNOWN_ROW_COUNT);
        }

        /** Sample while holding the source read lock, before journaling the pending update. */
        public PartitionStreamState withRowCounts(OlapTable table, long partitionId) {
            Partition partition = table.getPartition(partitionId);
            MaterializedIndex base = partition.getIndex(table.getBaseIndexId());
            MaterializedIndex changelog = table.hasChangelog()
                    ? partition.getIndex(table.getChangelogIndexMeta().getIndexId()) : null;
            return new PartitionStreamState(offset, consumptionTime, historicalTso,
                    StreamRowCountEstimator.rowCount(base), StreamRowCountEstimator.rowCount(changelog));
        }

        public PartitionStreamState atOffset(Long nextOffset) {
            return new PartitionStreamState(nextOffset, consumptionTime, null,
                    baseRowCount, changelogRowCount);
        }

        public PartitionStreamState atHistoricalTso(Long tso) {
            return new PartitionStreamState(null, consumptionTime, tso,
                    baseRowCount, changelogRowCount);
        }

        public PartitionStreamState committedAt(long timestamp) {
            return new PartitionStreamState(offset, timestamp, historicalTso,
                    baseRowCount, changelogRowCount);
        }

        public boolean matchesOffset(Long expectedOffset) {
            return Objects.equals(getBaselineOffset(), expectedOffset);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof PartitionStreamState)) {
                return false;
            }
            PartitionStreamState state = (PartitionStreamState) other;
            return Objects.equals(offset, state.offset) && consumptionTime == state.consumptionTime
                    && Objects.equals(historicalTso, state.historicalTso)
                    && baseRowCount == state.baseRowCount && changelogRowCount == state.changelogRowCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, consumptionTime, historicalTso, baseRowCount, changelogRowCount);
        }
    }

    public PartitionStreamState getPartitionState(long partitionId) {
        synchronized (partitionStatesLock) {
            return partitionStates.get(partitionId);
        }
    }

    Map<Long, PartitionStreamState> snapshotPartitionStates(Collection<Long> partitionIds) {
        Map<Long, PartitionStreamState> snapshot = Maps.newHashMapWithExpectedSize(partitionIds.size());
        synchronized (partitionStatesLock) {
            for (long id : partitionIds) {
                PartitionStreamState state = partitionStates.get(id);
                if (state != null) {
                    snapshot.put(id, state);
                }
            }
        }
        return snapshot;
    }

    /** Capture the next consumption state before its transaction is journaled. */
    public void unprotectedPrepareStreamUpdate(OlapTableStreamUpdate update) {
        if (!tryReadLock(0, TimeUnit.MILLISECONDS)) {
            return;
        }
        try {
            Map<Long, PartitionStreamState> current = snapshotPartitionStates(update.getPartitionUpdates().keySet());
            update.retainUnchangedCounts(current);
            OlapTable table = getBaseTableNullable();
            // The transaction may own sink/stream locks. Optional statistics cannot wait for source DDL.
            if (table == null || !table.tryReadLock(0, TimeUnit.MILLISECONDS)) {
                return;
            }
            try {
                if (table.isDropped) {
                    return;
                }
                update.captureRowCounts(table, current);
            } finally {
                table.readUnlock();
            }
        } finally {
            readUnlock();
        }
    }

    // for persist
    public OlapTableStream() {
        super();
    }

    public OlapTableStream(long id, String streamName, TableIf baseTable) {
        super(id, streamName, baseTable);
        Preconditions.checkArgument(baseTable instanceof OlapTable);
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
        Map<Long, PartitionStreamState> initial = new HashMap<>();
        if (!showInitialRows) {
            // set partition offset
            baseTable.getPartitions()
                    .forEach(p -> initial.put(p.getId(), PartitionStreamState.initial().atOffset(p.getTso())));
        } else {
            baseTable.getPartitions()
                    .stream()
                    .filter(p -> Partition.hasDataAtVersion(p.getVisibleVersion()))
                    .forEach(p -> initial.put(p.getId(),
                            PartitionStreamState.initial().atHistoricalTso(p.getTso())));
        }
        initial.replaceAll((id, state) -> state.withRowCounts(baseTable, id));
        synchronized (partitionStatesLock) {
            partitionStates.putAll(initial);
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
                    snapshotPartition(partition).appendRow(dataBatch, qualifiedDbName, name, id);
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
        long partitionId = partition.getId();
        PartitionStreamState state = getPartitionState(partitionId);
        boolean hasOffset = state != null && state.getOffset() != null;
        return new StreamConsumptionUnitSnapshot(
                partition.getName(), hasOffset,
                hasOffset ? state.getOffset() : 0,
                hasOffset ? partition.getTso() : 0,
                !hasOffset && partition.hasData(),
                hasOffset ? state.getConsumptionTime() : -1);
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

    public Cloud.TableStreamIdentityPB getCloudIdentity() {
        return Cloud.TableStreamIdentityPB.newBuilder()
                .setBaseDbId(getBaseTableInfo().getDbId())
                .setBaseTableId(getBaseTableInfo().getTableId())
                .setStreamDbId(getDatabase().getId())
                .setStreamId(getId())
                .build();
    }

    public boolean hasData(Partition partition) {
        // if all available visible data has been consumed, return false
        PartitionStreamState state = getPartitionState(partition.getId());
        return (state == null || state.getOffset() == null || !state.getOffset().equals(partition.getTso()))
                && partition.hasData();
    }

    public boolean hasHistoricalData(long partitionId) {
        PartitionStreamState state = getPartitionState(partitionId);
        return state != null && state.getHistoricalTso() != null;
    }

    public boolean hasConsumedData(long partitionId) {
        // An empty partition has the sentinel offset -1; only a positive committed TSO
        // counts as a consumption baseline for snapshot reads.
        PartitionStreamState state = getPartitionState(partitionId);
        return state != null && state.getOffset() != null && state.getOffset() > 0;
    }

    public Pair<Long, Long> getStreamUpdate(Long partitionId) {
        // Historical data takes precedence over the consumed offset until it is consumed.
        PartitionStreamState state = getPartitionState(partitionId);
        Long startTso = state == null ? null
                : state.getHistoricalTso() != null ? state.getHistoricalTso() : state.getOffset();
        return Pair.of(startTso, getBaseTableNullable().getPartition(partitionId).getTso());
    }

    @Override
    public void unprotectedCheckStreamUpdate(AbstractTableStreamUpdate update)
            throws UserException {
        Preconditions.checkArgument(update instanceof OlapTableStreamUpdate);
        // check valid
        OlapTableStreamUpdate olapUpdate = (OlapTableStreamUpdate) update;
        olapUpdate.checkPartitionOffset(getDBName(), getName(),
                snapshotPartitionStates(olapUpdate.getPartitionUpdates().keySet()));
    }

    @Override
    public void unprotectedUpdateStreamUpdate(AbstractTableStreamUpdate update, Long ts) {
        synchronized (partitionStatesLock) {
            ((OlapTableStreamUpdate) update).getPartitionUpdates().forEach((id, state) ->
                    partitionStates.put(id, state.getNextState().committedAt(ts)));
        }
    }

    Set<Long> unprotectedCollectStalePartitionOffsetIds(Set<Long> validPartitionIds) {
        Preconditions.checkState(isWriteLockHeldByCurrentThread(),
                "unprotectedCollectStalePartitionOffsetIds must be called with write lock held");
        Set<Long> stalePartitionIds = new HashSet<>();
        synchronized (partitionStatesLock) {
            for (Long partitionId : partitionStates.keySet()) {
                if (!validPartitionIds.contains(partitionId)) {
                    stalePartitionIds.add(partitionId);
                }
            }
        }
        return stalePartitionIds;
    }

    void unprotectedPrunePartitionOffsets(Set<Long> partitionIds) {
        Preconditions.checkState(isWriteLockHeldByCurrentThread(),
                "unprotectedPrunePartitionOffsets must be called with write lock held");
        synchronized (partitionStatesLock) {
            partitionIds.forEach(partitionStates::remove);
        }
    }
}
