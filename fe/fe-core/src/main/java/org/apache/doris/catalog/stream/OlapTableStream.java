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
import java.util.function.Predicate;

public class OlapTableStream extends BaseTableStream {

    @SerializedName("ps")
    private Map<Long, PartitionStreamState> partitionStates = new HashMap<>();

    // Nullable offset and historicalTso distinguish an unconsumed snapshot from an incremental baseline.
    // State is replaced as a whole when a consumption commits.
    public static class PartitionStreamState {
        @SerializedName("o")
        private final Long offset;
        @SerializedName("ct")
        private final long consumptionTime;
        @SerializedName("ht")
        private final Long historicalTso;

        public PartitionStreamState(Long offset, long consumptionTime, Long historicalTso) {
            this.offset = offset;
            this.consumptionTime = consumptionTime;
            this.historicalTso = historicalTso;
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
        if (!showInitialRows) {
            // set partition offset
            baseTable.getPartitions()
                    .forEach(p -> partitionStates.put(p.getId(), new PartitionStreamState(p.getTso(), -1, null)));
        } else {
            baseTable.getPartitions()
                    .stream()
                    .filter(p -> p.getVisibleVersion() > Partition.PARTITION_INIT_VERSION)
                    .forEach(p -> partitionStates.put(p.getId(),
                            new PartitionStreamState(null, -1, p.getTso())));
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
        PartitionStreamState state = partitionStates.get(partitionId);
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

    public boolean hasData(Partition partition) {
        // if all available visible data has been consumed, return false
        PartitionStreamState state = partitionStates.get(partition.getId());
        return (state == null || state.getOffset() == null || !state.getOffset().equals(partition.getTso()))
                && partition.hasData();
    }

    public boolean hasHistoricalData(long partitionId) {
        PartitionStreamState state = partitionStates.get(partitionId);
        return state != null && state.getHistoricalTso() != null;
    }

    public boolean hasConsumedData(long partitionId) {
        // An empty partition has the sentinel offset -1; only a positive committed TSO
        // counts as a consumption baseline for snapshot reads.
        PartitionStreamState state = partitionStates.get(partitionId);
        return state != null && state.getOffset() != null && state.getOffset() > 0;
    }

    public Pair<Long, Long> getStreamUpdate(Long partitionId) {
        // Historical data takes precedence over the consumed offset until it is consumed.
        PartitionStreamState state = partitionStates.get(partitionId);
        Long startTso = state == null ? null
                : state.getHistoricalTso() != null ? state.getHistoricalTso() : state.getOffset();
        return Pair.of(startTso, getBaseTableNullable().getPartition(partitionId).getTso());
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
            partitionStates.put(entry.getKey(), new PartitionStreamState(entry.getValue(), ts, null));
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
}
