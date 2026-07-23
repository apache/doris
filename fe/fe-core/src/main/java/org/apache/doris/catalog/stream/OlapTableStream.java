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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OlapTableStream extends BaseTableStream {

    @SerializedName("po")
    private Map<Long, Long> partitionOffset;

    @SerializedName("pct")
    private Map<Long, Long> partitionConsumptionTime;

    @SerializedName("hpt")
    private Map<Long, Long> historicalPartitionTSO;

    // for persist
    public OlapTableStream() {
        super();
    }

    public OlapTableStream(long id, String streamName, List<Column> fullSchema, TableIf baseTable) {
        super(id, streamName, fullSchema, baseTable);
        Preconditions.checkArgument(baseTable instanceof OlapTable);
        this.partitionOffset = new HashMap<>();
        this.partitionConsumptionTime = new HashMap<>();
        this.historicalPartitionTSO = new HashMap<>();
        this.baseTable = baseTable;
    }

    public OlapTableStream(String streamName, List<Column> fullSchema, TableIf baseTable) {
        this(-1, streamName, fullSchema, baseTable);
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

    // used for init, should inside base table read lock
    @Override
    public void setProperties(Map<String, String> properties) throws AnalysisException {
        super.setProperties(properties);
        // set offset according to baseTable
        if (!showInitialRows) {
            // set partition offset
            ((OlapTable) baseTable).getPartitions()
                    .forEach(p -> partitionOffset.put(p.getId(), p.getTso()));
        } else {
            ((OlapTable) baseTable).getPartitions()
                    .stream()
                    .filter(p -> p.getVisibleVersion() > Partition.PARTITION_INIT_VERSION)
                    .forEach(p -> {
                                historicalPartitionTSO.put(p.getId(), p.getTso());
                                    }
                    );
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
                Map<Long, Partition> id2name = table.getPartitions().stream().collect(Collectors.toMap(
                        p -> p.getId(),
                        p -> p,
                        (oldValue, newValue) -> newValue,
                        HashMap::new
                ));
                for (Map.Entry<Long, Partition> entry : id2name.entrySet()) {
                    TRow trow = new TRow();
                    // DB_NAME
                    trow.addToColumnValue(new TCell().setStringVal(qualifiedDbName));
                    // STREAM_NAME
                    trow.addToColumnValue(new TCell().setStringVal(name));
                    // STREAM_ID
                    trow.addToColumnValue(new TCell().setLongVal(id));
                    // UNIT
                    trow.addToColumnValue(new TCell().setStringVal(entry.getValue().getName()));
                    if (partitionOffset.containsKey(entry.getKey())) {
                        // CONSUMPTION_STATUS
                        trow.addToColumnValue(new TCell()
                                .setStringVal(String.valueOf(partitionOffset.get(entry.getKey()))));
                        // LAG
                        trow.addToColumnValue(new TCell()
                                .setStringVal(String.valueOf(
                                        entry.getValue().getTso()
                                                - partitionOffset.get(entry.getKey()))));
                        // LAST_CONSUMPTION_TIME
                        if (partitionConsumptionTime.containsKey(entry.getKey())) {
                            trow.addToColumnValue(new TCell()
                                    .setLongVal(partitionConsumptionTime.get(entry.getKey())));
                        } else {
                            trow.addToColumnValue(new TCell().setLongVal(-1));
                        }
                    } else {
                        // CONSUMPTION_STATUS
                        trow.addToColumnValue(new TCell().setStringVal("N/A"));
                        // LAG
                        if (entry.getValue().hasData()) {
                            // for partition with data and no consumption yet, lag is N/A
                            trow.addToColumnValue(new TCell().setStringVal("N/A"));
                        } else {
                            trow.addToColumnValue(new TCell().setStringVal("0"));
                        }
                        // LAST_CONSUMPTION_TIME
                        trow.addToColumnValue(new TCell().setLongVal(-1));
                    }
                    dataBatch.add(trow);
                }
            } finally {
                table.readUnlock();
            }
        }
    }

    public boolean hasData(Partition partition) {
        // if all available visible data has been consumed, return false
        return  (!partitionOffset.containsKey(partition.getId())
                || !partitionOffset.get(partition.getId()).equals(partition.getTso()))
                && partition.hasData();
    }

    public boolean hasHistoricalData(long partitionId) {
        return historicalPartitionTSO.containsKey(partitionId);
    }

    public boolean hasConsumedData(long partitionId) {
        return partitionOffset.containsKey(partitionId);
    }

    public Pair<Long, Long> getStreamUpdate(Long partitionId) {
        // if partition has historical data, return <historical tso, current tso>
        // otherwise, return <current consumed tso, current tso>
        Long left = partitionOffset.get(partitionId);
        if (historicalPartitionTSO.containsKey(partitionId)) {
            left = historicalPartitionTSO.get(partitionId);
        }
        return Pair.of(left, getBaseTableNullable().getPartition(partitionId).getTso());
    }

    @Override
    public void unprotectedCheckStreamUpdate(AbstractTableStreamUpdate update)
            throws UserException {
        Preconditions.checkArgument(update instanceof OlapTableStreamUpdate);
        // check valid
        ((OlapTableStreamUpdate) update).checkPartitionOffset(getDBName(), getName(), historicalPartitionTSO,
                partitionOffset);
    }

    @Override
    public void unprotectedUpdateStreamUpdate(AbstractTableStreamUpdate update, Long ts) {
        Map<Long, Long> next = ((OlapTableStreamUpdate) update).getNext();
        for (Map.Entry<Long, Long> entry : next.entrySet()) {
            if (historicalPartitionTSO.containsKey(entry.getKey())) {
                historicalPartitionTSO.remove(entry.getKey());
            }
            partitionOffset.put(entry.getKey(), entry.getValue());
            partitionConsumptionTime.put(entry.getKey(), ts);
        }
    }

    Set<Long> unprotectedCollectStalePartitionOffsetIds(Set<Long> validPartitionIds) {
        Preconditions.checkState(isWriteLockHeldByCurrentThread(),
                "unprotectedCollectStalePartitionOffsetIds must be called with write lock held");
        Set<Long> stalePartitionIds = new HashSet<>();
        for (Long partitionId : partitionOffset.keySet()) {
            if (!validPartitionIds.contains(partitionId)) {
                stalePartitionIds.add(partitionId);
            }
        }
        for (Long partitionId : partitionConsumptionTime.keySet()) {
            if (!validPartitionIds.contains(partitionId)) {
                stalePartitionIds.add(partitionId);
            }
        }
        if (historicalPartitionTSO != null) {
            for (Long partitionId : historicalPartitionTSO.keySet()) {
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
        for (Long partitionId : partitionIds) {
            partitionOffset.remove(partitionId);
            partitionConsumptionTime.remove(partitionId);
            if (historicalPartitionTSO != null) {
                historicalPartitionTSO.remove(partitionId);
            }
        }
    }

    // The base table may undergo schema change, so the stream's schema cannot be cached and
    // must be computed dynamically from the current base table schema on every access.
    @Override
    public List<Column> getFullSchema() {
        OlapTable baseTable = getBaseTableNullable();
        if (baseTable == null) {
            return ImmutableList.of();
        }
        List<Column> baseTableSchema = baseTable.getBaseSchema(false);
        List<Column> rebuildFullSchema = ImmutableList.<Column>builderWithExpectedSize(baseTableSchema.size() + 2)
                .addAll(baseTableSchema)
                .add(Column.STREAM_SEQ_VIRTUAL_COLUMN)
                .add(Column.STREAM_CHANGE_TYPE_VIRTUAL_COLUMN)
                .build();
        return rebuildFullSchema;

    }

    // The base table may undergo schema change, so the stream's schema cannot be cached and
    // must be computed dynamically from the current base table schema on every access.
    @Override
    public List<Column> getBaseSchema(boolean full) {
        if (full) {
            return getFullSchema();
        } else {
            OlapTable baseTable = getBaseTableNullable();
            if (baseTable == null) {
                return ImmutableList.of();
            }
            return baseTable.getBaseSchema(false);
        }
    }
}
