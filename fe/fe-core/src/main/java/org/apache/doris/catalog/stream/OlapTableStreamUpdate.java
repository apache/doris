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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.stream.OlapTableStream.PartitionStreamState;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.transaction.TransactionCommitFailedException;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class OlapTableStreamUpdate extends AbstractTableStreamUpdate {
    // partitionId -> (previous offset, next complete consumption state). A null previous offset
    // denotes a partition with no recorded consumption offset, including newly added partitions.
    @SerializedName(value = "updates")
    private final Map<Long, Pair<Long, PartitionStreamState>> partitionUpdates;

    public OlapTableStreamUpdate() {
        this.partitionUpdates = new HashMap<>();
    }

    public OlapTableStreamUpdate(Map<Long, Pair<Long, PartitionStreamState>> partitionUpdates) {
        this.partitionUpdates = new HashMap<>(partitionUpdates);
    }

    public OlapTableStreamUpdate(Map<Long, Long> prev, Map<Long, Long> next) {
        this();
        Preconditions.checkArgument(next.keySet().containsAll(prev.keySet()),
                "Every previous offset must have a next consumption state");
        next.forEach((id, offset) -> partitionUpdates.put(id,
                Pair.of(prev.get(id), new PartitionStreamState(offset, -1, null))));
    }

    public Map<Long, Pair<Long, PartitionStreamState>> getPartitionUpdates() {
        return partitionUpdates;
    }

    /** Called before transaction persistence; all pending fields are then published together. */
    public void retainUnchangedCounts(Map<Long, PartitionStreamState> current) {
        partitionUpdates.replaceAll((id, update) -> isUnchangedOffset(update.second, current.get(id))
                ? Pair.of(update.first, current.get(id)) : update);
    }

    public void captureRowCounts(OlapTable table, Map<Long, PartitionStreamState> current) {
        partitionUpdates.replaceAll((id, update) -> isUnchangedOffset(update.second, current.get(id))
                || table.getPartition(id) == null ? update
                : Pair.of(update.first, update.second.withRowCounts(table, id)));
    }

    private boolean isUnchangedOffset(PartitionStreamState nextState, PartitionStreamState current) {
        return current != null && current.getOffset() != null
                && Objects.equals(current.getOffset(), nextState.getOffset());
    }

    public OlapTableStreamUpdate committedAt(long timestamp) {
        OlapTableStreamUpdate committed = new OlapTableStreamUpdate();
        partitionUpdates.forEach((id, update) -> committed.partitionUpdates.put(id,
                Pair.of(update.first, update.second.committedAt(timestamp, false))));
        return committed;
    }

    @Override
    public void merge(AbstractTableStreamUpdate other) {
        Preconditions.checkArgument(other instanceof OlapTableStreamUpdate);
        ((OlapTableStreamUpdate) other).getPartitionUpdates().forEach((id, update) ->
                partitionUpdates.merge(id, update, (existing, incoming) ->
                        Pair.of(incoming.first != null ? incoming.first : existing.first, incoming.second)));
    }

    public void checkPartitionOffset(String dbName, String streamName, Map<Long, PartitionStreamState> partitionStates)
            throws UserException {
        for (Map.Entry<Long, Pair<Long, PartitionStreamState>> entry : partitionUpdates.entrySet()) {
            Long previousOffset = entry.getValue().first;
            PartitionStreamState state = partitionStates.get(entry.getKey());
            if (previousOffset != null) {
                if (state != null && state.getHistoricalTso() != null) {
                    if (!state.getHistoricalTso().equals(-previousOffset)) {
                        throw new TransactionCommitFailedException("history offset not consumed: "
                                + dbName + '-' + streamName + '-' + entry.getKey() + '-' + previousOffset
                                + " vs " + state.getHistoricalTso());
                    }
                } else if (state != null && state.getOffset() != null
                        && !state.getOffset().equals(previousOffset)) {
                    throw new TransactionCommitFailedException("target offset already consumed: "
                            + dbName + '-' + streamName + '-' + entry.getKey() + '-' + previousOffset
                            + " vs " + state.getOffset());
                }
            } else if (state != null && state.getOffset() != null) {
                throw new TransactionCommitFailedException(
                        "previous version missing for partition=" + entry.getKey() + ", db=" + dbName
                                + ", stream=" + streamName);
            }
        }
    }
}
