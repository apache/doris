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

package org.apache.doris.load.routineload;

import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TKafkaRLTaskProgress;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * this is description of kafka routine load progress
 * the data before offset was already loaded in Doris
 */
// {"partitionIdToOffset": {}}
public class KafkaProgress extends RoutineLoadProgress {
    public static final String OFFSET_BEGINNING = "OFFSET_BEGINNING"; // -2
    public static final String OFFSET_END = "OFFSET_END"; // -1
    // OFFSET_ZERO is just for show info, if user specified offset is 0
    public static final String OFFSET_ZERO = "OFFSET_ZERO";

    public static final long OFFSET_BEGINNING_VAL = -2;
    public static final long OFFSET_END_VAL = -1;

    // (partition id, begin offset)
    // the offset the next msg to be consumed
    private Map<Integer, Long> partitionIdToOffset = Maps.newConcurrentMap();

    public KafkaProgress() {
        super(LoadDataSourceType.KAFKA);
    }

    public KafkaProgress(TKafkaRLTaskProgress tKafkaRLTaskProgress) {
        super(LoadDataSourceType.KAFKA);
        this.partitionIdToOffset = tKafkaRLTaskProgress.getPartitionCmtOffset();
    }

    public Map<Integer, Long> getPartitionIdToOffset(List<Integer> partitionIds) {
        Map<Integer, Long> result = Maps.newHashMap();
        for (Map.Entry<Integer, Long> entry : partitionIdToOffset.entrySet()) {
            for (Integer partitionId : partitionIds) {
                if (entry.getKey().equals(partitionId)) {
                    result.put(partitionId, entry.getValue());
                }
            }
        }
        return result;
    }

    public void addPartitionOffset(Pair<Integer, Long> partitionOffset) {
        partitionIdToOffset.put(partitionOffset.first, partitionOffset.second);
    }

    public Long getOffsetByPartition(int kafkaPartition) {
        return partitionIdToOffset.get(kafkaPartition);
    }

    public boolean containsPartition(Integer kafkaPartition) {
        return partitionIdToOffset.containsKey(kafkaPartition);
    }

    public boolean hasPartition() {
        return !partitionIdToOffset.isEmpty();
    }

    // (partition id, end offset)
    // OFFSET_ZERO: user set offset == 0, no committed msg
    // OFFSET_END: user set offset = OFFSET_END, no committed msg
    // OFFSET_BEGINNING: user set offset = OFFSET_BEGINNING, no committed msg
    // other: current committed msg's offset
    private void getReadableProgress(Map<Integer, String> showPartitionIdToOffset) {
        for (Map.Entry<Integer, Long> entry : partitionIdToOffset.entrySet()) {
            if (entry.getValue() == 0) {
                showPartitionIdToOffset.put(entry.getKey(), OFFSET_ZERO);
            } else if (entry.getValue() == -1) {
                showPartitionIdToOffset.put(entry.getKey(), OFFSET_END);
            } else if (entry.getValue() == -2) {
                showPartitionIdToOffset.put(entry.getKey(), OFFSET_BEGINNING);
            } else {
                showPartitionIdToOffset.put(entry.getKey(), "" + (entry.getValue() - 1));
            }
        }
    }

    @Override
    public String toString() {
        Map<Integer, String> showPartitionIdToOffset = Maps.newHashMap();
        getReadableProgress(showPartitionIdToOffset);
        return "KafkaProgress [partitionIdToOffset="
                + Joiner.on("|").withKeyValueSeparator("_").join(showPartitionIdToOffset) + "]";
    }

    @Override
    public String toJsonString() {
        Map<Integer, String> showPartitionIdToOffset = Maps.newHashMap();
        getReadableProgress(showPartitionIdToOffset);
        Gson gson = new Gson();
        return gson.toJson(showPartitionIdToOffset);
    }

    @Override
    public void update(RoutineLoadProgress progress) {
        KafkaProgress newProgress = (KafkaProgress) progress;
        // + 1 to point to the next msg offset to be consumed
        newProgress.partitionIdToOffset.entrySet().stream()
                .forEach(entity -> this.partitionIdToOffset.put(entity.getKey(), entity.getValue() + 1));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(partitionIdToOffset.size());
        for (Map.Entry<Integer, Long> entry : partitionIdToOffset.entrySet()) {
            out.writeInt((Integer) entry.getKey());
            out.writeLong((Long) entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int size = in.readInt();
        partitionIdToOffset = new HashMap<>();
        for (int i = 0; i < size; i++) {
            partitionIdToOffset.put(in.readInt(), in.readLong());
        }
    }
}
