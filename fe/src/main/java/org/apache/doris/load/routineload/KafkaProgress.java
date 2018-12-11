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

import com.google.common.base.Joiner;
import org.apache.doris.common.io.Writable;
import org.apache.doris.thrift.TKafkaRLTaskProgress;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * this is description of kafka routine load progress
 * the data before offset was already loaded in doris
 */
// {"partitionIdToOffset": {}}
public class KafkaProgress extends RoutineLoadProgress {

    private Map<Integer, Long> partitionIdToOffset;

    public KafkaProgress() {
    }

    public KafkaProgress(TKafkaRLTaskProgress tKafkaRLTaskProgress) {
        this.partitionIdToOffset = tKafkaRLTaskProgress.getPartitionIdToOffset();
    }

    public Map<Integer, Long> getPartitionIdToOffset() {
        return partitionIdToOffset;
    }

    public void setPartitionIdToOffset(Map<Integer, Long> partitionIdToOffset) {
        this.partitionIdToOffset = partitionIdToOffset;
    }

    @Override
    public void update(RoutineLoadProgress progress) {
        KafkaProgress newProgress = (KafkaProgress) progress;
        newProgress.getPartitionIdToOffset().entrySet().parallelStream()
                .forEach(entity -> partitionIdToOffset.put(entity.getKey(), entity.getValue()));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(partitionIdToOffset.size());
        for (Map.Entry entry : partitionIdToOffset.entrySet()) {
            out.writeInt((Integer) entry.getKey());
            out.writeLong((Long) entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        partitionIdToOffset = new HashMap<>();
        for (int i = 0; i < size; i++) {
            partitionIdToOffset.put(in.readInt(), in.readLong());
        }
    }

    @Override
    public String toString() {
        return "KafkaProgress [partitionIdToOffset="
                + Joiner.on("|").withKeyValueSeparator("_").join(partitionIdToOffset) + "]";
    }
}
