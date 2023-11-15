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
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TPulsarRLTaskProgress;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * this is description of pulsar routine load progress
 * the data before position was already loaded in StarRocks
 */
// {"partitionToBacklogNum": {}}
public class PulsarProgress extends RoutineLoadProgress {
    private static final Logger LOG = LogManager.getLogger(PulsarProgress.class);

    // (partition, backlog num)
    @SerializedName("pbl")
    private Map<String, Long> partitionToBacklogNum = Maps.newConcurrentMap();
    // Initial positions will only be used at first schedule
    private Map<String, Long> partitionToInitialPosition = Maps.newConcurrentMap();

    public PulsarProgress() {
        super(LoadDataSourceType.PULSAR);
    }

    public PulsarProgress(TPulsarRLTaskProgress tPulsarRLTaskProgress) {
        super(LoadDataSourceType.PULSAR);
        this.partitionToBacklogNum = tPulsarRLTaskProgress.getPartitionBacklogNum();
    }

    public Map<String, Long> getPartitionToInitialPosition(List<String> partitions) {
        Map<String, Long> result = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : partitionToInitialPosition.entrySet()) {
            for (String partition : partitions) {
                if (entry.getKey().equals(partition)) {
                    result.put(partition, entry.getValue());
                }
            }
        }
        return result;
    }

    public List<Long> getBacklogNums() {
        return new ArrayList<Long>(partitionToBacklogNum.values());
    }

    public Long getInitialPosition(String partition) {
        if (partitionToInitialPosition.containsKey(partition)) {
            return partitionToInitialPosition.get(partition);
        } else {
            return -1L;
        }
    }

    public void addPartitionToInitialPosition(Pair<String, Long> partitionToInitialPosition) {
        this.partitionToInitialPosition.put(partitionToInitialPosition.first, partitionToInitialPosition.second);
    }

    public void modifyInitialPositions(List<Pair<String, Long>> partitionInitialPositions) {
        for (Pair<String, Long> pair : partitionInitialPositions) {
            this.partitionToInitialPosition.put(pair.first, pair.second);
        }
    }

    public void unprotectUpdate(List<String> currentPartitions, Long defaultInitialPosition) {
        partitionToInitialPosition.keySet().stream()
            .filter(entry -> !currentPartitions.contains(entry))
            .forEach(entry -> partitionToInitialPosition.remove(entry));

        if (defaultInitialPosition != null) {
            currentPartitions.stream()
                .filter(entry -> !partitionToInitialPosition.containsKey(entry))
                    .forEach(entry -> partitionToInitialPosition.put(entry, defaultInitialPosition));
        }
    }

    private void getReadableProgress(Map<String, String> showPartitionIdToPosition) {
        for (Map.Entry<String, Long> entry : partitionToBacklogNum.entrySet()) {
            showPartitionIdToPosition.put(entry.getKey() + "(BacklogNum)", String.valueOf(entry.getValue()));
        }
    }

    public Map<String, Long> getLag() {
        return partitionToBacklogNum;
    }

    @Override
    public String toString() {
        Map<String, String> showPartitionToBacklogNum = Maps.newHashMap();
        getReadableProgress(showPartitionToBacklogNum);
        return "PulsarProgress [partitionToBacklogNum="
            + Joiner.on("|").withKeyValueSeparator("_").join(showPartitionToBacklogNum) + "]";
    }

    @Override
    public String toJsonString() {
        Map<String, String> showPartitionToBacklogNum = Maps.newHashMap();
        getReadableProgress(showPartitionToBacklogNum);
        Gson gson = new Gson();
        return gson.toJson(showPartitionToBacklogNum);
    }

    @Override
    public void update(RLTaskTxnCommitAttachment attachment) {
        PulsarProgress newProgress = (PulsarProgress) attachment.getProgress();
        for (Map.Entry<String, Long> entry : newProgress.partitionToBacklogNum.entrySet()) {
            String partition = entry.getKey();
            Long backlogNum = entry.getValue();
            // Update progress
            this.partitionToBacklogNum.put(partition, backlogNum);
            // Remove initial position if exists
            partitionToInitialPosition.remove(partition);
        }
        LOG.debug("update pulsar progress: {}, task: {}, job: {}",
                newProgress.toJsonString(), DebugUtil.printId(attachment.getTaskId()), attachment.getJobId());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(partitionToBacklogNum.size());
        for (Map.Entry<String, Long> entry : partitionToBacklogNum.entrySet()) {
            Text.writeString(out, entry.getKey());
            out.writeLong((Long) entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int size = in.readInt();
        partitionToBacklogNum = new HashMap<>();
        for (int i = 0; i < size; i++) {
            partitionToBacklogNum.put(Text.readString(in), in.readLong());
        }
    }
}
