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

package org.apache.doris.job.offset.kafka;

import org.apache.doris.job.offset.Offset;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents the overall Kafka offset state for all partitions of a topic.
 * 
 * This class tracks the current consumption position for each partition
 * and is used for persistence and recovery of streaming job state.
 */
@Getter
@Setter
public class KafkaOffset implements Offset {
    
    /**
     * Map of partition ID to current offset (next offset to consume)
     */
    @SerializedName("partitionOffsets")
    private Map<Integer, Long> partitionOffsets;
    
    /**
     * The Kafka topic name
     */
    @SerializedName("topic")
    private String topic;
    
    /**
     * The catalog name for the Trino Kafka connector
     */
    @SerializedName("catalogName")
    private String catalogName;
    
    /**
     * The database name (schema) in the Trino Kafka connector
     */
    @SerializedName("databaseName")
    private String databaseName;
    
    public KafkaOffset() {
        this.partitionOffsets = new HashMap<>();
    }
    
    public KafkaOffset(String topic, String catalogName, String databaseName) {
        this.topic = topic;
        this.catalogName = catalogName;
        this.databaseName = databaseName;
        this.partitionOffsets = new HashMap<>();
    }
    
    /**
     * Update the offset for a specific partition.
     * 
     * @param partitionId the partition ID
     * @param newOffset the new offset value
     */
    public void updatePartitionOffset(int partitionId, long newOffset) {
        partitionOffsets.put(partitionId, newOffset);
    }
    
    /**
     * Get the current offset for a specific partition.
     * 
     * @param partitionId the partition ID
     * @return the current offset, or 0 if not found
     */
    public long getPartitionOffset(int partitionId) {
        return partitionOffsets.getOrDefault(partitionId, 0L);
    }
    
    /**
     * Initialize offsets for a list of partitions.
     * 
     * @param partitionIds list of partition IDs
     * @param initialOffset the initial offset value for each partition
     */
    public void initializePartitions(Iterable<Integer> partitionIds, long initialOffset) {
        for (Integer partitionId : partitionIds) {
            if (!partitionOffsets.containsKey(partitionId)) {
                partitionOffsets.put(partitionId, initialOffset);
            }
        }
    }
    
    @Override
    public String toSerializedJson() {
        return GsonUtils.GSON.toJson(this);
    }
    
    @Override
    public boolean isEmpty() {
        return partitionOffsets == null || partitionOffsets.isEmpty();
    }
    
    @Override
    public boolean isValidOffset() {
        return topic != null && !topic.isEmpty() && partitionOffsets != null;
    }
    
    @Override
    public String showRange() {
        if (partitionOffsets == null || partitionOffsets.isEmpty()) {
            return "{}";
        }
        String offsetsStr = partitionOffsets.entrySet().stream()
                .map(e -> String.format("p%d=%d", e.getKey(), e.getValue()))
                .collect(Collectors.joining(", "));
        return String.format("{topic=%s, offsets={%s}}", topic, offsetsStr);
    }
    
    @Override
    public String toString() {
        return String.format("KafkaOffset{topic='%s', catalog='%s', database='%s', partitionOffsets=%s}",
                topic, catalogName, databaseName, partitionOffsets);
    }
    
    /**
     * Create a deep copy of this KafkaOffset.
     * 
     * @return a new KafkaOffset instance with the same values
     */
    public KafkaOffset copy() {
        KafkaOffset copy = new KafkaOffset(topic, catalogName, databaseName);
        copy.partitionOffsets = new HashMap<>(this.partitionOffsets);
        return copy;
    }
}
