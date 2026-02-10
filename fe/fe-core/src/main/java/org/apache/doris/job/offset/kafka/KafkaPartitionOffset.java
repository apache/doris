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

/**
 * Represents the offset range for a single Kafka partition in a streaming task.
 * 
 * Each StreamingInsertTask processes exactly one partition to ensure
 * exactly-once semantics and precise offset tracking.
 */
@Getter
@Setter
public class KafkaPartitionOffset implements Offset {
    
    /**
     * The Kafka partition ID
     */
    @SerializedName("partitionId")
    private int partitionId;
    
    /**
     * The starting offset for this task (inclusive)
     */
    @SerializedName("startOffset")
    private long startOffset;
    
    /**
     * The ending offset for this task (exclusive)
     */
    @SerializedName("endOffset")
    private long endOffset;
    
    /**
     * The actual number of rows consumed (populated after task completion)
     */
    @SerializedName("consumedRows")
    private long consumedRows;
    
    public KafkaPartitionOffset() {
    }
    
    public KafkaPartitionOffset(int partitionId, long startOffset, long endOffset) {
        this.partitionId = partitionId;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.consumedRows = 0;
    }
    
    @Override
    public String toSerializedJson() {
        return GsonUtils.GSON.toJson(this);
    }
    
    @Override
    public boolean isEmpty() {
        return startOffset >= endOffset;
    }
    
    @Override
    public boolean isValidOffset() {
        return startOffset >= 0 && endOffset > startOffset;
    }
    
    @Override
    public String showRange() {
        return String.format("{partition=%d, range=[%d, %d)}", partitionId, startOffset, endOffset);
    }
    
    @Override
    public String toString() {
        return String.format("{partitionId=%d, startOffset=%d, endOffset=%d, consumedRows=%d}",
                partitionId, startOffset, endOffset, consumedRows);
    }
    
    /**
     * Calculate the expected number of rows for this partition offset range.
     * Note: This is the maximum possible rows, actual consumed rows may be less
     * if there are gaps in the offset sequence.
     */
    public long getExpectedRows() {
        return endOffset - startOffset;
    }
}
