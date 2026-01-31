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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TKinesisRLTaskProgress;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Progress tracking for Kinesis Routine Load jobs.
 * 
 * Kinesis uses sequence numbers instead of offsets like Kafka.
 * A sequence number is a unique identifier for each record within a shard.
 * Sequence numbers are string representations of 128-bit integers.
 * 
 * Special position values:
 * - TRIM_HORIZON: Start from the oldest record in the shard
 * - LATEST: Start from the newest record (records arriving after the iterator is created)
 * - AT_TIMESTAMP: Start from a specific timestamp
 * - Specific sequence number: Start from or after a specific sequence number
 */
public class KinesisProgress extends RoutineLoadProgress {
    private static final Logger LOG = LogManager.getLogger(KinesisProgress.class);

    // Special position constants
    public static final String POSITION_TRIM_HORIZON = "TRIM_HORIZON";
    public static final String POSITION_LATEST = "LATEST";
    public static final String POSITION_AT_TIMESTAMP = "AT_TIMESTAMP";

    // Internal representation for special positions
    // Using negative values since sequence numbers are always positive
    public static final String TRIM_HORIZON_VAL = "-2";
    public static final String LATEST_VAL = "-1";

    /**
     * Map from shard ID to sequence number.
     * The sequence number saved here is the next sequence number to be consumed.
     * 
     * Note: Unlike Kafka partitions which are integers, Kinesis shard IDs are strings
     * like "shardId-000000000000".
     */
    @SerializedName(value = "shardToSeqNum")
    private ConcurrentMap<String, String> shardIdToSequenceNumber = Maps.newConcurrentMap();

    private ReentrantLock lock = new ReentrantLock(true);

    public KinesisProgress() {
        super(LoadDataSourceType.KINESIS);
    }

    public KinesisProgress(TKinesisRLTaskProgress tKinesisRLTaskProgress) {
        super(LoadDataSourceType.KINESIS);
        this.shardIdToSequenceNumber = new ConcurrentHashMap<>();
        if (tKinesisRLTaskProgress.getShardCmtSeqNum() != null) {
            shardIdToSequenceNumber.putAll(tKinesisRLTaskProgress.getShardCmtSeqNum());
        }
    }

    public KinesisProgress(Map<String, String> shardIdToSequenceNumber) {
        super(LoadDataSourceType.KINESIS);
        this.shardIdToSequenceNumber = new ConcurrentHashMap<>();
        this.shardIdToSequenceNumber.putAll(shardIdToSequenceNumber);
    }

    public KinesisProgress(ConcurrentMap<String, String> shardIdToSequenceNumber) {
        super(LoadDataSourceType.KINESIS);
        this.shardIdToSequenceNumber = shardIdToSequenceNumber;
    }

    /**
     * Get sequence numbers for specified shard IDs.
     */
    public ConcurrentMap<String, String> getShardIdToSequenceNumber(List<String> shardIds) {
        ConcurrentMap<String, String> result = Maps.newConcurrentMap();
        for (Map.Entry<String, String> entry : shardIdToSequenceNumber.entrySet()) {
            for (String shardId : shardIds) {
                if (entry.getKey().equals(shardId)) {
                    result.put(shardId, entry.getValue());
                }
            }
        }
        return result;
    }

    /**
     * Add a shard with its starting position.
     */
    public void addShardPosition(Pair<String, String> shardPosition) {
        shardIdToSequenceNumber.put(shardPosition.first, shardPosition.second);
    }

    /**
     * Get the sequence number for a specific shard.
     */
    public String getSequenceNumberByShard(String shardId) {
        return shardIdToSequenceNumber.get(shardId);
    }

    /**
     * Get all shard to sequence number mappings.
     */
    public ConcurrentMap<String, String> getShardIdToSequenceNumber() {
        return shardIdToSequenceNumber;
    }

    /**
     * Check if the progress contains a specific shard.
     */
    public boolean containsShard(String shardId) {
        return shardIdToSequenceNumber.containsKey(shardId);
    }

    /**
     * Check if any shards are being tracked.
     */
    public boolean hasShards() {
        return !shardIdToSequenceNumber.isEmpty();
    }

    /**
     * Get human-readable progress information.
     */
    private void getReadableProgress(ConcurrentMap<String, String> showShardIdToPosition) {
        for (Map.Entry<String, String> entry : shardIdToSequenceNumber.entrySet()) {
            String position = entry.getValue();
            if (TRIM_HORIZON_VAL.equals(position)) {
                showShardIdToPosition.put(entry.getKey(), POSITION_TRIM_HORIZON);
            } else if (LATEST_VAL.equals(position)) {
                showShardIdToPosition.put(entry.getKey(), POSITION_LATEST);
            } else {
                // For actual sequence numbers, show the last consumed sequence number
                showShardIdToPosition.put(entry.getKey(), position);
            }
        }
    }

    /**
     * Check that all specified shards exist in the progress.
     */
    public void checkShards(List<Pair<String, String>> kinesisShardPositions) throws DdlException {
        for (Pair<String, String> pair : kinesisShardPositions) {
            if (!shardIdToSequenceNumber.containsKey(pair.first)) {
                throw new DdlException("The specified shard " + pair.first + " is not in the consumed shards");
            }
        }
    }

    /**
     * Modify the position for specific shards.
     */
    public void modifyPosition(List<Pair<String, String>> kinesisShardPositions) {
        for (Pair<String, String> pair : kinesisShardPositions) {
            shardIdToSequenceNumber.put(pair.first, pair.second);
        }
    }

    /**
     * Get shard ID and position pairs.
     */
    public List<Pair<String, String>> getShardPositionPairs(boolean alreadyConsumed) {
        List<Pair<String, String>> pairs = Lists.newArrayList();
        for (Map.Entry<String, String> entry : shardIdToSequenceNumber.entrySet()) {
            String position = entry.getValue();
            if (TRIM_HORIZON_VAL.equals(position)) {
                pairs.add(Pair.of(entry.getKey(), POSITION_TRIM_HORIZON));
            } else if (LATEST_VAL.equals(position)) {
                pairs.add(Pair.of(entry.getKey(), POSITION_LATEST));
            } else {
                // For actual sequence numbers
                pairs.add(Pair.of(entry.getKey(), position));
            }
        }
        return pairs;
    }

    /**
     * Calculate lag for each shard.
     * Note: Kinesis lag calculation is more complex than Kafka because:
     * 1. Sequence numbers are strings, not comparable integers
     * 2. GetRecords API returns millisBehindLatest which is more useful
     * 
     * This method returns -1 for shards where lag cannot be calculated.
     */
    public Map<String, Long> getLag(Map<String, Long> shardIdWithMillsBehindLatest) {
        Map<String, Long> lagMap = Maps.newHashMap();
        for (String shardId : shardIdToSequenceNumber.keySet()) {
            Long lag = shardIdWithMillsBehindLatest.get(shardId);
            lagMap.put(shardId, lag != null ? lag : -1L);
        }
        return lagMap;
    }

    @Override
    public String toString() {
        ConcurrentMap<String, String> showShardIdToPosition = Maps.newConcurrentMap();
        getReadableProgress(showShardIdToPosition);
        return "KinesisProgress [shardIdToSequenceNumber="
                + Joiner.on("|").withKeyValueSeparator("_").join(showShardIdToPosition) + "]";
    }

    @Override
    public String toJsonString() {
        ConcurrentMap<String, String> showShardIdToPosition = Maps.newConcurrentMap();
        getReadableProgress(showShardIdToPosition);
        Gson gson = new Gson();
        return gson.toJson(showShardIdToPosition);
    }

    @Override
    public void update(RLTaskTxnCommitAttachment attachment) {
        KinesisProgress newProgress = (KinesisProgress) attachment.getProgress();

        // Update sequence numbers for each shard
        // The sequence number in newProgress is the last successfully consumed sequence number
        // We store it directly (unlike Kafka where we add 1 to get the next offset)
        lock.lock();
        try {
            newProgress.shardIdToSequenceNumber.forEach((shardId, newSeqNum) -> {
                this.shardIdToSequenceNumber.put(shardId, newSeqNum);
            });
        } finally {
            lock.unlock();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("update kinesis progress: {}, task: {}, job: {}",
                    newProgress.toJsonString(), DebugUtil.printId(attachment.getTaskId()), attachment.getJobId());
        }
    }

    /**
     * Get total progress as the count of tracked shards.
     * Unlike Kafka where we sum offsets, Kinesis sequence numbers are not additive.
     */
    public Long totalProgress() {
        // Return the number of shards being tracked
        // Actual progress is better represented by millisBehindLatest
        return (long) shardIdToSequenceNumber.size();
    }
}
