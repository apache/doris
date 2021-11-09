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

package org.apache.doris.load.sync.position;

import com.google.common.collect.Maps;

import java.util.Map;

public class PositionMeta<T> {
    // max batch id
    private long maxBatchId;
    // batch Id -> position range
    private Map<Long, PositionRange<T>> batches;
    // channel Id -> commit position
    private Map<Long, T> commitPositions;
    // ack position
    private T ackPosition;
    // ack time
    private long ackTime;

    public PositionMeta() {
        this.maxBatchId = -1L;
        this.batches = Maps.newHashMap();
        this.commitPositions = Maps.newHashMap();
    }

    public void addBatch(long batchId, PositionRange<T> range) {
        updateMaxBatchId(batchId);
        batches.put(batchId, range);
    }

    public PositionRange<T> removeBatch(long batchId) {
        return batches.remove(batchId);
    }

    public void clearAllBatch() {
        batches.clear();
    }

    public void setCommitPosition(long channelId, T position) {
        commitPositions.put(channelId, position);
    }

    public T getCommitPosition(long channelId) {
        return commitPositions.get(channelId);
    }

    public void setAckPosition(T ackPosition) {
        this.ackPosition = ackPosition;
    }

    public T getAckPosition() {
        return this.ackPosition;
    }

    public void setAckTime(long ackTime) {
        this.ackTime = ackTime;
    }

    public long getAckTime() {
        return this.ackTime;
    }

    public T getLatestPosition() {
        if (!batches.containsKey(maxBatchId)) {
            return null;
        } else {
            return batches.get(maxBatchId).getEnd();
        }
    }

    private void updateMaxBatchId(long batchId) {
        if (maxBatchId < batchId) {
            maxBatchId = batchId;
        }
    }

    public void cleanUp() {
        this.maxBatchId = -1L;
        this.batches.clear();
        this.commitPositions.clear();
    }
}