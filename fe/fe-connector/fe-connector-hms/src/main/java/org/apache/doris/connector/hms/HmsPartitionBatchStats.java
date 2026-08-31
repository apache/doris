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

package org.apache.doris.connector.hms;

import java.io.Serializable;

/** Immutable execution statistics for one logical HMS partition-object request. */
public final class HmsPartitionBatchStats implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int requestedItems;
    private final int rpcAttempts;
    private final long rpcItems;
    private final int largestBatchSize;
    private final int smallestBatchSize;
    private final int fallbackCount;
    private final long logicalElapsedNanos;
    private final long rpcElapsedNanos;
    private final long maxRpcElapsedNanos;

    private HmsPartitionBatchStats(Builder builder) {
        this.requestedItems = builder.requestedItems;
        this.rpcAttempts = builder.rpcAttempts;
        this.rpcItems = builder.rpcItems;
        this.largestBatchSize = builder.largestBatchSize;
        this.smallestBatchSize = builder.smallestBatchSize;
        this.fallbackCount = builder.fallbackCount;
        this.logicalElapsedNanos = builder.logicalElapsedNanos;
        this.rpcElapsedNanos = builder.rpcElapsedNanos;
        this.maxRpcElapsedNanos = builder.maxRpcElapsedNanos;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getRequestedItems() {
        return requestedItems;
    }

    public int getRpcAttempts() {
        return rpcAttempts;
    }

    public long getRpcItems() {
        return rpcItems;
    }

    public int getLargestBatchSize() {
        return largestBatchSize;
    }

    public int getSmallestBatchSize() {
        return smallestBatchSize;
    }

    public int getFallbackCount() {
        return fallbackCount;
    }

    public long getLogicalElapsedNanos() {
        return logicalElapsedNanos;
    }

    public long getRpcElapsedNanos() {
        return rpcElapsedNanos;
    }

    public long getMaxRpcElapsedNanos() {
        return maxRpcElapsedNanos;
    }

    /** Keeps physical-attempt statistics while replacing the outer logical-request dimensions. */
    public HmsPartitionBatchStats forLogicalRequest(int logicalRequestedItems, long logicalElapsed) {
        return builder()
                .requestedItems(logicalRequestedItems)
                .rpcAttempts(rpcAttempts)
                .rpcItems(rpcItems)
                .largestBatchSize(largestBatchSize)
                .smallestBatchSize(smallestBatchSize)
                .fallbackCount(fallbackCount)
                .logicalElapsedNanos(logicalElapsed)
                .rpcElapsedNanos(rpcElapsedNanos)
                .maxRpcElapsedNanos(maxRpcElapsedNanos)
                .build();
    }

    public static final class Builder {
        private int requestedItems;
        private int rpcAttempts;
        private long rpcItems;
        private int largestBatchSize;
        private int smallestBatchSize;
        private int fallbackCount;
        private long logicalElapsedNanos;
        private long rpcElapsedNanos;
        private long maxRpcElapsedNanos;

        private Builder() {
        }

        public Builder requestedItems(int requestedItems) {
            this.requestedItems = requestedItems;
            return this;
        }

        public Builder rpcAttempts(int rpcAttempts) {
            this.rpcAttempts = rpcAttempts;
            return this;
        }

        public Builder rpcItems(long rpcItems) {
            this.rpcItems = rpcItems;
            return this;
        }

        public Builder largestBatchSize(int largestBatchSize) {
            this.largestBatchSize = largestBatchSize;
            return this;
        }

        public Builder smallestBatchSize(int smallestBatchSize) {
            this.smallestBatchSize = smallestBatchSize;
            return this;
        }

        public Builder fallbackCount(int fallbackCount) {
            this.fallbackCount = fallbackCount;
            return this;
        }

        public Builder logicalElapsedNanos(long logicalElapsedNanos) {
            this.logicalElapsedNanos = logicalElapsedNanos;
            return this;
        }

        public Builder rpcElapsedNanos(long rpcElapsedNanos) {
            this.rpcElapsedNanos = rpcElapsedNanos;
            return this;
        }

        public Builder maxRpcElapsedNanos(long maxRpcElapsedNanos) {
            this.maxRpcElapsedNanos = maxRpcElapsedNanos;
            return this;
        }

        public HmsPartitionBatchStats build() {
            return new HmsPartitionBatchStats(this);
        }
    }
}
