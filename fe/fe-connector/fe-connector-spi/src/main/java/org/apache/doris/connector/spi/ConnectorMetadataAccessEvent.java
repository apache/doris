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

package org.apache.doris.connector.spi;

import java.util.Objects;
import java.util.regex.Pattern;

/** One completed connector metadata access, emitted once for a logical request. */
public final class ConnectorMetadataAccessEvent {

    private static final Pattern OPERATION_PATTERN = Pattern.compile("[a-z][a-z0-9_.-]{0,127}");

    private final String operation;
    private final String source;
    private final int requestedItems;
    private final int rpcCount;
    private final long rpcItems;
    private final int largestBatchSize;
    private final int smallestBatchSize;
    private final int fallbackCount;
    private final long logicalElapsedMillis;
    private final long rpcElapsedMillis;
    private final long maxRpcElapsedMillis;
    private final boolean success;

    private ConnectorMetadataAccessEvent(Builder builder) {
        this.operation = builder.operation;
        this.source = builder.source;
        this.requestedItems = builder.requestedItems;
        this.rpcCount = builder.rpcCount;
        this.rpcItems = builder.rpcItems;
        this.largestBatchSize = builder.largestBatchSize;
        this.smallestBatchSize = builder.smallestBatchSize;
        this.fallbackCount = builder.fallbackCount;
        this.logicalElapsedMillis = builder.logicalElapsedMillis;
        this.rpcElapsedMillis = builder.rpcElapsedMillis;
        this.maxRpcElapsedMillis = builder.maxRpcElapsedMillis;
        this.success = builder.success;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getOperation() {
        return operation;
    }

    public String getSource() {
        return source;
    }

    public int getRequestedItems() {
        return requestedItems;
    }

    public int getRpcCount() {
        return rpcCount;
    }

    public int getLargestBatchSize() {
        return largestBatchSize;
    }

    public int getSmallestBatchSize() {
        return smallestBatchSize;
    }

    public long getRpcItems() {
        return rpcItems;
    }

    public int getFallbackCount() {
        return fallbackCount;
    }

    public long getLogicalElapsedMillis() {
        return logicalElapsedMillis;
    }

    public long getRpcElapsedMillis() {
        return rpcElapsedMillis;
    }

    public long getMaxRpcElapsedMillis() {
        return maxRpcElapsedMillis;
    }

    public boolean isSuccess() {
        return success;
    }

    public static final class Builder {
        private String operation;
        private String source;
        private int requestedItems;
        private int rpcCount;
        private long rpcItems;
        private int largestBatchSize;
        private int smallestBatchSize;
        private int fallbackCount;
        private long logicalElapsedMillis;
        private long rpcElapsedMillis;
        private long maxRpcElapsedMillis;
        private boolean success;

        private Builder() {
        }

        /**
         * Sets a stable, lower-case operation name. Values must describe an operation kind and must not contain
         * catalog, table, query, user, endpoint, or other request-specific identifiers.
         */
        public Builder operation(String operation) {
            this.operation = operation;
            return this;
        }

        /** Sets one of the bounded {@link ConnectorMetadataAccessSource} names. */
        public Builder source(String source) {
            this.source = source;
            return this;
        }

        public Builder requestedItems(int requestedItems) {
            this.requestedItems = requestedItems;
            return this;
        }

        public Builder rpcCount(int rpcCount) {
            this.rpcCount = rpcCount;
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

        public Builder rpcItems(long rpcItems) {
            this.rpcItems = rpcItems;
            return this;
        }

        public Builder fallbackCount(int fallbackCount) {
            this.fallbackCount = fallbackCount;
            return this;
        }

        public Builder logicalElapsedMillis(long logicalElapsedMillis) {
            this.logicalElapsedMillis = logicalElapsedMillis;
            return this;
        }

        public Builder rpcElapsedMillis(long rpcElapsedMillis) {
            this.rpcElapsedMillis = rpcElapsedMillis;
            return this;
        }

        public Builder maxRpcElapsedMillis(long maxRpcElapsedMillis) {
            this.maxRpcElapsedMillis = maxRpcElapsedMillis;
            return this;
        }

        public Builder success(boolean success) {
            this.success = success;
            return this;
        }

        public ConnectorMetadataAccessEvent build() {
            Objects.requireNonNull(operation, "operation");
            Objects.requireNonNull(source, "source");
            if (!OPERATION_PATTERN.matcher(operation).matches()) {
                throw new IllegalArgumentException(
                        "operation must be a stable lower-case metric name: " + operation);
            }
            ConnectorMetadataAccessSource.valueOf(source);
            requireNonNegative(requestedItems, "requestedItems");
            requireNonNegative(rpcCount, "rpcCount");
            requireNonNegative(rpcItems, "rpcItems");
            requireNonNegative(largestBatchSize, "largestBatchSize");
            requireNonNegative(smallestBatchSize, "smallestBatchSize");
            requireNonNegative(fallbackCount, "fallbackCount");
            requireNonNegative(logicalElapsedMillis, "logicalElapsedMillis");
            requireNonNegative(rpcElapsedMillis, "rpcElapsedMillis");
            requireNonNegative(maxRpcElapsedMillis, "maxRpcElapsedMillis");
            if (smallestBatchSize > largestBatchSize) {
                throw new IllegalArgumentException("smallestBatchSize must not exceed largestBatchSize");
            }
            return new ConnectorMetadataAccessEvent(this);
        }

        private static void requireNonNegative(long value, String field) {
            if (value < 0) {
                throw new IllegalArgumentException(field + " must be non-negative");
            }
        }
    }
}
