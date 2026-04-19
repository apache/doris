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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Scan range for MaxCompute (ODPS).
 *
 * <p>Carries the Base64-serialized {@code TableBatchReadSession} and split
 * parameters that BE needs to read via the ODPS Storage API.
 * {@link #getTableFormatType()} returns {@code "max_compute"} so that
 * {@code PluginDrivenScanNode} builds a {@code TMaxComputeFileDesc} instead
 * of the generic jdbc_params map.</p>
 */
public class MaxComputeScanRange implements ConnectorScanRange {
    private static final long serialVersionUID = 1L;

    public static final String PROP_SCAN_SERIALIZE = "table_batch_read_session";
    public static final String PROP_SESSION_ID = "session_id";
    public static final String PROP_SPLIT_TYPE = "split_type";
    public static final String PROP_READ_TIMEOUT = "read_timeout";
    public static final String PROP_CONNECT_TIMEOUT = "connect_timeout";
    public static final String PROP_RETRY_TIMES = "retry_times";

    public static final String SPLIT_TYPE_BYTE_SIZE = "byte_size";
    public static final String SPLIT_TYPE_ROW_OFFSET = "row_offset";

    private final long start;
    private final long length;
    private final Map<String, String> properties;

    private MaxComputeScanRange(Builder builder) {
        this.start = builder.start;
        this.length = builder.length;
        this.properties = Collections.unmodifiableMap(builder.properties);
    }

    @Override
    public ConnectorScanRangeType getRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public String getTableFormatType() {
        return "max_compute";
    }

    @Override
    public Optional<String> getPath() {
        String splitType = properties.getOrDefault(PROP_SPLIT_TYPE, SPLIT_TYPE_BYTE_SIZE);
        if (SPLIT_TYPE_BYTE_SIZE.equals(splitType)) {
            return Optional.of("/byte_size");
        }
        return Optional.of("/row_offset");
    }

    @Override
    public long getStart() {
        return start;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public List<String> getHosts() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for MaxComputeScanRange.
     */
    public static class Builder {
        private long start;
        private long length = -1;
        private final Map<String, String> properties = new HashMap<>();

        public Builder start(long start) {
            this.start = start;
            return this;
        }

        public Builder length(long length) {
            this.length = length;
            return this;
        }

        public Builder scanSerialize(String serializedSession) {
            properties.put(PROP_SCAN_SERIALIZE, serializedSession);
            return this;
        }

        public Builder sessionId(String sessionId) {
            properties.put(PROP_SESSION_ID, sessionId);
            return this;
        }

        public Builder splitType(String splitType) {
            properties.put(PROP_SPLIT_TYPE, splitType);
            return this;
        }

        public Builder readTimeout(int readTimeout) {
            properties.put(PROP_READ_TIMEOUT, String.valueOf(readTimeout));
            return this;
        }

        public Builder connectTimeout(int connectTimeout) {
            properties.put(PROP_CONNECT_TIMEOUT, String.valueOf(connectTimeout));
            return this;
        }

        public Builder retryTimes(int retryTimes) {
            properties.put(PROP_RETRY_TIMES, String.valueOf(retryTimes));
            return this;
        }

        public Builder property(String key, String value) {
            properties.put(key, value);
            return this;
        }

        public MaxComputeScanRange build() {
            return new MaxComputeScanRange(this);
        }
    }
}
