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
package org.apache.doris.flink.cfg;


import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Properties;

/**
 * JDBC sink batch options.
 */
public class DorisExecutionOptions implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final Integer DEFAULT_BATCH_SIZE = 10000;
    public static final Integer DEFAULT_MAX_RETRY_TIMES = 1;
    private static final Long DEFAULT_INTERVAL_MILLIS = 10000L;

    private final Integer batchSize;
    private final Integer maxRetries;
    private final Long batchIntervalMs;

    /**
     * Properties for the StreamLoad.
     */
    private final Properties streamLoadProp;

    private final Boolean enableDelete;


    public DorisExecutionOptions(Integer batchSize, Integer maxRetries, Long batchIntervalMs, Properties streamLoadProp, Boolean enableDelete) {
        Preconditions.checkArgument(maxRetries >= 0);
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.batchIntervalMs = batchIntervalMs;
        this.streamLoadProp = streamLoadProp;
        this.enableDelete = enableDelete;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static DorisExecutionOptions defaults() {
        Properties pro = new Properties();
        pro.setProperty("format", "json");
        pro.setProperty("strip_outer_array", "true");
        return new Builder().setStreamLoadProp(pro).build();
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public Integer getMaxRetries() {
        return maxRetries;
    }

    public Long getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public Properties getStreamLoadProp() {
        return streamLoadProp;
    }

    public Boolean getEnableDelete() {
        return enableDelete;
    }

    /**
     * Builder of {@link DorisExecutionOptions}.
     */
    public static class Builder {
        private Integer batchSize = DEFAULT_BATCH_SIZE;
        private Integer maxRetries = DEFAULT_MAX_RETRY_TIMES;
        private Long batchIntervalMs = DEFAULT_INTERVAL_MILLIS;
        private Properties streamLoadProp = new Properties();
        private Boolean enableDelete = false;

        public Builder setBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setMaxRetries(Integer maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder setBatchIntervalMs(Long batchIntervalMs) {
            this.batchIntervalMs = batchIntervalMs;
            return this;
        }

        public Builder setStreamLoadProp(Properties streamLoadProp) {
            this.streamLoadProp = streamLoadProp;
            return this;
        }

        public Builder setEnableDelete(Boolean enableDelete) {
            this.enableDelete = enableDelete;
            return this;
        }

        public DorisExecutionOptions build() {
            return new DorisExecutionOptions(batchSize, maxRetries, batchIntervalMs, streamLoadProp, enableDelete);
        }
    }


}
