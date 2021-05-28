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
import java.time.Duration;

/**
 * JDBC sink batch options.
 */
public class DorisExecutionOptions  implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Integer batchSize;
    private final Integer maxRetries;
    private final Long batchIntervalMs;

    public DorisExecutionOptions(Integer batchSize, Integer maxRetries,Long batchIntervalMs) {
        Preconditions.checkArgument(maxRetries >= 0);
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.batchIntervalMs = batchIntervalMs;
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

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder of {@link DorisExecutionOptions}.
     */
    public static class Builder {
        private Integer batchSize;
        private Integer maxRetries;
        private Long batchIntervalMs;

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

        public DorisExecutionOptions build() {
            return new DorisExecutionOptions(batchSize,maxRetries,batchIntervalMs);
        }
    }


}
