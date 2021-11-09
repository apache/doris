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


import java.io.Serializable;

/**
 * Doris read Options
 */
public class DorisReadOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private String readFields;
    private String filterQuery;
    private Integer requestTabletSize;
    private Integer requestConnectTimeoutMs;
    private Integer requestReadTimeoutMs;
    private Integer requestQueryTimeoutS;
    private Integer requestRetries;
    private Integer requestBatchSize;
    private Long execMemLimit;
    private Integer deserializeQueueSize;
    private Boolean deserializeArrowAsync;

    public DorisReadOptions(String readFields, String filterQuery, Integer requestTabletSize, Integer requestConnectTimeoutMs, Integer requestReadTimeoutMs,
                            Integer requestQueryTimeoutS, Integer requestRetries, Integer requestBatchSize, Long execMemLimit,
                            Integer deserializeQueueSize, Boolean deserializeArrowAsync) {
        this.readFields = readFields;
        this.filterQuery = filterQuery;
        this.requestTabletSize = requestTabletSize;
        this.requestConnectTimeoutMs = requestConnectTimeoutMs;
        this.requestReadTimeoutMs = requestReadTimeoutMs;
        this.requestQueryTimeoutS = requestQueryTimeoutS;
        this.requestRetries = requestRetries;
        this.requestBatchSize = requestBatchSize;
        this.execMemLimit = execMemLimit;
        this.deserializeQueueSize = deserializeQueueSize;
        this.deserializeArrowAsync = deserializeArrowAsync;
    }

    public String getReadFields() {
        return readFields;
    }

    public String getFilterQuery() {
        return filterQuery;
    }

    public Integer getRequestTabletSize() {
        return requestTabletSize;
    }

    public Integer getRequestConnectTimeoutMs() {
        return requestConnectTimeoutMs;
    }

    public Integer getRequestReadTimeoutMs() {
        return requestReadTimeoutMs;
    }

    public Integer getRequestRetries() {
        return requestRetries;
    }

    public Integer getRequestBatchSize() {
        return requestBatchSize;
    }

    public Integer getRequestQueryTimeoutS() {
        return requestQueryTimeoutS;
    }

    public Long getExecMemLimit() {
        return execMemLimit;
    }

    public Integer getDeserializeQueueSize() {
        return deserializeQueueSize;
    }

    public Boolean getDeserializeArrowAsync() {
        return deserializeArrowAsync;
    }


    public static Builder builder() {
        return new Builder();
    }

    public static DorisReadOptions defaults(){
        return DorisReadOptions.builder().build();
    }

    /**
     * Builder of {@link DorisReadOptions}.
     */
    public static class Builder {

        private String readFields;
        private String filterQuery;
        private Integer requestTabletSize;
        private Integer requestConnectTimeoutMs;
        private Integer requestReadTimeoutMs;
        private Integer requestQueryTimeoutS;
        private Integer requestRetries;
        private Integer requestBatchSize;
        private Long execMemLimit;
        private Integer deserializeQueueSize;
        private Boolean deserializeArrowAsync;


        public Builder setReadFields(String readFields) {
            this.readFields = readFields;
            return this;
        }

        public Builder setFilterQuery(String filterQuery) {
            this.filterQuery = filterQuery;
            return this;
        }

        public Builder setRequestTabletSize(Integer requestTabletSize) {
            this.requestTabletSize = requestTabletSize;
            return this;
        }

        public Builder setRequestConnectTimeoutMs(Integer requestConnectTimeoutMs) {
            this.requestConnectTimeoutMs = requestConnectTimeoutMs;
            return this;
        }

        public Builder setRequestReadTimeoutMs(Integer requestReadTimeoutMs) {
            this.requestReadTimeoutMs = requestReadTimeoutMs;
            return this;
        }

        public Builder setRequestQueryTimeoutS(Integer requesQueryTimeoutS) {
            this.requestQueryTimeoutS = requesQueryTimeoutS;
            return this;
        }

        public Builder setRequestRetries(Integer requestRetries) {
            this.requestRetries = requestRetries;
            return this;
        }

        public Builder setRequestBatchSize(Integer requestBatchSize) {
            this.requestBatchSize = requestBatchSize;
            return this;
        }

        public Builder setExecMemLimit(Long execMemLimit) {
            this.execMemLimit = execMemLimit;
            return this;
        }

        public Builder setDeserializeQueueSize(Integer deserializeQueueSize) {
            this.deserializeQueueSize = deserializeQueueSize;
            return this;
        }

        public Builder setDeserializeArrowAsync(Boolean deserializeArrowAsync) {
            this.deserializeArrowAsync = deserializeArrowAsync;
            return this;
        }

        public DorisReadOptions build() {
            return new DorisReadOptions(readFields, filterQuery, requestTabletSize, requestConnectTimeoutMs, requestReadTimeoutMs, requestQueryTimeoutS, requestRetries, requestBatchSize, execMemLimit, deserializeQueueSize, deserializeArrowAsync);
        }

    }


}
