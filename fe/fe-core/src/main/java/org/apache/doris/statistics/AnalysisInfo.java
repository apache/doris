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

package org.apache.doris.statistics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AnalysisInfo {

    public enum AnalysisMode {
        INCREMENTAL,
        FULL
    }

    public enum AnalysisMethod {
        SAMPLE,
        FULL
    }

    public enum AnalysisType {
        COLUMN,
        INDEX,
        HISTOGRAM
    }

    public enum JobType {
        // submit by user directly
        MANUAL,
        // submit by system automatically
        SYSTEM
    }

    public enum ScheduleType {
        ONCE,
        PERIOD,
        AUTOMATIC
    }

    public final long jobId;

    public final String catalogName;

    public final String dbName;

    public final String tblName;

    public final Long indexId;

    public final JobType jobType;

    public final AnalysisMode analysisMode;

    public final AnalysisMethod analysisMethod;

    public final AnalysisType analysisType;

    public final ScheduleType scheduleType;

    public final int samplePercent;

    public final int sampleRows;

    public final int maxBucketNum;

    // finished or failed
    public final long lastExecTimeInMs;

    public final String message;

    public AnalysisState state;

    protected static final Logger LOG = LogManager.getLogger(AnalysisInfo.class);

    protected abstract static class Builder<T extends Builder<T>> {
        protected long jobId;
        protected String catalogName;
        protected String dbName;
        protected String tblName;
        protected Long indexId;
        protected JobType jobType;
        protected AnalysisMode analysisMode;
        protected AnalysisMethod analysisMethod;
        protected AnalysisType analysisType;
        protected ScheduleType scheduleType;
        protected AnalysisState state;
        protected int samplePercent;
        protected int sampleRows;
        protected int maxBucketNum;
        protected long lastExecTimeInMs;
        protected String message;

        public Builder() {
        }

        public Builder(AnalysisInfo analysisInfo) {
            this.jobId = analysisInfo.jobId;
            this.catalogName = analysisInfo.catalogName;
            this.dbName = analysisInfo.dbName;
            this.tblName = analysisInfo.tblName;
            this.indexId = analysisInfo.indexId;
            this.jobType = analysisInfo.jobType;
            this.analysisMode = analysisInfo.analysisMode;
            this.analysisMethod = analysisInfo.analysisMethod;
            this.analysisType = analysisInfo.analysisType;
            this.scheduleType = analysisInfo.scheduleType;
            this.state = analysisInfo.state;
            this.samplePercent = analysisInfo.samplePercent;
            this.sampleRows = analysisInfo.sampleRows;
            this.maxBucketNum = analysisInfo.maxBucketNum;
            this.lastExecTimeInMs = analysisInfo.lastExecTimeInMs;
            this.message = analysisInfo.message;
        }

        public T jobId(long jobId) {
            this.jobId = jobId;
            return self();
        }

        public T catalogName(String catalogName) {
            this.catalogName = catalogName;
            return self();
        }

        public T dbName(String dbName) {
            this.dbName = dbName;
            return self();
        }

        public T tblName(String tblName) {
            this.tblName = tblName;
            return self();
        }

        public T indexId(Long indexId) {
            this.indexId = indexId;
            return self();
        }

        public T jobType(JobType jobType) {
            this.jobType = jobType;
            return self();
        }

        public T analysisMode(AnalysisMode analysisMode) {
            this.analysisMode = analysisMode;
            return self();
        }

        public T analysisMethod(AnalysisMethod analysisMethod) {
            this.analysisMethod = analysisMethod;
            return self();
        }

        public T analysisType(AnalysisType analysisType) {
            this.analysisType = analysisType;
            return self();
        }

        public T scheduleType(ScheduleType scheduleType) {
            this.scheduleType = scheduleType;
            return self();
        }

        public T state(AnalysisState state) {
            this.state = state;
            return self();
        }

        public T samplePercent(int samplePercent) {
            this.samplePercent = samplePercent;
            return self();
        }

        public T sampleRows(int sampleRows) {
            this.sampleRows = sampleRows;
            return self();
        }

        public T maxBucketNum(int maxBucketNum) {
            this.maxBucketNum = maxBucketNum;
            return self();
        }

        public T lastExecTimeInMs(long lastExecTimeInMs) {
            this.lastExecTimeInMs = lastExecTimeInMs;
            return self();
        }

        public T message(String message) {
            this.message = message;
            return self();
        }

        protected abstract T self();

        public abstract AnalysisInfo build();
    }

    protected AnalysisInfo(Builder<?> builder) {
        this.jobId = builder.jobId;
        this.catalogName = builder.catalogName;
        this.dbName = builder.dbName;
        this.tblName = builder.tblName;
        this.indexId = builder.indexId;
        this.jobType = builder.jobType;
        this.analysisMode = builder.analysisMode;
        this.analysisMethod = builder.analysisMethod;
        this.analysisType = builder.analysisType;
        this.scheduleType = builder.scheduleType;
        this.state = builder.state;
        this.samplePercent = builder.samplePercent;
        this.sampleRows = builder.sampleRows;
        this.maxBucketNum = builder.maxBucketNum;
        this.lastExecTimeInMs = builder.lastExecTimeInMs;
        this.message = builder.message;
    }
}
