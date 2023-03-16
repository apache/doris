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

import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisTaskInfo.JobType;
import org.apache.doris.statistics.AnalysisTaskInfo.ScheduleType;

public class AnalysisTaskInfoBuilder {
    private long jobId;
    private long taskId;
    private String catalogName;
    private String dbName;
    private String tblName;
    private String colName;
    private Long indexId;
    private JobType jobType;
    private AnalysisMethod analysisMethod;
    private AnalysisType analysisType;
    private String message;
    private int lastExecTimeInMs;
    private AnalysisState state;
    private ScheduleType scheduleType;

    public AnalysisTaskInfoBuilder setJobId(long jobId) {
        this.jobId = jobId;
        return this;
    }

    public AnalysisTaskInfoBuilder setTaskId(long taskId) {
        this.taskId = taskId;
        return this;
    }

    public AnalysisTaskInfoBuilder setCatalogName(String catalogName) {
        this.catalogName = catalogName;
        return this;
    }

    public AnalysisTaskInfoBuilder setDbName(String dbName) {
        this.dbName = dbName;
        return this;
    }

    public AnalysisTaskInfoBuilder setTblName(String tblName) {
        this.tblName = tblName;
        return this;
    }

    public AnalysisTaskInfoBuilder setColName(String colName) {
        this.colName = colName;
        return this;
    }

    public AnalysisTaskInfoBuilder setIndexId(Long indexId) {
        this.indexId = indexId;
        return this;
    }

    public AnalysisTaskInfoBuilder setJobType(JobType jobType) {
        this.jobType = jobType;
        return this;
    }

    public AnalysisTaskInfoBuilder setAnalysisMethod(AnalysisMethod analysisMethod) {
        this.analysisMethod = analysisMethod;
        return this;
    }

    public AnalysisTaskInfoBuilder setAnalysisType(AnalysisType analysisType) {
        this.analysisType = analysisType;
        return this;
    }

    public AnalysisTaskInfoBuilder setMessage(String message) {
        this.message = message;
        return this;
    }

    public AnalysisTaskInfoBuilder setLastExecTimeInMs(int lastExecTimeInMs) {
        this.lastExecTimeInMs = lastExecTimeInMs;
        return this;
    }

    public AnalysisTaskInfoBuilder setState(AnalysisState state) {
        this.state = state;
        return this;
    }

    public AnalysisTaskInfoBuilder setScheduleType(ScheduleType scheduleType) {
        this.scheduleType = scheduleType;
        return this;
    }

    public AnalysisTaskInfo build() {
        return new AnalysisTaskInfo(jobId, taskId, catalogName, dbName, tblName, colName, indexId, jobType,
                analysisMethod, analysisType, message, lastExecTimeInMs, state, scheduleType);
    }
}
