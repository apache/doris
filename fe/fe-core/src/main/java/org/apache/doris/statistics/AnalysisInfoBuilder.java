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

import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMode;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;

import org.quartz.CronExpression;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AnalysisInfoBuilder {
    private long jobId;
    private long taskId;
    private List<Long> taskIds;
    private String catalogName;
    private String dbName;
    private String tblName;
    private Map<String, Set<String>> colToPartitions;
    private Set<String> partitionNames;
    private String colName;
    private long indexId = -1L;
    private JobType jobType;
    private AnalysisMode analysisMode;
    private AnalysisMethod analysisMethod;
    private AnalysisType analysisType;
    private int maxBucketNum;
    private int samplePercent;
    private int sampleRows;
    private long periodTimeInMs;
    private long lastExecTimeInMs;
    private long timeCostInMs;
    private AnalysisState state;
    private ScheduleType scheduleType;
    private String message = "";
    private boolean externalTableLevelTask;
    private boolean partitionOnly;
    private boolean samplingPartition;

    private CronExpression cronExpression;

    public AnalysisInfoBuilder() {
    }

    public AnalysisInfoBuilder(AnalysisInfo info) {
        jobId = info.jobId;
        taskId = info.taskId;
        taskIds = info.taskIds;
        catalogName = info.catalogName;
        dbName = info.dbName;
        tblName = info.tblName;
        colToPartitions = info.colToPartitions;
        partitionNames = info.partitionNames;
        colName = info.colName;
        indexId = info.indexId;
        jobType = info.jobType;
        analysisMode = info.analysisMode;
        analysisMethod = info.analysisMethod;
        analysisType = info.analysisType;
        samplePercent = info.samplePercent;
        sampleRows = info.sampleRows;
        periodTimeInMs = info.periodTimeInMs;
        maxBucketNum = info.maxBucketNum;
        message = info.message;
        lastExecTimeInMs = info.lastExecTimeInMs;
        timeCostInMs = info.timeCostInMs;
        state = info.state;
        scheduleType = info.scheduleType;
        externalTableLevelTask = info.externalTableLevelTask;
        partitionOnly = info.partitionOnly;
        samplingPartition = info.samplingPartition;
    }

    public AnalysisInfoBuilder setJobId(long jobId) {
        this.jobId = jobId;
        return this;
    }

    public AnalysisInfoBuilder setTaskId(long taskId) {
        this.taskId = taskId;
        return this;
    }

    public AnalysisInfoBuilder setTaskIds(List<Long> taskIds) {
        this.taskIds = taskIds;
        return this;
    }

    public AnalysisInfoBuilder setCatalogName(String catalogName) {
        this.catalogName = catalogName;
        return this;
    }

    public AnalysisInfoBuilder setDbName(String dbName) {
        this.dbName = dbName;
        return this;
    }

    public AnalysisInfoBuilder setTblName(String tblName) {
        this.tblName = tblName;
        return this;
    }

    public AnalysisInfoBuilder setColToPartitions(Map<String, Set<String>> colToPartitions) {
        this.colToPartitions = colToPartitions;
        return this;
    }

    public AnalysisInfoBuilder setColName(String colName) {
        this.colName = colName;
        return this;
    }

    public AnalysisInfoBuilder setPartitionNames(Set<String> partitionNames) {
        this.partitionNames = partitionNames;
        return this;
    }

    public AnalysisInfoBuilder setIndexId(Long indexId) {
        this.indexId = indexId;
        return this;
    }

    public AnalysisInfoBuilder setJobType(JobType jobType) {
        this.jobType = jobType;
        return this;
    }

    public AnalysisInfoBuilder setAnalysisMode(AnalysisMode analysisMode) {
        this.analysisMode = analysisMode;
        return this;
    }

    public AnalysisInfoBuilder setAnalysisMethod(AnalysisMethod analysisMethod) {
        this.analysisMethod = analysisMethod;
        return this;
    }

    public AnalysisInfoBuilder setAnalysisType(AnalysisType analysisType) {
        this.analysisType = analysisType;
        return this;
    }

    public AnalysisInfoBuilder setMaxBucketNum(int maxBucketNum) {
        this.maxBucketNum = maxBucketNum;
        return this;
    }

    public AnalysisInfoBuilder setSamplePercent(int samplePercent) {
        this.samplePercent = samplePercent;
        return this;
    }

    public AnalysisInfoBuilder setSampleRows(int sampleRows) {
        this.sampleRows = sampleRows;
        return this;
    }

    public AnalysisInfoBuilder setPeriodTimeInMs(long periodTimeInMs) {
        this.periodTimeInMs = periodTimeInMs;
        return this;
    }

    public AnalysisInfoBuilder setMessage(String message) {
        this.message = message;
        return this;
    }

    public AnalysisInfoBuilder setLastExecTimeInMs(long lastExecTimeInMs) {
        this.lastExecTimeInMs = lastExecTimeInMs;
        return this;
    }

    public AnalysisInfoBuilder setTimeCostInMs(long timeCostInMs) {
        this.timeCostInMs = timeCostInMs;
        return this;
    }

    public AnalysisInfoBuilder setState(AnalysisState state) {
        this.state = state;
        return this;
    }

    public AnalysisInfoBuilder setScheduleType(ScheduleType scheduleType) {
        this.scheduleType = scheduleType;
        return this;
    }

    public AnalysisInfoBuilder setExternalTableLevelTask(boolean isTableLevel) {
        this.externalTableLevelTask = isTableLevel;
        return this;
    }

    public AnalysisInfoBuilder setPartitionOnly(boolean isPartitionOnly) {
        this.partitionOnly = isPartitionOnly;
        return this;
    }

    public AnalysisInfoBuilder setSamplingPartition(boolean samplingPartition) {
        this.samplingPartition = samplingPartition;
        return this;
    }

    public void setCronExpression(CronExpression cronExpression) {
        this.cronExpression = cronExpression;
    }

    public AnalysisInfo build() {
        return new AnalysisInfo(jobId, taskId, taskIds, catalogName, dbName, tblName, colToPartitions, partitionNames,
                colName, indexId, jobType, analysisMode, analysisMethod, analysisType, samplePercent,
                sampleRows, maxBucketNum, periodTimeInMs, message, lastExecTimeInMs, timeCostInMs, state, scheduleType,
                externalTableLevelTask, partitionOnly, samplingPartition, cronExpression);
    }

    public AnalysisInfoBuilder copy() {
        return new AnalysisInfoBuilder()
                .setJobId(jobId)
                .setTaskId(taskId)
                .setTaskIds(taskIds)
                .setCatalogName(catalogName)
                .setDbName(dbName)
                .setTblName(tblName)
                .setColToPartitions(colToPartitions)
                .setColName(colName)
                .setIndexId(indexId)
                .setJobType(jobType)
                .setAnalysisMode(analysisMode)
                .setAnalysisMethod(analysisMethod)
                .setAnalysisType(analysisType)
                .setSamplePercent(samplePercent)
                .setSampleRows(sampleRows)
                .setPeriodTimeInMs(periodTimeInMs)
                .setMaxBucketNum(maxBucketNum)
                .setMessage(message)
                .setLastExecTimeInMs(lastExecTimeInMs)
                .setTimeCostInMs(timeCostInMs)
                .setState(state)
                .setScheduleType(scheduleType)
                .setExternalTableLevelTask(externalTableLevelTask);
    }
}
