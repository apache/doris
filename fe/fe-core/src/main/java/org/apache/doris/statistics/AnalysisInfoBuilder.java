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

import org.apache.doris.common.Pair;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.AnalysisInfo.ScheduleType;

import org.apache.logging.log4j.core.util.CronExpression;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AnalysisInfoBuilder {
    private long jobId;
    private long taskId;
    private List<Long> taskIds;
    private long catalogId;
    private long dbId;
    private long tblId;
    private Set<Pair<String, String>> jobColumns;
    private Set<String> partitionNames;
    private String colName;
    private long indexId = -1L;
    private JobType jobType;
    private AnalysisMethod analysisMethod;
    private AnalysisType analysisType;
    private int maxBucketNum;
    private int samplePercent;
    private long sampleRows;
    private long periodTimeInMs;
    private long lastExecTimeInMs;
    private long timeCostInMs;
    private AnalysisState state;
    private ScheduleType scheduleType;
    private String message = "";
    private boolean partitionOnly;
    private boolean samplingPartition;
    private boolean isAllPartition;
    private long partitionCount;
    private CronExpression cronExpression;
    private boolean forceFull;
    private boolean usingSqlForExternalTable;
    private long tblUpdateTime;
    private long rowCount;
    private boolean userInject;
    private long updateRows;
    private JobPriority priority;
    private Map<Long, Long> partitionUpdateRows;
    private boolean enablePartition;

    public AnalysisInfoBuilder() {
    }

    public AnalysisInfoBuilder(AnalysisInfo info) {
        jobId = info.jobId;
        taskId = info.taskId;
        taskIds = info.taskIds;
        catalogId = info.catalogId;
        dbId = info.dbId;
        tblId = info.tblId;
        jobColumns = info.jobColumns;
        partitionNames = info.partitionNames;
        colName = info.colName;
        indexId = info.indexId;
        jobType = info.jobType;
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
        partitionOnly = info.partitionOnly;
        samplingPartition = info.samplingPartition;
        isAllPartition = info.isAllPartition;
        partitionCount = info.partitionCount;
        cronExpression = info.cronExpression;
        forceFull = info.forceFull;
        usingSqlForExternalTable = info.usingSqlForExternalTable;
        tblUpdateTime = info.tblUpdateTime;
        rowCount = info.rowCount;
        userInject = info.userInject;
        updateRows = info.updateRows;
        priority = info.priority;
        partitionUpdateRows = info.partitionUpdateRows;
        enablePartition = info.enablePartition;
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

    public AnalysisInfoBuilder setCatalogId(long catalogId) {
        this.catalogId = catalogId;
        return this;
    }

    public AnalysisInfoBuilder setDBId(long dbId) {
        this.dbId = dbId;
        return this;
    }

    public AnalysisInfoBuilder setTblId(long tblId) {
        this.tblId = tblId;
        return this;
    }

    public AnalysisInfoBuilder setJobColumns(Set<Pair<String, String>> jobColumns) {
        this.jobColumns = jobColumns;
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

    public AnalysisInfoBuilder setSampleRows(long sampleRows) {
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

    public AnalysisInfoBuilder setPartitionOnly(boolean isPartitionOnly) {
        this.partitionOnly = isPartitionOnly;
        return this;
    }

    public AnalysisInfoBuilder setSamplingPartition(boolean samplingPartition) {
        this.samplingPartition = samplingPartition;
        return this;
    }

    public AnalysisInfoBuilder setAllPartition(boolean isAllPartition) {
        this.isAllPartition = isAllPartition;
        return this;
    }

    public AnalysisInfoBuilder setPartitionCount(long partitionCount) {
        this.partitionCount = partitionCount;
        return this;
    }

    public AnalysisInfoBuilder setCronExpression(CronExpression cronExpression) {
        this.cronExpression = cronExpression;
        return this;
    }

    public AnalysisInfoBuilder setForceFull(boolean forceFull) {
        this.forceFull = forceFull;
        return this;
    }

    public AnalysisInfoBuilder setUsingSqlForExternalTable(boolean usingSqlForExternalTable) {
        this.usingSqlForExternalTable = usingSqlForExternalTable;
        return this;
    }

    public AnalysisInfoBuilder setTblUpdateTime(long tblUpdateTime) {
        this.tblUpdateTime = tblUpdateTime;
        return this;
    }

    public AnalysisInfoBuilder setRowCount(long rowCount) {
        this.rowCount = rowCount;
        return this;
    }

    public AnalysisInfoBuilder setUserInject(boolean userInject) {
        this.userInject = userInject;
        return this;
    }

    public AnalysisInfoBuilder setUpdateRows(long updateRows) {
        this.updateRows = updateRows;
        return this;
    }

    public AnalysisInfoBuilder setPriority(JobPriority priority) {
        this.priority = priority;
        return this;
    }

    public AnalysisInfoBuilder setPartitionUpdateRows(Map<Long, Long> partitionUpdateRows) {
        this.partitionUpdateRows = partitionUpdateRows;
        return this;
    }

    public AnalysisInfoBuilder setEnablePartition(boolean enablePartition) {
        this.enablePartition = enablePartition;
        return this;
    }

    public AnalysisInfo build() {
        return new AnalysisInfo(jobId, taskId, taskIds, catalogId, dbId, tblId, jobColumns, partitionNames,
                colName, indexId, jobType, analysisMethod, analysisType, samplePercent,
                sampleRows, maxBucketNum, periodTimeInMs, message, lastExecTimeInMs, timeCostInMs, state, scheduleType,
                partitionOnly, samplingPartition, isAllPartition, partitionCount,
                cronExpression, forceFull, usingSqlForExternalTable, tblUpdateTime, rowCount, userInject, updateRows,
                priority, partitionUpdateRows, enablePartition);
    }

}
