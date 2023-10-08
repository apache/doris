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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.CronExpression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class AnalysisInfo implements Writable {

    private static final Logger LOG = LogManager.getLogger(AnalysisInfo.class);

    // TODO: useless, remove it later
    public enum AnalysisMode {
        INCREMENTAL,
        FULL
    }

    public enum AnalysisMethod {
        SAMPLE,
        FULL
    }

    public enum AnalysisType {
        FUNDAMENTALS,
        INDEX,
        HISTOGRAM
    }

    public enum JobType {
        // submit by user directly
        MANUAL,
        // submit by system automatically
        SYSTEM;
    }

    public enum ScheduleType {
        // Job created by AutoCollector is also `ONCE` type, this is because it runs once only and should be removed
        // when its information is expired
        ONCE,
        PERIOD,
        AUTOMATIC
    }

    @SerializedName("jobId")
    public final long jobId;

    // When this AnalysisInfo represent a task, this is the task id for it.
    @SerializedName("taskId")
    public final long taskId;

    // When this AnalysisInfo represent a job, this is the list of task ids belong to this job.
    @SerializedName("taskIds")
    public final List<Long> taskIds;

    @SerializedName("catalogName")
    public final String catalogName;

    @SerializedName("dbName")
    public final String dbName;

    @SerializedName("tblName")
    public final String tblName;

    // TODO: Map here is wired, List is enough
    @SerializedName("colToPartitions")
    public final Map<String, Set<String>> colToPartitions;

    @SerializedName("partitionNames")
    public final Set<String> partitionNames;

    @SerializedName("colName")
    public final String colName;

    @SerializedName("indexId")
    public final long indexId;

    @SerializedName("jobType")
    public final JobType jobType;

    @SerializedName("analysisMode")
    public final AnalysisMode analysisMode;

    @SerializedName("analysisMethod")
    public final AnalysisMethod analysisMethod;

    @SerializedName("analysisType")
    public final AnalysisType analysisType;

    @SerializedName("samplePercent")
    public final int samplePercent;

    @SerializedName("sampleRows")
    public final long sampleRows;

    @SerializedName("maxBucketNum")
    public final int maxBucketNum;

    @SerializedName("periodTimeInMs")
    public final long periodTimeInMs;

    // finished or failed
    @SerializedName("lastExecTimeInMs")
    public long lastExecTimeInMs;

    // finished or failed
    @SerializedName("timeCostInMs")
    public long timeCostInMs;

    @SerializedName("state")
    public AnalysisState state;

    @SerializedName("scheduleType")
    public final ScheduleType scheduleType;

    @SerializedName("message")
    public String message;

    // True means this task is a table level task for external table.
    // This kind of task is mainly to collect the number of rows of a table.
    @SerializedName("externalTableLevelTask")
    public final boolean externalTableLevelTask;

    @SerializedName("partitionOnly")
    public final boolean partitionOnly;

    @SerializedName("samplingPartition")
    public final boolean samplingPartition;

    @SerializedName("isAllPartition")
    public final boolean isAllPartition;

    @SerializedName("partitionCount")
    public final long partitionCount;

    // For serialize
    @SerializedName("cronExpr")
    public String cronExprStr;

    @SerializedName("progress")
    public String progress;

    public CronExpression cronExpression;

    @SerializedName("forceFull")
    public final boolean forceFull;

    public AnalysisInfo(long jobId, long taskId, List<Long> taskIds, String catalogName, String dbName, String tblName,
            Map<String, Set<String>> colToPartitions, Set<String> partitionNames, String colName, Long indexId,
            JobType jobType, AnalysisMode analysisMode, AnalysisMethod analysisMethod, AnalysisType analysisType,
            int samplePercent, long sampleRows, int maxBucketNum, long periodTimeInMs, String message,
            long lastExecTimeInMs, long timeCostInMs, AnalysisState state, ScheduleType scheduleType,
            boolean isExternalTableLevelTask, boolean partitionOnly, boolean samplingPartition,
            boolean isAllPartition, long partitionCount, CronExpression cronExpression, boolean forceFull) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.taskIds = taskIds;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tblName = tblName;
        this.colToPartitions = colToPartitions;
        this.partitionNames = partitionNames;
        this.colName = colName;
        this.indexId = indexId;
        this.jobType = jobType;
        this.analysisMode = analysisMode;
        this.analysisMethod = analysisMethod;
        this.analysisType = analysisType;
        this.samplePercent = samplePercent;
        this.sampleRows = sampleRows;
        this.maxBucketNum = maxBucketNum;
        this.periodTimeInMs = periodTimeInMs;
        this.message = message;
        this.lastExecTimeInMs = lastExecTimeInMs;
        this.timeCostInMs = timeCostInMs;
        this.state = state;
        this.scheduleType = scheduleType;
        this.externalTableLevelTask = isExternalTableLevelTask;
        this.partitionOnly = partitionOnly;
        this.samplingPartition = samplingPartition;
        this.isAllPartition = isAllPartition;
        this.partitionCount = partitionCount;
        this.cronExpression = cronExpression;
        if (cronExpression != null) {
            this.cronExprStr = cronExpression.getCronExpression();
        }
        this.forceFull = forceFull;
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner("\n", getClass().getName() + ":\n", "\n");
        sj.add("JobId: " + jobId);
        sj.add("CatalogName: " + catalogName);
        sj.add("DBName: " + dbName);
        sj.add("TableName: " + tblName);
        sj.add("ColumnName: " + colName);
        sj.add("TaskType: " + analysisType);
        sj.add("TaskMode: " + analysisMode);
        sj.add("TaskMethod: " + analysisMethod);
        sj.add("Message: " + message);
        sj.add("CurrentState: " + state);
        if (samplePercent > 0) {
            sj.add("SamplePercent: " + samplePercent);
        }
        if (sampleRows > 0) {
            sj.add("SampleRows: " + sampleRows);
        }
        if (maxBucketNum > 0) {
            sj.add("MaxBucketNum: " + maxBucketNum);
        }
        if (colToPartitions != null) {
            sj.add("colToPartitions: " + getColToPartitionStr());
        }
        if (lastExecTimeInMs > 0) {
            sj.add("LastExecTime: " + StatisticsUtil.getReadableTime(lastExecTimeInMs));
        }
        if (timeCostInMs > 0) {
            sj.add("timeCost: " + timeCostInMs);
        }
        if (periodTimeInMs > 0) {
            sj.add("periodTimeInMs: " + StatisticsUtil.getReadableTime(periodTimeInMs));
        }
        if (StringUtils.isNotEmpty(cronExprStr)) {
            sj.add("cronExpr: " + cronExprStr);
        }
        sj.add("forceFull: " + forceFull);
        return sj.toString();
    }

    public AnalysisState getState() {
        return state;
    }

    public boolean isJob() {
        return taskId == -1;
    }

    public void addTaskId(long taskId) {
        taskIds.add(taskId);
    }

    public String getColToPartitionStr() {
        if (colToPartitions == null || colToPartitions.isEmpty()) {
            return "";
        }
        Gson gson = new Gson();
        return gson.toJson(colToPartitions);
    }

    private static Map<String, Set<String>> getColToPartition(String colToPartitionStr) {
        if (colToPartitionStr == null || colToPartitionStr.isEmpty()) {
            return null;
        }
        Gson gson = new Gson();
        Type type = new TypeToken<Map<String, Set<String>>>() {}.getType();
        return gson.fromJson(colToPartitionStr, type);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static AnalysisInfo read(DataInput dataInput) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_123) {
            AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
            analysisInfoBuilder.setJobId(dataInput.readLong());
            long taskId = dataInput.readLong();
            analysisInfoBuilder.setTaskId(taskId);
            analysisInfoBuilder.setCatalogName(Text.readString(dataInput));
            analysisInfoBuilder.setDbName(Text.readString(dataInput));
            analysisInfoBuilder.setTblName(Text.readString(dataInput));
            int size = dataInput.readInt();
            Map<String, Set<String>> colToPartitions = new HashMap<>();
            for (int i = 0; i < size; i++) {
                String k = Text.readString(dataInput);
                int partSize = dataInput.readInt();
                Set<String> parts = new HashSet<>();
                for (int j = 0; j < partSize; j++) {
                    parts.add(Text.readString(dataInput));
                }
                colToPartitions.put(k, parts);
            }
            analysisInfoBuilder.setColToPartitions(colToPartitions);
            analysisInfoBuilder.setColName(Text.readString(dataInput));
            analysisInfoBuilder.setIndexId(dataInput.readLong());
            analysisInfoBuilder.setJobType(JobType.valueOf(Text.readString(dataInput)));
            analysisInfoBuilder.setAnalysisMode(AnalysisMode.valueOf(Text.readString(dataInput)));
            analysisInfoBuilder.setAnalysisMethod(AnalysisMethod.valueOf(Text.readString(dataInput)));
            analysisInfoBuilder.setAnalysisType(AnalysisType.valueOf(Text.readString(dataInput)));
            analysisInfoBuilder.setSamplePercent(dataInput.readInt());
            analysisInfoBuilder.setSampleRows(dataInput.readInt());
            analysisInfoBuilder.setMaxBucketNum(dataInput.readInt());
            analysisInfoBuilder.setPeriodTimeInMs(dataInput.readLong());
            analysisInfoBuilder.setLastExecTimeInMs(dataInput.readLong());
            analysisInfoBuilder.setState(AnalysisState.valueOf(Text.readString(dataInput)));
            analysisInfoBuilder.setScheduleType(ScheduleType.valueOf(Text.readString(dataInput)));
            analysisInfoBuilder.setMessage(Text.readString(dataInput));
            analysisInfoBuilder.setExternalTableLevelTask(dataInput.readBoolean());
            return analysisInfoBuilder.build();
        } else {
            String json = Text.readString(dataInput);
            AnalysisInfo analysisInfo = GsonUtils.GSON.fromJson(json, AnalysisInfo.class);
            if (analysisInfo.cronExprStr != null) {
                try {
                    analysisInfo.cronExpression = new CronExpression(analysisInfo.cronExprStr);
                } catch (ParseException e) {
                    LOG.warn("Cron expression of job is invalid, there is a bug", e);
                }
            }
            return analysisInfo;
        }
    }
}
