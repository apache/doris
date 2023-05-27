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

import org.apache.doris.common.DdlException;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import java.util.Map;
import java.util.Set;


public class AnalysisJobInfo extends AnalysisInfo {

    public final long periodTimeInMs;

    public final Map<String, Set<Long>> colToPartitions;

    // TODO: use thrift
    public static AnalysisJobInfo fromResultRow(ResultRow resultRow) throws DdlException {
        long jobId = Long.parseLong(resultRow.getColumnValue("job_id"));
        String catalogName = resultRow.getColumnValue("catalog_name");
        String dbName = resultRow.getColumnValue("db_name");
        String tblName = resultRow.getColumnValue("tbl_name");
        long indexId = Long.parseLong(resultRow.getColumnValue("index_id"));
        String partitionNames = resultRow.getColumnValue("partitions");
        Map<String, Set<Long>> colToPartitions = StatisticsUtil.getColToPartition(partitionNames);
        JobType jobType = JobType.valueOf(resultRow.getColumnValue("job_type"));
        AnalysisType analysisType = AnalysisType.valueOf(resultRow.getColumnValue("analysis_type"));
        AnalysisMode analysisMode = AnalysisMode.valueOf(resultRow.getColumnValue("analysis_mode"));
        AnalysisMethod analysisMethod = AnalysisMethod.valueOf(resultRow.getColumnValue("analysis_method"));
        ScheduleType scheduleType = ScheduleType.valueOf(resultRow.getColumnValue("schedule_type"));
        AnalysisState state = AnalysisState.valueOf(resultRow.getColumnValue("state"));
        String samplePercentStr = resultRow.getColumnValue("sample_percent");
        int samplePercent = StatisticsUtil.convertStrToInt(samplePercentStr);
        String sampleRowsStr = resultRow.getColumnValue("sample_rows");
        int sampleRows = StatisticsUtil.convertStrToInt(sampleRowsStr);
        String maxBucketNumStr = resultRow.getColumnValue("max_bucket_num");
        int maxBucketNum = StatisticsUtil.convertStrToInt(maxBucketNumStr);
        String periodTimeInMsStr = resultRow.getColumnValue("period_time_in_ms");
        int periodTimeInMs = StatisticsUtil.convertStrToInt(periodTimeInMsStr);
        String lastExecTimeInMsStr = resultRow.getColumnValue("last_exec_time_in_ms");
        long lastExecTimeInMs = StatisticsUtil.convertStrToLong(lastExecTimeInMsStr);
        String message = resultRow.getColumnValue("message");

        return new AnalysisJobInfo.Builder()
                .jobId(jobId)
                .catalogName(catalogName)
                .dbName(dbName)
                .tblName(tblName)
                .indexId(indexId)
                .colToPartitions(colToPartitions)
                .jobType(jobType)
                .analysisMode(analysisMode)
                .analysisMethod(analysisMethod)
                .analysisType(analysisType)
                .scheduleType(scheduleType)
                .state(state)
                .samplePercent(samplePercent)
                .sampleRows(sampleRows)
                .maxBucketNum(maxBucketNum)
                .periodTimeInMs(periodTimeInMs)
                .lastExecTimeInMs(lastExecTimeInMs)
                .message(message)
                .build();
    }

    public static class Builder extends AnalysisInfo.Builder<Builder> {
        protected long periodTimeInMs;
        private Map<String, Set<Long>> colToPartitions;

        public Builder() {
        }

        public Builder(AnalysisJobInfo jobInfo) {
            super(jobInfo);
            this.periodTimeInMs = jobInfo.periodTimeInMs;
            this.colToPartitions = jobInfo.colToPartitions;
        }

        public Builder periodTimeInMs(long periodTimeInMs) {
            this.periodTimeInMs = periodTimeInMs;
            return this;
        }

        public Builder colToPartitions(Map<String, Set<Long>> colToPartitions) {
            this.colToPartitions = colToPartitions;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public AnalysisJobInfo build() {
            return new AnalysisJobInfo(this);
        }
    }

    private AnalysisJobInfo(Builder builder) {
        super(builder);
        this.periodTimeInMs = builder.periodTimeInMs;
        this.colToPartitions = builder.colToPartitions;
    }
}
