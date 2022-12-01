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

import java.util.StringJoiner;

public class AnalysisTaskInfo {

    private static final Logger LOG = LogManager.getLogger(AnalysisTaskInfo.class);


    public enum AnalysisMethod {
        SAMPLE,
        FULL
    }

    public enum AnalysisType {
        COLUMN,
        INDEX
    }

    public enum JobType {
        // submit by user directly
        MANUAL,
        // submit by system automatically
        SYSTEM
    }

    public enum ScheduleType {
        ONCE,
        PERIOD
    }

    public final long jobId;

    public final long taskId;

    public final String catalogName;

    public final String dbName;

    public final String tblName;

    public final String colName;

    public final Long indexId;

    public final JobType jobType;

    public final AnalysisMethod analysisMethod;

    public final AnalysisType analysisType;

    public String message;

    // finished or failed
    public int lastExecTimeInMs = 0;

    public AnalysisState state;

    public final ScheduleType scheduleType;

    public AnalysisTaskInfo(long jobId, long taskId, String catalogName, String dbName, String tblName,
            String colName, Long indexId, JobType jobType,
            AnalysisMethod analysisMethod, AnalysisType analysisType, String message, int lastExecTimeInMs,
            AnalysisState state, ScheduleType scheduleType) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tblName = tblName;
        this.colName = colName;
        this.indexId = indexId;
        this.jobType = jobType;
        this.analysisMethod = analysisMethod;
        this.analysisType = analysisType;
        this.message = message;
        this.lastExecTimeInMs = lastExecTimeInMs;
        this.state = state;
        this.scheduleType = scheduleType;
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner("\n", getClass().getName() + ":\n", "\n");
        sj.add("JobId: " + String.valueOf(jobId));
        sj.add("CatalogName: " + catalogName);
        sj.add("DBName: " + dbName);
        sj.add("TableName: " + tblName);
        sj.add("ColumnName: " + colName);
        sj.add("TaskType: " + analysisType.toString());
        sj.add("TaskMethod: " + analysisMethod.toString());
        sj.add("Message: " + message);
        sj.add("LastExecTime: " + String.valueOf(lastExecTimeInMs));
        sj.add("CurrentState: " + state.toString());
        return sj.toString();
    }

    public AnalysisState getState() {
        return state;
    }
}
