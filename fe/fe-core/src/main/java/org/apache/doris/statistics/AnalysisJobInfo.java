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

import java.util.Objects;
import java.util.StringJoiner;

public class AnalysisJobInfo {

    private static final Logger LOG = LogManager.getLogger(AnalysisJobInfo.class);

    public enum JobState {
        PENDING,
        RUNNING,
        FINISHED,
        FAILED;
    }

    public enum AnalysisType {
        SAMPLE,
        FULL;
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

    public final String catalogName;

    public final String dbName;

    public final String tblName;

    public final String colName;

    public final JobType jobType;

    public AnalysisType analysisType;

    public String message;

    // finished or failed
    public int lastExecTimeInMs = 0;

    private JobState state;

    public final ScheduleType scheduleType;

    public AnalysisJobInfo(long jobId, String catalogName, String dbName, String tblName, String colName,
            JobType jobType, ScheduleType scheduleType) {
        this.jobId = jobId;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tblName = tblName;
        this.colName = colName;
        this.jobType = jobType;
        this.scheduleType = scheduleType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, dbName, tblName, colName, analysisType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AnalysisJobInfo other = (AnalysisJobInfo) obj;
        return catalogName.equals(other.catalogName)
                && dbName.equals(other.dbName)
                && tblName.equals(other.tblName)
                && colName.equals(other.colName)
                && analysisType.equals(other.analysisType);
    }

    // TODO: log to meta
    public void updateState(JobState jobState) {
        this.state = jobState;
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner("\n", getClass().getName() + ":\n", "\n");
        sj.add("JobId: " + String.valueOf(jobId));
        sj.add("CatalogName: " + catalogName);
        sj.add("DBName: " + dbName);
        sj.add("TableName: " + tblName);
        sj.add("ColumnName: " + colName);
        sj.add("JobType: " + analysisType.toString());
        sj.add("Message: " + message);
        sj.add("LastExecTime: " + String.valueOf(lastExecTimeInMs));
        sj.add("CurrentState: " + state.toString());
        return sj.toString();
    }

    public JobState getState() {
        return state;
    }
}
