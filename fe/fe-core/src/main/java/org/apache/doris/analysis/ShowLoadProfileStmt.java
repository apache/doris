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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

// For stmt like:
// show load profile "/";   # list all saving load job ids
// show load profile "/10014"  # show task ids of specified job
// show load profile "/10014/e0f7390f5363419e-b416a2a79996083e/" # show instance list of the task
// show load profile "/10014/e0f7390f5363419e-b416a2a79996083e/e0f7390f5363419e-b416a2a799960906" # show instance tree graph
public class ShowLoadProfileStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA_TASK_IDS =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("TaskId", ScalarType.createVarchar(128)))
                    .addColumn(new Column("ActiveTime", ScalarType.createVarchar(64)))
                    .build();

    public enum PathType {
        JOB_IDS,
        TASK_IDS,
        INSTANCES,
        SINGLE_INSTANCE
    }

    private String idPath;
    private PathType pathType;

    private String jobId = "";
    private String taskId = "";
    private String instanceId = "";

    public ShowLoadProfileStmt(String idPath) {
        this.idPath = idPath;
    }

    public PathType getPathType() {
        return pathType;
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getInstanceId() {
        return instanceId;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(idPath)) {
            // list all query ids
            pathType = PathType.JOB_IDS;
            return;
        }

        if (!idPath.startsWith("/")) {
            throw new AnalysisException("Path must starts with '/'");
        }
        pathType = PathType.JOB_IDS;
        String[] parts = idPath.split("/");
        if (parts.length > 4) {
            throw new AnalysisException("Path must in format '/jobId/taskId/instanceId'");
        }

        for (int i = 0; i < parts.length; i++) {
            switch (i) {
                case 0:
                    pathType = PathType.JOB_IDS;
                    continue;
                case 1:
                    jobId = parts[i];
                    pathType = PathType.TASK_IDS;
                    break;
                case 2:
                    taskId = parts[i];
                    pathType = PathType.INSTANCES;
                    break;
                case 3:
                    instanceId = parts[i];
                    pathType = PathType.SINGLE_INSTANCE;
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SHOW LOAD PROFILE ").append(idPath);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        switch (pathType) {
            case JOB_IDS:
                return ShowQueryProfileStmt.META_DATA_QUERY_IDS;
            case TASK_IDS:
                return META_DATA_TASK_IDS;
            case INSTANCES:
                return ShowQueryProfileStmt.META_DATA_INSTANCES;
            case SINGLE_INSTANCE:
                return ShowQueryProfileStmt.META_DATA_SINGLE_INSTANCE;
            default:
                return null;
        }
    }
}
