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
import org.apache.doris.common.UserException;
import org.apache.doris.job.common.JobType;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.util.List;

/**
 * SHOW JOB TASKS [FOR JobName]
 */
public class ShowJobTaskStmt extends ShowStmt {

    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("TaskId")
                    .add("JobId")
                    .add("JobName")
                    .add("CreateTime")
                    .add("StartTime")
                    .add("EndTime")
                    .add("Status")
                    .add("ExecuteSql")
                    .add("Result")
                    .add("ErrorMsg")
                    .build();

    @Getter
    private final LabelName labelName;

    @Getter
    private String name; // optional

    @Getter
    JobType jobType;

    public ShowJobTaskStmt(LabelName labelName, JobType jobType) {
        this.labelName = labelName;
        this.jobType = jobType;
        this.name = labelName == null ? null : labelName.getLabelName();
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        CreateJobStmt.checkAuth();
    }

    public static List<String> getTitleNames() {
        return TITLE_NAMES;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
