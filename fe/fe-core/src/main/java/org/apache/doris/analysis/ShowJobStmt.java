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
import org.apache.doris.job.common.JobType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;

/**
 * SHOW JOB [FOR JobName]
 * eg: show event
 * return all job in connection db
 * eg: show event for test
 * return job named test in connection db
 */
public class ShowJobStmt extends ShowStmt {

    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("Id")
                    .add("Name")
                    .add("Definer")
                    .add("ExecuteType")
                    .add("RecurringStrategy")
                    .add("Status")
                    .add("ExecuteSql")
                    .add("CreateTime")
                    .add("Comment")
                    .build();

    private static final String MTMV_NAME_TITLE = "mtmv_name";

    private static final String NAME_TITLE = "name";
    private final LabelName labelName;

    @Getter
    private String dbFullName; // optional

    @Getter
    private JobType jobType; // optional

    /**
     * Supported job types, if we want to support more job types, we need to add them here.
     */
    @Getter
    private List<JobType> jobTypes = Arrays.asList(JobType.INSERT); // optional

    @Getter
    private String name; // optional
    @Getter
    private String pattern; // optional

    public ShowJobStmt(LabelName labelName, JobType jobType) {
        this.labelName = labelName;
        this.jobType = jobType;
        this.name = labelName == null ? null : labelName.getLabelName();
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        checkAuth();
    }

    private void checkAuth() throws AnalysisException {
        UserIdentity userIdentity = ConnectContext.get().getCurrentUserIdentity();
        if (!userIdentity.isRootUser()) {
            throw new AnalysisException("only root user can operate");
        }
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
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
