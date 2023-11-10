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
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.scheduler.constants.JobCategory;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

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
                    .add("lastExecuteTaskStatus")
                    .add("CreateTime")
                    .add("Comment")
                    .build();

    private static final String MTMV_NAME_TITLE = "mtmv_name";

    private static final String NAME_TITLE = "name";
    private final LabelName labelName;

    @Getter
    private String dbFullName; // optional

    @Getter
    private JobCategory jobCategory; // optional

    private String jobCategoryName; // optional

    @Getter
    private String name; // optional
    @Getter
    private String pattern; // optional

    public ShowJobStmt(String category, LabelName labelName, String pattern) {
        this.labelName = labelName;
        this.pattern = pattern;
        this.jobCategoryName = category;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        checkAuth();
        checkLabelName(analyzer);
        if (StringUtils.isBlank(jobCategoryName)) {
            this.jobCategory = JobCategory.SQL;
        } else {
            this.jobCategory = JobCategory.valueOf(jobCategoryName.toUpperCase());
        }
    }

    private void checkAuth() throws AnalysisException {
        UserIdentity userIdentity = ConnectContext.get().getCurrentUserIdentity();
        if (!userIdentity.isRootUser()) {
            throw new AnalysisException("only root user can operate");
        }
    }

    private void checkLabelName(Analyzer analyzer) throws AnalysisException {
        String dbName = labelName == null ? null : labelName.getDbName();
        if (Strings.isNullOrEmpty(dbName)) {
            dbFullName = analyzer.getContext().getDatabase();
            if (Strings.isNullOrEmpty(dbFullName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            dbFullName = ClusterNamespace.getFullName(getClusterName(), dbName);
        }
        name = labelName == null ? null : labelName.getLabelName();
    }

    public static List<String> getTitleNames() {
        return TITLE_NAMES;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : TITLE_NAMES) {
            if (this.jobCategory.equals(JobCategory.MTMV) && title.equals(NAME_TITLE)) {
                builder.addColumn(new Column(MTMV_NAME_TITLE, ScalarType.createVarchar(30)));
            }
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
