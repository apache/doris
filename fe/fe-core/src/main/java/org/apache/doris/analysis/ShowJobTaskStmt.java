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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * SHOW JOB TASKS [FOR JobName]
 */
public class ShowJobTaskStmt extends ShowStmt {

    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("JobId")
                    .add("TaskId")
                    .add("StartTime")
                    .add("EndTime")
                    .add("Status")
                    .add("ErrorMsg")
                    .build();

    private final LabelName labelName;
    private String dbFullName; // optional
    private String name; // optional

    public ShowJobTaskStmt(LabelName labelName) {
        this.labelName = labelName;
    }

    public String getDbFullName() {
        return dbFullName;
    }

    public String getName() {
        return name;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        checkAuth();
        checkLabelName(analyzer);
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
        if (null == labelName) {
            throw new AnalysisException("Job name is null");
        }
        name = labelName.getLabelName();
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
