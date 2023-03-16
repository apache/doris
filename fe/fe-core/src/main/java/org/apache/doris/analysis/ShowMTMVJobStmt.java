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
import org.apache.doris.common.UserException;
import org.apache.doris.mtmv.metadata.MTMVJob;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

public class ShowMTMVJobStmt extends ShowStmt {

    private final String jobName; // optional
    private final String dbName; // optional
    private final TableName mvName; // optional

    private String analyzedDbName;

    public ShowMTMVJobStmt() {
        this.jobName = null;
        this.dbName = null;
        this.mvName = null;
    }

    public ShowMTMVJobStmt(String jobName) {
        this.jobName = jobName;
        this.dbName = null;
        this.mvName = null;
    }

    public ShowMTMVJobStmt(String dbName, TableName mvName) {
        this.jobName = null;
        this.dbName = dbName;
        this.mvName = mvName;
    }

    public boolean isShowAllJobs() {
        return analyzedDbName == null && mvName == null && jobName == null;
    }

    public boolean isShowAllJobsFromDb() {
        return analyzedDbName != null && mvName == null;
    }

    public boolean isShowAllJobsOnMv() {
        return mvName != null;
    }

    public boolean isSpecificJob() {
        return jobName != null;
    }

    public String getDbName() {
        return analyzedDbName;
    }

    public String getMVName() {
        return mvName != null ? mvName.getTbl() : null;
    }

    public String getJobName() {
        return jobName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (dbName != null && mvName != null && mvName.getDb() != null && !dbName.equals(mvName.getDb())) {
            throw new UserException("Database name should be same when they both been set.");
        }
        analyzedDbName = dbName;
        if (Strings.isNullOrEmpty(analyzedDbName)) {
            if (mvName != null) {
                analyzedDbName = mvName.getDb();
            }
            if (Strings.isNullOrEmpty(analyzedDbName)) {
                analyzedDbName = analyzer.getDefaultDb();
            }
        }
        if (!Strings.isNullOrEmpty(analyzedDbName)) {
            analyzedDbName = ClusterNamespace.getFullName(getClusterName(), analyzedDbName);
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : MTMVJob.SHOW_TITLE_NAMES) {
            if (title.equals("Query")) {
                builder.addColumn(new Column(title, ScalarType.createVarchar(10240)));
            } else {
                builder.addColumn(new Column(title, ScalarType.createVarchar(1024)));
            }
        }
        return builder.build();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW MTMV JOB");

        if (!Strings.isNullOrEmpty(jobName)) {
            sb.append(" FOR ");
            sb.append(getJobName());
        }
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append(" FROM ");
            sb.append(ClusterNamespace.getNameFromFullName(dbName));
        }
        if (mvName != null) {
            sb.append(" ON ");
            sb.append(mvName.toSql());
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

}
