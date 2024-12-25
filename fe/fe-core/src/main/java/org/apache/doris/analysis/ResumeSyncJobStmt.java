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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

// RESUME SYNC JOB statement used to resume sync job.
//
// syntax:
//      RESUME SYNC JOB [db.]jobName
public class ResumeSyncJobStmt extends DdlStmt implements NotFallbackInParser {

    private JobName jobName;

    public ResumeSyncJobStmt(JobName jobName) {
        this.jobName = jobName;
    }

    public String getJobName() {
        return jobName.getName();
    }

    public String getDbFullName() {
        return jobName.getDbName();
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        jobName.analyze(analyzer);
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("RESUME SYNC JOB ");
        stringBuilder.append(jobName.toSql());
        return stringBuilder.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.RESUME;
    }
}
