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
import org.apache.doris.job.common.JobStatus;

import com.google.common.base.Strings;
import lombok.Getter;

public class AlterJobStatusStmt extends DdlStmt implements NotFallbackInParser {

    private Expr expr;

    private static final String columnName = "jobName";

    @Getter
    private String jobName;

    @Getter
    private JobStatus jobStatus;

    @Getter
    private boolean isDrop = false;

    @Getter
    private boolean ifExists;

    public AlterJobStatusStmt(Expr whereClause, JobStatus jobStatus) {
        this.expr = whereClause;
        this.jobStatus = jobStatus;
    }

    public AlterJobStatusStmt(Expr whereClause, boolean isDrop, boolean ifExists) {
        this.expr = whereClause;
        this.isDrop = isDrop;
        this.ifExists = ifExists;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        CreateJobStmt.checkAuth();
        String inputCol = ((SlotRef) expr.getChild(0)).getColumnName();
        if (!inputCol.equalsIgnoreCase(columnName)) {
            throw new AnalysisException("Current not support " + inputCol);
        }
        if (!(expr.getChild(1) instanceof StringLiteral)) {
            throw new AnalysisException("Value must is string");
        }

        String inputValue = expr.getChild(1).getStringValue();
        if (Strings.isNullOrEmpty(inputValue)) {
            throw new AnalysisException("Value can't is null");
        }
        this.jobName = inputValue;
        if (CreateJobStmt.isInnerJob(jobName)) {
            throw new AnalysisException("Can't alter inner job status");
        }
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }
}
