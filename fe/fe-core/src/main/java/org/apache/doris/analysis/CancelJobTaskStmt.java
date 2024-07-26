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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/StringLiteral.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import lombok.Getter;

public class CancelJobTaskStmt extends DdlStmt {

    @Getter
    private String jobName;

    @Getter
    private Long taskId;

    private Expr expr;

    private static final String jobNameKey = "jobName";

    private static final String taskIdKey = "taskId";

    public CancelJobTaskStmt(Expr whereExpr) {
        this.expr = whereExpr;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        CreateJobStmt.checkAuth();
        CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
        if (!compoundPredicate.getOp().equals(CompoundPredicate.Operator.AND)) {
            throw new AnalysisException("Only allow compound predicate with operator AND");
        }
        String jobNameInput = ((SlotRef) compoundPredicate.getChildren().get(0).getChild(0)).getColumnName();
        if (!jobNameKey.equalsIgnoreCase(jobNameInput)) {
            throw new AnalysisException("Current not support " + jobNameInput);
        }

        if (!(compoundPredicate.getChildren().get(0).getChild(1) instanceof StringLiteral)) {
            throw new AnalysisException("JobName value must is string");
        }
        this.jobName = compoundPredicate.getChildren().get(0).getChild(1).getStringValue();
        String taskIdInput = ((SlotRef) compoundPredicate.getChildren().get(1).getChild(0)).getColumnName();
        if (!taskIdKey.equalsIgnoreCase(taskIdInput)) {
            throw new AnalysisException("Current not support " + taskIdInput);
        }
        if (!(compoundPredicate.getChildren().get(1).getChild(1) instanceof IntLiteral)) {
            throw new AnalysisException("task id  value must is large int");
        }
        this.taskId = ((IntLiteral) compoundPredicate.getChildren().get(1).getChild(1)).getLongValue();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CANCEL;
    }
}
