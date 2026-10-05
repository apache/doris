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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/StatementBase.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.qe.OriginStatement;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;

public abstract class StatementBase {
    // Set this variable if this QueryStmt is the top level query from an EXPLAIN <query>
    protected ExplainOptions explainOptions = null;
    private OriginStatement origStmt;
    private UserIdentity userInfo;
    private boolean isPrepared = false;

    protected StatementBase() { }

    /**
     * C'tor for cloning.
     */
    protected StatementBase(StatementBase other) {
        explainOptions = other.explainOptions;
    }

    public void setIsExplain(ExplainOptions options) {
        this.explainOptions = options;
    }

    public boolean isExplain() {
        return this.explainOptions != null;
    }

    public boolean isVerbose() {
        return explainOptions != null && explainOptions.isVerbose();
    }

    public ExplainOptions getExplainOptions() {
        return explainOptions;
    }

    public boolean isPrepared() {
        return this.isPrepared;
    }

    public StmtType stmtType() {
        return StmtType.OTHER;
    }

    /**
     * Returns the output column labels of this statement, if applicable, or an empty list
     * if not applicable (not all statements produce an output result set).
     * Subclasses must override this as necessary.
     */
    public List<String> getColLabels() {
        return Collections.<String>emptyList();
    }

    /**
     * Returns the unresolved result expressions of this statement, if applicable, or an
     * empty list if not applicable (not all statements produce an output result set).
     * Subclasses must override this as necessary.
     */
    public List<Expr> getResultExprs() {
        return Collections.<Expr>emptyList();
    }

    public void setOrigStmt(OriginStatement origStmt) {
        Preconditions.checkState(origStmt != null);
        this.origStmt = origStmt;
    }

    public OriginStatement getOrigStmt() {
        return origStmt;
    }

    public UserIdentity getUserInfo() {
        return userInfo;
    }

    public void setUserInfo(UserIdentity userInfo) {
        this.userInfo = userInfo;
    }

    // Override this method and return true
    // if the stmt contains some information which need to be encrypted in audit log
    public boolean needAuditEncryption() {
        return false;
    }

}
