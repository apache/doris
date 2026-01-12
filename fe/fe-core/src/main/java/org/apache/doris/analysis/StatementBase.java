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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.OriginStatement;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class StatementBase implements ParseNode {
    private static final Logger LOG = LogManager.getLogger(StatementBase.class);
    private String clusterName;

    // Set this variable if this QueryStmt is the top level query from an EXPLAIN <query>
    protected ExplainOptions explainOptions = null;

    /////////////////////////////////////////
    // BEGIN: Members that need to be reset()

    // END: Members that need to be reset()
    /////////////////////////////////////////

    private OriginStatement origStmt;

    private UserIdentity userInfo;

    private boolean isPrepared = false;
    // select * from tbl where a = ? and b = ?
    // `?` is the placeholder
    private ArrayList<PlaceHolderExpr> placeholders = new ArrayList<>();

    protected StatementBase() { }

    /**
     * C'tor for cloning.
     */
    protected StatementBase(StatementBase other) {
        explainOptions = other.explainOptions;
    }

    /**
     * Analyzes the statement and throws an AnalysisException if analysis fails. A failure
     * could be due to a problem with the statement or because one or more tables/views
     * were missing from the catalog.
     * It is up to the analysis() implementation to ensure the maximum number of missing
     * tables/views get collected in the Analyzer before failing analyze().
     * Should call the method firstly when override the method, the analyzer param should be
     * the one which statement would use.
     */
    public void analyze() throws UserException {
    }

    public void checkPriv() throws AnalysisException {
    }

    public void setIsExplain(ExplainOptions options) {
        this.explainOptions = options;
    }

    public void setPlaceHolders(ArrayList<PlaceHolderExpr> placeholders) {
        LOG.debug("setPlaceHolders {}", placeholders);
        this.placeholders = new ArrayList<PlaceHolderExpr>(placeholders);
    }

    public boolean isExplain() {
        return this.explainOptions != null;
    }

    public ArrayList<PlaceHolderExpr> getPlaceHolders() {
        return this.placeholders;
    }

    public boolean isVerbose() {
        return explainOptions != null && explainOptions.isVerbose();
    }

    public ExplainOptions getExplainOptions() {
        return explainOptions;
    }

    public void setIsPrepared() {
        this.isPrepared = true;
    }

    public boolean isPrepared() {
        return this.isPrepared;
    }

    /*
     * Print SQL syntax corresponding to this node.
     *
     * @see org.apache.doris.parser.ParseNode#toSql()
     */
    @Override
    public String toSql() {
        return "";
    }

    public StmtType stmtType() {
        return StmtType.OTHER;
    }

    public abstract RedirectStatus getRedirectStatus();

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

    /**
     * Resets the internal analysis state of this node.
     * For easier maintenance, class members that need to be reset are grouped into
     * a 'section' clearly indicated by comments as follows:
     *
     * class SomeStmt extends StatementBase {
     *   ...
     *   /////////////////////////////////////////
     *   // BEGIN: Members that need to be reset()
     *
     *   <member declarations>
     *
     *   // END: Members that need to be reset()
     *   /////////////////////////////////////////
     *   ...
     * }
     *
     * In general, members that are set or modified during analyze() must be reset().
     * TODO: Introduce this same convention for Exprs, possibly by moving clone()/reset()
     * into the ParseNode interface for clarity.
     */
    public void reset() {
    }

    // Override this method and return true
    // if the stmt contains some information which need to be encrypted in audit log
    public boolean needAuditEncryption() {
        return false;
    }

}
