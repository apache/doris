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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.thrift.TQueryOptions;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class StatementBase implements ParseNode {

    private String clusterName;

    // Set this variable if this QueryStmt is the top level query from an EXPLAIN <query>
    protected ExplainOptions explainOptions = null;

    /////////////////////////////////////////
    // BEGIN: Members that need to be reset()

    // Analyzer that was used to analyze this statement.
    protected Analyzer analyzer;

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
        analyzer = other.analyzer;
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
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (isAnalyzed()) {
            return;
        }
        this.analyzer = analyzer;
        if (analyzer.getStatementClazz() == null) {
            analyzer.setStatementClazz(this.getClass());
        }
        if (Strings.isNullOrEmpty(analyzer.getClusterName())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_SELECT_CLUSTER);
        }
        this.clusterName = analyzer.getClusterName();
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public boolean isAnalyzed() {
        return analyzer != null;
    }

    public void setIsExplain(ExplainOptions options) {
        this.explainOptions = options;
    }

    public boolean isExplain() {
        return this.explainOptions != null;
    }

    public void setPlaceHolders(ArrayList<PlaceHolderExpr> placeholders) {
        this.placeholders = new ArrayList<PlaceHolderExpr>(placeholders);
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
     * Sets the column labels of this statement, if applicable. No-op of the statement does
     * not produce an output result set.
     */
    public void setColLabels(List<String> colLabels) {
        List<String> oldLabels = getColLabels();
        if (oldLabels == colLabels) {
            return;
        }
        oldLabels.clear();
        oldLabels.addAll(colLabels);
    }

    /**
     * Returns the unresolved result expressions of this statement, if applicable, or an
     * empty list if not applicable (not all statements produce an output result set).
     * Subclasses must override this as necessary.
     */
    public List<Expr> getResultExprs() {
        return Collections.<Expr>emptyList();
    }

    /**
     * Casts the result expressions and derived members (e.g., destination column types for
     * CTAS) to the given types. No-op if this statement does not have result expressions.
     * Throws when casting fails. Subclasses may override this as necessary.
     */
    public void castResultExprs(List<Type> types) throws AnalysisException {
        List<Expr> resultExprs = getResultExprs();
        Preconditions.checkNotNull(resultExprs);
        Preconditions.checkState(resultExprs.size() == types.size());
        for (int i = 0; i < types.size(); ++i) {
            //The specific type of the date type is determined by the
            //actual type of the return value, not by the function return value type in FE Function
            //such as the result of str_to_date may be either DATE or DATETIME
            if (resultExprs.get(i).getType().isDateType() && types.get(i).isDateType()) {
                continue;
            }
            if (!resultExprs.get(i).getType().equals(types.get(i))) {
                resultExprs.set(i, resultExprs.get(i).castTo(types.get(i)));
            }
        }
    }

    /**
     * Uses the given 'rewriter' to transform all Exprs in this statement according
     * to the rules specified in the 'rewriter'. Replaces the original Exprs with the
     * transformed ones in-place. Subclasses that have Exprs to be rewritten must
     * override this method. Valid to call after analyze().
     */
    public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
        throw new IllegalStateException(
                "rewriteExprs() not implemented for this stmt: " + getClass().getSimpleName());
    }

    /**
     * fold constant exprs in statement
     * @throws AnalysisException
     * @param rewriter
     */
    public void foldConstant(ExprRewriter rewriter, TQueryOptions tQueryOptions) throws AnalysisException {
        throw new IllegalStateException(
                "foldConstant() not implemented for this stmt: " + getClass().getSimpleName());
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
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
        analyzer = null;
    }

    // Override this method and return true
    // if the stmt contains some information which need to be encrypted in audit log
    public boolean needAuditEncryption() {
        return false;
    }

}
