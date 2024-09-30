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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.statistics.JobPriority;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

/**
 * ShowAutoAnalyzeJobsStmt is used to show pending auto analysis jobs.
 * syntax:
 *    SHOW AUTO ANALYZE JOBS
 *        [TABLE]
 *        [
 *            WHERE
 *            [PRIORITY = ["HIGH"|"MID"|"LOW"]]
 *        ]
 */
public class ShowAutoAnalyzeJobsStmt extends ShowStmt implements NotFallbackInParser {
    private static final String PRIORITY = "priority";
    private static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("catalog_name")
            .add("db_name")
            .add("tbl_name")
            .add("col_list")
            .add("priority")
            .build();

    private final TableName tableName;
    private final Expr whereClause;

    public ShowAutoAnalyzeJobsStmt(TableName tableName, Expr whereClause) {
        this.tableName = tableName;
        this.whereClause = whereClause;
    }

    // extract from predicate
    private String jobPriority;

    public String getPriority() {
        Preconditions.checkArgument(isAnalyzed(),
                "The stateValue must be obtained after the parsing is complete");
        return jobPriority;
    }

    public Expr getWhereClause() {
        Preconditions.checkArgument(isAnalyzed(),
                "The whereClause must be obtained after the parsing is complete");
        return whereClause;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (!ConnectContext.get().getSessionVariable().enableStats) {
            throw new UserException("Analyze function is forbidden, you should add `enable_stats=true`"
                    + "in your FE conf file");
        }
        super.analyze(analyzer);
        if (tableName != null) {
            tableName.analyze(analyzer);
            String catalogName = tableName.getCtl();
            String dbName = tableName.getDb();
            String tblName = tableName.getTbl();
            checkShowAnalyzePriv(catalogName, dbName, tblName);
        }

        // analyze where clause if not null
        if (whereClause != null) {
            analyzeSubPredicate(whereClause);
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(128)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    private void checkShowAnalyzePriv(String catalogName, String dbName, String tblName) throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), catalogName, dbName, tblName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    "SHOW ANALYZE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbName + ": " + tblName);
        }
    }

    private void analyzeSubPredicate(Expr subExpr) throws AnalysisException {
        if (subExpr == null) {
            return;
        }

        boolean valid = true;

        CHECK: {
            if (subExpr instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
                if (binaryPredicate.getOp() != BinaryPredicate.Operator.EQ) {
                    valid = false;
                    break CHECK;
                }
            } else {
                valid = false;
                break CHECK;
            }

            // left child
            if (!(subExpr.getChild(0) instanceof SlotRef)) {
                valid = false;
                break CHECK;
            }
            String leftKey = ((SlotRef) subExpr.getChild(0)).getColumnName();
            if (!PRIORITY.equalsIgnoreCase(leftKey)) {
                valid = false;
                break CHECK;
            }

            // right child
            if (!(subExpr.getChild(1) instanceof StringLiteral)) {
                valid = false;
                break CHECK;
            }

            String value = subExpr.getChild(1).getStringValue();
            if (Strings.isNullOrEmpty(value)) {
                valid = false;
                break CHECK;
            }

            jobPriority = value.toUpperCase();
            try {
                JobPriority.valueOf(jobPriority);
            } catch (Exception e) {
                valid = false;
            }
        }

        if (!valid) {
            throw new AnalysisException("Where clause should looks like: "
                    + "PRIORITY = \"HIGH|MID|LOW\"");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW AUTO ANALYZE");

        if (tableName != null) {
            sb.append(" ");
            sb.append(tableName.toSql());
        }

        if (whereClause != null) {
            sb.append(" ");
            sb.append("WHERE");
            sb.append(" ");
            sb.append(whereClause.toSql());
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    public TableName getTableName() {
        return tableName;
    }
}
