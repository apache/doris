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
import org.apache.doris.statistics.AnalysisState;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

/**
 * ShowAnalyzeStmt is used to show statistics job info.
 * syntax:
 *    SHOW ANALYZE
 *        [TABLE | ID]
 *        [
 *            WHERE
 *            [STATE = ["PENDING"|"RUNNING"|"FINISHED"|"FAILED"]]
 *        ]
 *        [ORDER BY ...]
 *        [LIMIT limit];
 */
public class ShowAnalyzeStmt extends ShowStmt {
    private static final String STATE_NAME = "state";
    private static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("job_id")
            .add("catalog_name")
            .add("db_name")
            .add("tbl_name")
            .add("col_name")
            .add("job_type")
            .add("analysis_type")
            .add("message")
            .add("last_exec_time_in_ms")
            .add("state")
            .add("progress")
            .add("schedule_type")
            .build();

    private long jobId;
    private final TableName dbTableName;
    private final Expr whereClause;

    // extract from predicate
    private String stateValue;

    private final boolean auto;


    public ShowAnalyzeStmt(TableName dbTableName,
            Expr whereClause, boolean auto) {
        this.dbTableName = dbTableName;
        this.whereClause = whereClause;
        this.auto = auto;

    }

    public ShowAnalyzeStmt(long jobId,
            Expr whereClause) {
        Preconditions.checkArgument(jobId > 0, "JobId must greater than 0.");
        this.jobId = jobId;
        this.dbTableName = null;
        this.whereClause = whereClause;
        this.auto = false;
    }

    public long getJobId() {
        return jobId;
    }

    public String getStateValue() {
        Preconditions.checkArgument(isAnalyzed(),
                "The stateValue must be obtained after the parsing is complete");
        return stateValue;
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
        if (dbTableName != null) {
            dbTableName.analyze(analyzer);
            String dbName = dbTableName.getDb();
            String tblName = dbTableName.getTbl();
            checkShowAnalyzePriv(dbName, tblName);
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

    private void checkShowAnalyzePriv(String dbName, String tblName) throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), dbName, tblName, PrivPredicate.SHOW)) {
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
            if (!STATE_NAME.equalsIgnoreCase(leftKey)) {
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

            stateValue = value.toUpperCase();
            try {
                AnalysisState.valueOf(stateValue);
            } catch (Exception e) {
                valid = false;
            }
        }

        if (!valid) {
            throw new AnalysisException("Where clause should looks like: "
                    + "STATE = \"PENDING|RUNNING|FINISHED|FAILED");
        }
    }

    private int analyzeColumn(String columnName) throws AnalysisException {
        for (String title : TITLE_NAMES) {
            if (title.equalsIgnoreCase(columnName)) {
                return TITLE_NAMES.indexOf(title);
            }
        }
        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW ANALYZE");

        if (jobId != 0) {
            sb.append(" ");
            sb.append(jobId);
        }

        if (dbTableName != null) {
            sb.append(" ");
            sb.append(dbTableName.toSql());
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

    public TableName getDbTableName() {
        return dbTableName;
    }

    public boolean isAuto() {
        return auto;
    }
}
