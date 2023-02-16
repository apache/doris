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
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.statistics.AnalysisState;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

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
            .add("schedule_type")
            .build();

    private Long jobId;
    private TableName dbTableName;
    private Expr whereClause;
    private LimitElement limitElement;
    private List<OrderByElement> orderByElements;

    // after analyzed
    private String catalogName;
    private String dbName;
    private String tblName;

    private String stateValue;
    private ArrayList<OrderByPair> orderByPairs;

    public ShowAnalyzeStmt() {
    }

    public ShowAnalyzeStmt(TableName dbTableName,
                           Expr whereClause,
                           List<OrderByElement> orderByElements,
                           LimitElement limitElement) {
        this.dbTableName = dbTableName;
        this.whereClause = whereClause;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
    }

    public ShowAnalyzeStmt(Long jobId,
            Expr whereClause,
            List<OrderByElement> orderByElements,
            LimitElement limitElement) {
        this.jobId = jobId;
        this.dbTableName = null;
        this.whereClause = whereClause;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
    }

    public ImmutableList<String> getTitleNames() {
        return TITLE_NAMES;
    }

    public Long getJobId() {
        return jobId;
    }

    public String getCatalogName() {
        Preconditions.checkArgument(isAnalyzed(),
                "The catalogName must be obtained after the parsing is complete");
        return catalogName;
    }

    public String getDbName() {
        Preconditions.checkArgument(isAnalyzed(),
                "The dbName must be obtained after the parsing is complete");
        return dbName;
    }

    public String getTblName() {
        Preconditions.checkArgument(isAnalyzed(),
                "The tblName must be obtained after the parsing is complete");
        return tblName;
    }

    public String getStateValue() {
        Preconditions.checkArgument(isAnalyzed(),
                "The stateValue must be obtained after the parsing is complete");
        return stateValue;
    }

    public ArrayList<OrderByPair> getOrderByPairs() {
        Preconditions.checkArgument(isAnalyzed(),
                "The orderByPairs must be obtained after the parsing is complete");
        return orderByPairs;
    }

    public String getWhereClause() {
        Preconditions.checkArgument(isAnalyzed(),
                "The whereClause must be obtained after the parsing is complete");

        StringBuilder clauseBuilder = new StringBuilder();

        if (jobId != null) {
            clauseBuilder.append("job_Id = ").append(jobId);
        }

        if (!Strings.isNullOrEmpty(catalogName)) {
            clauseBuilder.append(clauseBuilder.length() > 0 ? " AND " : "")
                    .append("catalog_name = \"").append(catalogName).append("\"");
        }

        if (!Strings.isNullOrEmpty(dbName)) {
            clauseBuilder.append(clauseBuilder.length() > 0 ? " AND " : "")
                    .append("db_name = \"").append(dbName).append("\"");
        }

        if (!Strings.isNullOrEmpty(tblName)) {
            clauseBuilder.append(clauseBuilder.length() > 0 ? " AND " : "")
                    .append("tbl_name = \"").append(tblName).append("\"");
        }

        if (!Strings.isNullOrEmpty(stateValue)) {
            clauseBuilder.append(clauseBuilder.length() > 0 ? " AND " : "")
                    .append("state = \"").append(stateValue).append("\"");
        }

        return clauseBuilder.toString();
    }

    public long getLimit() {
        if (limitElement != null && limitElement.hasLimit()) {
            return limitElement.getLimit();
        }
        return -1L;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        catalogName = analyzer.getEnv().getInternalCatalog().getName();

        if (dbTableName != null) {
            dbTableName.analyze(analyzer);
            String dbName = dbTableName.getDb();
            String tblName = dbTableName.getTbl();
            checkShowAnalyzePriv(dbName, tblName);
            this.dbName = dbName;
            this.tblName = tblName;
        }

        // analyze where clause if not null
        if (whereClause != null) {
            analyzeSubPredicate(whereClause);
        }

        // analyze order by
        if (orderByElements != null && !orderByElements.isEmpty()) {
            orderByPairs = new ArrayList<>();
            for (OrderByElement orderByElement : orderByElements) {
                if (orderByElement.getExpr() instanceof SlotRef) {
                    SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                    int index = analyzeColumn(slotRef.getColumnName());
                    OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                    orderByPairs.add(orderByPair);
                } else {
                    throw new AnalysisException("Should order by column");
                }
            }
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
                    + "STATE = \"PENDING|SCHEDULING|RUNNING|FINISHED|FAILED|CANCELLED\"");
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

        if (jobId != null) {
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

        // Order By clause
        if (orderByElements != null) {
            sb.append(" ");
            sb.append("ORDER BY");
            sb.append(" ");
            IntStream.range(0, orderByElements.size()).forEach(i -> {
                sb.append(orderByElements.get(i).getExpr().toSql());
                sb.append((orderByElements.get(i).getIsAsc()) ? " ASC" : " DESC");
                sb.append((i + 1 != orderByElements.size()) ? ", " : "");
            });
        }

        if (getLimit() != -1L) {
            sb.append(" ");
            sb.append("LIMIT");
            sb.append(" ");
            sb.append(getLimit());
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
