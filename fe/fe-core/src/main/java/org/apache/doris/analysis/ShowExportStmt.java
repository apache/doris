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

import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.ExportProcNode;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.load.ExportJobState;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

// SHOW EXPORT STATUS statement used to get status of load job.
//
// syntax:
//      SHOW EXPORT [FROM db] [where ...]
public class ShowExportStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowExportStmt.class);

    private String dbName;
    private final Expr whereClause;
    private final LimitElement limitElement;
    private final List<OrderByElement> orderByElements;

    private long jobId = 0;
    private String label = null;
    private boolean isLabelUseLike = false;
    private String stateValue = null;

    private ExportJobState jobState;

    private ArrayList<OrderByPair> orderByPairs;

    public ShowExportStmt(String db, Expr whereExpr, List<OrderByElement> orderByElements, LimitElement limitElement) {
        this.dbName = db;
        this.whereClause = whereExpr;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
    }

    public String getDbName() {
        return dbName;
    }

    public ArrayList<OrderByPair> getOrderByPairs() {
        return this.orderByPairs;
    }

    public long getLimit() {
        if (limitElement != null && limitElement.hasLimit()) {
            return limitElement.getLimit();
        }
        return -1L;
    }

    public long getJobId() {
        return this.jobId;
    }

    public ExportJobState getJobState() {
        if (Strings.isNullOrEmpty(stateValue)) {
            return null;
        }
        return jobState;
    }

    public String getLabel() {
        return label;
    }

    public boolean isLabelUseLike() {
        return isLabelUseLike;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        // analyze where clause if not null
        if (whereClause != null) {
            analyzePredicate(whereClause);
        }

        // order by
        if (orderByElements != null && !orderByElements.isEmpty()) {
            orderByPairs = new ArrayList<>();
            for (OrderByElement orderByElement : orderByElements) {
                if (!(orderByElement.getExpr() instanceof SlotRef)) {
                    throw new AnalysisException("Should order by column");
                }
                SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                int index = ExportProcNode.analyzeColumn(slotRef.getColumnName());
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                orderByPairs.add(orderByPair);
            }
        }
    }

    private void analyzePredicate(Expr whereExpr) throws AnalysisException {
        if (whereExpr == null) {
            return;
        }

        boolean valid = false;

        // enumerate all possible conditions
        if (whereExpr.getChild(0) instanceof SlotRef) {
            String leftKey = ((SlotRef) whereExpr.getChild(0)).getColumnName().toLowerCase();

            if (whereExpr instanceof BinaryPredicate && ((BinaryPredicate) whereExpr).getOp() == Operator.EQ) {
                if ("id".equals(leftKey) && whereExpr.getChild(1) instanceof IntLiteral) {
                    jobId = ((IntLiteral) whereExpr.getChild(1)).getLongValue();
                    valid = true;

                } else if ("state".equals(leftKey) && whereExpr.getChild(1) instanceof StringLiteral) {
                    String value = whereExpr.getChild(1).getStringValue();
                    if (!Strings.isNullOrEmpty(value)) {
                        stateValue = value.toUpperCase();
                        try {
                            jobState = ExportJobState.valueOf(stateValue);
                            valid = true;
                        } catch (IllegalArgumentException e) {
                            LOG.warn("illegal state argument in export stmt. stateValue={}, error={}", stateValue, e);
                        }
                    }

                } else if ("label".equals(leftKey) && whereExpr.getChild(1) instanceof StringLiteral) {
                    label = whereExpr.getChild(1).getStringValue();
                    valid = true;
                }

            } else if (whereExpr instanceof LikePredicate
                    && ((LikePredicate) whereExpr).getOp() == LikePredicate.Operator.LIKE) {
                if ("label".equals(leftKey) && whereExpr.getChild(1) instanceof StringLiteral) {
                    label = whereExpr.getChild(1).getStringValue();
                    isLabelUseLike = true;
                    valid = true;
                }
            }
        }

        if (!valid) {
            throw new AnalysisException("Where clause should looks like below: "
                    + " ID = $your_job_id, or STATE = \"PENDING|EXPORTING|FINISHED|CANCELLED\", "
                    + "or LABEL = \"xxx\" or LABEL like \"xxx%\"");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW EXPORT ");
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("FROM `").append(dbName).append("`");
        }

        if (whereClause != null) {
            sb.append(" WHERE ").append(whereClause.toSql());
        }

        // Order By clause
        if (orderByElements != null) {
            sb.append(" ORDER BY ");
            for (int i = 0; i < orderByElements.size(); ++i) {
                sb.append(orderByElements.get(i).getExpr().toSql());
                sb.append((orderByElements.get(i).getIsAsc()) ? " ASC" : " DESC");
                sb.append((i + 1 != orderByElements.size()) ? ", " : "");
            }
        }

        if (getLimit() != -1L) {
            sb.append(" LIMIT ").append(getLimit());
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ExportProcNode.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
