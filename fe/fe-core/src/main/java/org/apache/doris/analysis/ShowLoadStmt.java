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
import org.apache.doris.common.proc.LoadProcDir;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.load.LoadJob.JobState;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// SHOW LOAD STATUS statement used to get status of load job.
//
// syntax:
//      SHOW LOAD [FROM db] [LIKE mask]
public class ShowLoadStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowLoadStmt.class);

    private String dbName;
    private Expr whereClause;
    private LimitElement limitElement;
    private List<OrderByElement> orderByElements;

    protected String labelValue;
    protected String stateValue;
    protected boolean isAccurateMatch;
    protected String copyIdValue;
    protected String tableNameValue;
    protected String fileValue;
    protected boolean isCopyIdAccurateMatch;
    protected boolean isTableNameAccurateMatch;
    protected boolean isFilesAccurateMatch;

    private ArrayList<OrderByPair> orderByPairs;

    public ShowLoadStmt(String db, Expr labelExpr, List<OrderByElement> orderByElements, LimitElement limitElement) {
        this.dbName = db;
        this.whereClause = labelExpr;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;

        this.labelValue = null;
        this.stateValue = null;
        this.isAccurateMatch = false;
        this.copyIdValue = null;
        this.isCopyIdAccurateMatch = false;
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

    public long getOffset() {
        if (limitElement != null && limitElement.hasOffset()) {
            return limitElement.getOffset();
        }
        return -1L;
    }

    public String getLabelValue() {
        return this.labelValue;
    }

    public String getCopyIdValue() {
        return this.copyIdValue;
    }

    public String getTableNameValue() {
        return this.tableNameValue;
    }

    public String getFileValue() {
        return this.fileValue;
    }

    public Set<JobState> getStates() {
        if (Strings.isNullOrEmpty(stateValue)) {
            return null;
        }

        Set<JobState> states = new HashSet<JobState>();
        JobState state = JobState.valueOf(stateValue);
        states.add(state);

        if (state == JobState.FINISHED) {
            states.add(JobState.QUORUM_FINISHED);
        }
        return states;
    }

    public org.apache.doris.load.loadv2.JobState getStateV2() {
        if (Strings.isNullOrEmpty(stateValue)) {
            return null;
        }
        return org.apache.doris.load.loadv2.JobState.valueOf(stateValue);
    }

    public boolean isAccurateMatch() {
        return isAccurateMatch;
    }

    public boolean isCopyIdAccurateMatch() {
        return isCopyIdAccurateMatch;
    }

    public boolean isTableNameAccurateMatch() {
        return isTableNameAccurateMatch;
    }

    public boolean isFileAccurateMatch() {
        return isFilesAccurateMatch;
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
            if (whereClause instanceof CompoundPredicate) {
                analyzeCompoundPredicate(whereClause);
            } else {
                analyzeSubPredicate(whereClause);
            }
        }

        // order by
        if (orderByElements != null && !orderByElements.isEmpty()) {
            orderByPairs = new ArrayList<OrderByPair>();
            for (OrderByElement orderByElement : orderByElements) {
                if (!(orderByElement.getExpr() instanceof SlotRef)) {
                    throw new AnalysisException("Should order by column");
                }
                SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                int index = LoadProcDir.analyzeColumn(slotRef.getColumnName());
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                orderByPairs.add(orderByPair);
            }
        }
    }

    protected void analyzeCompoundPredicate(Expr whereClause) throws AnalysisException {
        CompoundPredicate cp = (CompoundPredicate) whereClause;
        if (cp.getOp() != org.apache.doris.analysis.CompoundPredicate.Operator.AND) {
            throw new AnalysisException("Only allow compound predicate with operator AND");
        }
        // check whether left.columnName equals to right.columnName
        checkPredicateName(cp.getChild(0), cp.getChild(1));
        analyzeSubPredicate(cp.getChild(0));
        analyzeSubPredicate(cp.getChild(1));
    }

    private void checkPredicateName(Expr leftChild, Expr rightChild) throws AnalysisException {
        String leftChildColumnName = ((SlotRef) leftChild.getChild(0)).getColumnName();
        String rightChildColumnName = ((SlotRef) rightChild.getChild(0)).getColumnName();
        if (leftChildColumnName.equals(rightChildColumnName)) {
            throw new AnalysisException("column names on both sides of operator AND should be diffrent");
        }
    }

    protected void analyzeSubPredicate(Expr subExpr) throws AnalysisException {
        if (subExpr == null) {
            return;
        }

        boolean valid = true;
        boolean hasLabel = false;
        boolean hasState = false;

        CHECK: {
            if (subExpr instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
                if (binaryPredicate.getOp() != Operator.EQ) {
                    valid = false;
                    break CHECK;
                }
            } else if (subExpr instanceof LikePredicate) {
                LikePredicate likePredicate = (LikePredicate) subExpr;
                if (likePredicate.getOp() != LikePredicate.Operator.LIKE) {
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
            if (leftKey.equalsIgnoreCase("label")) {
                hasLabel = true;
            } else if (leftKey.equalsIgnoreCase("state")) {
                hasState = true;
            } else {
                valid = false;
                break CHECK;
            }

            if (hasState && !(subExpr instanceof BinaryPredicate)) {
                valid = false;
                break CHECK;
            }

            if (hasLabel && subExpr instanceof BinaryPredicate) {
                isAccurateMatch = true;
            }

            // right child
            if (!(subExpr.getChild(1) instanceof StringLiteral)) {
                valid = false;
                break CHECK;
            }

            String value = ((StringLiteral) subExpr.getChild(1)).getStringValue();
            if (Strings.isNullOrEmpty(value)) {
                valid = false;
                break CHECK;
            }

            if (hasLabel && !isAccurateMatch && !value.contains("%")) {
                value = "%" + value + "%";
            }
            if (hasLabel) {
                labelValue = value;
            } else if (hasState) {
                stateValue = value.toUpperCase();

                try {
                    JobState.valueOf(stateValue);
                } catch (Exception e) {
                    valid = false;
                    break CHECK;
                }
            }
        }

        if (!valid) {
            throw new AnalysisException("Where clause should looks like: LABEL = \"your_load_label\","
                    + " or LABEL LIKE \"matcher\", " + " or STATE = \"PENDING|ETL|LOADING|FINISHED|CANCELLED\", "
                    + " or compound predicate with operator AND");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW LOAD ");
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

        if (getOffset() != -1L) {
            sb.append(" OFFSET ").append(getOffset());
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
        for (String title : LoadProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
