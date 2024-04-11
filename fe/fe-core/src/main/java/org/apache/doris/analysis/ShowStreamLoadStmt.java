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
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

;

// SHOW STREAM LOAD STATUS statement used to get record of stream load job.
//
// syntax:
//      SHOW STREAM LOAD [FROM db] [LIKE mask]
public class ShowStreamLoadStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowStreamLoadStmt.class);

    public enum StreamLoadState {
        SUCCESS,
        FAIL
    }

    private String dbName;
    private final Expr whereClause;
    private final LimitElement limitElement;
    private final List<OrderByElement> orderByElements;

    private String labelValue;
    private String stateValue;
    private boolean isAccurateMatch;

    private ArrayList<OrderByPair> orderByPairs;

    private static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Label").add("Db").add("Table")
            .add("ClientIp").add("Status").add("Message").add("Url").add("TotalRows")
            .add("LoadedRows").add("FilteredRows").add("UnselectedRows").add("LoadBytes")
            .add("StartTime").add("FinishTime").add("User").add("Comment")
            .build();

    public ShowStreamLoadStmt(String db, Expr labelExpr,
            List<OrderByElement> orderByElements, LimitElement limitElement) {
        this.dbName = db;
        this.whereClause = labelExpr;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;

        this.labelValue = null;
        this.stateValue = null;
        this.isAccurateMatch = false;
    }

    public String getDbName() {
        return dbName;
    }

    public ArrayList<OrderByPair> getOrderByPairs() {
        return this.orderByPairs;
    }

    public ArrayList<OrderByPair> getOrderByFinishTime() {
        ArrayList<OrderByPair> orderByFinishTime = new ArrayList<OrderByPair>();
        int index = 0;
        try {
            index = analyzeColumn("FinishTime");
        } catch (AnalysisException e) {
            // CHECKSTYLE IGNORE THIS LINE
        }
        OrderByPair orderByPair = new OrderByPair(index, false);
        orderByFinishTime.add(orderByPair);
        return orderByFinishTime;
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

    public StreamLoadState getState() {
        if (Strings.isNullOrEmpty(stateValue)) {
            return null;
        }

        StreamLoadState state = null;
        try {
            state = StreamLoadState.valueOf(stateValue);
        } catch (Exception e) {
            // CHECKSTYLE IGNORE THIS LINE
        }
        return state;
    }

    public boolean isAccurateMatch() {
        return isAccurateMatch;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
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
                CompoundPredicate cp = (CompoundPredicate) whereClause;
                if (cp.getOp() != CompoundPredicate.Operator.AND) {
                    throw new AnalysisException("Only allow compound predicate with operator AND");
                }

                analyzeSubPredicate(cp.getChild(0));
                analyzeSubPredicate(cp.getChild(1));
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
                int index = analyzeColumn(slotRef.getColumnName());
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                orderByPairs.add(orderByPair);
            }
        }
    }

    private void analyzeSubPredicate(Expr subExpr) throws AnalysisException {
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
            } else if (leftKey.equalsIgnoreCase("status")) {
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

            if (hasLabel) {
                labelValue = value;
            } else if (hasState) {
                stateValue = value.toUpperCase();

                try {
                    StreamLoadState.valueOf(stateValue);
                } catch (Exception e) {
                    valid = false;
                    break CHECK;
                }
            }
        }

        if (!valid) {
            throw new AnalysisException("Where clause should looks like: LABEL = \"your_load_label\","
                    + " or LABEL LIKE \"matcher\", " + " or STATUS = \"SUCCESS|FAIL\", "
                    + " or compound predicate with operator AND");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW STREAM LOAD ");
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
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    private int analyzeColumn(String columnName) throws AnalysisException {
        for (String title : TITLE_NAMES) {
            if (title.equalsIgnoreCase(columnName)) {
                return TITLE_NAMES.indexOf(title);
            }
        }

        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }
}
