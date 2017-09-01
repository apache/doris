// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.analysis;

import com.baidu.palo.analysis.BinaryPredicate.Operator;
import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.ColumnType;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.proc.LoadProcDir;
import com.baidu.palo.common.util.OrderByPair;
import com.baidu.palo.load.LoadJob.JobState;
import com.baidu.palo.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

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

    private String labelValue;
    private String stateValue;
    private boolean isAccurateMatch;

    private ArrayList<OrderByPair> orderByPairs;

    public ShowLoadStmt(String db, Expr labelExpr, List<OrderByElement> orderByElements, LimitElement limitElement) {
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

    public long getLimit() {
        if (limitElement != null && limitElement.hasLimit()) {
            return limitElement.getLimit();
        }
        return -1L;
    }

    public String getLabelValue() {
        return this.labelValue;
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

    public boolean isAccurateMatch() {
        return isAccurateMatch;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            dbName = ClusterNamespace.getFullName(getClusterName(), dbName);
        }
        final String userNameWithoutPrefix = ClusterNamespace.getNameFromFullName(dbName);
        final String dbNameWithoutPrefix = ClusterNamespace.getNameFromFullName(dbName);
        // check access
        if (!analyzer.getCatalog().getUserMgr().checkAccess(analyzer.getUser(), dbName, AccessPrivilege.READ_ONLY)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED, userNameWithoutPrefix,
                    dbNameWithoutPrefix);
        }

        // analyze where clause if not null
        if (whereClause != null) {
            if (whereClause instanceof CompoundPredicate) {
                CompoundPredicate cp = (CompoundPredicate) whereClause;
                if (cp.getOp() != com.baidu.palo.analysis.CompoundPredicate.Operator.AND) {
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
                int index = LoadProcDir.analyzeColumn(slotRef.getColumnName());
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                orderByPairs.add(orderByPair);
            }
        }
    }

    private void analyzeSubPredicate(Expr subExpr) throws AnalysisException {
        boolean valid = true;
        boolean hasLabel = false;
        boolean hasState = false;
        while (subExpr != null) {
            if (subExpr instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
                if (binaryPredicate.getOp() != Operator.EQ) {
                    valid = false;
                    break;
                }
            } else if (subExpr instanceof LikePredicate) {
                LikePredicate likePredicate = (LikePredicate) subExpr;
                if (likePredicate.getOp() != LikePredicate.Operator.LIKE) {
                    valid = false;
                    break;
                }
            } else {
                valid = false;
                break;
            }

            // left child
            if (!(subExpr.getChild(0) instanceof SlotRef)) {
                valid = false;
                break;
            }
            String leftKey = ((SlotRef) subExpr.getChild(0)).getColumnName();
            if (leftKey.equalsIgnoreCase("label")) {
                hasLabel = true;
            } else if (leftKey.equalsIgnoreCase("state")) {
                hasState = true;
            } else {
                valid = false;
                break;
            }

            if (hasState && !(subExpr instanceof BinaryPredicate)) {
                valid = false;
                break;
            }

            if (hasLabel && subExpr instanceof BinaryPredicate) {
                isAccurateMatch = true;
            }

            // right child
            if (!(subExpr.getChild(1) instanceof StringLiteral)) {
                valid = false;
                break;
            }

            String value = ((StringLiteral) subExpr.getChild(1)).getStringValue();
            if (Strings.isNullOrEmpty(value)) {
                valid = false;
                break;
            }

            if (hasLabel) {
                labelValue = value;
            } else if (hasState) {
                stateValue = value.toUpperCase();

                try {
                    JobState.valueOf(stateValue);
                } catch (Exception e) {
                    valid = false;
                    break;
                }
            }

            break;
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
            builder.addColumn(new Column(title, ColumnType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
