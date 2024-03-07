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
import org.apache.doris.catalog.Resource.ResourceType;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// SHOW Resource statement used to show resource of Doris.
//
// syntax:
//      SHOW RESOURCES [LIKE mask]
public class ShowResourcesStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowResourcesStmt.class);

    private String pattern;
    private Expr whereClause;
    private LimitElement limitElement;
    private List<OrderByElement> orderByElements;

    private String nameValue;
    private String typeValue;
    private boolean isAccurateMatch;

    private ArrayList<OrderByPair> orderByPairs;

    public ShowResourcesStmt() {
    }

    public ShowResourcesStmt(String pattern, Expr labelExpr,
            List<OrderByElement> orderByElements, LimitElement limitElement) {
        this.pattern = pattern;
        this.whereClause = labelExpr;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;

        this.nameValue = null;
        this.typeValue = null;
        this.isAccurateMatch = false;
    }

    public String getPattern() {
        return pattern;
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

    public String getNameValue() {
        return this.nameValue;
    }

    public Set<String> getTypeSet() {
        if (Strings.isNullOrEmpty(typeValue)) {
            return null;
        }

        Set<String> resources = new HashSet<>();
        resources.add(typeValue.toUpperCase());

        return resources;
    }

    public boolean isAccurateMatch() {
        return isAccurateMatch;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // If pattern is null, analyze where clause,
        // otherwise, ignore where clause.
        if (this.pattern == null) {
            // analyze where clause
            boolean isValid;
            if (whereClause instanceof CompoundPredicate) {
                CompoundPredicate cp = (CompoundPredicate) whereClause;
                if (cp.getOp() != org.apache.doris.analysis.CompoundPredicate.Operator.AND) {
                    throw new AnalysisException("Only allow compound predicate with operator AND");
                }
                isValid = isWhereClauseValid(cp.getChild(0)) && isWhereClauseValid(cp.getChild(1));
            } else {
                isValid = isWhereClauseValid(whereClause);
            }

            if (!isValid) {
                throw new AnalysisException("Where clause should looks like: NAME = \"your_resource_name\","
                        + " or NAME LIKE \"matcher\", " + " or RESOURCETYPE = \"resource_type\", "
                        + " or compound predicate with operator AND");
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
                int index = ResourceMgr.analyzeColumn(slotRef.getColumnName());
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                orderByPairs.add(orderByPair);
            }
        }
    }

    private boolean isWhereClauseValid(Expr expr) {
        if (expr == null) {
            return true;
        }

        if (!(expr instanceof BinaryPredicate) && !(expr instanceof LikePredicate)) {
            return false;
        }

        if (expr instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
            if (binaryPredicate.getOp() != BinaryPredicate.Operator.EQ) {
                return false;
            }
        }

        if (expr instanceof LikePredicate) {
            LikePredicate likePredicate = (LikePredicate) expr;
            if (likePredicate.getOp() != LikePredicate.Operator.LIKE) {
                return false;
            }
        }

        // left child
        if (!(expr.getChild(0) instanceof SlotRef)) {
            return false;
        }
        String leftKey = ((SlotRef) expr.getChild(0)).getColumnName();

        // right child
        if (!(expr.getChild(1) instanceof StringLiteral)) {
            return false;
        }
        String value = ((StringLiteral) expr.getChild(1)).getStringValue();
        if (Strings.isNullOrEmpty(value)) {
            return false;
        }

        if (leftKey.equalsIgnoreCase("Name")) {
            if (expr instanceof BinaryPredicate) {
                isAccurateMatch = true;
            }
            nameValue = value;
            return true;
        }

        if (leftKey.equalsIgnoreCase("ResourceType") && expr instanceof BinaryPredicate) {
            typeValue = value.toUpperCase();
            try {
                ResourceType.valueOf(typeValue);
            } catch (Exception e) {
                return false;
            }
            return true;
        }

        return false;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW RESOURCES");
        if (pattern != null) {
            sb.append(" LIKE '").append(pattern).append("'");
        } else {
            if (whereClause != null) {
                sb.append(" WHERE ").append(whereClause.toSql());
            }
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
        for (String title : ResourceMgr.RESOURCE_PROC_NODE_TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        if (ConnectContext.get().getSessionVariable().getForwardToMaster()) {
            return RedirectStatus.FORWARD_NO_SYNC;
        } else {
            return RedirectStatus.NO_FORWARD;
        }
    }
}
