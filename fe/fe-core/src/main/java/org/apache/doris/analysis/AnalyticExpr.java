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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/AnalyticExpr.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.thrift.TExprNode;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Representation of an analytic function call with OVER clause.
 * All "subexpressions" (such as the actual function call parameters as well as the
 * partition/ordering exprs, etc.) are embedded as children in order to allow expr
 * substitution:
 *   function call params: child 0 .. #params
 *   partition exprs: children #params + 1 .. #params + #partition-exprs
 *   ordering exprs:
 *     children #params + #partition-exprs + 1 ..
 *       #params + #partition-exprs + #order-by-elements
 *   exprs in windowing clause: remaining children
 *
 * Note that it's wrong to embed the FunctionCallExpr itself as a child,
 * because in 'COUNT(..) OVER (..)' the 'COUNT(..)' is not part of a standard aggregate
 * computation and must not be substituted as such. However, the parameters of the
 * analytic function call might reference the output of an aggregate computation
 * and need to be substituted as such; example: COUNT(COUNT(..)) OVER (..)
 */
public class AnalyticExpr extends Expr {

    private FunctionCallExpr fnCall;
    private final List<Expr> partitionExprs;
    // These elements are modified to point to the corresponding child exprs to keep them
    // in sync through expr substitutions.
    private List<OrderByElement> orderByElements = Lists.newArrayList();
    private AnalyticWindow window;

    // If set, requires the window to be set to null in resetAnalysisState(). Required for
    // proper substitution/cloning because standardization may set a window that is illegal
    // in SQL, and hence, will fail analysis().
    private boolean resetWindow = false;

    // SQL string of this AnalyticExpr before standardization. Returned in toSqlImpl().
    private String sqlString;

    public AnalyticExpr(FunctionCallExpr fnCall, List<Expr> partitionExprs,
                        List<OrderByElement> orderByElements, AnalyticWindow window) {
        Preconditions.checkNotNull(fnCall);
        this.fnCall = fnCall;
        this.partitionExprs = partitionExprs != null ? partitionExprs : new ArrayList<Expr>();

        if (orderByElements != null) {
            this.orderByElements.addAll(orderByElements);
        }

        this.window = window;
        setChildren();
    }

    /**
     * clone() c'tor
     */
    protected AnalyticExpr(AnalyticExpr other) {
        super(other);
        fnCall = (FunctionCallExpr) other.fnCall.clone();

        for (OrderByElement e : other.orderByElements) {
            orderByElements.add(e.clone());
        }

        partitionExprs = Expr.cloneList(other.partitionExprs);
        window = (other.window != null ? other.window.clone() : null);
        resetWindow = other.resetWindow;
        sqlString = other.sqlString;
        setChildren();
    }

    public FunctionCallExpr getFnCall() {
        return fnCall;
    }

    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

    @Override
    public String getExprName() {
        if (!this.exprName.isPresent()) {
            this.exprName = Optional.of(Utils.normalizeName(getFnCall().getExprName(), DEFAULT_EXPR_NAME));
        }
        return this.exprName.get();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fnCall, orderByElements, window);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }

        AnalyticExpr o = (AnalyticExpr) obj;

        if (!fnCall.equals(o.getFnCall())) {
            return false;
        }

        if ((window == null) != (o.window == null)) {
            return false;
        }

        if (window != null) {
            if (!window.equals(o.window)) {
                return false;
            }
        }

        return orderByElements.equals(o.orderByElements);
    }

    /**
     * Analytic exprs cannot be constant.
     */
    @Override
    protected boolean isConstantImpl() {
        return false;
    }

    @Override
    public Expr clone() {
        return new AnalyticExpr(this);
    }

    @Override
    public String debugString() {
        return MoreObjects.toStringHelper(this)
               .add("fn", getFnCall())
               .add("window", window)
               .addValue(super.debugString())
               .toString();
    }

    @Override
    protected void toThrift(TExprNode msg) {
    }

    /**
     * Keep fnCall_, partitionExprs_ and orderByElements_ in sync with children_.
     */
    private void syncWithChildren() {
        int numArgs = fnCall.getChildren().size();

        for (int i = 0; i < numArgs; ++i) {
            fnCall.setChild(i, getChild(i));
        }

        int numPartitionExprs = partitionExprs.size();

        for (int i = 0; i < numPartitionExprs; ++i) {
            partitionExprs.set(i, getChild(numArgs + i));
        }

        for (int i = 0; i < orderByElements.size(); ++i) {
            orderByElements.get(i).setExpr(getChild(numArgs + numPartitionExprs + i));
        }
    }

    /**
     * Populate children_ from fnCall_, partitionExprs_, orderByElements_
     */
    private void setChildren() {
        getChildren().clear();
        addChildren(fnCall.getChildren());
        addChildren(partitionExprs);

        for (OrderByElement e : orderByElements) {
            addChild(e.getExpr());
        }

        if (window != null) {
            if (window.getLeftBoundary().getExpr() != null) {
                addChild(window.getLeftBoundary().getExpr());
            }

            if (window.getRightBoundary() != null
                    && window.getRightBoundary().getExpr() != null) {
                addChild(window.getRightBoundary().getExpr());
            }
        }
    }

    @Override
    protected void resetAnalysisState() {
        super.resetAnalysisState();
        fnCall.resetAnalysisState();

        if (resetWindow) {
            window = null;
        }

        resetWindow = false;
        // sync with children, now that they've been reset
        syncWithChildren();
    }

    @Override
    public String toSqlImpl() {
        if (sqlString != null) {
            return sqlString;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(fnCall.toSql()).append(" OVER (");
        boolean needsSpace = false;
        if (!partitionExprs.isEmpty()) {
            sb.append("PARTITION BY ").append(exprListToSql(partitionExprs));
            needsSpace = true;
        }
        if (!orderByElements.isEmpty()) {
            List<String> orderByStrings = Lists.newArrayList();
            for (OrderByElement e : orderByElements) {
                orderByStrings.add(e.toSql());
            }
            if (needsSpace) {
                sb.append(" ");
            }
            sb.append("ORDER BY ").append(Joiner.on(", ").join(orderByStrings));
            needsSpace = true;
        }
        if (window != null) {
            if (needsSpace) {
                sb.append(" ");
            }
            sb.append(window.toSql());
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        if (sqlString != null) {
            return sqlString;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(fnCall.toSql(disableTableName, needExternalSql, tableType, table)).append(" OVER (");
        boolean needsSpace = false;
        if (!partitionExprs.isEmpty()) {
            sb.append("PARTITION BY ").append(exprListToSql(partitionExprs));
            needsSpace = true;
        }
        if (!orderByElements.isEmpty()) {
            List<String> orderByStrings = Lists.newArrayList();
            for (OrderByElement e : orderByElements) {
                orderByStrings.add(e.toSql(disableTableName, needExternalSql, tableType, table));
            }
            if (needsSpace) {
                sb.append(" ");
            }
            sb.append("ORDER BY ").append(Joiner.on(", ").join(orderByStrings));
            needsSpace = true;
        }
        if (window != null) {
            if (needsSpace) {
                sb.append(" ");
            }
            sb.append(window.toSql(disableTableName, needExternalSql, tableType, table));
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String toDigestImpl() {
        StringBuilder sb = new StringBuilder();
        sb.append(fnCall.toDigest()).append(" OVER (");
        boolean needsSpace = false;
        if (!partitionExprs.isEmpty()) {
            sb.append("PARTITION BY ").append(exprListToDigest(partitionExprs));
            needsSpace = true;
        }
        if (!orderByElements.isEmpty()) {
            List<String> orderByStrings = Lists.newArrayList();
            for (OrderByElement e : orderByElements) {
                orderByStrings.add(e.toDigest());
            }
            if (needsSpace) {
                sb.append(" ");
            }
            sb.append("ORDER BY ").append(Joiner.on(", ").join(orderByStrings));
            needsSpace = true;
        }
        if (window != null) {
            if (needsSpace) {
                sb.append(" ");
            }
            sb.append(window.toDigest());
        }
        sb.append(")");
        return sb.toString();
    }

    private String exprListToSql(List<? extends Expr> exprs) {
        if (exprs == null || exprs.isEmpty()) {
            return "";
        }
        List<String> strings = Lists.newArrayList();
        for (Expr expr : exprs) {
            strings.add(expr.toSql());
        }
        return Joiner.on(", ").join(strings);
    }

    private String exprListToDigest(List<? extends Expr> exprs) {
        if (exprs == null || exprs.isEmpty()) {
            return "";
        }
        List<String> strings = Lists.newArrayList();
        for (Expr expr : exprs) {
            strings.add(expr.toDigest());
        }
        return Joiner.on(", ").join(strings);
    }

    @Override
    public boolean supportSerializable() {
        return false;
    }
}
