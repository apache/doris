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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/SelectListItem.java
// and modified by Doris

package org.apache.doris.analysis;

import com.google.common.base.Preconditions;

public class SelectListItem {
    private Expr expr;
    // for "[name.]*"
    private final TableName tblName;
    private final boolean isStar;
    private String alias;

    public SelectListItem(Expr expr, String alias) {
        super();
        Preconditions.checkNotNull(expr);
        this.expr = expr;
        this.alias = alias;
        this.tblName = null;
        this.isStar = false;
    }

    private SelectListItem(TableName tblName) {
        super();
        this.expr = null;
        this.tblName = tblName;
        this.isStar = true;
    }

    protected SelectListItem(SelectListItem other) {
        if (other.expr == null) {
            expr = null;
        } else {
            expr = other.expr.clone().reset();
        }
        tblName = other.tblName;
        isStar = other.isStar;
        alias = other.alias;
    }

    @Override
    public SelectListItem clone() {
        return new SelectListItem(this);
    }

    // select list item corresponding to "[[db.]tbl.]*"
    public static SelectListItem createStarItem(TableName tblName) {
        return new SelectListItem(tblName);
    }

    public boolean isStar() {
        return isStar;
    }

    public TableName getTblName() {
        return tblName;
    }

    public Expr getExpr() {
        return expr;
    }

    public void setExpr(Expr expr) {
        this.expr = expr;
    }

    public String getAlias() {
        return alias;
    }

    public String toSql() {
        if (!isStar) {
            Preconditions.checkNotNull(expr);
            String aliasSql = null;
            if (alias != null) {
                aliasSql = "`" + alias + "`";
            }
            return expr.toSql() + ((aliasSql == null) ? "" : " " + aliasSql);
        } else if (tblName != null) {
            return tblName.toString() + ".*";
        } else {
            return "*";
        }
    }

    public String toDigest() {
        if (!isStar) {
            Preconditions.checkNotNull(expr);
            String aliasSql = null;
            if (alias != null) {
                aliasSql = "`" + alias + "`";
            }
            return expr.toDigest() + ((aliasSql == null) ? "" : " " + aliasSql);
        } else if (tblName != null) {
            return tblName.toString() + ".*";
        } else {
            return "*";
        }
    }

    /**
     * Return a column label for the select list item. Without generate column name
     * automatically.
     */
    @Deprecated
    public String toColumnLabel() {
        Preconditions.checkState(!isStar());
        if (alias != null) {
            return alias;
        }
        // Abbreviate the toSql() for analytic exprs.
        if (expr instanceof AnalyticExpr) {
            AnalyticExpr analyticExpr = (AnalyticExpr) expr;
            return analyticExpr.getFnCall().toSql() + " OVER(...)";
        }
        return expr.toColumnLabel();
    }

    /**
     * Return a column label for the select list item. Support to generate
     * column label automatically when can not get the column label exactly.
     * Need the position of selectListItem to generate column label
     */
    public String toColumnLabel(int position) {
        Preconditions.checkState(!isStar(), "select item should not be star when get column label");
        if (alias != null) {
            return alias;
        }
        if (expr instanceof SlotRef) {
            return expr.getExprName();
        }
        return "__" + expr.getExprName() + "_" + position;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
