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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/OrderByElement.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Combination of expr and ASC/DESC, and nulls ordering.
 */
public class OrderByElement {
    private Expr expr;
    private final boolean isAsc;

    // Represents the NULLs ordering specified: true when "NULLS FIRST", false when
    // "NULLS LAST", and null if not specified.
    private final Boolean nullsFirstParam;

    public OrderByElement(Expr expr, boolean isAsc, Boolean nullsFirstParam) {
        super();
        this.expr = expr;
        this.isAsc = isAsc;
        this.nullsFirstParam = nullsFirstParam;
    }

    public void setExpr(Expr e) {
        this.expr = e;
    }

    public Expr getExpr() {
        return expr;
    }

    public boolean getIsAsc() {
        return isAsc;
    }

    public Boolean getNullsFirstParam() {
        return nullsFirstParam;
    }

    public OrderByElement clone() {
        OrderByElement clone = new OrderByElement(
                expr.clone(), isAsc, nullsFirstParam);
        return clone;
    }

    /**
     * Returns a new list of OrderByElements with the same (cloned) expressions but the
     * ordering direction reversed (asc becomes desc, nulls first becomes nulls last, etc.)
     */
    public static List<OrderByElement> reverse(List<OrderByElement> src) {
        List<OrderByElement> result = Lists.newArrayListWithCapacity(src.size());

        for (int i = 0; i < src.size(); ++i) {
            OrderByElement element = src.get(i);
            OrderByElement reverseElement =
                    new OrderByElement(element.getExpr().clone(), !element.isAsc,
                            !nullsFirst(element.nullsFirstParam, element.isAsc));
            result.add(reverseElement);
        }

        return result;
    }

    /**
     * Extracts the order-by exprs from the list of order-by elements and returns them.
     */
    public static List<Expr> getOrderByExprs(List<OrderByElement> src) {
        List<Expr> result = Lists.newArrayListWithCapacity(src.size());

        for (OrderByElement element : src) {
            result.add(element.getExpr());
        }

        return result;
    }

    /**
     * Returns a new list of order-by elements with the order by exprs of src substituted
     * according to smap. Preserves the other sort params from src.
     * @throws AnalysisException
     */
    public static ArrayList<OrderByElement> substitute(List<OrderByElement> src,
            ExprSubstitutionMap smap, Analyzer analyzer) throws AnalysisException {
        ArrayList<OrderByElement> result = Lists.newArrayListWithCapacity(src.size());

        for (OrderByElement element : src) {
            result.add(new OrderByElement(element.getExpr().substitute(smap, analyzer, false),
                    element.isAsc, element.nullsFirstParam));
        }

        return result;
    }

    public String toSql() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(expr.toSql());
        strBuilder.append(isAsc ? " ASC" : " DESC");

        // When ASC and NULLS LAST or DESC and NULLS FIRST, we do not print NULLS FIRST/LAST
        // because it is the default behavior and we want to avoid printing NULLS FIRST/LAST
        // whenever possible as it is incompatible with Hive (SQL compatibility with Hive is
        // important for views).
        if (nullsFirstParam != null) {
            if (isAsc && nullsFirstParam) {
                // If ascending, nulls are last by default, so only add if nulls first.
                strBuilder.append(" NULLS FIRST");
            } else if (!isAsc && !nullsFirstParam) {
                // If descending, nulls are first by default, so only add if nulls last.
                strBuilder.append(" NULLS LAST");
            }
        }

        return strBuilder.toString();
    }

    public String toDigest() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(expr.toDigest());
        strBuilder.append(isAsc ? " ASC" : " DESC");
        if (nullsFirstParam != null) {
            if (isAsc && nullsFirstParam) {
                // If ascending, nulls are last by default, so only add if nulls first.
                strBuilder.append(" NULLS FIRST");
            } else if (!isAsc && !nullsFirstParam) {
                // If descending, nulls are first by default, so only add if nulls last.
                strBuilder.append(" NULLS LAST");
            }
        }
        return strBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        OrderByElement o = (OrderByElement) obj;
        return expr.equals(o.expr) && isAsc == o.isAsc  && nullsFirstParam == o.nullsFirstParam;
    }

    /**
     * Compute nullsFirst.
     *
     * @param nullsFirstParam True if "NULLS FIRST", false if "NULLS LAST", or null if
     *                        the NULLs order was not specified.
     * @param isAsc
     * @return Returns true if nulls are ordered first or false if nulls are ordered last.
     *         Independent of isAsc.
     */
    public static boolean nullsFirst(Boolean nullsFirstParam, boolean isAsc) {
        return nullsFirstParam == null ? !isAsc : nullsFirstParam;
    }
}
