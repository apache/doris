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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/SelectList.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.rewrite.ExprRewriter;

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Select list items plus distinct clause.
 */
public class SelectList {
    public static final String SET_VAR_KEY = "set_var";

    private boolean isDistinct;
    private boolean isExcept;
    private Map<String, String> optHints;
    private List<OrderByElement> orderByElements;

    // ///////////////////////////////////////
    // BEGIN: Members that need to be reset()

    private final List<SelectListItem> items;

    // END: Members that need to be reset()
    // ///////////////////////////////////////

    public SelectList(SelectList other) {
        items = Lists.newArrayList();
        for (SelectListItem item : other.items) {
            items.add(item.clone());
        }
        isDistinct = other.isDistinct;
        isExcept = other.isExcept;
    }

    public List<Expr> getExprs() {
        List<Expr> exprs = new ArrayList<Expr>();
        for (SelectListItem item : items) {
            exprs.add(item.getExpr());
        }
        return exprs;
    }

    public SelectList() {
        items = Lists.newArrayList();
        this.isDistinct = false;
        this.isExcept = false;
    }

    public SelectList(List<SelectListItem> items, boolean isDistinct) {
        this.isDistinct = isDistinct;
        this.isExcept = false;
        this.items = items;
    }

    public List<SelectListItem> getItems() {
        return items;
    }

    public void addItem(SelectListItem item) {
        items.add(item);
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setIsDistinct(boolean value) {
        isDistinct = value;
    }

    public boolean isExcept() {
        return isExcept;
    }

    public void setIsExcept(boolean except) {
        isExcept = except;
    }

    public Map<String, String> getOptHints() {
        return optHints;
    }

    public void setOptHints(Map<String, Map<String, String>> optHints) {
        if (optHints != null) {
            this.optHints = optHints.get(SET_VAR_KEY);
        }
    }

    public void setOrderByElements(List<OrderByElement> orderByElements) {
        if (orderByElements != null) {
            this.orderByElements = orderByElements;
        }
    }

    public void reset() {
        for (SelectListItem item : items) {
            if (!item.isStar()) {
                item.getExpr().reset();
            }
        }
    }

    public void rewriteExprs(ExprRewriter rewriter, Analyzer analyzer)
            throws AnalysisException {
        for (SelectListItem item : items) {
            if (item.isStar()) {
                continue;
            }
            // equal subquery in select list
            if (item.getExpr().contains(Predicates.instanceOf(Subquery.class))) {
                List<Subquery> subqueryExprs = Lists.newArrayList();
                item.getExpr().collect(Subquery.class, subqueryExprs);
                for (Subquery s : subqueryExprs) {
                    s.getStatement().rewriteExprs(rewriter);
                }
            }
            item.setExpr(rewriter.rewrite(item.getExpr(), analyzer));
        }
    }

    @Override
    public SelectList clone() {
        return new SelectList(this);
    }
}
