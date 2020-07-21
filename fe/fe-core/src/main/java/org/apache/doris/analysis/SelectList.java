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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.rewrite.ExprRewriter;

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Select list items plus distinct clause.
 */
public class SelectList {
    private boolean isDistinct;

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
    }

    public SelectList() {
        items = Lists.newArrayList();
        this.isDistinct = false;
    }
    
    public SelectList(List<SelectListItem> items, boolean isDistinct) {
        this.isDistinct = isDistinct;
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
