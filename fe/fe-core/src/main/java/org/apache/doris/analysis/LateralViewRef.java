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

import org.apache.doris.catalog.InlineView;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * lateralView: LATERAL VIEW udtf(expression) tableAlias AS columnAlias (',' columnAlias)
 * fromClause: FROM baseTable (lateralView)
 */
public class LateralViewRef extends TableRef {

    private Expr expr;
    private String viewName;
    private String columnName;
    private TableRef relatedTableRef;

    // after analyzed
    private FunctionCallExpr fnExpr;
    private List<SlotRef> originSlotRefList = Lists.newArrayList();
    private InlineView view;
    private SlotRef explodeSlotRef;

    public LateralViewRef(Expr expr, String viewName, String columnName) {
        super(null, viewName);
        this.expr = expr;
        this.viewName = viewName;
        this.columnName = columnName;
    }

    public void setRelatedTable(TableRef relatedTableRef) {
        this.relatedTableRef = relatedTableRef;
    }

    public FunctionCallExpr getFnExpr() {
        return fnExpr;
    }

    @Override
    public void analyze() throws UserException {
    }

    @Override
    public TableRef clone() {
        return new LateralViewRef(this.expr.clone(), this.viewName, this.columnName);
    }


    @Override
    public TupleDescriptor createTupleDescriptor() throws AnalysisException {
        return null;
    }

    // 1. it must be a scalar function
    private void checkScalarFunction(Expr child0) throws AnalysisException {

    }

    @Override
    public String toSql() {
        return "lateral view " + expr.toSql() + " `" + viewName + "` as `" + columnName + "`";
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public void reset() {
        isAnalyzed = false;
        expr.reset();
        fnExpr = null;
        originSlotRefList = Lists.newArrayList();
        view = null;
        explodeSlotRef = null;
        // There is no need to call the reset function of @relatedTableRef here.
        // The main reason is that @lateralViewRef itself is an attribute of @relatedTableRef
        // The reset of @lateralViewRef happens in the reset() of @relatedTableRef.
    }
}
