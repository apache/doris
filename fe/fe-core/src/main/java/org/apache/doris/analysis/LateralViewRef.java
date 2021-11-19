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
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.InlineView;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import com.google.common.base.Preconditions;
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
    private Table relatedTable;

    // after analyzed
    private FunctionCallExpr fnExpr;
    private Column originColumn;
    private SlotRef originSlotRef;
    private InlineView view;
    private SlotRef explodeSlotRef;

    public LateralViewRef(Expr expr, String viewName, String columnName) {
        super(null, viewName);
        this.expr = expr;
        this.viewName = viewName;
        this.columnName = columnName;
    }

    public void setRelatedTable(Table relatedTable) {
        this.relatedTable = relatedTable;
    }

    public FunctionCallExpr getFnExpr() {
        return fnExpr;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (!analyzer.getContext().getSessionVariable().isEnableLateralView()) {
            throw new AnalysisException("The session variables `enable_lateral_view` is false");
        }

        if (isAnalyzed) {
            return;
        }
        Preconditions.checkNotNull(relatedTable);
        // analyze table
        if (!(relatedTable instanceof OlapTable)) {
            throw new AnalysisException("Only doris table could be exploded");
        }
        // analyze function and slot
        if (!(expr instanceof FunctionCallExpr)) {
            throw new AnalysisException("Only support function call expr in lateral view");
        }
        fnExpr = (FunctionCallExpr) expr;
        fnExpr.setTableFnCall(true);
        fnExpr.analyze(analyzer);
        if (!fnExpr.getFnName().getFunction().equals(FunctionSet.EXPLODE_SPLIT)) {
            throw new AnalysisException("Only support explode function in lateral view");
        }
        if (!(fnExpr.getChild(0) instanceof SlotRef)) {
            throw new AnalysisException("Explode column must be varchar column");
        }
        if (!(fnExpr.getChild(1) instanceof StringLiteral)) {
            throw new AnalysisException("Split separator of explode must be a string const");
        }
        originSlotRef = ((SlotRef) fnExpr.getChild(0));
        originColumn = originSlotRef.getColumn();
        if (originColumn == null) {
            throw new AnalysisException("The explode column must be a real column in table");
        }
        if (!originColumn.getType().isStringType()) {
           throw new AnalysisException("The explode column must be VARCHAR/CHAR/STRING type");
        }
        // analyze lateral view
        desc = analyzer.registerTableRef(this);
        explodeSlotRef = new SlotRef(new TableName(null, viewName), columnName);
        explodeSlotRef.analyze(analyzer);
        isAnalyzed = true;  // true now that we have assigned desc
    }

    @Override
    public TupleDescriptor createTupleDescriptor(Analyzer analyzer) throws AnalysisException {
        // Create a fake catalog table for the lateral view
        List<Column> columnList = Lists.newArrayList();
        columnList.add(new Column(columnName, originColumn.getType(),
                false, null, originColumn.isAllowNull(),
                null, ""));
        view = new InlineView(viewName, columnList);

        // Create the non-materialized tuple and set the fake table in it.
        TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor();
        result.setTable(view);
        return result;
    }

    public void materializeRequiredSlots() {
        originSlotRef.getDesc().setIsMaterialized(true);
        explodeSlotRef.getDesc().setIsMaterialized(true);
    }
}
