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
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.InlineView;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
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
    public void analyze(Analyzer analyzer) throws UserException {
        if (isAnalyzed) {
            return;
        }
        Preconditions.checkNotNull(relatedTableRef);
        // analyze function and slot
        if (!(expr instanceof FunctionCallExpr)) {
            throw new AnalysisException("Only support function call expr in lateral view");
        }

        analyzeFunctionExpr(analyzer);

        // analyze lateral view
        desc = analyzer.registerTableRef(this);
        explodeSlotRef = new SlotRef(new TableName(null, null, viewName), columnName);
        explodeSlotRef.analyze(analyzer);
        explodeSlotRef.getDesc().setIsNullable(
                explodeSlotRef.getDesc().getIsNullable() || relatedTableRef.getDesc().getSlots()
                        .stream().anyMatch(slotDescriptor -> slotDescriptor.getIsNullable()));
        isAnalyzed = true;  // true now that we have assigned desc
    }

    @Override
    public TableRef clone() {
        return new LateralViewRef(this.expr.clone(), this.viewName, this.columnName);
    }

    private void analyzeFunctionExpr(Analyzer analyzer) throws AnalysisException {
        fnExpr = (FunctionCallExpr) expr;
        fnExpr.setTableFnCall(true);
        checkAndSupplyDefaultTableName(fnExpr);
        fnExpr.analyze(analyzer);
        for (Expr expr : fnExpr.getChildren()) {
            checkScalarFunction(expr);
        }
    }

    @Override
    public TupleDescriptor createTupleDescriptor(Analyzer analyzer) throws AnalysisException {
        // Create a fake catalog table for the lateral view
        List<Column> columnList = Lists.newArrayList();
        columnList.add(new Column(columnName, fnExpr.getFn().getReturnType(), false, null,
                fnExpr.getFn().getNullableMode() == NullableMode.ALWAYS_NULLABLE, null, ""));
        view = new InlineView(viewName, columnList);

        // Create the non-materialized tuple and set the fake table in it.
        TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor();
        result.setTable(view);
        return result;
    }

    public void materializeRequiredSlots(ExprSubstitutionMap baseTblSmap, Analyzer analyzer) throws AnalysisException {
        Expr substituteFnExpr = fnExpr;
        if (relatedTableRef instanceof InlineViewRef) {
            substituteFnExpr = fnExpr.trySubstitute(baseTblSmap, analyzer, false);
        }
        substituteFnExpr.collect(SlotRef.class, originSlotRefList);
        for (SlotRef originSlotRef : originSlotRefList) {
            originSlotRef.getDesc().setIsMaterialized(true);
        }
        explodeSlotRef.getDesc().setIsMaterialized(true);
    }

    // The default table name must be origin table name
    // If there is table name in slot ref which is different from origin, it will thrown exception.
    private void checkAndSupplyDefaultTableName(FunctionCallExpr expr) throws AnalysisException {
        List<SlotRef> slotRefList = Lists.newArrayList();
        expr.collect(SlotRef.class, slotRefList);
        TableName relatedTableName = relatedTableRef.getAliasAsName();
        for (SlotRef slotRef : slotRefList) {
            TableName tableName = slotRef.getOriginTableName();
            if (tableName == null) {
                // t1 lateral view explode_split(k1, ",")
                slotRef.setTblName(relatedTableName.cloneWithoutAnalyze());
                return;
            }
            if (tableName.getDb() != null && !tableName.getDb().equalsIgnoreCase(relatedTableName.getDb())) {
                // db2.t1 lateral view explode_split(db1.t1.k1, ",")
                throw new AnalysisException("The column " + slotRef.toSql()
                        + " in lateral view must come from the origin table "
                        + relatedTableRef.toSql());
            }

            if (tableName.getTbl() != null) {
                switch (Config.lower_case_table_names) {
                    case 0:
                        if (tableName.getTbl().equals(relatedTableName.getTbl())) {
                            // t1 lateral view explode_split(t1.k1, ",")
                            tableName.setDb(relatedTableName.getDb());
                            return;
                        }
                        break;
                    case 1:
                    case 2:
                        if (tableName.getTbl().equalsIgnoreCase(relatedTableName.getTbl())) {
                            tableName.setTbl(relatedTableName.getTbl());
                            tableName.setDb(relatedTableName.getDb());
                            return;
                        }
                        break;
                    default:
                        throw new AnalysisException("Not support specify table name in table function "
                                + "when config.lower_case_table_names is not 0, 1 or 2");
                }
                // t1 lateral view explode_split(t2.k1, ",")
                throw new AnalysisException("The column " + slotRef.toSql()
                        + " in lateral view must come from the origin table "
                        + relatedTableName.toSql());
            }
        }
    }

    // 1. it must be a scalar function
    private void checkScalarFunction(Expr child0) throws AnalysisException {
        List<GroupingFunctionCallExpr> groupingFunctionCallExprList = Lists.newArrayList();
        child0.collect(GroupingFunctionCallExpr.class, groupingFunctionCallExprList);
        if (!groupingFunctionCallExprList.isEmpty()) {
            throw new AnalysisException("Grouping function are not allowed in lateral view.");
        }
        if (child0.containsAggregate()) {
            throw new AnalysisException("Agg function are not allowed in lateral view.");
        }
        List<AnalyticExpr> analyticExprList = Lists.newArrayList();
        child0.collect(AnalyticExpr.class, analyticExprList);
        if (!analyticExprList.isEmpty()) {
            throw new AnalysisException("Analytic expr are not allowed in lateral view.");
        }
        List<Subquery> subqueryList = Lists.newArrayList();
        child0.collect(Subquery.class, subqueryList);
        if (!subqueryList.isEmpty()) {
            throw new AnalysisException("Subquery is not allowed in lateral view");
        }
    }

    @Override
    public String toSql() {
        return "lateral view " + expr.toSql() + " " + viewName + " as " + columnName;
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
