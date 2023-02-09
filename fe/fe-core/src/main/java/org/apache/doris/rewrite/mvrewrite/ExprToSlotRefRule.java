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

package org.apache.doris.rewrite.mvrewrite;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.rewrite.ExprRewriteRule;
import org.apache.doris.rewrite.ExprRewriter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrite expr to mv_expr
 */
public class ExprToSlotRefRule implements ExprRewriteRule {

    public static final ExprRewriteRule INSTANCE = new ExprToSlotRefRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        // todo: support more pattern
        if (expr instanceof FunctionCallExpr && ((FunctionCallExpr) expr).isAggregate()) {
            return matchAggregateExpr((FunctionCallExpr) expr, analyzer);
        } else if (expr instanceof ArithmeticExpr || expr instanceof FunctionCallExpr) {
            return matchExpr(expr, analyzer);
        } else if (expr instanceof SlotRef) {
            return matchSlotRef((SlotRef) expr, analyzer);
        } else {
            return expr;
        }
    }

    private Expr matchAggregateExprCount(FunctionCallExpr expr, Analyzer analyzer, OlapTable olapTable,
            TableName tableName)
            throws AnalysisException {
        if (!expr.isDistinct()) {
            return expr;
        }

        FunctionCallExpr toBitmap = new FunctionCallExpr(FunctionSet.TO_BITMAP, expr.getChildren());
        toBitmap.setType(Type.BITMAP);
        List<Expr> params = Lists.newArrayList();
        params.add(toBitmap);
        FunctionCallExpr newCount = new FunctionCallExpr(FunctionSet.BITMAP_UNION_COUNT, params);

        Expr result = matchAggregateExprBitmap(newCount, analyzer, olapTable, tableName);
        if (result != newCount) {
            result.analyzeNoThrow(analyzer);
            return result;
        }
        return expr;
    }

    private Expr matchAggregateExprCountApprox(FunctionCallExpr expr, Analyzer analyzer, OlapTable olapTable,
            TableName tableName)
            throws AnalysisException {
        FunctionCallExpr hllHash = new FunctionCallExpr(FunctionSet.HLL_HASH, expr.getChildren());
        hllHash.setType(Type.HLL);
        List<Expr> params = Lists.newArrayList();
        params.add(hllHash);
        FunctionCallExpr newCount = new FunctionCallExpr(FunctionSet.HLL_UNION_AGG, params);

        Expr result = matchAggregateExprHll(newCount, analyzer, olapTable, tableName);
        if (result != newCount) {
            result.analyzeNoThrow(analyzer);
            return result;
        }
        return expr;
    }

    private Expr matchAggregateExprBitmap(Expr expr, Analyzer analyzer, OlapTable olapTable, TableName tableName)
            throws AnalysisException {
        Expr child0 = expr.getChild(0);
        if (!child0.getType().isBitmapType()) {
            return expr;
        }
        String mvColumnName = CreateMaterializedViewStmt
                .mvColumnBuilder(AggregateType.BITMAP_UNION, child0.toSqlWithoutTbl());
        Column mvColumn = olapTable.getVisibleColumn(mvColumnName);

        if (mvColumn == null) {
            mvColumn = olapTable
                    .getVisibleColumn(mvColumnName.replace(FunctionSet.TO_BITMAP, FunctionSet.TO_BITMAP_WITH_CHECK));
        }

        if (mvColumn != null) {
            expr = expr.clone();
            expr.setChild(0, rewriteExpr(tableName, mvColumn, analyzer));
        }

        return expr;
    }

    private Expr matchAggregateExprHll(Expr expr, Analyzer analyzer, OlapTable olapTable, TableName tableName)
            throws AnalysisException {
        Expr child0 = expr.getChild(0);
        if (!child0.getType().isHllType()) {
            return expr;
        }
        String mvColumnName = CreateMaterializedViewStmt
                .mvColumnBuilder(AggregateType.HLL_UNION, child0.toSqlWithoutTbl());
        Column mvColumn = olapTable.getVisibleColumn(mvColumnName);

        if (mvColumn != null) {
            expr = expr.clone();
            expr.setChild(0, rewriteExpr(tableName, mvColumn, analyzer));
        }

        return expr;
    }

    private Expr matchAggregateExpr(FunctionCallExpr expr, Analyzer analyzer)
            throws AnalysisException {
        List<SlotRef> slots = new ArrayList<>();
        expr.collect(SlotRef.class, slots);

        if (expr.getChildren().size() != 1 || slots.size() != 1) {
            return expr;
        }

        TableIf table = slots.get(0).getTable();
        if (table == null || !(table instanceof OlapTable)) {
            return expr;
        }
        OlapTable olapTable = (OlapTable) table;

        String fnName = expr.getFnName().getFunction();
        if (fnName.equalsIgnoreCase(FunctionSet.COUNT)) {
            Expr result = matchAggregateExprCount(expr, analyzer, olapTable, slots.get(0).getTableName());
            if (result != expr) {
                return result;
            }
        }
        if (fnName.equalsIgnoreCase(FunctionSet.APPROX_COUNT_DISTINCT) || fnName.equalsIgnoreCase(FunctionSet.NDV)) {
            Expr result = matchAggregateExprCountApprox(expr, analyzer, olapTable, slots.get(0).getTableName());
            if (result != expr) {
                return result;
            }
        }
        if (fnName.equalsIgnoreCase(FunctionSet.BITMAP_UNION)
                || fnName.equalsIgnoreCase(FunctionSet.BITMAP_UNION_COUNT)) {
            return matchAggregateExprBitmap(expr, analyzer, olapTable, slots.get(0).getTableName());
        }
        if (fnName.equalsIgnoreCase(FunctionSet.HLL_UNION)
                || fnName.equalsIgnoreCase(FunctionSet.HLL_UNION_AGG)
                || fnName.equalsIgnoreCase(FunctionSet.HLL_RAW_AGG)) {
            return matchAggregateExprHll(expr, analyzer, olapTable, slots.get(0).getTableName());
        }

        Expr child0 = expr.getChild(0);

        Column mvColumn = olapTable.getVisibleColumn(
                CreateMaterializedViewStmt.mvAggregateColumnBuilder(expr.getFnName().getFunction(),
                        child0.toSqlWithoutTbl()));
        if (mvColumn == null) {
            mvColumn = olapTable.getVisibleColumn(
                    CreateMaterializedViewStmt.mvAggregateColumnBuilder(expr.getFnName().getFunction(),
                            CreateMaterializedViewStmt.mvColumnBuilder(child0.toSqlWithoutTbl())));
        }

        if (mvColumn != null) {
            expr = (FunctionCallExpr) expr.clone();
            expr.setChild(0, rewriteExpr(slots.get(0).getTableName(), mvColumn, analyzer));
        }

        return expr;
    }

    private Expr matchExpr(Expr expr, Analyzer analyzer)
            throws AnalysisException {
        List<SlotRef> slots = new ArrayList<>();
        expr.collect(SlotRef.class, slots);

        if (slots.size() != 1) {
            return expr;
        }

        SlotRef queryColumnSlotRef = slots.get(0);

        TableIf table = queryColumnSlotRef.getTable();
        if (table == null || !(table instanceof OlapTable)) {
            return expr;
        }
        OlapTable olapTable = (OlapTable) table;

        Column mvColumn = olapTable.getVisibleColumn(expr.toSqlWithoutTbl());
        if (mvColumn == null) {
            mvColumn = olapTable.getVisibleColumn(
                    MaterializedIndexMeta
                            .normalizeName(CreateMaterializedViewStmt.mvColumnBuilder(expr.toSqlWithoutTbl())));
        }

        if (mvColumn == null) {
            mvColumn = olapTable.getVisibleColumn(CreateMaterializedViewStmt.mvColumnBuilder(expr.toSqlWithoutTbl()));
        }

        if (mvColumn == null) {
            return expr;
        }

        return rewriteExpr(queryColumnSlotRef.getTableName(), mvColumn, analyzer);
    }

    private Expr matchSlotRef(SlotRef slot, Analyzer analyzer)
            throws AnalysisException {
        TableIf table = slot.getTable();
        if (table == null || !(table instanceof OlapTable)) {
            return slot;
        }
        OlapTable olapTable = (OlapTable) table;

        Column mvColumn = olapTable
                .getVisibleColumn(CreateMaterializedViewStmt.mvColumnBuilder(slot.toSqlWithoutTbl()));
        if (mvColumn == null) {
            mvColumn = olapTable.getVisibleColumn(
                    MaterializedIndexMeta
                            .normalizeName(CreateMaterializedViewStmt.mvColumnBuilder(slot.toSqlWithoutTbl())));
        }

        if (mvColumn == null) {
            return slot;
        }

        return rewriteExpr(slot.getTableName(), mvColumn, analyzer);
    }

    private Expr rewriteExpr(TableName tableName, Column mvColumn, Analyzer analyzer) {
        Preconditions.checkNotNull(mvColumn);
        Preconditions.checkNotNull(tableName);
        SlotRef mvSlotRef = new SlotRef(tableName, mvColumn.getName());
        mvSlotRef.analyzeNoThrow(analyzer);
        return mvSlotRef;
    }
}
