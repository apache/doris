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
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.rewrite.ExprRewriteRule;
import org.apache.doris.rewrite.ExprRewriter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Rewrite expr to mv_expr
 */
public class ExprToSlotRefRule implements ExprRewriteRule {

    private Set<TupleId> disableTuplesMVRewriter = Sets.newHashSet();
    private ExprSubstitutionMap mvSMap;
    private ExprSubstitutionMap aliasSMap;

    public void setInfoMVRewriter(Set<TupleId> disableTuplesMVRewriter, ExprSubstitutionMap mvSMap,
            ExprSubstitutionMap aliasSMap) {
        this.disableTuplesMVRewriter.addAll(disableTuplesMVRewriter);
        this.mvSMap = mvSMap;
        this.aliasSMap = aliasSMap;
    }

    private Pair<OlapTable, TableName> getTable(Expr expr) {
        List<SlotRef> slots = new ArrayList<>();
        expr.collect(SlotRef.class, slots);

        OlapTable result = null;
        TableName tableName = null;
        for (SlotRef slot : slots) {
            TableIf table = slot.getTable();
            if (table == null) {
                continue;
            }
            if (!(table instanceof OlapTable)) {
                return null;
            }

            if (result == null) {
                result = (OlapTable) table;
                tableName = slot.getTableName();
            } else if (!tableName.equals(slot.getTableName())) {
                return null;
            }
        }
        return Pair.of(result, tableName);
    }

    public boolean isDisableTuplesMVRewriter(Expr expr) {
        boolean result;
        try {
            result = expr.isRelativedByTupleIds(disableTuplesMVRewriter.stream().collect(Collectors.toList()));
        } catch (Exception e) {
            result = true;
        }
        return result;
    }

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (isDisableTuplesMVRewriter(expr)) {
            return expr;
        }

        Pair<OlapTable, TableName> info = getTable(expr);
        if (info == null || info.first == null || info.second == null) {
            return expr;
        }

        if (expr instanceof FunctionCallExpr && ((FunctionCallExpr) expr).isAggregate()) {
            return matchAggregateExpr((FunctionCallExpr) expr, analyzer, info.first, info.second);
        } else {
            return matchExpr(expr, analyzer, info.first, info.second);
        }
    }

    private Expr matchAggregateExprCount(FunctionCallExpr expr, Analyzer analyzer, OlapTable olapTable,
            TableName tableName)
            throws AnalysisException {
        if (expr.isDistinct()) {
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
        } else {
            if (expr.getChildren().size() != 1) {
                return expr;
            }
            Expr caseWhen = CountFieldToSum.slotToCaseWhen(expr.getChildren().get(0));
            List<Expr> params = Lists.newArrayList();
            params.add(caseWhen);
            FunctionCallExpr newCount = new FunctionCallExpr("sum", params);

            String mvColumnName = CreateMaterializedViewStmt
                    .mvColumnBuilder(AggregateType.SUM, caseWhen.toSqlWithoutTbl());
            Column mvColumn = olapTable.getVisibleColumn(mvColumnName);

            if (mvColumn != null) {
                newCount.setChild(0, new SlotRef(null, mvColumnName));
                newCount.analyzeNoThrow(analyzer);
                return newCount;
            }
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
            expr.setChild(0, rewriteExpr(tableName, mvColumn, analyzer, expr.getChild(0)));
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
            expr.setChild(0, rewriteExpr(tableName, mvColumn, analyzer, expr.getChild(0)));
        }

        return expr;
    }

    private Expr matchAggregateExpr(FunctionCallExpr expr, Analyzer analyzer, OlapTable olapTable,
            TableName tableName)
            throws AnalysisException {
        if (expr.getChildren().size() != 1) {
            return expr;
        }

        String fnName = expr.getFnName().getFunction();
        if (fnName.equalsIgnoreCase(FunctionSet.COUNT)) {
            Expr result = matchAggregateExprCount(expr, analyzer, olapTable, tableName);
            if (result != expr) {
                return result;
            }
        }
        if (fnName.equalsIgnoreCase(FunctionSet.APPROX_COUNT_DISTINCT) || fnName.equalsIgnoreCase(FunctionSet.NDV)) {
            Expr result = matchAggregateExprCountApprox(expr, analyzer, olapTable, tableName);
            if (result != expr) {
                return result;
            }
        }
        if (fnName.equalsIgnoreCase(FunctionSet.BITMAP_UNION)
                || fnName.equalsIgnoreCase(FunctionSet.BITMAP_UNION_COUNT)) {
            return matchAggregateExprBitmap(expr, analyzer, olapTable, tableName);
        }
        if (fnName.equalsIgnoreCase(FunctionSet.HLL_UNION)
                || fnName.equalsIgnoreCase(FunctionSet.HLL_UNION_AGG)
                || fnName.equalsIgnoreCase(FunctionSet.HLL_RAW_AGG)) {
            return matchAggregateExprHll(expr, analyzer, olapTable, tableName);
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
            expr.setChild(0, rewriteExpr(tableName, mvColumn, analyzer, expr.getChild(0)));
        }

        return expr;
    }

    private Expr matchExpr(Expr expr, Analyzer analyzer, OlapTable olapTable,
            TableName tableName)
            throws AnalysisException {
        Column mvColumn = olapTable
                .getVisibleColumn(CreateMaterializedViewStmt.mvColumnBuilder(expr.toSqlWithoutTbl()));

        if (mvColumn == null) {
            return expr;
        } else {
            Expr rhs = mvColumn.getDefineExpr();
            if (rhs == null || !rhs.getClass().equals(expr.getClass())) {
                return expr;
            }
        }

        return rewriteExpr(tableName, mvColumn, analyzer, expr);
    }

    private Expr rewriteExpr(TableName tableName, Column mvColumn, Analyzer analyzer, Expr originExpr)
            throws AnalysisException {
        Preconditions.checkNotNull(mvColumn);
        Preconditions.checkNotNull(tableName);
        SlotRef mvSlotRef = new SlotRef(tableName, mvColumn.getDefineName());
        mvSlotRef.analyzeNoThrow(analyzer);

        originExpr.analyze(analyzer);
        mvSMap.put(mvSlotRef, originExpr.trySubstitute(aliasSMap, analyzer, false).clone());
        return mvSlotRef;
    }
}
