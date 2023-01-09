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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.rewrite.ExprRewriteRule;
import org.apache.doris.rewrite.ExprRewriter;

import com.google.common.base.Preconditions;

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
        if (expr instanceof ArithmeticExpr || expr instanceof FunctionCallExpr) {
            return matchExpr(expr, analyzer, clauseType);
        } else if (expr instanceof SlotRef) {
            return matchSlotRef((SlotRef) expr, analyzer, clauseType);
        } else {
            return expr;
        }
    }

    private Expr matchExpr(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType)
            throws AnalysisException {
        List<SlotRef> slots = new ArrayList<>();
        expr.collect(SlotRef.class, slots);

        if (slots.size() != 1) {
            return expr;
        }

        SlotRef queryColumnSlotRef = slots.get(0);

        Column column = queryColumnSlotRef.getColumn();
        TableIf table = queryColumnSlotRef.getTable();
        if (column == null || table == null || !(table instanceof OlapTable)) {
            return expr;
        }
        OlapTable olapTable = (OlapTable) table;

        Column mvColumn = olapTable.getVisibleColumn(expr.toSql());
        if (mvColumn == null) {
            mvColumn = olapTable.getVisibleColumn(
                    MaterializedIndexMeta.normalizeName(CreateMaterializedViewStmt.mvColumnBuilder(expr.toSql())));
        }

        if (mvColumn == null) {
            mvColumn = olapTable.getVisibleColumn(CreateMaterializedViewStmt.mvColumnBuilder(expr.toSql()));
        }

        if (mvColumn == null) {
            return expr;
        }

        return rewriteExpr(queryColumnSlotRef, mvColumn, analyzer);
    }

    private Expr matchSlotRef(SlotRef slot, Analyzer analyzer, ExprRewriter.ClauseType clauseType)
            throws AnalysisException {
        Column column = slot.getColumn();
        TableIf table = slot.getTable();
        if (column == null || table == null || !(table instanceof OlapTable)) {
            return slot;
        }
        OlapTable olapTable = (OlapTable) table;

        Column mvColumn = olapTable.getVisibleColumn(CreateMaterializedViewStmt.mvColumnBuilder(slot.toSql()));
        if (mvColumn == null) {
            mvColumn = olapTable.getVisibleColumn(
                    MaterializedIndexMeta.normalizeName(CreateMaterializedViewStmt.mvColumnBuilder(slot.toSql())));
        }

        if (mvColumn == null) {
            return slot;
        }

        return rewriteExpr(slot, mvColumn, analyzer);
    }

    private Expr rewriteExpr(SlotRef queryColumnSlotRef, Column mvColumn, Analyzer analyzer) {
        Preconditions.checkNotNull(mvColumn);
        Preconditions.checkNotNull(queryColumnSlotRef);
        TableName tableName = queryColumnSlotRef.getTableName();
        Preconditions.checkNotNull(tableName);
        SlotRef mvSlotRef = new SlotRef(tableName, mvColumn.getName());
        mvSlotRef.analyzeNoThrow(analyzer);
        return mvSlotRef;
    }
}
