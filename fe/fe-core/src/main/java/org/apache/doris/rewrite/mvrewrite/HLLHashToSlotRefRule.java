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
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.rewrite.ExprRewriteRule;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.doris.rewrite.ExprRewriter;

import java.util.List;

/*
Rewrite hll_union(hll_hash(c1)) to hll_union(mv_hll_union_c1)
Rewrite hll_raw_agg(hll_hash(c1)) to hll_raw_agg(mv_hll_union_c1)
Rewrite hll_union_agg(hll_hash(c1)) to hll_union_agg(mv_hll_union_c1)
 */
public class HLLHashToSlotRefRule implements ExprRewriteRule {

    public static final ExprRewriteRule INSTANCE = new HLLHashToSlotRefRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        SlotRef queryColumnSlotRef;
        Column mvColumn;

        // meet the condition
        if (!(expr instanceof FunctionCallExpr)) {
            return expr;
        }
        FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
        String fnNameString = fnExpr.getFnName().getFunction();
        if (!fnNameString.equalsIgnoreCase("hll_union")
                && !fnNameString.equalsIgnoreCase("hll_raw_agg")
                && !fnNameString.equalsIgnoreCase("hll_union_agg")) {
            return expr;
        }
        if (!(fnExpr.getChild(0) instanceof FunctionCallExpr)) {
            return expr;
        }
        FunctionCallExpr child0FnExpr = (FunctionCallExpr) fnExpr.getChild(0);
        if (!child0FnExpr.getFnName().getFunction().equalsIgnoreCase("hll_hash")) {
            return expr;
        }
        if (child0FnExpr.getChild(0) instanceof SlotRef) {
            queryColumnSlotRef = (SlotRef) child0FnExpr.getChild(0);
        } else if (child0FnExpr.getChild(0) instanceof CastExpr) {
            CastExpr castExpr = (CastExpr) child0FnExpr.getChild(0);
            if (!(castExpr.getChild(0) instanceof SlotRef)) {
                return expr;
            }
            queryColumnSlotRef = (SlotRef) castExpr.getChild(0);
        } else {
            return expr;
        }
        Column column = queryColumnSlotRef.getColumn();
        Table table = queryColumnSlotRef.getTable();
        if (column == null || table == null || !(table instanceof OlapTable)) {
            return expr;
        }
        OlapTable olapTable = (OlapTable) table;

        // check column
        String queryColumnName = column.getName();
        String mvColumnName = CreateMaterializedViewStmt
                .mvColumnBuilder(AggregateType.HLL_UNION.name().toLowerCase(), queryColumnName);
        mvColumn = olapTable.getVisibleColumn(mvColumnName);
        if (mvColumn == null) {
            return expr;
        }

        // equal expr
        return rewriteExpr(fnNameString, queryColumnSlotRef, mvColumn, analyzer);
    }

    private Expr rewriteExpr(String fnName, SlotRef queryColumnSlotRef, Column mvColumn, Analyzer analyzer) {
        Preconditions.checkNotNull(mvColumn);
        Preconditions.checkNotNull(queryColumnSlotRef);
        TableName tableName = queryColumnSlotRef.getTableName();
        Preconditions.checkNotNull(tableName);
        SlotRef mvSlotRef = new SlotRef(tableName, mvColumn.getName());
        List<Expr> newFnParams = Lists.newArrayList();
        newFnParams.add(mvSlotRef);
        FunctionCallExpr result = new FunctionCallExpr(fnName, newFnParams);
        result.analyzeNoThrow(analyzer);
        return result;
    }
}
