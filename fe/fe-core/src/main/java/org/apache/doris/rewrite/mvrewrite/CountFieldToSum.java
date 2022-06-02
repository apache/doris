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
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.rewrite.ExprRewriteRule;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.doris.rewrite.ExprRewriter;

import java.util.List;

/**
 * Rewrite count(k1) to sum(mv_count_k1) when MV Column exists.
 * For example:
 * Table: (k1 int ,k2 varchar)
 * MV: (k1 int, mv_count_k2 bigint sum)
 *       mv_count_k1 = case when k2 is null then 0 else 1
 * Query: select k1, count(k2) from table group by k1
 * Rewritten query: select k1, sum(mv_count_k2) from table group by k1
 */
public class CountFieldToSum implements ExprRewriteRule {
    public static final ExprRewriteRule INSTANCE = new CountFieldToSum();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        // meet condition
        if (!(expr instanceof FunctionCallExpr)) {
            return expr;
        }
        FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
        if (!fnExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.COUNT)) {
            return expr;
        }
        if (fnExpr.getChildren().size() != 1 || !(fnExpr.getChild(0) instanceof SlotRef)) {
            return expr;
        }
        if (fnExpr.getParams().isDistinct()) {
            return expr;
        }
        SlotRef fnChild0 = (SlotRef) fnExpr.getChild(0);
        Column column = fnChild0.getColumn();
        Table table = fnChild0.getTable();
        if (column == null || table == null || !(table instanceof OlapTable)) {
            return expr;
        }
        OlapTable olapTable = (OlapTable) table;

        // check column
        String queryColumnName = column.getName();
        String mvColumnName = CreateMaterializedViewStmt.mvColumnBuilder(FunctionSet.COUNT, queryColumnName);
        Column mvColumn = olapTable.getVisibleColumn(mvColumnName);
        if (mvColumn == null) {
            return expr;
        }

        // rewrite expr
        return rewriteExpr(mvColumn, analyzer);
    }

    private Expr rewriteExpr(Column mvColumn, Analyzer analyzer) {
        Preconditions.checkNotNull(mvColumn);
        // Notice that we shouldn't set table name field of mvSlotRef here, for we will analyze the new mvSlotRef
        // later, if the table name was set here, the Analyzer::registerColumnRef would invoke
        // Analyzer::resolveColumnRef(TableName, String) which only try to find the column from the tupleByAlias,
        // as at the most time the alias is not equal with the origin table name, so it would cause the unexpected
        // exception to Unknown column, because we can't find an alias which named as origin table name that has
        // required column.
        SlotRef mvSlotRef = new SlotRef(null, mvColumn.getName());
        List<Expr> newFnParams = Lists.newArrayList();
        newFnParams.add(mvSlotRef);
        FunctionCallExpr result = new FunctionCallExpr("sum", newFnParams);
        result.analyzeNoThrow(analyzer);
        return result;
    }
}
