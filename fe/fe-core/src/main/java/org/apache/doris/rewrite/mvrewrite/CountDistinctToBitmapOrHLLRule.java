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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rewrite.ExprRewriteRule;
import org.apache.doris.rewrite.ExprRewriter;

/**
 * For agg keys type, the count distinct could be rewritten to bitmap or hll depends on the type of column.
 * For example:
 * Table: (k1 int, k2 bitmap bitmap_union) agg key(k1)
 * Query: select k1, count(distinct k2) from table group by k1
 * Rewritten query: select k1, bitmap_union_count(k2) from table group by k1
 * <p>
 * Table: (k1 int, k2 hll hll_union) agg key(k1)
 * Query: select k1, count(distinct k2) from table group by k1
 * Rewritten query: select k1, hll_union_agg(k2) from table group by k1
 * <p>
 * Attention: this rule only apply AGG keys type.
 */
public class CountDistinctToBitmapOrHLLRule implements ExprRewriteRule {
    public static final ExprRewriteRule INSTANCE = new CountDistinctToBitmapOrHLLRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (ConnectContext.get() == null || !ConnectContext.get().getSessionVariable().isRewriteCountDistinct()) {
            return expr;
        }

        // meet condition
        if (!(expr instanceof FunctionCallExpr)) {
            return expr;
        }
        FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
        if (!fnExpr.isCountDistinctBitmapOrHLL()) {
            return expr;
        }
        // rewrite expr
        FunctionParams newParams = new FunctionParams(false, fnExpr.getParams().exprs());
        if (fnExpr.getChild(0).getType().isBitmapType()) {
            FunctionCallExpr bitmapExpr = new FunctionCallExpr(FunctionSet.BITMAP_UNION_COUNT, newParams);
            bitmapExpr.analyzeNoThrow(analyzer);
            return bitmapExpr;
        } else {
            FunctionCallExpr hllExpr = new FunctionCallExpr("hll_union_agg", newParams);
            hllExpr.analyzeNoThrow(analyzer);
            return hllExpr;
        }
    }
}
