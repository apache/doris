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

package org.apache.doris.rewrite;


import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.common.AnalysisException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This rule replaces a constant Expr with its equivalent LiteralExpr by evaluating the
 * Expr in the BE. Exprs that are already LiteralExprs are not changed.
 *
 * TODO: Expressions fed into this rule are currently not required to be analyzed
 * in order to support constant folding in expressions that contain unresolved
 * references to select-list aliases (such expressions cannot be analyzed).
 * The cross-dependencies between rule transformations and analysis are vague at the
 * moment and make rule application overly complicated.
 *
 * Examples:
 * 1 + 1 + 1 --> 3
 * toupper('abc') --> 'ABC'
 * cast('2016-11-09' as timestamp) --> TIMESTAMP '2016-11-09 00:00:00'
 */
public class FoldConstantsRule implements ExprRewriteRule {
    private static final Logger LOG = LogManager.getLogger(FoldConstantsRule.class);

    public static ExprRewriteRule INSTANCE = new FoldConstantsRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
        // evaluate `case when expr` when possible
        if (expr instanceof CaseExpr) {
            return CaseExpr.computeCaseExpr((CaseExpr) expr);
        }

        // Avoid calling Expr.isConstant() because that would lead to repeated traversals
        // of the Expr tree. Assumes the bottom-up application of this rule. Constant
        // children should have been folded at this point.
        for (Expr child : expr.getChildren()) {
            if (!child.isLiteral() && !(child instanceof CastExpr)) {
                return expr;
            }
        }

        if (expr.isLiteral() || !expr.isConstant()) {
            return expr;
        }

        // Do not constant fold cast(null as dataType) because we cannot preserve the
        // cast-to-types and that can lead to query failures, e.g., CTAS
        if (expr instanceof CastExpr) {
            CastExpr castExpr = (CastExpr) expr;
            if (castExpr.getChild(0) instanceof NullLiteral) {
                return expr;
            }
        }

        // Analyze constant exprs, if necessary. Note that the 'expr' may become non-constant
        // after analysis (e.g., aggregate functions).
        if (!expr.isAnalyzed()) {
            expr.analyze(analyzer);
            if (!expr.isConstant()) {
                return expr;
            }
        }
        return expr.getResultValue();
    }
}

