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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

// if the input arg is not nullable, so the function of is_null/is_not_null
// result is very certain and there is no need to look at data in column.
// just given a literal, so that BE could deal with it as const
public class RewriteIsNullIsNotNullRule implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new RewriteIsNullIsNotNullRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr functionExpr = ((FunctionCallExpr) expr);
            Function fn = functionExpr.getFn();
            if (fn == null) {
                return expr;
            }
            if (fn.getFunctionName().getFunction().equals("is_null_pred")
                    && functionExpr.getChild(0).isNullable() == false) {
                LiteralExpr literal = LiteralExpr.create("0", Type.BOOLEAN);
                return literal;
            }
            if (fn.getFunctionName().getFunction().equals("is_not_null_pred")
                    && functionExpr.getChild(0).isNullable() == false) {
                LiteralExpr literal = LiteralExpr.create("1", Type.BOOLEAN);
                return literal;
            }
        }
        return expr;
    }
}
