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
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;

/**
 * this rule try to convert date expression, if date is invalid, it will be
 * converted into null literal to avoid scanning all partitions
 * if a date data is invalid, Doris will try to cast it as datetime firstly,
 * only support rewriting pattern: slot + operator + date literal
 * Examples:
 * date = "2020-10-32" will throw analysis exception when in on clause or where clause,
 * and be converted to be NULL when in other clause
 */
public class RewriteDateLiteralRule implements ExprRewriteRule {
    public static final ExprRewriteRule INSTANCE = new RewriteDateLiteralRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (!(expr instanceof BinaryPredicate)) {
            return expr;
        }
        Expr lchild = expr.getChild(0);
        if (!lchild.getType().isDateType()) {
            return expr;
        }
        Expr valueExpr = expr.getChild(1);
        if (!valueExpr.getType().isDateType()) {
            return expr;
        }
        if (!valueExpr.isConstant()) {
            return expr;
        }
        // Only consider CastExpr and try our best to convert non-date_literal
        // to date_literal，to be compatible with MySQL
        if (valueExpr instanceof CastExpr) {
            Expr childExpr = valueExpr.getChild(0);
            if (childExpr instanceof LiteralExpr) {
                try {
                    String dateStr = childExpr.getStringValue();
                    DateLiteral dateLiteral = new DateLiteral();
                    dateLiteral.fromDateStr(dateStr);
                    expr.setChild(1, dateLiteral);
                } catch (AnalysisException e) {
                    if (ConnectContext.get() != null) {
                        ConnectContext.get().getState().reset();
                    }
                    if (clauseType == ExprRewriter.ClauseType.OTHER_CLAUSE) {
                        return new NullLiteral();
                    } else {
                        throw new AnalysisException("Incorrect datetime value: "
                                + valueExpr.toSql() + " in expression: " + expr.toSql());
                    }
                }
            }
        }
        return expr;
    }
}
