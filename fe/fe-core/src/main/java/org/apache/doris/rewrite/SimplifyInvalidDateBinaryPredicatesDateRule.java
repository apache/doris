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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.NotLiteralExprPredicate;

/**
 * this rule try to convert date expression, if date is invalid, it will be
 * converted into null literal to avoid to scan all partitions
 * if a date data is invalid or contains nanosecond, it will be convert into CastExpr
 * only support rewriting pattern: slot + operator + date literal
 * Examples:
 * date = "2020-10-32" => NULL
 */
public class SimplifyInvalidDateBinaryPredicatesDateRule implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new SimplifyInvalidDateBinaryPredicatesDateRule();
    public static final int DATETIME_STRING_MAX_LENGTH = new String("yyyy-MM-dd HH:ii:ss").length();
    private static final NotLiteralExprPredicate NOT_LITERAL_EXPR_PREDICATE = new NotLiteralExprPredicate();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
        if (!(expr instanceof BinaryPredicate)) return expr;
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

        // This is not a very good implementation and tricky.
        // We have to handle the following cases:
        // A. k1 is datetime, sql with "k1 > to_date(now())" will be converted to k1 > cast(to_date("xxxx-xx-xx"))
        // B. k1 is datetime, sql with "k1 > '2021-10-32 10:00:00.100010'" will be converted to k1 > cast('2021-10-32 10:00:00.100010' as datetime)
        // C. k1 is datetime, sql with "k1 > '2021-10-32'" will be converted to k1 > cast('2021-10-32' as datetime), and finally to converted to NullLiteral.
        if (valueExpr instanceof CastExpr) {
            valueExpr = valueExpr.getChild(0);
            if (valueExpr.contains(NOT_LITERAL_EXPR_PREDICATE)) {
                // Case A.
                return expr;
            }
            String dateStr = valueExpr.toSql();
            if (dateStr.length() > DATETIME_STRING_MAX_LENGTH && dateStr.contains(".")) {
                // Case B
                return expr;
            }
            // Case C
            return new NullLiteral();
        } else {
            if (valueExpr.contains(NOT_LITERAL_EXPR_PREDICATE)) {
                return expr;
            }
        }
        return expr;
    }
}
