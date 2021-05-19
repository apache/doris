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
        if (valueExpr instanceof CastExpr) {
            String dateStr = valueExpr.toSql();
            // if it contains millisecond, microsecond, nanosecond, do nothing
            if (dateStr.length() > DATETIME_STRING_MAX_LENGTH && dateStr.contains(".")) {
                return expr;
            }
            return new NullLiteral();
        }
        return expr;
    }
}