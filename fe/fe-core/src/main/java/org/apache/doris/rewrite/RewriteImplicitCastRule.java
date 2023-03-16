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
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.rewrite.ExprRewriter.ClauseType;

import java.util.List;

/**
 * During the analysis of BinaryPredicate, it will generate a CastExpr if the slot implicitly in the below case:
 *  SELECT * FROM t1 WHERE t1.col1 = '1';
 * col1 is integer column.
 * This will prevent the binary predicate from pushing down to OlapScan which would impact the performance.
 * TODO: For now, we only handle the implicit cast which cast left child with integer type
 *       and right child with StringLiteral type to BIGINT.
 *       We could do more than that in the future, such as LARGEINT type.
 */
public class RewriteImplicitCastRule implements ExprRewriteRule {

    public static RewriteImplicitCastRule INSTANCE = new RewriteImplicitCastRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ClauseType clauseType) throws AnalysisException {
        if (!(expr instanceof BinaryPredicate)) {
            return expr;
        }
        BinaryPredicate predicate = (BinaryPredicate) expr;
        if (!(predicate.getChild(0) instanceof CastExpr)) {
            return expr;
        }
        CastExpr castExpr = (CastExpr) predicate.getChild(0);
        if (!castExpr.isImplicit()
                || !(predicate.getChild(1) instanceof IntLiteral)
                || !clauseType.equals(ClauseType.WHERE_CLAUSE)) {
            return expr;
        }
        Expr childOfCast = castExpr.getChild(0);
        IntLiteral rightChild = (IntLiteral) predicate.getChild(1);
        long rightValue = rightChild.getValue();
        Type leftType = childOfCast.getType();
        // no need to handle boolean since compare between a boolean with a string in not allow in doris
        if (Type.TINYINT.equals(leftType)) {
            return process(Type.TINYINT,
                    rightValue, IntLiteral.TINY_INT_MIN, IntLiteral.TINY_INT_MAX, expr, childOfCast);
        }
        if (Type.SMALLINT.equals(leftType)) {
            return process(Type.SMALLINT,
                    rightValue, IntLiteral.SMALL_INT_MIN, IntLiteral.SMALL_INT_MAX, expr, childOfCast);
        }
        if (Type.INT.equals(leftType)) {
            return process(Type.INT,
                    rightValue, IntLiteral.INT_MIN, IntLiteral.INT_MAX, expr, childOfCast);
        }
        if (Type.BIGINT.equals(leftType)) {
            return process(Type.INT,
                    rightValue, IntLiteral.BIG_INT_MIN, IntLiteral.BIG_INT_MAX, expr, childOfCast);
        }
        return expr;
    }

    // TODO: Let min, max value of a specific type be a part of definition of Type may
    //       be much better.By doing that, We would be able to abstract code in this block to
    //       a method.
    private Expr process(Type type,
            long rightValue,
            long min,
            long max,
            Expr expr,
            Expr childOfCast) throws AnalysisException {
        // TODO: We could do constant folding here.
        if (rightValue < min || rightValue > max) {
            return expr;
        }
        replaceLeftChildWithNumericLiteral(expr, childOfCast, new IntLiteral(rightValue, type));
        return expr;
    }

    private void replaceLeftChildWithNumericLiteral(Expr expr, Expr left, IntLiteral right) {
        List<Expr> children = expr.getChildren();
        children.clear();
        children.add(0, left);
        children.add(1, right);
    }
}
