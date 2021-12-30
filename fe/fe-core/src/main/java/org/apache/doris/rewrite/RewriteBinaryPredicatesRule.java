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
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

/**
 * Rewrite binary predicate.
 */
public class RewriteBinaryPredicatesRule implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new RewriteBinaryPredicatesRule();

    /**
     * Convert the binary predicate of the form <CastExpr<SlotRef(ResultType=BIGINT)>> <op><DecimalLiteral> to the binary
     * predicate of <SlotRef(ResultType=BIGINT)> <new op> <new DecimalLiteral>, thereby allowing the binary predicate
     * The predicate pushes down and completes the bucket clipped.
     *
     * Examples & background
     * For query "select * from T where t1 = 2.0", when the ResultType of column t1 is equal to BIGINT, in the binary
     * predicate analyze, the type will be unified to DECIMALV2, so the binary predicate will be converted to
     * <CastExpr<SlotRef>> <op In the form of ><DecimalLiteral>, because Cast wraps the t1 column, it cannot be pushed down,
     * resulting in poor performance.
     * We convert it to the equivalent query "select * from T where t1 = 2" to push down and improve performance.
     *
     * Applicable scene:
     * The performance and results of the following scenarios are equivalent.
     * 1) "select * from T where t1 = 2.0" is converted to "select * from T where t1 = 2"
     * 2) "select * from T where t1 = 2.1" is converted to "select * from T where 2 = 2.1" (`EMPTY`)
     * 3) "select * from T where t1 != 2.0" is converted to "select * from T where t1 != 2"
     * 4) "select * from T where t1 != 2.1" is converted to "select * from T"
     * 5) "select * from T where t1 <= 2.0" is converted to "select * from T where t1 <= 2"
     * 6) "select * from T where t1 <= 2.1" is converted to "select * from T where t1 <3"
     * 7) "select * from T where t1 >= 2.0" is converted to "select * from T where t1 >= 2"
     * 8) "select * from T where t1 >= 2.1" is converted to "select * from T where t1> 2"
     * 9) "select * from T where t1 <2.0" is converted to "select * from T where t1 <2"
     * 10) "select * from T where t1 <2.1" is converted to "select * from T where t1 <3"
     * 11) "select * from T where t1> 2.0" is converted to "select * from T where t1> 2"
     * 12) "select * from T where t1> 2.1" is converted to "select * from T where t1> 2"
     */
    private Expr rewriteBigintSlotRefCompareDecimalLiteral(Expr expr0, Expr expr1, BinaryPredicate.Operator op)
            throws AnalysisException {
        if (((DecimalLiteral) expr1).getDoubleValue() % (int) (((DecimalLiteral) expr1).getDoubleValue()) != 0) {
            if (op == BinaryPredicate.Operator.EQ || op == BinaryPredicate.Operator.EQ_FOR_NULL) {
                return new BoolLiteral(false);
            } else if (op == BinaryPredicate.Operator.NE) {
                return new BoolLiteral(true);
            } else if (op == BinaryPredicate.Operator.LE) {
                ((DecimalLiteral) expr1).roundCeiling();
                op = BinaryPredicate.Operator.LT;
            } else if (op == BinaryPredicate.Operator.GE) {
                ((DecimalLiteral) expr1).roundFloor();
                op = BinaryPredicate.Operator.GT;
            } else if (op == BinaryPredicate.Operator.LT) {
                ((DecimalLiteral) expr1).roundCeiling();
            } else if (op == BinaryPredicate.Operator.GT) {
                ((DecimalLiteral) expr1).roundFloor();
            }
        }
        expr0 = expr0.getChild(0);
        expr1 = expr1.castTo(Type.BIGINT);
        return new BinaryPredicate(op, expr0, expr1);
    }

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (!(expr instanceof BinaryPredicate)) return expr;
        BinaryPredicate.Operator op = ((BinaryPredicate) expr).getOp();
        Expr expr0 = expr.getChild(0);
        Expr expr1 = expr.getChild(1);
        if (expr0 instanceof CastExpr && expr0.getType() == Type.DECIMALV2 && expr0.getChild(0) instanceof SlotRef
                && expr0.getChild(0).getType().getResultType() == Type.BIGINT && expr1 instanceof DecimalLiteral) {
            return rewriteBigintSlotRefCompareDecimalLiteral(expr0, expr1, op);
        }
        return expr;
    }
}
