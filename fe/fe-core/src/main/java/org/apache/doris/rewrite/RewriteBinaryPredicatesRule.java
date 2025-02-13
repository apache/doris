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
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;

/**
 * Rewrite binary predicate.
 */
public class RewriteBinaryPredicatesRule implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new RewriteBinaryPredicatesRule();

    /**
     * Convert the binary predicate of the form
     * <CastExpr<SlotRef(ResultType=BIGINT)>> <op><DecimalLiteral> to the binary
     * predicate of <SlotRef(ResultType=BIGINT)> <new op> <new DecimalLiteral>, thereby allowing the binary predicate
     * The predicate pushes down and completes the bucket clipped.
     *
     * Examples & background
     * For query "select * from T where t1 = 2.0", when the ResultType of column t1 is equal to BIGINT, in the binary
     * predicate analyze, the type will be unified to DECIMALV2, so the binary predicate will be converted to
     * <CastExpr<SlotRef>> <op In the form of ><DecimalLiteral>,
     * because Cast wraps the t1 column, it cannot be pushed down, resulting in poor performance.
     * We convert it to the equivalent query "select * from T where t1 = 2" to push down and improve performance.
     *
     * Applicable scene:
     * The performance and results of the following scenarios are equivalent.
     * 1) "select * from T where t1 = 2.0" is converted to "select * from T where t1 = 2"
     * 2) "select * from T where t1 = 2.1" is converted to "select * from T where 2 = 2.1" (`EMPTY`)
     * 3) "select * from T where t1 != 2.0" is converted to "select * from T where t1 != 2"
     * 4) "select * from T where t1 != 2.1" is converted to "select * from T"
     * 5) "select * from T where t1 <= 2.0" is converted to "select * from T where t1 <= 2"
     * 6) "select * from T where t1 <= 2.1" is converted to "select * from T where t1 <=2"
     * 7) "select * from T where t1 >= 2.0" is converted to "select * from T where t1 >= 2"
     * 8) "select * from T where t1 >= 2.1" is converted to "select * from T where t1> 2"
     * 9) "select * from T where t1 <2.0" is converted to "select * from T where t1 <2"
     * 10) "select * from T where t1 <2.1" is converted to "select * from T where t1 <=2"
     * 11) "select * from T where t1> 2.0" is converted to "select * from T where t1> 2"
     * 12) "select * from T where t1> 2.1" is converted to "select * from T where t1> 2"
     */
    private Expr rewriteBigintSlotRefCompareDecimalLiteral(Expr expr0, Type expr0ColumnType, DecimalLiteral expr1,
            BinaryPredicate.Operator op) {
        try {
            // Convert childExpr to column type and compare the converted values. There are 3 possible situations:
            // case 1. The value of childExpr exceeds the range of the column type, then castTo() will throw an
            //   exception. For example, the value of childExpr is 128.0 and the column type is tinyint.
            // case 2. childExpr is converted to column type, but the value of childExpr loses precision.
            //   For example, 2.1 is converted to 2;
            // case 3. childExpr is precisely converted to column type. For example, 2.0 is converted to 2.
            LiteralExpr newExpr = (LiteralExpr) expr1.castTo(expr0ColumnType);
            int compResult = expr1.compareLiteral(newExpr);
            // case 2
            if (compResult != 0) {
                if (op == Operator.EQ || op == Operator.EQ_FOR_NULL) {
                    return new BoolLiteral(false);
                } else if (op == Operator.NE) {
                    return new BoolLiteral(true);
                }

                if (compResult > 0) {
                    if (op == Operator.LT) {
                        op = Operator.LE;
                    } else if (op == Operator.GE) {
                        op = Operator.GT;
                    }
                } else {
                    if (op == Operator.LE) {
                        op = Operator.LT;
                    } else if (op == Operator.GT) {
                        op = Operator.GE;
                    }
                }
            }
            // case 3
            return new BinaryPredicate(op, expr0.castTo(expr0ColumnType), newExpr);
        } catch (AnalysisException e) {
            if (ConnectContext.get() != null) {
                ConnectContext.get().getState().reset();
            }
            // case 1
            IntLiteral colTypeMinValue = IntLiteral.createMinValue(expr0ColumnType);
            IntLiteral colTypeMaxValue = IntLiteral.createMaxValue(expr0ColumnType);
            if (op == Operator.NE || ((expr1).compareLiteral(colTypeMinValue) < 0 && (op == Operator.GE
                    || op == Operator.GT)) || ((expr1).compareLiteral(colTypeMaxValue) > 0 && (op == Operator.LE
                    || op == Operator.LT))) {
                return new BoolLiteral(true);
            }
            return new BoolLiteral(false);
        }
    }

    /**
     * Convert the binary predicate between datetime and date
     * 1) with date column col <= '2024-06-01 23:59:39'
     *    converted from cast (`col` to datetime ) <= '2024-06-01 23:59:39' to
     *    `col` <= '2024-06-01'
     * 2) with datetime column convert(col, date) <= '2024-06-01'
     *    converted from cast (`col` to date ) <= '2024-06-01' to
     *    cast(cast(`col` to date) to datetime) <= '2024-06-01 00:00:00'
     */
    private Expr processDateLikeTypeCoercion(Expr expr) throws AnalysisException {
        BinaryPredicate.Operator op = ((BinaryPredicate) expr).getOp();
        Expr left = expr.getChild(0);
        Expr right = expr.getChild(1);

        CastExpr cast = (CastExpr) left;
        if (cast.getChildren().size() != 1) {
            return expr;
        }

        // 1) datetime&datetimev2 to date&datev2
        if ((cast.getChild(0).getType().isDate()
                || cast.getChild(0).getType().isDateV2()) && cast.getChild(0) instanceof SlotRef) {
            if ((right.getType().isDatetime() || right.getType().isDatetimeV2())
                    && !cannotAdjust((DateLiteral) right, op)) {
                left = cast.getChild(0);
                DateLiteral dateLiteral = (DateLiteral) right;
                right = new DateLiteral(dateLiteral.getYear(), dateLiteral.getMonth(), dateLiteral.getDay(),
                        cast.getChild(0).getType());
                if ((op == BinaryPredicate.Operator.GE || op == BinaryPredicate.Operator.LT)
                        && (dateLiteral.getHour() != 0 || dateLiteral.getMinute() != 0 || dateLiteral.getSecond() != 0)
                ) {
                    right = ((DateLiteral) right).plusDays(1);
                }
            }
        }

        // 2) date&datev2 to datetime&datetimev2
        if (cast.getChild(0).getType().isDatetime() && cast.getChild(0) instanceof SlotRef) {
            if (right.getType().isDate()) {
                left = new CastExpr(cast.getChild(0).getType(), cast);
                DateLiteral dateLiteral = (DateLiteral) right;
                right = new DateLiteral(dateLiteral.getYear(), dateLiteral.getMonth(), dateLiteral.getDay(),
                    0, 0, 0, cast.getChild(0).getType());
            }
        }

        if (left != expr.getChild(0) || right != expr.getChild(1)) {
            return new BinaryPredicate(op, left, right);
        } else {
            return expr;
        }
    }

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (!(expr instanceof BinaryPredicate)) {
            return expr;
        }
        BinaryPredicate.Operator op = ((BinaryPredicate) expr).getOp();
        Expr expr0 = expr.getChild(0);
        Expr expr1 = expr.getChild(1);
        if (expr0 instanceof CastExpr && (expr0.getType() == Type.DECIMALV2 || expr0.getType().isDecimalV3())
                && expr0.getChild(0) instanceof SlotRef
                && expr0.getChild(0).getType().getResultType() == Type.BIGINT
                && expr0.getChild(0).getType().getPrimitiveType() != PrimitiveType.BOOLEAN
                && expr1 instanceof DecimalLiteral) {
            return rewriteBigintSlotRefCompareDecimalLiteral(expr0,
                    expr0.getChild(0).getType(), (DecimalLiteral) expr1, op);
        }

        // date like type
        if (expr0 instanceof CastExpr && expr1 instanceof DateLiteral
                && expr0.getType().isDateType() && expr1.getType().isDateType()) {
            return processDateLikeTypeCoercion(expr);
        }

        return expr;
    }

    private boolean cannotAdjust(DateLiteral l, BinaryPredicate.Operator op) {
        return op == BinaryPredicate.Operator.EQ && (l.getHour() != 0 || l.getMinute() != 0 || l.getSecond() != 0);
    }
}
