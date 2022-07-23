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
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;

import java.math.BigDecimal;

/**
 * Rewrite binary predicate.
 */
public class RoundLiteralInBinaryPredicatesRule implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new RoundLiteralInBinaryPredicatesRule();

    private Expr rewriteDecimalLiteral(Expr expr) {
        Operator op = ((BinaryPredicate) expr).getOp();
        Expr expr0 = expr.getChild(0);
        Expr expr1 = expr.getChild(1);
        if (expr1.getType().isDecimalV3() && expr1 instanceof DecimalLiteral) {
            DecimalLiteral literal = (DecimalLiteral) expr1;
            if (expr0.getType().isDecimalV3()
                    && ((ScalarType) expr0.getType()).getScalarScale()
                    < ((ScalarType) expr1.getType()).getScalarScale()) {
                switch (op) {
                    case EQ: {
                        BigDecimal originValue = literal.getValue();
                        literal.roundCeiling(((ScalarType) expr0.getType()).getScalarScale());
                        if (literal.getValue().equals(originValue)) {
                            expr.setChild(1, literal);
                            return expr;
                        } else {
                            return new BoolLiteral(false);
                        }
                    }
                    case NE: {
                        BigDecimal originValue = literal.getValue();
                        literal.roundCeiling(((ScalarType) expr0.getType()).getScalarScale());
                        if (literal.getValue().equals(originValue)) {
                            expr.setChild(1, literal);
                            return expr;
                        } else {
                            return new BoolLiteral(true);
                        }
                    }
                    case GT:
                    case LE: {
                        literal.roundFloor(((ScalarType) expr0.getType()).getScalarScale());
                        expr.setChild(1, literal);
                        return expr;
                    }
                    case LT:
                    case GE: {
                        literal.roundCeiling(((ScalarType) expr0.getType()).getScalarScale());
                        expr.setChild(1, literal);
                        return expr;
                    }
                    default:
                        return expr;
                }
            }
        }
        return expr;
    }

    private Expr rewriteDateLiteral(Expr expr) {
        Operator op = ((BinaryPredicate) expr).getOp();
        Expr expr0 = expr.getChild(0);
        Expr expr1 = expr.getChild(1);
        if (expr0.getType().isDatetimeV2() && expr1 instanceof DateLiteral && expr1.getType().isDatetimeV2()) {
            DateLiteral literal = (DateLiteral) expr1;
            if (((ScalarType) expr0.getType()).getScalarScale()
                    < ((ScalarType) expr1.getType()).getScalarScale()) {
                switch (op) {
                    case EQ: {
                        long originValue = literal.getMicrosecond();
                        literal.roundCeiling(((ScalarType) expr0.getType()).getScalarScale());
                        if (literal.getMicrosecond() == originValue) {
                            expr.setChild(1, literal);
                            return expr;
                        } else {
                            return new BoolLiteral(false);
                        }
                    }
                    case NE: {
                        long originValue = literal.getMicrosecond();
                        literal.roundCeiling(((ScalarType) expr0.getType()).getScalarScale());
                        if (literal.getMicrosecond() == originValue) {
                            expr.setChild(1, literal);
                            return expr;
                        } else {
                            return new BoolLiteral(true);
                        }
                    }
                    case GT:
                    case LE: {
                        literal.roundFloor(((ScalarType) expr0.getType()).getScalarScale());
                        expr.setChild(1, literal);
                        return expr;
                    }
                    case LT:
                    case GE: {
                        literal.roundCeiling(((ScalarType) expr0.getType()).getScalarScale());
                        expr.setChild(1, literal);
                        return expr;
                    }
                    default:
                        return expr;
                }
            }
        }
        return expr;
    }

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (!(expr instanceof BinaryPredicate)) {
            return expr;
        }
        Expr tmpExpr = rewriteDecimalLiteral(expr);
        return rewriteDateLiteral(tmpExpr);
    }
}
