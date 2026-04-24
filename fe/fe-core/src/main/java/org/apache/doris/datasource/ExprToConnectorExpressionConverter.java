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

package org.apache.doris.datasource;

import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.BetweenPredicate;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorBetween;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorLike;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorNot;
import org.apache.doris.connector.api.pushdown.ConnectorOr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts a Doris {@link Expr} tree into a {@link ConnectorExpression} tree
 * for SPI boundary crossing.
 *
 * <p>This converter handles all common predicate types used in filter pushdown:
 * binary comparisons, compound predicates (AND/OR/NOT), IN lists, BETWEEN,
 * IS NULL, LIKE/REGEXP, and generic function calls.</p>
 */
public final class ExprToConnectorExpressionConverter {

    private static final Logger LOG = LogManager.getLogger(ExprToConnectorExpressionConverter.class);

    private ExprToConnectorExpressionConverter() {
    }

    /**
     * Converts a Doris {@link Expr} to a {@link ConnectorExpression}.
     * Falls back to {@link ConnectorFunctionCall} with the SQL text for
     * unrecognised expression types.
     */
    public static ConnectorExpression convert(Expr expr) {
        if (expr instanceof BinaryPredicate) {
            return convertBinaryPredicate((BinaryPredicate) expr);
        } else if (expr instanceof CompoundPredicate) {
            return convertCompoundPredicate((CompoundPredicate) expr);
        } else if (expr instanceof InPredicate) {
            return convertInPredicate((InPredicate) expr);
        } else if (expr instanceof BetweenPredicate) {
            return convertBetweenPredicate((BetweenPredicate) expr);
        } else if (expr instanceof IsNullPredicate) {
            return convertIsNullPredicate((IsNullPredicate) expr);
        } else if (expr instanceof LikePredicate) {
            return convertLikePredicate((LikePredicate) expr);
        } else if (expr instanceof SlotRef) {
            return convertSlotRef((SlotRef) expr);
        } else if (expr instanceof LiteralExpr) {
            return convertLiteral((LiteralExpr) expr);
        } else if (expr instanceof CastExpr) {
            return convert(expr.getChild(0));
        } else if (expr instanceof ArithmeticExpr) {
            return convertArithmeticExpr((ArithmeticExpr) expr);
        } else if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fce = (FunctionCallExpr) expr;
            String fnName = fce.getFnName().getFunction();
            // Nereids translates Like/Regexp to FunctionCallExpr via visitScalarFunction,
            // so we need to detect them here and convert to ConnectorLike.
            if ("like".equalsIgnoreCase(fnName) && fce.getChildren().size() == 2) {
                return new ConnectorLike(ConnectorLike.Operator.LIKE,
                        convert(fce.getChild(0)), convert(fce.getChild(1)));
            } else if ("regexp".equalsIgnoreCase(fnName) && fce.getChildren().size() == 2) {
                return new ConnectorLike(ConnectorLike.Operator.REGEXP,
                        convert(fce.getChild(0)), convert(fce.getChild(1)));
            }
            return convertFunctionCall(fce);
        } else {
            return fallback(expr);
        }
    }

    /**
     * Converts a list of conjuncts (AND-connected predicates) into a single
     * {@link ConnectorExpression}. If the list has one element, returns it
     * directly. If empty, returns a boolean-true literal.
     */
    public static ConnectorExpression convertConjuncts(List<Expr> conjuncts) {
        if (conjuncts == null || conjuncts.isEmpty()) {
            return ConnectorLiteral.ofBoolean(true);
        }
        if (conjuncts.size() == 1) {
            return convert(conjuncts.get(0));
        }
        List<ConnectorExpression> converted = new ArrayList<>(conjuncts.size());
        for (Expr expr : conjuncts) {
            converted.add(convert(expr));
        }
        return new ConnectorAnd(converted);
    }

    /**
     * Converts a Doris {@link Type} to a {@link ConnectorType}.
     */
    public static ConnectorType typeToConnectorType(Type type) {
        if (type.isArrayType()) {
            ArrayType at = (ArrayType) type;
            return ConnectorType.arrayOf(typeToConnectorType(at.getItemType()));
        } else if (type.isMapType()) {
            MapType mt = (MapType) type;
            return ConnectorType.mapOf(
                    typeToConnectorType(mt.getKeyType()),
                    typeToConnectorType(mt.getValueType()));
        } else if (type.isStructType()) {
            StructType st = (StructType) type;
            List<String> names = new ArrayList<>();
            List<ConnectorType> types = new ArrayList<>();
            for (StructField field : st.getFields()) {
                names.add(field.getName());
                types.add(typeToConnectorType(field.getType()));
            }
            return ConnectorType.structOf(names, types);
        } else if (type instanceof ScalarType) {
            return scalarTypeToConnectorType((ScalarType) type);
        } else {
            return ConnectorType.of(type.toSql());
        }
    }

    // ---- private conversion methods ----

    private static ConnectorExpression convertBinaryPredicate(BinaryPredicate pred) {
        ConnectorComparison.Operator op;
        switch (pred.getOp()) {
            case EQ:
                op = ConnectorComparison.Operator.EQ;
                break;
            case NE:
                op = ConnectorComparison.Operator.NE;
                break;
            case LT:
                op = ConnectorComparison.Operator.LT;
                break;
            case LE:
                op = ConnectorComparison.Operator.LE;
                break;
            case GT:
                op = ConnectorComparison.Operator.GT;
                break;
            case GE:
                op = ConnectorComparison.Operator.GE;
                break;
            case EQ_FOR_NULL:
                op = ConnectorComparison.Operator.EQ_FOR_NULL;
                break;
            default:
                return fallback(pred);
        }
        return new ConnectorComparison(op,
                convert(pred.getChild(0)),
                convert(pred.getChild(1)));
    }

    private static ConnectorExpression convertCompoundPredicate(CompoundPredicate pred) {
        switch (pred.getOp()) {
            case AND: {
                List<ConnectorExpression> conjuncts = new ArrayList<>();
                flattenAnd(pred, conjuncts);
                return new ConnectorAnd(conjuncts);
            }
            case OR: {
                List<ConnectorExpression> disjuncts = new ArrayList<>();
                flattenOr(pred, disjuncts);
                return new ConnectorOr(disjuncts);
            }
            case NOT:
                return new ConnectorNot(convert(pred.getChild(0)));
            default:
                return fallback(pred);
        }
    }

    private static void flattenAnd(Expr expr, List<ConnectorExpression> out) {
        if (expr instanceof CompoundPredicate
                && ((CompoundPredicate) expr).getOp() == CompoundPredicate.Operator.AND) {
            flattenAnd(expr.getChild(0), out);
            flattenAnd(expr.getChild(1), out);
        } else {
            out.add(convert(expr));
        }
    }

    private static void flattenOr(Expr expr, List<ConnectorExpression> out) {
        if (expr instanceof CompoundPredicate
                && ((CompoundPredicate) expr).getOp() == CompoundPredicate.Operator.OR) {
            flattenOr(expr.getChild(0), out);
            flattenOr(expr.getChild(1), out);
        } else {
            out.add(convert(expr));
        }
    }

    private static ConnectorExpression convertInPredicate(InPredicate pred) {
        ConnectorExpression value = convert(pred.getChild(0));
        List<ConnectorExpression> inList = pred.getListChildren().stream()
                .map(ExprToConnectorExpressionConverter::convert)
                .collect(Collectors.toList());
        return new ConnectorIn(value, inList, pred.isNotIn());
    }

    private static ConnectorExpression convertBetweenPredicate(BetweenPredicate pred) {
        ConnectorExpression base = new ConnectorBetween(
                convert(pred.getChild(0)),
                convert(pred.getChild(1)),
                convert(pred.getChild(2)));
        if (pred.isNotBetween()) {
            return new ConnectorNot(base);
        }
        return base;
    }

    private static ConnectorExpression convertIsNullPredicate(IsNullPredicate pred) {
        return new ConnectorIsNull(convert(pred.getChild(0)), pred.isNotNull());
    }

    private static ConnectorExpression convertLikePredicate(LikePredicate pred) {
        ConnectorLike.Operator op = pred.getOp() == LikePredicate.Operator.REGEXP
                ? ConnectorLike.Operator.REGEXP
                : ConnectorLike.Operator.LIKE;
        return new ConnectorLike(op,
                convert(pred.getChild(0)),
                convert(pred.getChild(1)));
    }

    private static ConnectorExpression convertSlotRef(SlotRef slot) {
        String colName = slot.getColumnName();
        ConnectorType ct = typeToConnectorType(slot.getType());
        return new ConnectorColumnRef(colName, ct);
    }

    private static ConnectorExpression convertLiteral(LiteralExpr literal) {
        ConnectorType ct = typeToConnectorType(literal.getType());
        if (literal instanceof NullLiteral) {
            return ConnectorLiteral.ofNull(ct);
        } else if (literal instanceof BoolLiteral) {
            return new ConnectorLiteral(ct, ((BoolLiteral) literal).getValue());
        } else if (literal instanceof IntLiteral) {
            return new ConnectorLiteral(ct, ((IntLiteral) literal).getValue());
        } else if (literal instanceof FloatLiteral) {
            return new ConnectorLiteral(ct, ((FloatLiteral) literal).getValue());
        } else if (literal instanceof DecimalLiteral) {
            return new ConnectorLiteral(ct, ((DecimalLiteral) literal).getValue());
        } else if (literal instanceof StringLiteral) {
            return new ConnectorLiteral(ct, ((StringLiteral) literal).getValue());
        } else if (literal instanceof DateLiteral) {
            return convertDateLiteral((DateLiteral) literal, ct);
        } else {
            return new ConnectorLiteral(ct, literal.getStringValue());
        }
    }

    private static ConnectorExpression convertDateLiteral(DateLiteral dl, ConnectorType ct) {
        Type dorisType = dl.getType();
        if (dorisType.equals(Type.DATE) || dorisType.equals(Type.DATEV2)) {
            LocalDate ld = LocalDate.of(
                    (int) dl.getYear(), (int) dl.getMonth(), (int) dl.getDay());
            return new ConnectorLiteral(ct, ld);
        } else {
            LocalDateTime ldt = LocalDateTime.of(
                    (int) dl.getYear(), (int) dl.getMonth(), (int) dl.getDay(),
                    (int) dl.getHour(), (int) dl.getMinute(), (int) dl.getSecond(),
                    (int) (dl.getMicrosecond() * 1000));
            return new ConnectorLiteral(ct, ldt);
        }
    }

    private static ConnectorExpression convertArithmeticExpr(ArithmeticExpr arith) {
        String opSymbol = arith.getOp().toString();
        ConnectorType retType = typeToConnectorType(arith.getType());
        List<ConnectorExpression> args = arith.getChildren().stream()
                .map(ExprToConnectorExpressionConverter::convert)
                .collect(Collectors.toList());
        return new ConnectorFunctionCall(opSymbol, retType, args);
    }

    private static ConnectorExpression convertFunctionCall(FunctionCallExpr fce) {
        String fnName = fce.getFnName().getFunction();
        ConnectorType retType = typeToConnectorType(fce.getType());
        List<ConnectorExpression> args = fce.getChildren().stream()
                .map(ExprToConnectorExpressionConverter::convert)
                .collect(Collectors.toList());
        return new ConnectorFunctionCall(fnName, retType, args);
    }

    private static ConnectorExpression fallback(Expr expr) {
        LOG.debug("Unsupported Expr type {}, cannot convert to ConnectorExpression",
                expr.getClass().getSimpleName());
        // Return a function call with the SQL text and no children so the
        // builder can render it as a pre-formatted SQL fragment.
        ConnectorType retType = typeToConnectorType(expr.getType());
        String sql;
        try {
            sql = expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE);
        } catch (Exception e) {
            sql = expr.debugString();
        }
        return new ConnectorFunctionCall(sql, retType, Collections.emptyList());
    }

    private static ConnectorType scalarTypeToConnectorType(ScalarType st) {
        String name = st.getPrimitiveType().toString();
        int precision = st.getScalarPrecision();
        int scale = st.getScalarScale();
        if (precision > 0 || scale > 0) {
            return ConnectorType.of(name, precision, scale);
        }
        return ConnectorType.of(name);
    }
}
