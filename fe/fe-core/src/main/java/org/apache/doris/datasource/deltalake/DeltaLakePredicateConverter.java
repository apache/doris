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

package org.apache.doris.datasource.deltalake;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Type;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

/**
 * Converts Doris conjunct expressions to Delta Kernel Predicate for data skipping.
 *
 * The converted predicates enable Delta Kernel to skip data files based on
 * column statistics (min/max/nullCount) stored in the Delta transaction log.
 * Even if some predicates cannot be converted, correctness is not affected —
 * only performance (fewer files can be skipped).
 *
 * Supported conversions:
 *   - BinaryPredicate (=, !=, <, <=, >, >=)
 *   - IsNullPredicate / IS NOT NULL
 *   - CompoundPredicate (AND, OR, NOT)
 *   - BoolLiteral (TRUE/FALSE)
 *
 * Note: InPredicate is NOT directly supported by Delta Kernel's Predicate API.
 * A single IN(a,b,c) is expanded to (col=a OR col=b OR col=c).
 */
public class DeltaLakePredicateConverter {
    private static final Logger LOG = LogManager.getLogger(DeltaLakePredicateConverter.class);

    private DeltaLakePredicateConverter() {
        // utility class
    }

    /**
     * Convert a list of Doris conjunct expressions (ANDed together) to a
     * single Delta Kernel Predicate.
     *
     * Returns Optional.empty() if no predicates can be converted, which means
     * Delta Kernel will scan all files.
     */
    public static Optional<Predicate> convertToKernelPredicate(List<Expr> conjuncts) {
        if (conjuncts == null || conjuncts.isEmpty()) {
            return Optional.empty();
        }

        Predicate result = null;
        for (Expr expr : conjuncts) {
            Optional<Predicate> converted = convertExpr(expr);
            if (converted.isPresent()) {
                if (result == null) {
                    result = converted.get();
                } else {
                    result = new Predicate("AND", result, converted.get());
                }
            }
        }

        return Optional.ofNullable(result);
    }

    /**
     * Convert a single Doris expression to a Delta Kernel Predicate.
     * Returns empty if the expression cannot be converted.
     */
    private static Optional<Predicate> convertExpr(Expr expr) {
        if (expr instanceof BoolLiteral) {
            return convertBoolLiteral((BoolLiteral) expr);
        } else if (expr instanceof CompoundPredicate) {
            return convertCompoundPredicate((CompoundPredicate) expr);
        } else if (expr instanceof BinaryPredicate) {
            return convertBinaryPredicate((BinaryPredicate) expr);
        } else if (expr instanceof IsNullPredicate) {
            return convertIsNullPredicate((IsNullPredicate) expr);
        } else if (expr instanceof InPredicate) {
            return convertInPredicate((InPredicate) expr);
        }

        LOG.debug("Unsupported expression type for Delta Lake predicate conversion: {}",
                expr.getClass().getSimpleName());
        return Optional.empty();
    }

    private static Optional<Predicate> convertBoolLiteral(BoolLiteral boolLiteral) {
        if (boolLiteral.getValue()) {
            return Optional.of(new Predicate("ALWAYS_TRUE", java.util.Collections.emptyList()));
        } else {
            return Optional.of(new Predicate("ALWAYS_FALSE", java.util.Collections.emptyList()));
        }
    }

    private static Optional<Predicate> convertCompoundPredicate(CompoundPredicate compound) {
        switch (compound.getOp()) {
            case AND: {
                Optional<Predicate> left = convertExpr(compound.getChild(0));
                Optional<Predicate> right = convertExpr(compound.getChild(1));
                if (left.isPresent() && right.isPresent()) {
                    return Optional.of(new Predicate("AND", left.get(), right.get()));
                } else if (left.isPresent()) {
                    return left;
                } else if (right.isPresent()) {
                    return right;
                }
                return Optional.empty();
            }
            case OR: {
                Optional<Predicate> left = convertExpr(compound.getChild(0));
                Optional<Predicate> right = convertExpr(compound.getChild(1));
                // For OR, both sides must be convertible
                if (left.isPresent() && right.isPresent()) {
                    return Optional.of(new Predicate("OR", left.get(), right.get()));
                }
                return Optional.empty();
            }
            case NOT: {
                Optional<Predicate> child = convertExpr(compound.getChild(0));
                if (child.isPresent()) {
                    return Optional.of(new Predicate("NOT", child.get()));
                }
                return Optional.empty();
            }
            default:
                return Optional.empty();
        }
    }

    private static Optional<Predicate> convertBinaryPredicate(BinaryPredicate pred) {
        SlotRef slotRef = extractSlotRef(pred.getChild(0));
        LiteralExpr literalExpr = null;
        boolean reversed = false;

        if (slotRef == null && pred.getChild(0).isLiteral()) {
            // literal op column → swap
            literalExpr = (LiteralExpr) pred.getChild(0);
            slotRef = extractSlotRef(pred.getChild(1));
            reversed = true;
        } else if (pred.getChild(1).isLiteral()) {
            literalExpr = (LiteralExpr) pred.getChild(1);
        }

        if (slotRef == null || literalExpr == null) {
            return Optional.empty();
        }

        String colName = slotRef.getColumnName();
        Column column = new Column(colName);
        Literal literal = dorisLiteralToDeltaLiteral(literalExpr);
        if (literal == null) {
            return Optional.empty();
        }

        BinaryPredicate.Operator op = pred.getOp();
        if (reversed) {
            op = op.commutative();
        }

        return convertBinaryOp(op, column, literal);
    }

    private static Optional<Predicate> convertBinaryOp(
            BinaryPredicate.Operator op, Column column, Literal literal) {
        switch (op) {
            case EQ:
            case EQ_FOR_NULL:
                return Optional.of(new Predicate("=", column, literal));
            case NE:
                return Optional.of(new Predicate("NOT", new Predicate("=", column, literal)));
            case LT:
                return Optional.of(new Predicate("<", column, literal));
            case LE:
                return Optional.of(new Predicate("<=", column, literal));
            case GT:
                return Optional.of(new Predicate(">", column, literal));
            case GE:
                return Optional.of(new Predicate(">=", column, literal));
            default:
                return Optional.empty();
        }
    }

    private static Optional<Predicate> convertIsNullPredicate(IsNullPredicate pred) {
        SlotRef slotRef = extractSlotRef(pred.getChild(0));
        if (slotRef == null) {
            return Optional.empty();
        }
        Column column = new Column(slotRef.getColumnName());
        if (pred.isNotNull()) {
            return Optional.of(new Predicate("IS_NOT_NULL", column));
        } else {
            return Optional.of(new Predicate("IS_NULL", column));
        }
    }

    /**
     * Convert IN/NOT IN predicate.
     * Delta Kernel does not have a native IN predicate, so we expand:
     *   col IN (a, b, c) → (col = a OR col = b OR col = c)
     *   col NOT IN (a, b, c) → (col != a AND col != b AND col != c)
     * Only expand for small lists (≤ 20 elements) to avoid excessive predicate tree.
     */
    private static Optional<Predicate> convertInPredicate(InPredicate pred) {
        SlotRef slotRef = extractSlotRef(pred.getChild(0));
        if (slotRef == null) {
            return Optional.empty();
        }

        int numValues = pred.getChildren().size() - 1;
        if (numValues <= 0 || numValues > 20) {
            return Optional.empty();
        }

        String colName = slotRef.getColumnName();
        Column column = new Column(colName);

        Predicate result = null;
        for (int i = 1; i < pred.getChildren().size(); i++) {
            if (!(pred.getChild(i) instanceof LiteralExpr)) {
                return Optional.empty();
            }
            Literal literal = dorisLiteralToDeltaLiteral((LiteralExpr) pred.getChild(i));
            if (literal == null) {
                return Optional.empty();
            }

            Predicate equalPred = new Predicate("=", column, literal);
            if (pred.isNotIn()) {
                // NOT IN: AND together negated equalities
                Predicate notEqual = new Predicate("NOT", equalPred);
                result = (result == null) ? notEqual : new Predicate("AND", result, notEqual);
            } else {
                // IN: OR together equalities
                result = (result == null) ? equalPred : new Predicate("OR", result, equalPred);
            }
        }

        return Optional.ofNullable(result);
    }

    /**
     * Convert a Doris LiteralExpr to a Delta Kernel Literal.
     * Returns null if the type is not supported.
     */
    private static Literal dorisLiteralToDeltaLiteral(LiteralExpr expr) {
        if (expr instanceof BoolLiteral) {
            return Literal.ofBoolean(((BoolLiteral) expr).getValue());
        } else if (expr instanceof IntLiteral) {
            IntLiteral intLiteral = (IntLiteral) expr;
            Type type = intLiteral.getType();
            if (type.isInteger32Type()) {
                return Literal.ofInt((int) intLiteral.getValue());
            } else {
                return Literal.ofLong(intLiteral.getValue());
            }
        } else if (expr instanceof FloatLiteral) {
            FloatLiteral floatLiteral = (FloatLiteral) expr;
            if (floatLiteral.getType() == Type.FLOAT) {
                return Literal.ofFloat((float) floatLiteral.getValue());
            } else {
                return Literal.ofDouble(floatLiteral.getValue());
            }
        } else if (expr instanceof DecimalLiteral) {
            DecimalLiteral decimalLiteral = (DecimalLiteral) expr;
            java.math.BigDecimal value = decimalLiteral.getValue();
            return Literal.ofDecimal(value, value.precision(), value.scale());
        } else if (expr instanceof StringLiteral) {
            return Literal.ofString(expr.getStringValue());
        } else if (expr instanceof DateLiteral) {
            DateLiteral dateLiteral = (DateLiteral) expr;
            // Delta Lake stores dates as number of days since epoch
            // and timestamps as microseconds since epoch
            Type type = dateLiteral.getType();
            if (type.isDateType()) {
                return Literal.ofDate((int) dateLiteral.daynr());
            } else {
                return Literal.ofTimestamp(dateLiteral.getUnixTimestampWithMicroseconds(
                        java.util.TimeZone.getDefault()));
            }
        }

        LOG.debug("Unsupported literal type for Delta Lake: {}", expr.getClass().getSimpleName());
        return null;
    }

    private static SlotRef extractSlotRef(Expr expr) {
        if (expr instanceof SlotRef) {
            return (SlotRef) expr;
        } else if (expr instanceof CastExpr) {
            if (expr.getChild(0) instanceof SlotRef) {
                return (SlotRef) expr.getChild(0);
            }
        }
        return null;
    }
}
