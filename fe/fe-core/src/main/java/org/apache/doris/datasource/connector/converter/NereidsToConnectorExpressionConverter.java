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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorBetween;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a Nereids {@link Expression} tree into an engine-neutral {@link ConnectorExpression} tree for the
 * O5-2 write-time conflict-detection path (P6.3-T07b). It is the Nereids twin of the analyzed-plan-side
 * {@link ExprToConnectorExpressionConverter} (same package), produced from the analyzed DELETE/UPDATE/MERGE
 * plan rather than a legacy {@code Expr}.
 *
 * <p><b>Node matrix = the legacy iceberg conflict matrix</b> ({@code
 * IcebergNereidsUtils.convertNereidsToIcebergExpression}): {@code And}/{@code Or}/{@code Not}, the five
 * comparisons ({@code EqualTo}/{@code GreaterThan}/{@code GreaterThanEqual}/{@code LessThan}/{@code
 * LessThanEqual}), {@code InPredicate}, {@code IsNull}, {@code Between}. Comparisons require a bare
 * {@link SlotReference} on one side and a {@link Literal} on the other. Loose conversion preserves the
 * legacy operator behavior; strict conversion mirrors the operator when it normalises operands to
 * column-on-left.</p>
 *
 * <p><b>Anything else yields {@code null}</b> — {@code NullSafeEqual}, {@code Cast}-wrapped columns,
 * column-to-column comparisons, bare literals, etc. The legacy conflict path drops exactly these. Dropping a
 * conjunct only ever <i>widens</i> the resulting conflict-detection filter (more conservative, never missing
 * a real concurrent-write conflict); pushing a form legacy drops would <i>narrow</i> it and risk a missed
 * conflict. AND drops unconvertible conjuncts (fewer ANDed predicates = wider); OR is all-or-nothing (a
 * dropped disjunct would narrow it). See deviations-log DV-T07b-matrix.</p>
 *
 * <p>Literal encoding routes through {@link ExprToConnectorExpressionConverter#convert} on
 * {@link Literal#toLegacyLiteral()}, so the neutral {@link org.apache.doris.connector.spi.ConnectorType}
 * tokens (uppercase {@code INT}/{@code BIGINT}/... + decimal precision/scale) are byte-identical to the scan
 * side — the connector's shared {@code IcebergPredicateConverter} type matrix then behaves the same for both
 * paths. Column types likewise go through {@link ExprToConnectorExpressionConverter#typeToConnectorType}.</p>
 */
public final class NereidsToConnectorExpressionConverter {

    private static final Logger LOG = LogManager.getLogger(NereidsToConnectorExpressionConverter.class);

    private NereidsToConnectorExpressionConverter() {
    }

    /** Convert a Nereids predicate to a neutral {@link ConnectorExpression}, or {@code null} if not
     * representable in the legacy iceberg conflict matrix (a safe over-approximation). */
    public static ConnectorExpression convert(Expression expr) {
        return convert(expr, false);
    }

    /**
     * Strict variant of {@link #convert}: it refuses to <i>drop</i> anything. The loose variant is a safe
     * over-approximation for its consumers (a dropped {@code AND} arm only widens a conflict-detection filter),
     * but the whole-file "metadata delete" fast path consumes the result as the <i>exact</i> statement predicate:
     * a dropped conjunct would widen the delete and drop data files that still hold live rows. Strict mode
     * therefore collapses the whole expression to {@code null} — "not usable" — as soon as any node outside the
     * matrix is met, so the caller falls back to the row-level path instead of guessing.
     */
    public static ConnectorExpression convertStrict(Expression expr) {
        return convert(expr, true);
    }

    private static ConnectorExpression convert(Expression expr, boolean strict) {
        if (expr == null) {
            return null;
        }
        if (expr instanceof And) {
            return convertAnd((And) expr, strict);
        } else if (expr instanceof Or) {
            return convertOr((Or) expr, strict);
        } else if (expr instanceof Not) {
            // SQL negation is not exact for a destructive whole-file predicate: Iceberg's notEqual/notIn
            // matching treats NULL rows as matching, while SQL NOT(UNKNOWN) is UNKNOWN (deletes nothing), so
            // lowering NOT(col = x) would drop files whose rows are all NULL. Only IS NOT NULL keeps SQL
            // semantics under negation; every other negated form falls back to the row-level path in strict mode.
            if (strict && !(((Not) expr).child() instanceof IsNull)) {
                return null;
            }
            ConnectorExpression child = convert(((Not) expr).child(), strict);
            return child == null ? null : new ConnectorNot(child);
        } else if (expr instanceof EqualTo) {
            return convertComparison(expr, ConnectorComparison.Operator.EQ, strict);
        } else if (expr instanceof GreaterThan) {
            return convertComparison(expr, ConnectorComparison.Operator.GT, strict);
        } else if (expr instanceof GreaterThanEqual) {
            return convertComparison(expr, ConnectorComparison.Operator.GE, strict);
        } else if (expr instanceof LessThan) {
            return convertComparison(expr, ConnectorComparison.Operator.LT, strict);
        } else if (expr instanceof LessThanEqual) {
            return convertComparison(expr, ConnectorComparison.Operator.LE, strict);
        } else if (expr instanceof InPredicate) {
            return convertIn((InPredicate) expr);
        } else if (expr instanceof IsNull) {
            return convertIsNull((IsNull) expr);
        } else if (expr instanceof Between) {
            return convertBetween((Between) expr);
        }
        // NullSafeEqual / Cast-wrapped column / bare literal / etc.: not in the legacy conflict matrix -> drop.
        return null;
    }

    private static ConnectorExpression convertAnd(And and, boolean strict) {
        List<ConnectorExpression> conjuncts = new ArrayList<>();
        boolean complete = flattenAnd(and, conjuncts, strict);
        if (!complete) {
            return null;
        }
        conjuncts.removeIf(c -> c == null);
        if (conjuncts.isEmpty()) {
            return null;
        }
        return conjuncts.size() == 1 ? conjuncts.get(0) : new ConnectorAnd(conjuncts);
    }

    private static boolean flattenAnd(Expression expr, List<ConnectorExpression> out, boolean strict) {
        if (expr instanceof And) {
            for (Expression child : expr.children()) {
                if (!flattenAnd(child, out, strict)) {
                    return false;
                }
            }
            return true;
        } else {
            ConnectorExpression converted = convert(expr, strict);
            if (converted == null && strict) {
                return false;
            }
            out.add(converted);
            return true;
        }
    }

    private static ConnectorExpression convertOr(Or or, boolean strict) {
        List<ConnectorExpression> disjuncts = new ArrayList<>();
        if (!flattenOr(or, disjuncts, strict) || disjuncts.isEmpty()) {
            return null;
        }
        return disjuncts.size() == 1 ? disjuncts.get(0) : new ConnectorOr(disjuncts);
    }

    private static boolean flattenOr(Expression expr, List<ConnectorExpression> out, boolean strict) {
        if (expr instanceof Or) {
            for (Expression child : expr.children()) {
                if (!flattenOr(child, out, strict)) {
                    return false;
                }
            }
            return true;
        }
        ConnectorExpression c = convert(expr, strict);
        if (c == null) {
            return false;
        }
        out.add(c);
        return true;
    }

    private static ConnectorExpression convertComparison(
            Expression cmp, ConnectorComparison.Operator op, boolean strict) {
        Expression left = cmp.child(0);
        Expression right = cmp.child(1);
        SlotReference slot;
        Literal literal;
        if (left instanceof SlotReference && right instanceof Literal) {
            slot = (SlotReference) left;
            literal = (Literal) right;
        } else if (left instanceof Literal && right instanceof SlotReference) {
            slot = (SlotReference) right;
            literal = (Literal) left;
            if (strict) {
                // Exact destructive predicates must commute the operator with the operands: `1 < id` is
                // `id > 1`. Loose conflict-filter behavior is deliberately unchanged by this feature.
                op = commute(op);
            }
        } else {
            return null;
        }
        ConnectorExpression litExpr = convertLiteral(literal);
        if (litExpr == null) {
            return null;
        }
        return new ConnectorComparison(op, columnRef(slot), litExpr);
    }

    /** Mirrors {@code op} across the operands: {@code 1 < id} is {@code id > 1}. */
    private static ConnectorComparison.Operator commute(ConnectorComparison.Operator op) {
        switch (op) {
            case LT:
                return ConnectorComparison.Operator.GT;
            case LE:
                return ConnectorComparison.Operator.GE;
            case GT:
                return ConnectorComparison.Operator.LT;
            case GE:
                return ConnectorComparison.Operator.LE;
            default:
                // EQ / NE / EQ_FOR_NULL are symmetric.
                return op;
        }
    }

    private static ConnectorExpression convertIn(InPredicate in) {
        if (!(in.child(0) instanceof SlotReference)) {
            return null;
        }
        List<ConnectorExpression> inList = new ArrayList<>();
        for (int i = 1; i < in.children().size(); i++) {
            Expression child = in.child(i);
            if (!(child instanceof Literal)) {
                return null;
            }
            ConnectorExpression lit = convertLiteral((Literal) child);
            if (lit == null) {
                return null;
            }
            inList.add(lit);
        }
        return new ConnectorIn(columnRef((SlotReference) in.child(0)), inList, false);
    }

    private static ConnectorExpression convertIsNull(IsNull isNull) {
        if (!(isNull.child() instanceof SlotReference)) {
            return null;
        }
        return new ConnectorIsNull(columnRef((SlotReference) isNull.child()), false);
    }

    private static ConnectorExpression convertBetween(Between between) {
        Expression compareExpr = between.getCompareExpr();
        Expression lower = between.getLowerBound();
        Expression upper = between.getUpperBound();
        if (!(compareExpr instanceof SlotReference)
                || !(lower instanceof Literal) || !(upper instanceof Literal)) {
            return null;
        }
        ConnectorExpression lo = convertLiteral((Literal) lower);
        ConnectorExpression hi = convertLiteral((Literal) upper);
        if (lo == null || hi == null) {
            return null;
        }
        return new ConnectorBetween(columnRef((SlotReference) compareExpr), lo, hi);
    }

    private static ConnectorColumnRef columnRef(SlotReference slot) {
        return new ConnectorColumnRef(slot.getName(),
                ExprToConnectorExpressionConverter.typeToConnectorType(slot.getDataType().toCatalogDataType()));
    }

    // Route literals through the analyzed-plan-side converter (Expr -> ConnectorExpression) so the neutral
    // type token + value are byte-identical to the scan path. toLegacyLiteral() is a pure transformation.
    private static ConnectorExpression convertLiteral(Literal literal) {
        try {
            return ExprToConnectorExpressionConverter.convert(literal.toLegacyLiteral());
        } catch (Exception e) {
            LOG.debug("cannot convert nereids literal {} to a connector literal: {}", literal, e.getMessage());
            return null;
        }
    }
}
