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

import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorBetween;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorNot;
import org.apache.doris.connector.api.pushdown.ConnectorOr;
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
 * {@link SlotReference} on one side and a {@link Literal} on the other (operands are normalised to
 * column-on-left, the operator unchanged — mirroring legacy {@code convertNereidsBinaryPredicate}).</p>
 *
 * <p><b>Anything else yields {@code null}</b> — {@code NullSafeEqual}, {@code Cast}-wrapped columns,
 * column-to-column comparisons, bare literals, etc. The legacy conflict path drops exactly these. Dropping a
 * conjunct only ever <i>widens</i> the resulting conflict-detection filter (more conservative, never missing
 * a real concurrent-write conflict); pushing a form legacy drops would <i>narrow</i> it and risk a missed
 * conflict. AND drops unconvertible conjuncts (fewer ANDed predicates = wider); OR is all-or-nothing (a
 * dropped disjunct would narrow it). See deviations-log DV-T07b-matrix.</p>
 *
 * <p>Literal encoding routes through {@link ExprToConnectorExpressionConverter#convert} on
 * {@link Literal#toLegacyLiteral()}, so the neutral {@link org.apache.doris.connector.api.ConnectorType}
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
        if (expr == null) {
            return null;
        }
        if (expr instanceof And) {
            return convertAnd((And) expr);
        } else if (expr instanceof Or) {
            return convertOr((Or) expr);
        } else if (expr instanceof Not) {
            ConnectorExpression child = convert(((Not) expr).child());
            return child == null ? null : new ConnectorNot(child);
        } else if (expr instanceof EqualTo) {
            return convertComparison(expr, ConnectorComparison.Operator.EQ);
        } else if (expr instanceof GreaterThan) {
            return convertComparison(expr, ConnectorComparison.Operator.GT);
        } else if (expr instanceof GreaterThanEqual) {
            return convertComparison(expr, ConnectorComparison.Operator.GE);
        } else if (expr instanceof LessThan) {
            return convertComparison(expr, ConnectorComparison.Operator.LT);
        } else if (expr instanceof LessThanEqual) {
            return convertComparison(expr, ConnectorComparison.Operator.LE);
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

    private static ConnectorExpression convertAnd(And and) {
        List<ConnectorExpression> conjuncts = new ArrayList<>();
        flattenAnd(and, conjuncts);
        conjuncts.removeIf(c -> c == null);
        if (conjuncts.isEmpty()) {
            return null;
        }
        return conjuncts.size() == 1 ? conjuncts.get(0) : new ConnectorAnd(conjuncts);
    }

    private static void flattenAnd(Expression expr, List<ConnectorExpression> out) {
        if (expr instanceof And) {
            for (Expression child : expr.children()) {
                flattenAnd(child, out);
            }
        } else {
            out.add(convert(expr));
        }
    }

    private static ConnectorExpression convertOr(Or or) {
        List<ConnectorExpression> disjuncts = new ArrayList<>();
        if (!flattenOr(or, disjuncts) || disjuncts.isEmpty()) {
            return null;
        }
        return disjuncts.size() == 1 ? disjuncts.get(0) : new ConnectorOr(disjuncts);
    }

    private static boolean flattenOr(Expression expr, List<ConnectorExpression> out) {
        if (expr instanceof Or) {
            for (Expression child : expr.children()) {
                if (!flattenOr(child, out)) {
                    return false;
                }
            }
            return true;
        }
        ConnectorExpression c = convert(expr);
        if (c == null) {
            return false;
        }
        out.add(c);
        return true;
    }

    private static ConnectorExpression convertComparison(Expression cmp, ConnectorComparison.Operator op) {
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
        } else {
            return null;
        }
        ConnectorExpression litExpr = convertLiteral(literal);
        if (litExpr == null) {
            return null;
        }
        return new ConnectorComparison(op, columnRef(slot), litExpr);
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
