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

import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Converts a connector-neutral {@link ConnectorExpression} residual predicate (as returned by
 * {@code ConnectorMetadata.getSyntheticScanPredicates}) BACK into a bound Nereids {@link Expression}. It is the
 * reverse of the forward converters {@link NereidsToConnectorExpressionConverter} /
 * {@link ExprToConnectorExpressionConverter} (same package) and is connector-agnostic: it only maps
 * {@code ConnectorExpression} node types to Nereids nodes — the column names and literal values come entirely
 * from the connector's payload, so there is ZERO source-specific logic here.
 *
 * <p>The analysis-time rule that injects a connector's synthetic scan predicate (e.g. hudi's incremental
 * {@code _hoodie_commit_time} window) calls this to turn each returned {@code ConnectorExpression} into a
 * {@code LogicalFilter} conjunct, binding {@link ConnectorColumnRef}s to the scan-output {@link SlotReference}s
 * by name.</p>
 *
 * <p><b>TOTAL and FAIL-LOUD</b> — the deliberate INVERSE of the forward converter's drop-on-unknown contract.
 * The forward converter returns {@code null} for unrepresentable nodes because dropping a conjunct only WIDENS
 * a write-conflict filter (safe). Here the opposite holds: dropping a conjunct from a residual SCAN predicate
 * UNDER-filters and leaks rows the connector meant to exclude (a correctness bug). So this throws on any node
 * outside the supported grammar, on any column reference that does not resolve to a scan-output slot, and on
 * any non-string literal — never silently returning null.</p>
 *
 * <p><b>Supported grammar</b> (what a connector residual predicate uses today): {@link ConnectorAnd},
 * {@link ConnectorComparison} with an order/equality operator ({@code EQ/LT/LE/GT/GE}), {@link ConnectorColumnRef}
 * bound by name, and STRING {@link ConnectorLiteral}s. Extend the dispatch (and add a
 * {@code ConnectorType -> Nereids DataType} mapper for typed literals) when a connector needs a wider shape.</p>
 */
public final class ConnectorExpressionToNereidsConverter {

    private ConnectorExpressionToNereidsConverter() {
    }

    /**
     * Converts {@code expr} into a bound Nereids {@link Expression}, resolving {@link ConnectorColumnRef}s
     * against {@code boundSlots} (scan-output slots keyed by column name).
     *
     * @throws AnalysisException on an unsupported node, an unresolvable column reference, or a non-string literal
     */
    public static Expression convert(ConnectorExpression expr, Map<String, SlotReference> boundSlots) {
        if (expr instanceof ConnectorAnd) {
            return convertAnd((ConnectorAnd) expr, boundSlots);
        }
        if (expr instanceof ConnectorComparison) {
            return convertComparison((ConnectorComparison) expr, boundSlots);
        }
        throw new AnalysisException(
                "cannot reverse-convert connector residual predicate node: " + describe(expr));
    }

    private static Expression convertAnd(ConnectorAnd and, Map<String, SlotReference> boundSlots) {
        List<ConnectorExpression> conjuncts = and.getConjuncts();
        if (conjuncts.isEmpty()) {
            throw new AnalysisException("cannot reverse-convert an empty ConnectorAnd");
        }
        List<Expression> converted = new ArrayList<>(conjuncts.size());
        for (ConnectorExpression conjunct : conjuncts) {
            converted.add(convert(conjunct, boundSlots));
        }
        // Nereids And(List) requires >= 2 children; a single conjunct is returned bare (mirrors the forward
        // convertAnd's single-conjunct unwrap).
        return converted.size() == 1 ? converted.get(0) : new And(converted);
    }

    private static Expression convertComparison(ConnectorComparison cmp, Map<String, SlotReference> boundSlots) {
        Expression left = convertLeaf(cmp.getLeft(), boundSlots);
        Expression right = convertLeaf(cmp.getRight(), boundSlots);
        switch (cmp.getOperator()) {
            case EQ:
                return new EqualTo(left, right);
            case LT:
                return new LessThan(left, right);
            case LE:
                return new LessThanEqual(left, right);
            case GT:
                return new GreaterThan(left, right);
            case GE:
                return new GreaterThanEqual(left, right);
            default:
                // NE / EQ_FOR_NULL have no single-node Nereids inverse and no residual predicate emits them
                // today. Fail loud rather than approximate (an approximation could change filtered rows).
                throw new AnalysisException(
                        "unsupported comparison operator in connector residual predicate: " + cmp.getOperator());
        }
    }

    private static Expression convertLeaf(ConnectorExpression operand, Map<String, SlotReference> boundSlots) {
        if (operand instanceof ConnectorColumnRef) {
            String columnName = ((ConnectorColumnRef) operand).getColumnName();
            SlotReference slot = boundSlots.get(columnName);
            if (slot == null) {
                // Fail loud: silently dropping a predicate that references a column the scan does not output would
                // UNDER-filter and leak rows the connector meant to exclude. Post visible-meta-column exposure this
                // is unreachable for a correct hudi table; if it fires it surfaces a real binding bug, not wrong
                // results.
                throw new AnalysisException("connector residual predicate references column '" + columnName
                        + "' which is not in the scan output");
            }
            return slot;
        }
        if (operand instanceof ConnectorLiteral) {
            return convertLiteral((ConnectorLiteral) operand);
        }
        throw new AnalysisException(
                "cannot reverse-convert connector comparison operand: " + describe(operand));
    }

    private static Expression convertLiteral(ConnectorLiteral literal) {
        // Only STRING literals are supported today (residual predicates compare fixed-width instant strings
        // lexicographically). Generalizing to typed literals needs a ConnectorType -> Nereids DataType mapper (the
        // forward path only maps DataType -> ConnectorType); until then fail loud on any other type so a numeric
        // coercion can never silently change compare semantics.
        if (!"STRING".equalsIgnoreCase(literal.getType().getTypeName())) {
            throw new AnalysisException("unsupported connector residual-predicate literal type: "
                    + literal.getType().getTypeName());
        }
        Object value = literal.getValue();
        if (value == null) {
            throw new AnalysisException("a null connector residual-predicate literal is not supported");
        }
        return new StringLiteral(value.toString());
    }

    private static String describe(ConnectorExpression expr) {
        return expr == null ? "null" : expr.getClass().getSimpleName();
    }
}
