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

package org.apache.doris.connector.spi.pushdown;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Binary comparison: left op right.
 *
 * <p>{@code left} is normally a {@link ConnectorColumnRef} and {@code right} a {@link ConnectorLiteral}, but
 * neither is guaranteed: the engine builds this node from whatever the two operands converted to. A connector
 * that only supports column-op-literal must check both sides and drop the conjunct otherwise (see the package
 * javadoc, Rule 1) rather than assume.</p>
 *
 * <p>Operator semantics, all with SQL three-valued logic — a comparison against NULL is UNKNOWN, not false:</p>
 * <ul>
 *   <li>{@code EQ} / {@code NE} / {@code LT} / {@code LE} / {@code GT} / {@code GE} — the ordinary
 *       {@code =}, {@code !=}, {@code <}, {@code <=}, {@code >}, {@code >=}. Rows where either side is NULL
 *       match none of them.</li>
 *   <li>{@code EQ_FOR_NULL} — Doris' null-safe equality {@code <=>}. See below; it has TWO cases.</li>
 * </ul>
 *
 * <p><b>{@code EQ_FOR_NULL} must be split into two cases; collapsing them loses rows:</b></p>
 * <ul>
 *   <li>right operand is a NULL literal ({@link ConnectorLiteral#isNull()}) — equivalent to
 *       {@code IS NULL}.</li>
 *   <li>right operand is a NON-NULL literal — equivalent to plain {@code EQ}, and specifically NOT
 *       {@code IS NULL}. Translating {@code c <=> 5} into {@code c IS NULL} silently drops every matching
 *       row, because the connector prunes the files holding {@code c = 5} before BE ever sees them.</li>
 * </ul>
 *
 * <p>A connector whose dialect has no null-safe form must drop the whole conjunct (package javadoc, Rule 1).
 * It must never substitute a narrower predicate. Shipped precedent for the correct shape: the iceberg and
 * trino converters branch on {@code isNull()}; the maxcompute converter has no null-safe operator remotely and
 * therefore refuses to push the conjunct at all.</p>
 */
public final class ConnectorComparison implements ConnectorExpression {

    private static final long serialVersionUID = 1L;

    /** Comparison operator. */
    public enum Operator {
        EQ("="),
        NE("!="),
        LT("<"),
        LE("<="),
        GT(">"),
        GE(">="),
        EQ_FOR_NULL("<=>");

        private final String symbol;

        Operator(String symbol) {
            this.symbol = symbol;
        }

        public String getSymbol() {
            return symbol;
        }
    }

    private final Operator operator;
    private final ConnectorExpression left;
    private final ConnectorExpression right;

    public ConnectorComparison(Operator operator,
            ConnectorExpression left, ConnectorExpression right) {
        this.operator = Objects.requireNonNull(operator, "operator");
        this.left = Objects.requireNonNull(left, "left");
        this.right = Objects.requireNonNull(right, "right");
    }

    public Operator getOperator() {
        return operator;
    }

    public ConnectorExpression getLeft() {
        return left;
    }

    public ConnectorExpression getRight() {
        return right;
    }

    @Override
    public List<ConnectorExpression> getChildren() {
        return Arrays.asList(left, right);
    }

    @Override
    public String toString() {
        return "(" + left + " " + operator.getSymbol() + " " + right + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorComparison)) {
            return false;
        }
        ConnectorComparison that = (ConnectorComparison) o;
        return operator == that.operator
                && left.equals(that.left) && right.equals(that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator, left, right);
    }
}
