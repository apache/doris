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

package org.apache.doris.connector.api.pushdown;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Binary comparison: left op right.
 *
 * <p>Supported operators: EQ, NE, LT, LE, GT, GE, EQ_FOR_NULL.</p>
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
