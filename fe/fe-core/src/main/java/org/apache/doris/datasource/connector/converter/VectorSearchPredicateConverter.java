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
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;

import java.util.ArrayList;
import java.util.List;

/** Exact predicate conversion: a bounded search must never silently lose an AND arm. */
public final class VectorSearchPredicateConverter {
    private VectorSearchPredicateConverter() {
    }

    public static ConnectorExpression convert(Expression expression) {
        if (expression instanceof And) {
            List<ConnectorExpression> children = new ArrayList<>();
            for (Expression child : expression.children()) {
                ConnectorExpression converted = convert(child);
                if (converted == null) {
                    return null;
                }
                children.add(converted);
            }
            return new ConnectorAnd(children);
        }
        if (expression instanceof NullSafeEqual) {
            Expression left = expression.child(0);
            Expression right = expression.child(1);
            if (right instanceof SlotReference && left instanceof Literal) {
                Expression previous = left;
                left = right;
                right = previous;
            }
            if (left instanceof SlotReference && right instanceof Literal) {
                return NereidsToConnectorExpressionConverter.convertStrict(right instanceof NullLiteral
                        ? new IsNull(left) : new EqualTo(left, right));
            }
            return null;
        }
        if (expression instanceof Not) {
            // NOT(NULL <=> value) is true, whereas ordinary != discards NULL rows.
            if (expression.child(0) instanceof NullSafeEqual) {
                return null;
            }
            ConnectorExpression child = convert(expression.child(0));
            if (child instanceof ConnectorComparison
                    && ((ConnectorComparison) child).getOperator() == ConnectorComparison.Operator.EQ) {
                ConnectorComparison comparison = (ConnectorComparison) child;
                return new ConnectorComparison(ConnectorComparison.Operator.NE,
                        comparison.getLeft(), comparison.getRight());
            }
            if (child instanceof ConnectorIn) {
                ConnectorIn in = (ConnectorIn) child;
                return new ConnectorIn(in.getValue(), in.getInList(), !in.isNegated());
            }
        }
        return NereidsToConnectorExpressionConverter.convertStrict(expression);
    }
}
