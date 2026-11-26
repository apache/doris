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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.ColumnRange;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;

import java.util.Optional;

/** Infer an enclosing result range without requiring the expression to be monotonic. */
public interface RangeInferable extends ExpressionTrait {
    int getRangeInputChildIndex();

    // Replace the range input with a constant while preserving the other arguments.
    Expression withConstantArgs(Expression literal);

    /**
     * Return a range containing every possible result, or empty when no safe range can be inferred.
     * A null lower or upper endpoint means that the input is unbounded on that side.
     */
    Optional<ColumnRange> inferRange(Literal lower, Literal upper, ExpressionRewriteContext context);

    /** Fold the input endpoints, retaining null for an unbounded side, and reject SQL NULL or unfolded results. */
    default Optional<Pair<Literal, Literal>> evaluateRangeEndpoints(
            Literal lower, Literal upper, ExpressionRewriteContext context) {
        Expression lowerValue = lower == null ? null
                : FoldConstantRuleOnFE.evaluate(withConstantArgs(lower), context);
        Expression upperValue = upper == null ? null
                : FoldConstantRuleOnFE.evaluate(withConstantArgs(upper), context);
        if (lowerValue instanceof NullLiteral || upperValue instanceof NullLiteral
                || lowerValue != null && !(lowerValue instanceof Literal)
                || upperValue != null && !(upperValue instanceof Literal)) {
            return Optional.empty();
        }
        return Optional.of(Pair.of((Literal) lowerValue, (Literal) upperValue));
    }

    /** Build inclusive result bounds. Input endpoint exclusions need not hold after applying a function. */
    static Optional<ColumnRange> createRange(Literal lower, Literal upper) {
        ColumnRange range = ColumnRange.all();
        if (lower != null) {
            range = range.withLowerBound(lower);
        }
        if (upper != null) {
            range = range.withUpperBound(upper);
        }
        if (range.isEmptyRange() || !range.span().hasLowerBound() && !range.span().hasUpperBound()) {
            return Optional.empty();
        }
        return Optional.of(range);
    }
}
