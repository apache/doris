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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A bracket array literal '[e1, e2, ...]' whose items are constant expressions that
 * cannot be folded to a literal at parse time (e.g. cast or arithmetic over literals).
 *
 * <p>Unlike {@link org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral}, the items
 * are still unbound constant expressions, so whether they stay constant must be
 * re-validated after function binding. For example '[random()]' and '[sum(1)]' look
 * constant while they are {@link org.apache.doris.nereids.analyzer.UnboundFunction}
 * (which defaults to deterministic and is neither an aggregate nor a table generating
 * function), but become volatile / aggregate after binding.
 *
 * <p>This node preserves the bracket-array origin through analysis. During analysis it is
 * lowered to the scalar 'array' function after all bound items are validated to be constant.
 */
public class BracketArray extends Expression {

    public BracketArray(List<Expression> items) {
        super(ImmutableList.copyOf(Objects.requireNonNull(items, "items should not null")));
    }

    public BracketArray(Expression... items) {
        this(ImmutableList.copyOf(items));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitBracketArray(this, context);
    }

    @Override
    public boolean nullable() {
        // lowered to the array() function during analysis, which is AlwaysNotNullable
        return false;
    }

    @Override
    public String computeToSql() throws UnboundException {
        return children().stream()
                .map(Expression::toSql)
                .collect(Collectors.joining(", ", "[", "]"));
    }

    @Override
    public String toString() {
        return children().stream()
                .map(Object::toString)
                .collect(Collectors.joining(", ", "[", "]"));
    }

    @Override
    public String toDigest() {
        return children().stream()
                .map(Expression::toDigest)
                .collect(Collectors.joining(", ", "[", "]"));
    }

    @Override
    public BracketArray withChildren(List<Expression> children) {
        return new BracketArray(children);
    }
}
