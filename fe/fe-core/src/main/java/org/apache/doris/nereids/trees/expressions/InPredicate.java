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
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * In predicate expression.
 */
public class InPredicate extends Expression {

    private final Expression compareExpr;
    private final List<Expression> options;

    public InPredicate(Expression compareExpr, List<Expression> options) {
        super(new Builder<Expression>().add(compareExpr).addAll(options).build().toArray(new Expression[0]));
        this.compareExpr = Objects.requireNonNull(compareExpr, "Compare Expr cannot be null");
        this.options = ImmutableList.copyOf(Objects.requireNonNull(options, "In list cannot be null"));
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitInPredicate(this, context);
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public boolean nullable() throws UnboundException {
        return children().stream().anyMatch(Expression::nullable);
    }

    @Override
    public InPredicate withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() > 1);
        return new InPredicate(children.get(0), ImmutableList.copyOf(children).subList(1, children.size()));
    }

    @Override
    public String toString() {
        return compareExpr + " IN " + options.stream()
            .map(Expression::toString)
            .collect(Collectors.joining(", ", "(", ")"));
    }

    @Override
    public String toSql() {
        return compareExpr.toSql() + " IN " + options.stream()
            .map(Expression::toSql)
            .collect(Collectors.joining(", ", "(", ")"));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InPredicate that = (InPredicate) o;
        return Objects.equals(compareExpr, that.getCompareExpr())
            && Objects.equals(options, that.getOptions());
    }

    @Override
    public int hashCode() {
        return Objects.hash(compareExpr, options);
    }

    public Expression getCompareExpr() {
        return compareExpr;
    }

    public List<Expression> getOptions() {
        return options;
    }

    /**
     * Return true when all children are Literal , otherwise, return false.
     */
    public boolean isLiteralChildren() {
        for (Expression expression : options) {
            if (!(expression instanceof Literal)) {
                return false;
            }
        }
        return true;
    }
}
