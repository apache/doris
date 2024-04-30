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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * In predicate expression.
 * InPredicate could not use traits PropagateNullable, because fold constant will do wrong fold, such as
 * 3 in (1, null, 3, 4) => null
 */
public class InPredicate extends Expression {

    private final Expression compareExpr;
    private final List<Expression> options;

    public InPredicate(Expression compareExpr, Collection<Expression> options) {
        super(new Builder<Expression>().add(compareExpr).addAll(options).build());
        this.compareExpr = Objects.requireNonNull(compareExpr, "Compare Expr cannot be null");
        this.options = ImmutableList.copyOf(Objects.requireNonNull(options, "In list cannot be null"));
    }

    public InPredicate(Expression compareExpr, Collection<Expression> options, boolean inferred) {
        super(new Builder<Expression>().add(compareExpr).addAll(options).build(), inferred);
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
    public InPredicate withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() > 1);
        return new InPredicate(children.get(0), ImmutableList.copyOf(children).subList(1, children.size()));
    }

    @Override
    public boolean nullable() throws UnboundException {
        return children().stream().anyMatch(Expression::nullable);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (children().get(0).getDataType().isStructType()) {
            // we should check in value list is all struct type
            for (int i = 1; i < children().size(); i++) {
                if (!children().get(i).getDataType().isStructType() && !children().get(i).getDataType().isNullType()) {
                    throw new AnalysisException("in predicate struct should compare with struct type list, but got : "
                            + children().get(i).getDataType().toSql());
                }
            }
            return;
        }

        if (children().get(0).getDataType().isArrayType()) {
            // we should check in value list is all list type
            for (int i = 1; i < children().size(); i++) {
                if (!children().get(i).getDataType().isArrayType() && !children().get(i).getDataType().isNullType()) {
                    throw new AnalysisException("in predicate list should compare with struct type list, but got : "
                            + children().get(i).getDataType().toSql());
                }
            }
            return;
        }

        children().forEach(c -> {
            if (c.getDataType().isObjectType()) {
                throw new AnalysisException("in predicate could not contains object type: " + this.toSql());
            }
            if (c.getDataType().isComplexType()) {
                throw new AnalysisException("in predicate could not contains complex type: " + this.toSql());
            }
        });
    }

    @Override
    public Expression withInferred(boolean inferred) {
        return new InPredicate(children.get(0), ImmutableList.copyOf(children).subList(1, children.size()), true);
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
            .map(Expression::toSql).sorted()
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
