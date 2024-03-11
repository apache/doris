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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The internal representation of
 * CASE [expr] WHEN expr THEN expr [WHEN expr THEN expr ...] [ELSE expr] END
 * Each When/Then is stored as two consecutive children (whenExpr, thenExpr).
 * If a case expr is given, convert it to equalTo(caseExpr, whenExpr) and set it to whenExpr.
 * If an else expr is given then it is the last child.
 */
public class CaseWhen extends Expression {

    private final List<WhenClause> whenClauses;
    private final Optional<Expression> defaultValue;
    private Supplier<List<DataType>> dataTypesForCoercion;

    public CaseWhen(List<WhenClause> whenClauses) {
        super((List) whenClauses);
        this.whenClauses = ImmutableList.copyOf(Objects.requireNonNull(whenClauses));
        defaultValue = Optional.empty();
        this.dataTypesForCoercion = computeDataTypesForCoercion();
    }

    /** CaseWhen */
    public CaseWhen(List<WhenClause> whenClauses, Expression defaultValue) {
        super(ImmutableList.<Expression>builderWithExpectedSize(whenClauses.size() + 1)
                .addAll(whenClauses)
                .add(defaultValue)
                .build());
        this.whenClauses = ImmutableList.copyOf(Objects.requireNonNull(whenClauses));
        this.defaultValue = Optional.of(Objects.requireNonNull(defaultValue));
        this.dataTypesForCoercion = computeDataTypesForCoercion();
    }

    public List<WhenClause> getWhenClauses() {
        return whenClauses;
    }

    public Optional<Expression> getDefaultValue() {
        return defaultValue;
    }

    /** dataTypesForCoercion */
    public List<DataType> dataTypesForCoercion() {
        return this.dataTypesForCoercion.get();
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCaseWhen(this, context);
    }

    @Override
    public DataType getDataType() {
        return child(0).getDataType();
    }

    @Override
    public boolean nullable() {
        for (WhenClause whenClause : whenClauses) {
            if (whenClause.nullable()) {
                return true;
            }
        }
        return defaultValue.map(Expression::nullable).orElse(true);
    }

    @Override
    public String toString() {
        StringBuilder output = new StringBuilder("CASE");
        for (Expression child : children()) {
            if (child instanceof WhenClause) {
                output.append(child.toString());
            } else {
                output.append(" ELSE ").append(child.toString());
            }
        }
        output.append(" END");
        return output.toString();
    }

    @Override
    public String toSql() throws UnboundException {
        StringBuilder output = new StringBuilder("CASE");
        for (Expression child : children()) {
            if (child instanceof WhenClause) {
                output.append(child.toSql());
            } else {
                output.append(" ELSE ").append(child.toSql());
            }
        }
        output.append(" END");
        return output.toString();
    }

    @Override
    public CaseWhen withChildren(List<Expression> children) {
        Preconditions.checkArgument(!children.isEmpty(), "case when should has at least 1 child");
        List<WhenClause> whenClauseList = new ArrayList<>();
        Expression defaultValue = null;
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i) instanceof WhenClause) {
                whenClauseList.add((WhenClause) children.get(i));
            } else if (children.size() - 1 == i) {
                defaultValue = children.get(i);
            } else {
                throw new AnalysisException("The children format needs to be [WhenClause+, DefaultValue?]");
            }
        }
        if (defaultValue == null) {
            return new CaseWhen(whenClauseList);
        }
        return new CaseWhen(whenClauseList, defaultValue);
    }

    private Supplier<List<DataType>> computeDataTypesForCoercion() {
        return Suppliers.memoize(() -> {
            Builder<DataType> dataTypes = ImmutableList.builderWithExpectedSize(
                    whenClauses.size() + (defaultValue.isPresent() ? 1 : 0));
            for (WhenClause whenClause : whenClauses) {
                dataTypes.add(whenClause.getDataType());
            }
            defaultValue.ifPresent(expression -> dataTypes.add(expression.getDataType()));
            return dataTypes.build();
        });
    }
}
