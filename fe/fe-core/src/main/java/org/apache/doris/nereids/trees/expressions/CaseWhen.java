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
 * If a case expr is given (simple case), the value is stored as the first child.
 * During analysis, the value is consumed: EqualTo(value, whenCondition) is constructed
 * for each WhenClause, producing a standard searched case form.
 * If an else expr is given then it is the last child.
 *
 * Children layout: [value?, WhenClause+, defaultValue?]
 */
public class CaseWhen extends Expression implements NeedSessionVarGuard {

    private final Optional<Expression> value;
    private final List<WhenClause> whenClauses;
    private final Optional<Expression> defaultValue;
    private final Supplier<List<DataType>> dataTypesForCoercion;

    public CaseWhen(List<WhenClause> whenClauses) {
        super((List) whenClauses);
        this.value = Optional.empty();
        this.whenClauses = ImmutableList.copyOf(Objects.requireNonNull(whenClauses));
        this.defaultValue = Optional.empty();
        this.dataTypesForCoercion = computeDataTypesForCoercion();
    }

    /** CaseWhen with default value (searched case) */
    public CaseWhen(List<WhenClause> whenClauses, Expression defaultValue) {
        super(ImmutableList.<Expression>builderWithExpectedSize(whenClauses.size() + 1)
                .addAll(whenClauses)
                .add(defaultValue)
                .build());
        this.value = Optional.empty();
        this.whenClauses = ImmutableList.copyOf(Objects.requireNonNull(whenClauses));
        this.defaultValue = Optional.of(Objects.requireNonNull(defaultValue));
        this.dataTypesForCoercion = computeDataTypesForCoercion();
    }

    /** Simple case: CASE value WHEN ... */
    public CaseWhen(Expression value, List<WhenClause> whenClauses) {
        super(ImmutableList.<Expression>builderWithExpectedSize(whenClauses.size() + 1)
                .add(Objects.requireNonNull(value))
                .addAll(whenClauses)
                .build());
        this.value = Optional.of(value);
        this.whenClauses = ImmutableList.copyOf(Objects.requireNonNull(whenClauses));
        this.defaultValue = Optional.empty();
        this.dataTypesForCoercion = computeDataTypesForCoercion();
    }

    /** Simple case with default value: CASE value WHEN ... ELSE ... END */
    public CaseWhen(Expression value, List<WhenClause> whenClauses, Expression defaultValue) {
        super(ImmutableList.<Expression>builderWithExpectedSize(whenClauses.size() + 2)
                .add(Objects.requireNonNull(value))
                .addAll(whenClauses)
                .add(defaultValue)
                .build());
        this.value = Optional.of(value);
        this.whenClauses = ImmutableList.copyOf(Objects.requireNonNull(whenClauses));
        this.defaultValue = Optional.of(Objects.requireNonNull(defaultValue));
        this.dataTypesForCoercion = computeDataTypesForCoercion();
    }

    public Optional<Expression> getValue() {
        return value;
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
        return whenClauses.get(0).getDataType();
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
        value.ifPresent(v -> output.append(" ").append(v.toString()));
        for (WhenClause whenClause : whenClauses) {
            output.append(whenClause.toString());
        }
        defaultValue.ifPresent(dv -> output.append(" ELSE ").append(dv.toString()));
        output.append(" END");
        return output.toString();
    }

    @Override
    public String toDigest() {
        StringBuilder sb = new StringBuilder("CASE");
        value.ifPresent(v -> sb.append(" ").append(v.toDigest()));
        for (WhenClause whenClause : whenClauses) {
            sb.append(whenClause.toDigest());
        }
        defaultValue.ifPresent(dv -> sb.append(" ELSE ").append(dv.toDigest()));
        sb.append(" END");
        return sb.toString();
    }

    @Override
    public String computeToSql() throws UnboundException {
        StringBuilder output = new StringBuilder("CASE");
        value.ifPresent(v -> output.append(" ").append(v.toSql()));
        for (WhenClause whenClause : whenClauses) {
            output.append(whenClause.toSql());
        }
        defaultValue.ifPresent(dv -> output.append(" ELSE ").append(dv.toSql()));
        output.append(" END");
        return output.toString();
    }

    @Override
    public CaseWhen withChildren(List<Expression> children) {
        Preconditions.checkArgument(!children.isEmpty(), "case when should has at least 1 child");
        int i = 0;
        Expression value = null;
        // First non-WhenClause child before any WhenClause is the simple case value.
        // Note: value is always consumed during analysis phase; post-analysis,
        // the first child is always a WhenClause, so this branch is only taken pre-analysis.
        if (i < children.size() && !(children.get(i) instanceof WhenClause)) {
            value = children.get(i);
            i++;
        }
        List<WhenClause> whenClauseList = new ArrayList<>();
        while (i < children.size() && children.get(i) instanceof WhenClause) {
            whenClauseList.add((WhenClause) children.get(i));
            i++;
        }
        Preconditions.checkArgument(!whenClauseList.isEmpty(), "case when should has at least 1 when clause");
        Expression defaultValue = null;
        if (i < children.size()) {
            defaultValue = children.get(i);
            i++;
        }
        Preconditions.checkArgument(i == children.size(),
                "The children format needs to be [Value?, WhenClause+, DefaultValue?]");
        if (value != null) {
            return defaultValue != null
                    ? new CaseWhen(value, whenClauseList, defaultValue)
                    : new CaseWhen(value, whenClauseList);
        }
        return defaultValue != null
                ? new CaseWhen(whenClauseList, defaultValue)
                : new CaseWhen(whenClauseList);
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
