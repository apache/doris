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

import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Number of rows returned by inspection in subquery.
 */
public class AssertNumRowsElement extends Expression implements LeafExpression, AlwaysNotNullable {
    /**
     * Assertion type.
     */
    public enum Assertion {
        EQ, // val1 == val2
        NE, // val1 != val2
        LT, // val1 < val2
        LE, // val1 <= val2
        GT, // val1 > val2
        GE; // val1 >= val2
    }

    private final long desiredNumOfRows;
    private final String subqueryString;
    private final Assertion assertion;

    public AssertNumRowsElement(long desiredNumOfRows, String subqueryString,
            Assertion assertion) {
        super(ImmutableList.of());
        this.desiredNumOfRows = desiredNumOfRows;
        this.subqueryString = subqueryString;
        this.assertion = assertion;
    }

    public long getDesiredNumOfRows() {
        return desiredNumOfRows;
    }

    public String getSubqueryString() {
        return subqueryString;
    }

    public Assertion getAssertion() {
        return assertion;
    }

    @Override
    public AssertNumRowsElement withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.isEmpty());
        return this;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("AssertNumRowsElement",
                "desiredNumOfRows",
                Long.toString(desiredNumOfRows),
                "assertion", assertion);
    }

    @Override
    public String toSql() {
        return toString();
    }

    @Override
    protected String getExpressionName() {
        if (!this.exprName.isPresent()) {
            this.exprName = Optional.of(Utils.normalizeName(assertion.name().toLowerCase(), DEFAULT_EXPRESSION_NAME));
        }
        return this.exprName.get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AssertNumRowsElement that = (AssertNumRowsElement) o;
        return Objects.equals(this.desiredNumOfRows, that.getDesiredNumOfRows())
                && Objects.equals(this.subqueryString, that.getSubqueryString())
                && Objects.equals(this.assertion, that.getAssertion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(desiredNumOfRows, subqueryString, assertion);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAssertNumRowsElement(this, context);
    }
}
