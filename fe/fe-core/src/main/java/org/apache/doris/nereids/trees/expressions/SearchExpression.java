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
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SearchDslParser;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import java.util.List;
import java.util.Objects;

/**
 * SearchExpression represents a search query with bound slot references.
 * This is created by RewriteSearchToSlots rule from Search scalar function.
 */
public class SearchExpression extends Expression {
    private final String dslString;
    private final SearchDslParser.QsPlan qsPlan;

    public SearchExpression(String dslString, SearchDslParser.QsPlan qsPlan, List<Expression> slotChildren) {
        super(slotChildren);
        this.dslString = Objects.requireNonNull(dslString, "dslString cannot be null");
        this.qsPlan = Objects.requireNonNull(qsPlan, "qsPlan cannot be null");
    }

    public String getDslString() {
        return dslString;
    }

    public SearchDslParser.QsPlan getQsPlan() {
        return qsPlan;
    }

    public List<Expression> getSlotChildren() {
        return children();
    }

    @Override
    public boolean nullable() throws UnboundException {
        // Search expressions can be null if any child slot is null
        return children().stream().anyMatch(Expression::nullable);
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public boolean foldable() {
        // SearchExpression should never be foldable to prevent constant evaluation
        return false;
    }

    @Override
    public SearchExpression withChildren(List<Expression> children) {
        // Validate that all children are SlotReference or ElementAt (for variant subcolumns)
        for (Expression child : children) {
            if (!(child instanceof SlotReference || child instanceof ElementAt)) {
                throw new IllegalArgumentException(
                        "SearchExpression children must be SlotReference or ElementAt instances");
            }
        }
        return new SearchExpression(dslString, qsPlan, children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitSearchExpression(this, context);
    }

    @Override
    public String toString() {
        return "search('" + dslString + "')";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        SearchExpression that = (SearchExpression) o;
        return Objects.equals(dslString, that.dslString)
                && Objects.equals(qsPlan, that.qsPlan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dslString, qsPlan);
    }
}
