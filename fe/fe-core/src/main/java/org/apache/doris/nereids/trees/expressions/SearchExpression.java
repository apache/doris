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

import org.apache.doris.analysis.SearchDslParser;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import java.util.List;
import java.util.Objects;

/**
 * SearchExpression represents a search query with bound slot references.
 * This is created by RewriteSearchToSlots rule from Search scalar function.
 *
 * <p>Each child binds one DSL field, in the order of the QsPlan field bindings: a slot, or element_at on a slot
 * for a variant subcolumn. BE evaluates the expression only with the inverted indexes of those fields inside an
 * OLAP scan, never row by row. Rewrites are free to move the expression; CheckAfterRewrite verifies on the final
 * plan that it sits in a scan (filter conjunct or virtual column) and that {@link #bindsOnlyFields()} holds.
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
        // A SEARCH is UNKNOWN wherever its inverted indexes cannot answer the DSL, not only where its fields are
        // NULL: BE returns an all-rows null bitmap for a clause type an index does not implement (a range on a BKD
        // index), an unparseable value, or a missing iterator. A scan writes that bitmap into a virtual column only
        // when this expression is nullable (segment_iterator.cpp, _output_index_result_column), so declaring it
        // non-nullable over NOT NULL fields would turn UNKNOWN into FALSE and make NOT search(...) true everywhere.
        return true;
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
        // Rewrites may replace a field with NULL: null-rejection inference does so on a temporary copy, and
        // NULL padding of an outer join side (e.g. ON false, outer-to-anti join) does so in the plan.
        // Such a SEARCH no longer binds an index field; CheckAfterRewrite rejects it via bindsOnlyFields.
        for (Expression child : children) {
            if (!(child instanceof SlotReference || child instanceof ElementAt
                    || child instanceof NullLiteral)) {
                throw new IllegalArgumentException(
                        "SEARCH field binding must be a slot, subcolumn, or NULL, found "
                                + child.getClass().getSimpleName());
            }
        }
        return new SearchExpression(dslString, qsPlan, children);
    }

    /**
     * Whether every child still binds an index field: a slot, or a variant subcolumn of a slot.
     */
    public boolean bindsOnlyFields() {
        return children().stream().allMatch(SearchExpression::isFieldBinding);
    }

    private static boolean isFieldBinding(Expression expression) {
        Expression current = expression;
        while (current instanceof ElementAt) {
            current = current.child(0);
        }
        return current instanceof SlotReference;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitSearchExpression(this, context);
    }

    @Override
    public String computeToSql() {
        return "search(" + new StringLiteral(dslString).toSql() + ")";
    }

    @Override
    public String toString() {
        return computeToSql();
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
