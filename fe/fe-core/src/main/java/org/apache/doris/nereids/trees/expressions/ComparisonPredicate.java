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
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import java.util.Objects;

/**
 * Comparison predicate expression.
 * Such as: "=", "<", "<=", ">", ">=", "<=>"
 */
public abstract class ComparisonPredicate<LEFT_CHILD_TYPE extends Expression, RIGHT_CHILD_TYPE extends Expression>
        extends Expression implements BinaryExpression<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {
    /**
     * Constructor of ComparisonPredicate.
     *
     * @param nodeType node type of expression
     * @param left     left child of comparison predicate
     * @param right    right child of comparison predicate
     */
    public ComparisonPredicate(NodeType nodeType, LEFT_CHILD_TYPE left, RIGHT_CHILD_TYPE right) {
        super(nodeType, left, right);
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public String sql() {
        String nodeType = getType().toString();
        return left().sql() + ' ' + nodeType + ' ' + right().sql();
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitComparisonPredicate(this, context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, left(), right());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ComparisonPredicate other = (ComparisonPredicate) o;
        return (type == other.getType()) && Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }
}
