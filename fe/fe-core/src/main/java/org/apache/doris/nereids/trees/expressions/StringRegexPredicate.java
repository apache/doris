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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import java.util.Objects;

/**
 * string regex expression.
 * Such as: like, regexp
 */
public abstract class StringRegexPredicate<LEFT_CHILD_TYPE extends Expression, RIGHT_CHILD_TYPE extends Expression>
        extends Expression implements BinaryExpression<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {
    /**
     * Constructor of StringRegexPredicate.
     *
     * @param nodeType node type of expression
     * @param left     left child of string regex
     * @param right    right child of string regex
     */
    public StringRegexPredicate(NodeType nodeType, LEFT_CHILD_TYPE left, RIGHT_CHILD_TYPE right) {
        super(nodeType, left, right);
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public String toSql() {
        String nodeType = getType().toString();
        return left().toSql() + ' ' + nodeType + ' ' + right().toSql();
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitStringRegexPredicate(this, context);
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
        StringRegexPredicate other = (StringRegexPredicate) o;
        return (type == other.getType()) && Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }
}
