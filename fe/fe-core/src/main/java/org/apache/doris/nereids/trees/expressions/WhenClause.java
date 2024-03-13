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
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.typecoercion.ExpectsInputTypes;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * captures info of a single WHEN expr THEN expr clause.
 */
public class WhenClause extends Expression implements BinaryExpression, ExpectsInputTypes {

    public static final List<DataType> EXPECTS_INPUT_TYPES
            = ImmutableList.of(BooleanType.INSTANCE, AnyDataType.INSTANCE_WITHOUT_INDEX);

    public WhenClause(Expression operand, Expression result) {
        super(ImmutableList.of(operand, result));
    }

    private WhenClause(List<Expression> children) {
        super(children);
    }

    public Expression getOperand() {
        return left();
    }

    public Expression getResult() {
        return right();
    }

    @Override
    public String toSql() {
        return " WHEN " + left().toSql() + " THEN " + right().toSql();
    }

    @Override
    public WhenClause withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new WhenClause(children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitWhenClause(this, context);
    }

    @Override
    public DataType getDataType() {
        // when left() then right()
        // Depends on the data type of the result
        return right().getDataType();
    }

    @Override
    public boolean nullable() throws UnboundException {
        // Depends on whether the result is nullable or not
        return right().nullable();
    }

    @Override
    public List<DataType> expectedInputTypes() {
        return EXPECTS_INPUT_TYPES;
    }

    @Override
    public int hashCode() {
        return Objects.hash(left(), right());
    }

    @Override
    public String toString() {
        return " WHEN " + left().toString() + " THEN " + right().toString();
    }
}
