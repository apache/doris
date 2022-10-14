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
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.typecoercion.ExpectsInputTypes;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * Not expression: not a.
 */
public class Not extends Expression implements UnaryExpression, ExpectsInputTypes, PropagateNullable {

    public static final List<AbstractDataType> EXPECTS_INPUT_TYPES = ImmutableList.of(BooleanType.INSTANCE);

    public Not(Expression child) {
        super(child);
    }

    @Override
    public boolean nullable() throws UnboundException {
        return child().nullable();
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return child().getDataType();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitNot(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Not other = (Not) o;
        return Objects.equals(child(), other.child());
    }

    @Override
    public int hashCode() {
        return child().hashCode();
    }

    @Override
    public String toString() {
        return "( not " + child().toString() + ")";
    }

    @Override
    public String toSql() {
        return "( not " + child().toSql() + ")";
    }

    @Override
    public Not withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Not(children.get(0));
    }

    @Override
    public List<AbstractDataType> expectedInputTypes() {
        return EXPECTS_INPUT_TYPES;
    }
}
