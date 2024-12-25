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

import org.apache.doris.nereids.analyzer.UnboundVariable.VariableType;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

/**
 * variable for session / global / user variable
 */
public class Variable extends Expression implements LeafExpression {

    private final String name;
    private final VariableType type;
    private final Expression realExpression;

    public Variable(String name, VariableType type, Expression realExpression) {
        this.name = Objects.requireNonNull(name, "name should not be null");
        this.type = Objects.requireNonNull(type, "type should not be null");
        this.realExpression = Objects.requireNonNull(realExpression, "realExpression should not be null");
    }

    public String getName() {
        return name;
    }

    public VariableType getType() {
        return type;
    }

    public Expression getRealExpression() {
        return realExpression;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitVariable(this, context);
    }

    @Override
    public boolean isConstant() {
        return realExpression.isConstant();
    }

    @Override
    public boolean nullable() {
        return realExpression.nullable();
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return realExpression.getDataType();
    }

    @Override
    public String toString() throws UnboundException {
        if (type == VariableType.USER) {
            return "@" + name;
        } else {
            return "@@" + name;
        }
    }

    @Override
    public String computeToSql() throws UnboundException {
        return toString();
    }

    @Override
    public Variable withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.isEmpty());
        return this;
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
        Variable variable = (Variable) o;
        return Objects.equals(name, variable.name) && type == variable.type && Objects.equals(
                realExpression, variable.realExpression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), name, type, realExpression);
    }
}
