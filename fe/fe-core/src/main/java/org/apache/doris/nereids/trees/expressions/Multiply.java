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

import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Multiply Expression.
 */
public class Multiply<LEFT_CHILD_TYPE extends Expression, RIGHT_CHILD_TYPE extends Expression>
        extends Arithmetic implements BinaryExpression<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {

    public Multiply(LEFT_CHILD_TYPE left, RIGHT_CHILD_TYPE right) {
        super(ArithmeticOperator.MULTIPLY, left, right);
    }

    @Override
    public String toSql() {
        return left().toSql() + ' ' + getArithmeticOperator().toString()
                + ' ' + right().toSql();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMultiply(this, context);
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new Multiply<>(children.get(0), children.get(1));
    }
}
