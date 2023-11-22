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

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * A DIV B
 */
public class IntegralDivide extends BinaryArithmetic implements AlwaysNullable {

    public IntegralDivide(Expression left, Expression right) {
        super(ImmutableList.of(left, right), Operator.INT_DIVIDE);
    }

    private IntegralDivide(List<Expression> children) {
        super(children, Operator.INT_DIVIDE);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitIntegralDivide(this, context);
    }

    // Divide is implemented as a scalar function which return type is always nullable.
    @Override
    public boolean nullable() throws UnboundException {
        return true;
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new IntegralDivide(children);
    }
}
