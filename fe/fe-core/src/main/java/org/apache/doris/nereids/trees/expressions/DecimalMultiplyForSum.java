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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DecimalV3Type;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Restore SUM's join multiplicity without reducing its decimal scale. Unlike ordinary
 * multiplication, multiplying a partial sum by an integer count must preserve SUM's type.
 * Keep this separate from Multiply so arithmetic simplification cannot rebuild it with
 * the ordinary decimal overflow-scale rules. BE executes the existing multiply function.
 */
public class DecimalMultiplyForSum extends BinaryArithmetic {
    public DecimalMultiplyForSum(Expression sum, Expression count) {
        super(ImmutableList.of(sum, count), Operator.MULTIPLY);
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new DecimalMultiplyForSum(children.get(0), children.get(1));
    }

    @Override
    public DecimalV3Type getDataTypeForDecimalV3(DecimalV3Type sumType, DecimalV3Type countType) {
        Preconditions.checkArgument(countType.getScale() == 0);
        return sumType;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitBinaryArithmetic(this, context);
    }
}
