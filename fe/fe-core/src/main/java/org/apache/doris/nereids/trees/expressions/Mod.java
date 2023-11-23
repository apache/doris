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
import org.apache.doris.nereids.types.DecimalV3Type;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Mod Expression.
 */
public class Mod extends BinaryArithmetic implements AlwaysNullable {

    public Mod(Expression left, Expression right) {
        super(ImmutableList.of(left, right), Operator.MOD);
    }

    private Mod(List<Expression> children) {
        super(children, Operator.MOD);
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new Mod(children);
    }

    @Override
    public DecimalV3Type getDataTypeForDecimalV3(DecimalV3Type t1, DecimalV3Type t2) {
        int targetScale = Math.max(t1.getScale(), t2.getScale());
        int integralPart = Math.max(t1.getRange(), t2.getRange());
        return processDecimalV3OverFlow(integralPart, targetScale, integralPart);
    }

    @Override
    public boolean nullable() throws UnboundException {
        return true;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMod(this, context);
    }
}
