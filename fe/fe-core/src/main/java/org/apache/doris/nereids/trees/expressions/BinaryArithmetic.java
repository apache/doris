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
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

/**
 * binary arithmetic operator. Such as +, -, *, /.
 */
public abstract class BinaryArithmetic extends BinaryOperator implements PropagateNullable {

    private final Operator legacyOperator;

    public BinaryArithmetic(Expression left, Expression right, Operator legacyOperator) {
        super(left, right, legacyOperator.toString());
        this.legacyOperator = legacyOperator;
    }

    public Operator getLegacyOperator() {
        return legacyOperator;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return left().getDataType();
    }

    @Override
    public boolean nullable() throws UnboundException {
        return child(0).nullable() || child(1).nullable();
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitBinaryArithmetic(this, context);
    }
}
