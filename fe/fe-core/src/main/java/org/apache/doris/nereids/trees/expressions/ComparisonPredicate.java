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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

/**
 * Comparison predicate expression.
 * Such as: "=", "<", "<=", ">", ">=", "<=>"
 */
public abstract class ComparisonPredicate extends BinaryOperator {

    /**
     * Constructor of ComparisonPredicate.
     *
     * @param left     left child of comparison predicate
     * @param right    right child of comparison predicate
     */
    public ComparisonPredicate(Expression left, Expression right, String symbol) {
        super(left, right, symbol);
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitComparisonPredicate(this, context);
    }

    @Override
    public AbstractDataType inputType() {
        return AnyDataType.INSTANCE;
    }

    /**
     * Commute between left and right children.
     */
    public abstract ComparisonPredicate commute();
}
