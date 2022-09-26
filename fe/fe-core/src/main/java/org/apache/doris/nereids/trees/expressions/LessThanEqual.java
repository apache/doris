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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Less than and equal expression: a <= b.
 */
public class LessThanEqual extends ComparisonPredicate implements PropagateNullable {
    /**
     * Constructor of Less Than And Equal.
     *
     * @param left  left child of Less Than And Equal
     * @param right right child of Less Than And Equal
     */
    public LessThanEqual(Expression left, Expression right) {
        super(left, right, "<=");
    }

    @Override
    public boolean nullable() throws UnboundException {
        return left().nullable() || right().nullable();
    }

    @Override
    public String toString() {
        return "(" + left() + " <= " + right() + ")";
    }

    @Override
    public LessThanEqual withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LessThanEqual(children.get(0), children.get(1));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitLessThanEqual(this, context);
    }

    @Override
    public ComparisonPredicate commute() {
        return new GreaterThanEqual(right(), left());
    }
}
