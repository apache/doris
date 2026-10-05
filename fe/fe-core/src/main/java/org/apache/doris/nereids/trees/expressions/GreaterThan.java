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
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Greater than expression: a > b.
 */
public class GreaterThan extends ComparisonPredicate implements PropagateNullable {
    /**
     * Constructor of Greater Than ComparisonPredicate.
     *
     * @param left left child of greater than
     * @param right right child of greater than
     */
    public GreaterThan(Expression left, Expression right) {
        this(left, right, false);
    }

    public GreaterThan(Expression left, Expression right, boolean inferred) {
        super(ImmutableList.of(left, right), ">", inferred);
    }

    private GreaterThan(List<Expression> children) {
        this(children, false);
    }

    private GreaterThan(List<Expression> children, boolean inferred) {
        super(children, ">", inferred);
    }

    @Override
    public boolean nullable() throws UnboundException {
        return left().nullable() || right().nullable();
    }

    @Override
    public String toString() {
        return "(" + left() + " > " + right() + ")";
    }

    @Override
    public GreaterThan withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new GreaterThan(children, this.isInferred());
    }

    @Override
    public Expression withInferred(boolean inferred) {
        return new GreaterThan(this.children, inferred);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitGreaterThan(this, context);
    }

    @Override
    public ComparisonPredicate commute() {
        return new LessThan(right(), left());
    }
}
