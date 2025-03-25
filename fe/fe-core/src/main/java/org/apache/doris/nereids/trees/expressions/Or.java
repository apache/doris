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
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Or predicate expression.
 */
public class Or extends CompoundPredicate {

    /**
     * Desc: Constructor for CompoundPredicate.
     *
     * @param left  left child of comparison predicate
     * @param right right child of comparison predicate
     */
    public Or(Expression left, Expression right) {
        super(ExpressionUtils.mergeList(
                ExpressionUtils.extractDisjunction(left),
                ExpressionUtils.extractDisjunction(right)), "OR");
    }

    public Or(List<Expression> children) {
        super(children, "OR");
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 2);
        return new Or(children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitOr(this, context);
    }

    @Override
    public CompoundPredicate flip() {
        return new And(children);
    }

    @Override
    public CompoundPredicate flip(List<Expression> children) {
        return new And(children);
    }

    @Override
    public Class<? extends CompoundPredicate> flipType() {
        return And.class;
    }

    @Override
    protected List<Expression> extract() {
        return ExpressionUtils.extractDisjunction(this);
    }

    @Override
    public List<Expression> children() {
        if (flattenChildren.isEmpty()) {
            for (Expression child : children) {
                if (child instanceof Or) {
                    flattenChildren.addAll(((Or) child).extract());
                } else {
                    flattenChildren.add(child);
                }
            }
        }
        return flattenChildren;
    }
}
