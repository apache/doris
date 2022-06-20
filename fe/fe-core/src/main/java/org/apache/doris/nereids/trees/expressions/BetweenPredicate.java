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
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

/**
 * Between predicate expression.
 */
public class BetweenPredicate extends Expression {

    private Expression compareExpr;
    private Expression lowerBound;
    private Expression upperBound;
    /**
     * Constructor of ComparisonPredicate.
     *
     * @param compareExpr    compare of expression
     * @param lowerBound     left child of between predicate
     * @param upperBound     right child of between predicate
     */

    public BetweenPredicate(Expression compareExpr, Expression lowerBound,
            Expression upperBound) {
        super(NodeType.BETWEEN, compareExpr, lowerBound, upperBound);
        this.compareExpr = compareExpr;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public String sql() {
        return compareExpr.sql()
                + " BETWEEN "
                + lowerBound.sql() + " AND " + upperBound.sql();
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitBetweenPredicate(this, context);
    }

    public Expression getCompareExpr() {
        return compareExpr;
    }

    public Expression getLowerBound() {
        return  lowerBound;
    }

    public Expression getUpperBound() {
        return upperBound;
    }

}
