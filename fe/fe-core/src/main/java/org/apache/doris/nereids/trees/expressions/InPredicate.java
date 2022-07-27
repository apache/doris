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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * In predicate expression.
 */
public class InPredicate extends Expression {

    private Expression compareExpr;
    private List<Expression> optionsList;

    public InPredicate(Expression compareExpr, List<Expression> optionsList) {
        super(new Builder<Expression>().add(compareExpr).addAll(optionsList).build().toArray(new Expression[0]));
        this.compareExpr = compareExpr;
        this.optionsList = ImmutableList.copyOf(Objects.requireNonNull(optionsList, "In list cannot be null"));
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitInPredicate(this, context);
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public boolean nullable() throws UnboundException {
        return optionsList.stream().map(Expression::nullable)
            .reduce((a, b) -> a || b).get();
    }

    @Override
    public String toString() {
        return compareExpr + " IN " + optionsList.stream()
            .map(Expression::toString)
            .collect(Collectors.joining(",", "(", ")"));
    }

    @Override
    public String toSql() {
        return compareExpr.toSql() + " IN " + optionsList.stream()
            .map(Expression::toSql)
            .collect(Collectors.joining(",", "(", ")"));
    }

    public Expression getCompareExpr() {
        return compareExpr;
    }

    public List<Expression> getOptionsList() {
        return optionsList;
    }
}
