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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;
import java.util.Objects;

/**
 * In Predicate Expression.
 */
public class InPredicate extends Expression {
    private Expression compareExpression;
    private List<Expression> inExpressions;

    public InPredicate(Expression compareExpression, List<Expression> inExpressions) {
        super(NodeType.IN,  (new Builder<Expression>().add(compareExpression).addAll(inExpressions).build().toArray(new Expression[0])));
        this.compareExpression = compareExpression;
        this.inExpressions = ImmutableList.copyOf(Objects.requireNonNull(inExpressions, "in list can not be null"));
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public String sql() {
        String sql = "";
        for (Expression expression : inExpressions) {
            sql += expression.sql();
            sql += ", ";
        }
        return compareExpression.sql() + " IN " + sql;
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitInPredicate(this, context);
    }
}
