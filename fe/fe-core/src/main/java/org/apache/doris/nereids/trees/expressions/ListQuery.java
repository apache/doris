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

import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * Encapsulate LogicalPlan as Expression.
 * just for subquery.
 */
public class ListQuery extends SubqueryExpr implements LeafExpression {
    public ListQuery(LogicalPlan subquery) {
        super(Objects.requireNonNull(subquery, "subquery can not be null"));
    }

    @Override
    public DataType getDataType() {
        Preconditions.checkArgument(queryPlan.getOutput().size() == 1);
        return queryPlan.getOutput().get(0).getDataType();
    }

    @Override
    public String toSql() {
        return " (LISTQUERY) " + super.toSql();
    }

    @Override
    public String toString() {
        return " (LISTQUERY) " + super.toString();
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitListQuery(this, context);
    }
}
