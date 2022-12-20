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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Preconditions;

import java.util.List;

/** MultiDistinctSum */
public class MultiDistinctSum extends AggregateFunction
        implements UnaryExpression, AlwaysNotNullable, ExplicitlyCastableSignature {
    public MultiDistinctSum(Expression arg0) {
        super("multi_distinct_sum", true, arg0);
    }

    public MultiDistinctSum(boolean isDistinct, Expression arg0) {
        super("multi_distinct_sum", true, arg0);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return new Sum(getArgument(0)).getSignatures();
    }

    @Override
    public MultiDistinctSum withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new MultiDistinctSum(children.get(0));
    }

    @Override
    public MultiDistinctSum withDistinctAndChildren(boolean isDistinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new MultiDistinctSum(isDistinct, children.get(0));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMultiDistinctSum(this, context);
    }
}
