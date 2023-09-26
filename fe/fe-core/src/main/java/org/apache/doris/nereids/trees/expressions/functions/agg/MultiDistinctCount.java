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
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** MultiDistinctCount */
public class MultiDistinctCount extends AggregateFunction
        implements AlwaysNotNullable, ExplicitlyCastableSignature, MultiDistinction {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BigIntType.INSTANCE).varArgs(AnyDataType.INSTANCE_WITHOUT_INDEX)
    );

    // MultiDistinctCount is created in AggregateStrategies phase
    // can't change getSignatures to use type coercion rule to add a cast expr
    // because AggregateStrategies phase is after type coercion
    public MultiDistinctCount(Expression arg0, Expression... varArgs) {
        super("multi_distinct_count", true, ExpressionUtils.mergeArguments(arg0, varArgs).stream()
                .map(arg -> arg.getDataType() instanceof DateLikeType ? new Cast(arg, BigIntType.INSTANCE) : arg)
                .collect(ImmutableList.toImmutableList()));
    }

    public MultiDistinctCount(boolean distinct, Expression arg0, Expression... varArgs) {
        super("multi_distinct_count", distinct, ExpressionUtils.mergeArguments(arg0, varArgs).stream()
                .map(arg -> arg.getDataType() instanceof DateLikeType ? new Cast(arg, BigIntType.INSTANCE) : arg)
                .collect(ImmutableList.toImmutableList()));
    }

    @Override
    public MultiDistinctCount withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() > 0);
        if (children.size() > 1) {
            return new MultiDistinctCount(distinct, children.get(0),
                    children.subList(1, children.size()).toArray(new Expression[0]));
        } else {
            return new MultiDistinctCount(distinct, children.get(0));
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMultiDistinctCount(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
