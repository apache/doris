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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;

/**
 * Follow legacy planner cast agg_state combinator's children if we need cast it to another agg_state type when insert
 */
public class ConvertAggStateCast extends AbstractExpressionRewriteRule {

    public static ConvertAggStateCast INSTANCE = new ConvertAggStateCast();

    @Override
    public Expression visitCast(Cast cast, ExpressionRewriteContext context) {
        Expression child = cast.child();
        DataType originalType = child.getDataType();
        DataType targetType = cast.getDataType();
        if (originalType instanceof AggStateType
                && targetType instanceof AggStateType
                && child instanceof StateCombinator) {
            AggStateType original = (AggStateType) originalType;
            AggStateType target = (AggStateType) targetType;
            if (original.getSubTypes().size() != target.getSubTypes().size()) {
                return processCastChild(cast, context);
            }
            if (!original.getFunctionName().equalsIgnoreCase(target.getFunctionName())) {
                return processCastChild(cast, context);
            }
            ImmutableList.Builder<Expression> newChildren = ImmutableList.builderWithExpectedSize(child.arity());
            for (int i = 0; i < child.arity(); i++) {
                Expression newChild = TypeCoercionUtils.castIfNotSameType(child.child(i), target.getSubTypes().get(i));
                if (newChild.nullable() != target.getSubTypeNullables().get(i)) {
                    if (newChild.nullable()) {
                        newChild = new NonNullable(newChild);
                    } else {
                        newChild = new Nullable(newChild);
                    }
                }
                newChildren.add(newChild);
            }
            child = child.withChildren(newChildren.build());
            return processCastChild(cast.withChildren(ImmutableList.of(child)), context);
        }
        return processCastChild(cast, context);
    }

    private Expression processCastChild(Cast cast, ExpressionRewriteContext context) {
        Expression child = visit(cast.child(), context);
        if (child != cast.child()) {
            cast = cast.withChildren(ImmutableList.of(child));
        }
        return cast;
    }
}
