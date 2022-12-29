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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** MultiDistinctCount */
public class ApproxCountDistinct extends AggregateFunction
        implements AlwaysNotNullable, ExplicitlyCastableSignature {
    public ApproxCountDistinct(Expression arg0, Expression... varArgs) {
        super("approx_count_distinct", false, ExpressionUtils.mergeArguments(arg0, varArgs));
    }

    public ApproxCountDistinct(boolean isDistinct, Expression arg0, Expression... varArgs) {
        super("approx_count_distinct", false, ExpressionUtils.mergeArguments(arg0, varArgs));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        List<DataType> argumentsTypes = getArgumentsTypes();
        return ImmutableList.of(FunctionSignature.of(BigIntType.INSTANCE, (List) argumentsTypes));
    }

    @Override
    public ApproxCountDistinct withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() > 0);
        if (children.size() > 1) {
            return new ApproxCountDistinct(children.get(0),
                    children.subList(1, children.size()).toArray(new Expression[0]));
        } else {
            return new ApproxCountDistinct(children.get(0));
        }
    }

    @Override
    public ApproxCountDistinct withDistinctAndChildren(boolean isDistinct, List<Expression> children) {
        if (children.size() > 1) {
            return new ApproxCountDistinct(isDistinct, children.get(0),
                    children.subList(1, children.size()).toArray(new Expression[0]));
        } else {
            return new ApproxCountDistinct(isDistinct, children.get(0));
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitApproxCountDistinct(this, context);
    }
}
