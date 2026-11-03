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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;

/**
 * min_topk(x, k): the k smallest distinct non-NULL values of x with their row counts.
 * See {@link TopKFunction} for the returned array and the truncation encoding.
 */
public class MinTopK extends TopKFunction {

    public MinTopK(Expression arg0, Expression arg1) {
        this(false, arg0, arg1);
    }

    public MinTopK(boolean distinct, Expression arg0, Expression arg1) {
        super("min_topk", distinct, Arrays.asList(arg0, arg1));
    }

    /** constructor for withChildren and reuse signature */
    private MinTopK(AggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public MinTopK withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new MinTopK(getFunctionParams(distinct, children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMinTopK(this, context);
    }
}
