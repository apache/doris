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
 * topk_min_count(x, k): the k smallest distinct non-NULL values of x with their row counts.
 * See {@link TopKCountFunction} for the returned array and the truncation encoding.
 */
public class TopKMinCount extends TopKCountFunction {

    public TopKMinCount(Expression arg0, Expression arg1) {
        this(false, arg0, arg1);
    }

    public TopKMinCount(boolean distinct, Expression arg0, Expression arg1) {
        super("topk_min_count", distinct, Arrays.asList(arg0, arg1));
    }

    /** constructor for withChildren and reuse signature */
    private TopKMinCount(AggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public TopKMinCount withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new TopKMinCount(getFunctionParams(distinct, children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTopKMinCount(this, context);
    }
}
