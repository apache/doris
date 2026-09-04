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
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** State combinator for datasketches_hll_union_agg. */
public class DataSketchesHllUnionAggState extends StateCombinator {

    /** Constructor with one argument. */
    public DataSketchesHllUnionAggState(Expression arg) {
        this(ImmutableList.of(arg));
    }

    /** Constructor with two arguments. */
    public DataSketchesHllUnionAggState(Expression arg0, Expression arg1) {
        this(ImmutableList.of(arg0, arg1));
    }

    /** Constructor with one argument and a DISTINCT marker. */
    public DataSketchesHllUnionAggState(boolean distinct, Expression arg) {
        this(arg);
    }

    /** Constructor with two arguments and a DISTINCT marker. */
    public DataSketchesHllUnionAggState(boolean distinct, Expression arg0, Expression arg1) {
        this(arg0, arg1);
    }

    private DataSketchesHllUnionAggState(List<Expression> arguments) {
        super(arguments, createNested(arguments));
    }

    private static DataSketchesHllUnionAgg createNested(List<Expression> arguments) {
        Preconditions.checkArgument(arguments.size() == 1 || arguments.size() == 2);
        return arguments.size() == 1
                ? new DataSketchesHllUnionAgg(arguments.get(0))
                : new DataSketchesHllUnionAgg(arguments.get(0), arguments.get(1));
    }

    @Override
    public int arity() {
        // StateCombinator inherits UnaryExpression, but this state also supports lg_max_k.
        return children().size();
    }

    @Override
    public DataSketchesHllUnionAggState withChildren(List<Expression> children) {
        return new DataSketchesHllUnionAggState(children);
    }

    @Override
    public void checkLegalityAfterRewrite() {
        getNestedFunction().checkLegalityAfterRewrite();
    }
}
