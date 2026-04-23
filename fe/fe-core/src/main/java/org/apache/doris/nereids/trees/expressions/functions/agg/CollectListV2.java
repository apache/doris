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
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * AggregateFunction 'collect_list_v2'. This is the v2 version of collect_list, which uses
 * binary serialization for complex types instead of JSON text, improving performance.
 */
public class CollectListV2 extends NotNullableAggregateFunction
        implements ExplicitlyCastableSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(ArrayType.of(new FollowToAnyDataType(0))).args(new AnyDataType(0)),
            FunctionSignature.ret(ArrayType.of(new FollowToAnyDataType(0)))
                    .args(new AnyDataType(0), IntegerType.INSTANCE)
    );

    /**
     * constructor with 1 argument.
     */
    public CollectListV2(Expression arg) {
        super("collect_list_v2", arg);
    }

    /**
     * constructor with 2 argument.
     */
    public CollectListV2(Expression arg0, Expression arg1) {
        super("collect_list_v2", arg0, arg1);
    }

    /**
     * constructor with 1 argument.
     */
    public CollectListV2(boolean distinct, Expression arg) {
        super("collect_list_v2", distinct, arg);
    }

    /**
     * constructor with 2 argument.
     */
    public CollectListV2(boolean distinct, Expression arg0, Expression arg1) {
        super("collect_list_v2", distinct, arg0, arg1);
    }

    /** constructor for withChildren and reuse signature */
    private CollectListV2(AggregateFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withDistinctAndChildren.
     */
    @Override
    public CollectListV2 withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1 || children.size() == 2);
        return new CollectListV2(getFunctionParams(distinct, children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public Expression resultForEmptyInput() {
        return new ArrayLiteral(new ArrayList<>(), this.getDataType());
    }
}
