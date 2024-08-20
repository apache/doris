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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToArgumentType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'array_reverse_split'.
 */
public class ArrayReverseSplit extends ScalarFunction implements PropagateNullable, HighOrderFunction {
    // arg0 = Array<T>, return_value = Array<Array<T>>
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(ArrayType.of(new FollowToArgumentType(0))).args(
                    ArrayType.of(AnyDataType.INSTANCE_WITHOUT_INDEX),
                    ArrayType.of(BooleanType.INSTANCE)));

    /**
     * constructor with arguments.
     */
    public ArrayReverseSplit(Expression arg0, Expression arg1) {
        super("array_reverse_split", arg0, arg1);
    }

    /**
     * constructor with arguments.
     * array_split(lambda, a1, ...) = array_split(a1, array_map(lambda, a1, ...))
     */
    public ArrayReverseSplit(Expression arg) {
        super("array_reverse_split", arg instanceof Lambda ? arg.child(1).child(0) : arg, new ArrayMap(arg));
        if (!(arg instanceof Lambda)) {
            throw new AnalysisException(
                    String.format("The 1st arg of %s must be lambda but is %s", getName(), arg));
        }
    }

    @Override
    public ArrayReverseSplit withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1 || children.size() == 2);
        if (children.size() == 1) {
            return new ArrayReverseSplit(children.get(0));
        } else {
            return new ArrayReverseSplit(children.get(0), children.get(1));
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArrayReverseSplit(this, context);
    }

    @Override
    public List<FunctionSignature> getImplSignature() {
        return SIGNATURES;
    }
}
