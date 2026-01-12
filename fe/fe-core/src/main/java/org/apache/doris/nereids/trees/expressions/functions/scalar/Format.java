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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Format function */
public class Format extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, PropagateNullable {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(StringType.INSTANCE).varArgs(StringType.INSTANCE, TinyIntType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE).varArgs(StringType.INSTANCE, SmallIntType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE).varArgs(StringType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE).varArgs(StringType.INSTANCE, BigIntType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE).varArgs(StringType.INSTANCE, LargeIntType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE).varArgs(StringType.INSTANCE, FloatType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE).varArgs(StringType.INSTANCE, DoubleType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE).varArgs(StringType.INSTANCE, StringType.INSTANCE));

    /**
     * constructor with 2 or more arguments.
     */
    public Format(Expression arg0, Expression arg1, Expression... varArgs) {
        super("format", ExpressionUtils.mergeArguments(arg0, arg1, varArgs));
    }

    /**
     * withChildren.
     */
    @Override
    public Format withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 2);
        return new Format(children.get(0), children.get(1),
                children.subList(2, children.size()).toArray(new Expression[0]));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitFormat(this, context);
    }
}
