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
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.MoreFieldsThread;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

/**
 * ScalarFunction 'lcm'.
 */
public class Lcm extends ScalarFunction
        implements BinaryExpression, ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(SmallIntType.INSTANCE).args(TinyIntType.INSTANCE, TinyIntType.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE).args(SmallIntType.INSTANCE, SmallIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(IntegerType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(LargeIntType.INSTANCE).args(BigIntType.INSTANCE, BigIntType.INSTANCE));

    /**
     * constructor with 2 arguments.
     */
    public Lcm(Expression arg0, Expression arg1) {
        super("lcm", arg0, arg1);
    }

    /** constructor for withChildren and reuse signature */
    private Lcm(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public FunctionSignature computeSignature(FunctionSignature signature) {
        signature = super.computeSignature(signature);
        // match tinyint here, promote to smallint. and so on.
        DataType targetArgType = signature.getArgType(0);

        if (!targetArgType.acceptsType(signature.getArgType(1))) {
            throw new RuntimeException(String.format("Lcm: arg1 != arg2, arg1 is %s, arg2 is %s",
                    targetArgType.toSql(), signature.getArgType(1).toSql()));
        }

        // promotion() dont cast bigint to largeint, so a special judgment is required.
        if (targetArgType.isBigIntType()) {
            targetArgType = LargeIntType.INSTANCE;
        } else {
            targetArgType = targetArgType.promotion();
        }
        return signature.withArgumentTypes(false, Arrays.asList(targetArgType, targetArgType));
    }

    /**
     * withChildren.
     */
    @Override
    public Lcm withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return MoreFieldsThread.keepFunctionSignature(() -> new Lcm(getFunctionParams(children)));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitLcm(this, context);
    }
}
