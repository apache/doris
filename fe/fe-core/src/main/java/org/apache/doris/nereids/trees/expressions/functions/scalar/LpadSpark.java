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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'lpad_spark'.
 * Differences from Doris default lpad:
 * - supports 2-arg form; default lpad only supports 3 args.
 * - when len < 0, returns empty string instead of NULL.
 * - when pad == "" and input length < len, returns original input instead of empty string.
 * <p>
 * For VARBINARY, semantics follow Spark {@code ByteArray.lpad}: byte-granular padding;
 * 2-arg form uses a single {@code 0x00} pad byte (Spark default). When pad is empty,
 * the result is the first {@code min(len, input_bytes)} bytes of the input.
 */
public class LpadSpark extends ScalarFunction
        implements ExplicitlyCastableSignature, AlwaysNullable, PropagateNullLiteral {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.<FunctionSignature>of(
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(VarcharType.SYSTEM_DEFAULT, IntegerType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(StringType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(VarcharType.SYSTEM_DEFAULT, IntegerType.INSTANCE, VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(StringType.INSTANCE, IntegerType.INSTANCE, StringType.INSTANCE),
            FunctionSignature.ret(VarBinaryType.INSTANCE)
                    .args(VarBinaryType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(VarBinaryType.INSTANCE)
                    .args(VarBinaryType.INSTANCE, IntegerType.INSTANCE, VarBinaryType.INSTANCE)
    );

    /**
     * constructor with 2 arguments.
     */
    public LpadSpark(Expression arg0, Expression arg1) {
        super("lpad_spark", arg0, arg1);
    }

    /**
     * constructor with 3 arguments.
     */
    public LpadSpark(Expression arg0, Expression arg1, Expression arg2) {
        super("lpad_spark", arg0, arg1, arg2);
    }

    /** constructor for withChildren and reuse signature */
    private LpadSpark(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withChildren.
     */
    @Override
    public LpadSpark withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2 || children.size() == 3);
        return new LpadSpark(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitLpadSpark(this, context);
    }
}
