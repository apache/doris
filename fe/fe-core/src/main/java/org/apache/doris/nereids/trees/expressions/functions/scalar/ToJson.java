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
import org.apache.doris.nereids.trees.expressions.functions.NullOrIdenticalSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * to_json convert type to json
 */
public class ToJson extends ScalarFunction
        implements UnaryExpression, NullOrIdenticalSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(JsonType.INSTANCE).args(TinyIntType.INSTANCE),
            FunctionSignature.ret(JsonType.INSTANCE).args(SmallIntType.INSTANCE),
            FunctionSignature.ret(JsonType.INSTANCE).args(IntegerType.INSTANCE),
            FunctionSignature.ret(JsonType.INSTANCE).args(BigIntType.INSTANCE),
            FunctionSignature.ret(JsonType.INSTANCE).args(LargeIntType.INSTANCE),
            FunctionSignature.ret(JsonType.INSTANCE).args(BooleanType.INSTANCE),
            FunctionSignature.ret(JsonType.INSTANCE).args(FloatType.INSTANCE),
            FunctionSignature.ret(JsonType.INSTANCE).args(DoubleType.INSTANCE),
            FunctionSignature.ret(JsonType.INSTANCE).args(DecimalV3Type.WILDCARD),
            FunctionSignature.ret(JsonType.INSTANCE).args(StringType.INSTANCE));

    /**
     * constructor with 1 or more arguments.
     */
    public ToJson(Expression arg) {
        super("to_json", arg);
    }

    /** constructor for withChildren and reuse signature */
    private ToJson(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withChildren.
     */
    @Override
    public ToJson withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1, "ToJson should have exactly one argument");
        return new ToJson(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        DataType firstChildType = child(0).getDataType();
        if (firstChildType.isStructType() || firstChildType.isArrayType()) {
            return ImmutableList.of(FunctionSignature.ret(JsonType.INSTANCE).args(firstChildType));
        } else {
            return SIGNATURES;
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitToJson(this, context);
    }
}
