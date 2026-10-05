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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ComputePrecision;
import org.apache.doris.nereids.trees.expressions.functions.ComputeSignatureHelper;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * for debug only, compute crc32 hash value as the same way in
 * `VOlapTablePartitionParam::find_tablets()`
 */
public class Crc32Internal extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, AlwaysNotNullable, ComputePrecision {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BigIntType.INSTANCE).varArgs(AnyDataType.INSTANCE_WITHOUT_INDEX));

    /**
     * constructor with 1 or more arguments.
     */
    public Crc32Internal(Expression arg, Expression... varArgs) {
        super("crc32_internal", ExpressionUtils.mergeArguments(arg, varArgs));
    }

    /** constructor for withChildren and reuse signature */
    private Crc32Internal(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withChildren.
     */
    @Override
    public Crc32Internal withChildren(List<Expression> children) {
        Preconditions.checkArgument(!children.isEmpty());
        return new Crc32Internal(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public FunctionSignature computePrecision(FunctionSignature signature) {
        return signature;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCrc32Internal(this, context);
    }

    /**
     * Override computeSignature to skip legacy date type conversion.
     * This function needs to preserve the original DateTime/Date types without
     * converting to V2 types.
     */
    @Override
    public FunctionSignature computeSignature(FunctionSignature signature) {
        FunctionSignature sig = signature;
        sig = ComputeSignatureHelper.implementAnyDataTypeWithOutIndexNoLegacyDateUpgrade(sig, getArguments());
        sig = ComputeSignatureHelper.implementAnyDataTypeWithIndexNoLegacyDateUpgrade(sig, getArguments());
        sig = ComputeSignatureHelper.computePrecision(this, sig, getArguments());
        sig = ComputeSignatureHelper.implementFollowToArgumentReturnType(sig, getArguments());
        sig = ComputeSignatureHelper.normalizeDecimalV2(sig, getArguments());
        sig = ComputeSignatureHelper.ensureNestedNullableOfArray(sig, getArguments());
        return sig;
    }

}
