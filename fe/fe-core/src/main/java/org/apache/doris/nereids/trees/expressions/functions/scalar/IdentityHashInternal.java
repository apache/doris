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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ComputePrecision;
import org.apache.doris.nereids.trees.expressions.functions.ComputeSignatureHelper;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.List;

/**
 * for debug only, compute identity bucket hash as the same way in
 * `VOlapTablePartitionParam::find_tablets()` for tables whose distribution_hash_type
 * is identity. The trailing argument is the bucket count for the modulus, so the
 * returned value is directly the bucket index.
 */
public class IdentityHashInternal extends ScalarFunction
        implements ExplicitlyCastableSignature, AlwaysNotNullable, ComputePrecision {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BigIntType.INSTANCE).varArgs(AnyDataType.INSTANCE_WITHOUT_INDEX));

    /**
     * constructor with 2 or more arguments: distribution columns plus the bucket count.
     */
    public IdentityHashInternal(Expression arg, Expression... varArgs) {
        super("identity_hash_internal", ExpressionUtils.mergeArguments(arg, varArgs));
    }

    /** constructor for withChildren and reuse signature */
    private IdentityHashInternal(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        checkLegalityAfterRewrite();
    }

    @Override
    public void checkLegalityAfterRewrite() {
        List<Expression> arguments = getArguments();
        if (arguments.size() < 2) {
            throw new AnalysisException(
                    "identity_hash_internal needs at least one distribution column and the bucket count");
        }
        Expression last = arguments.get(arguments.size() - 1);
        if (!(last instanceof IntegerLikeLiteral)) {
            throw new AnalysisException(String.format(
                    "the bucket count argument of %s must be an integer literal, but is %s",
                    getName(), last.toSql()));
        }
        BigDecimal bucketCount = ((IntegerLikeLiteral) last).getBigDecimalValue();
        if (bucketCount.signum() <= 0 || bucketCount.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0) {
            throw new AnalysisException(String.format(
                    "the bucket count argument of %s must be a positive integer, but is %s",
                    getName(), last.toSql()));
        }
        for (int i = 0; i < arguments.size() - 1; i++) {
            DataType type = arguments.get(i).getDataType();
            if (!(type.isNumericType() || type.isBooleanType() || type.isStringLikeType()
                    || type.isVarBinaryType() || type.isDateLikeType() || type.isTimeType()
                    || type.isIPType() || type.isUuidType() || type.isNullType())) {
                throw new AnalysisException(String.format(
                        "unsupported distribution argument type %s for %s", type.toSql(), getName()));
            }
        }
    }

    /**
     * withChildren.
     */
    @Override
    public IdentityHashInternal withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 2,
                "identity_hash_internal needs at least one distribution column and the bucket count");
        return new IdentityHashInternal(getFunctionParams(children));
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
        return visitor.visitIdentityHashInternal(this, context);
    }

    /**
     * Override computeSignature to skip legacy date type conversion, mirroring Crc32Internal:
     * the distribution columns must keep their original DateTime/Date encodings.
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
