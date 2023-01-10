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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.FunctionSignature.TripleFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.FollowToArgumentType;
import org.apache.doris.nereids.util.ResponsibilityChain;

import java.util.List;
import java.util.function.BiFunction;

/** ComputeSignatureHelper */
public class ComputeSignatureHelper {
    /** implementAbstractReturnType */
    public static FunctionSignature implementAbstractReturnType(
            FunctionSignature signature, List<Expression> arguments) {
        if (!(signature.returnType instanceof DataType)) {
            if (signature.returnType instanceof FollowToArgumentType) {
                int argumentIndex = ((FollowToArgumentType) signature.returnType).argumentIndex;
                return signature.withReturnType(arguments.get(argumentIndex).getDataType());
            }
            throw new AnalysisException("Not implemented abstract return type: " + signature.returnType);
        }
        return signature;
    }

    /** upgradeDateOrDateTimeToV2 */
    public static FunctionSignature upgradeDateOrDateTimeToV2(
            FunctionSignature signature, List<Expression> arguments) {
        Type type = signature.returnType.toCatalogDataType();
        if ((type.isDate() || type.isDatetime()) && Config.enable_date_conversion) {
            Type legacyReturnType = ScalarType.getDefaultDateType(type);
            signature = signature.withReturnType(DataType.fromCatalogType(legacyReturnType));
        }
        return signature;
    }

    /** upgradeDecimalV2ToV3 */
    public static FunctionSignature upgradeDecimalV2ToV3(
            FunctionSignature signature, List<Expression> arguments) {
        AbstractDataType returnType = signature.returnType;
        Type type = returnType.toCatalogDataType();
        if ((type.isDate() || type.isDatetime()) && Config.enable_date_conversion) {
            Type legacyReturnType = ScalarType.getDefaultDateType(returnType.toCatalogDataType());
            signature = signature.withReturnType(DataType.fromCatalogType(legacyReturnType));
        }
        return signature;
    }

    public static FunctionSignature normalizeDecimalV2(
            FunctionSignature signature, List<Expression> arguments) {
        if ((signature.returnType instanceof DecimalV2Type && signature.returnType != DecimalV2Type.SYSTEM_DEFAULT)) {
            return signature.withReturnType(DecimalV2Type.SYSTEM_DEFAULT);
        }
        return signature;
    }

    /** computePrecision */
    public static FunctionSignature computePrecision(
            ComputeSignature computeSignature, FunctionSignature signature, List<Expression> arguments) {
        if (!(signature.returnType instanceof DateType)) {
            return signature;
        }
        if (computeSignature instanceof ComputePrecision) {
            return ((ComputePrecision) computeSignature).computePrecision(signature);
        } else {
            DataType returnType = (DataType) signature.returnType;
            if (returnType.isDecimalV3Type()
                    || (returnType.isDateTimeV2Type() && !(computeSignature instanceof DateTimeWithPrecision))) {
                if (!arguments.isEmpty() && arguments.get(0).getDataType().isDecimalV3Type()
                        && returnType.isDecimalV3Type()) {
                    return signature.withReturnType(arguments.get(0).getDataType());
                } else if (!arguments.isEmpty() && arguments.get(0).getDataType().isDateTimeV2Type()
                        && returnType.isDateTimeV2Type()) {
                    return signature.withReturnType(arguments.get(0).getDataType());
                }
            }
        }
        return signature;
    }

    /** dynamicComputePropertiesOfArray */
    public static FunctionSignature dynamicComputePropertiesOfArray(
            FunctionSignature signature, List<Expression> arguments) {
        if (!(signature.returnType instanceof ArrayType)) {
            return signature;
        }

        // fill item type by the type of first item
        ArrayType arrayType = (ArrayType) signature.returnType;

        // Now Array type do not support ARRAY<NOT_NULL>, set it to true temporarily
        boolean containsNull = true;

        // fill containsNull if any array argument contains null
        /* boolean containsNull = arguments
                .stream()
                .map(Expression::getDataType)
                .filter(argType -> argType instanceof ArrayType)
                .map(ArrayType.class::cast)
                .anyMatch(ArrayType::containsNull);*/
        return signature.withReturnType(
                ArrayType.of(arrayType.getItemType(), arrayType.containsNull() || containsNull));
    }

    static class ComputeSignatureChain {
        private ResponsibilityChain<SignatureContext> computeChain;

        public ComputeSignatureChain(
                ResponsibilityChain<SignatureContext> computeChain) {
            this.computeChain = computeChain;
        }

        public static ComputeSignatureChain from(
                ComputeSignature computeSignature, FunctionSignature signature, List<Expression> arguments) {
            return new ComputeSignatureChain(ResponsibilityChain.from(
                    new SignatureContext(computeSignature, signature, arguments)));
        }

        public ComputeSignatureChain then(
                BiFunction<FunctionSignature, List<Expression>, FunctionSignature> computeFunction) {
            computeChain.then(ctx -> new SignatureContext(ctx.computeSignature,
                    computeFunction.apply(ctx.signature, ctx.arguments), ctx.arguments));
            return this;
        }

        public ComputeSignatureChain then(
                TripleFunction<ComputeSignature, FunctionSignature, List<Expression>,
                        FunctionSignature> computeFunction) {
            computeChain.then(ctx -> new SignatureContext(ctx.computeSignature,
                    computeFunction.apply(ctx.computeSignature, ctx.signature, ctx.arguments), ctx.arguments));
            return this;
        }

        public FunctionSignature get() {
            return computeChain.get().signature;
        }
    }

    static class SignatureContext {
        ComputeSignature computeSignature;
        FunctionSignature signature;
        List<Expression> arguments;

        public SignatureContext(
                ComputeSignature computeSignature, FunctionSignature signature, List<Expression> arguments) {
            this.computeSignature = computeSignature;
            this.signature = signature;
            this.arguments = arguments;
        }
    }
}
