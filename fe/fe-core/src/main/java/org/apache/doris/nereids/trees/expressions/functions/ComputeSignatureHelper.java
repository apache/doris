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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.FollowToArgumentType;
import org.apache.doris.nereids.util.ResponsibilityChain;

import com.google.common.base.Preconditions;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

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
        if (!(signature.returnType instanceof DataType)) {
            return signature;
        }
        if (computeSignature instanceof DateTimeWithPrecision) {
            return signature;
        }
        if (computeSignature instanceof ComputePrecision) {
            return ((ComputePrecision) computeSignature).computePrecision(signature);
        }
        if (signature.argumentsTypes.stream().anyMatch(DateTimeV2Type.class::isInstance)) {
            signature = defaultDateTimeV2PrecisionPromotion(signature, arguments);
        }
        if (signature.argumentsTypes.stream().anyMatch(DecimalV3Type.class::isInstance)) {
            // do decimal v3 precision
            signature = defaultDecimalV3PrecisionPromotion(signature, arguments);
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

    private static FunctionSignature defaultDateTimeV2PrecisionPromotion(
            FunctionSignature signature, List<Expression> arguments) {
        DateTimeV2Type finalType = null;
        for (int i = 0; i < arguments.size(); i++) {
            AbstractDataType targetType;
            if (i >= signature.argumentsTypes.size()) {
                Preconditions.checkState(signature.getVarArgType().isPresent(),
                        "argument size larger than signature");
                targetType = signature.getVarArgType().get();
            } else {
                targetType = signature.getArgType(i);
            }
            if (!(targetType instanceof DateTimeV2Type)) {
                continue;
            }
            if (finalType == null) {
                if (arguments.get(i) instanceof StringLikeLiteral) {
                    // We need to determine the scale based on the string literal.
                    StringLikeLiteral str = (StringLikeLiteral) arguments.get(i);
                    finalType = DateTimeV2Type.forTypeFromString(str.getStringValue());
                } else {
                    finalType = DateTimeV2Type.forType(arguments.get(i).getDataType());
                }
            } else {
                finalType = DateTimeV2Type.getWiderDatetimeV2Type(finalType,
                        DateTimeV2Type.forType(arguments.get(i).getDataType()));
            }
        }
        DateTimeV2Type argType = finalType;
        List<AbstractDataType> newArgTypes = signature.argumentsTypes.stream().map(t -> {
            if (t instanceof DateTimeV2Type) {
                return argType;
            } else {
                return t;
            }
        }).collect(Collectors.toList());
        signature = signature.withArgumentTypes(signature.hasVarArgs, newArgTypes);
        if (signature.returnType instanceof DateTimeV2Type) {
            signature = signature.withReturnType(argType);
        }
        return signature;
    }

    private static FunctionSignature defaultDecimalV3PrecisionPromotion(
            FunctionSignature signature, List<Expression> arguments) {
        DataType finalType = null;
        for (int i = 0; i < arguments.size(); i++) {
            AbstractDataType targetType;
            if (i >= signature.argumentsTypes.size()) {
                Preconditions.checkState(signature.getVarArgType().isPresent(),
                        "argument size larger than signature");
                targetType = signature.getVarArgType().get();
            } else {
                targetType = signature.getArgType(i);
            }
            if (!(targetType instanceof DecimalV3Type)) {
                continue;
            }
            if (finalType == null) {
                finalType = DecimalV3Type.forType(arguments.get(i).getDataType());
            } else {
                Expression arg = arguments.get(i);
                DecimalV3Type argType;
                if (arg.isLiteral() && arg.getDataType().isIntegralType()) {
                    // create decimalV3 with minimum scale enough to hold the integral literal
                    argType = DecimalV3Type.createDecimalV3Type(new BigDecimal(((Literal) arg).getStringValue()));
                } else {
                    argType = DecimalV3Type.forType(arg.getDataType());
                }
                finalType = DecimalV3Type.widerDecimalV3Type((DecimalV3Type) finalType, argType, true);
            }
            Preconditions.checkState(finalType.isDecimalV3Type(),
                    "decimalv3 precision promotion failed.");
        }
        DataType argType = finalType;
        List<AbstractDataType> newArgTypes = signature.argumentsTypes.stream().map(t -> {
            if (t instanceof DecimalV3Type) {
                return argType;
            } else {
                return t;
            }
        }).collect(Collectors.toList());
        signature = signature.withArgumentTypes(signature.hasVarArgs, newArgTypes);
        if (signature.returnType instanceof DecimalV3Type) {
            signature = signature.withReturnType(argType);
        }
        return signature;
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
