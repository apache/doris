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
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToArgumentType;
import org.apache.doris.nereids.util.ResponsibilityChain;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/** ComputeSignatureHelper */
public class ComputeSignatureHelper {

    /** implementAbstractReturnType */
    public static FunctionSignature implementFollowToArgumentReturnType(
            FunctionSignature signature, List<Expression> arguments) {
        if (signature.returnType instanceof FollowToArgumentType) {
            int argumentIndex = ((FollowToArgumentType) signature.returnType).argumentIndex;
            return signature.withReturnType(arguments.get(argumentIndex).getDataType());
        }
        return signature;
    }

    private static DataType replaceAnyDataTypeWithOutIndex(DataType sigType, DataType expressionType) {
        if (expressionType instanceof NullType) {
            if (sigType instanceof ArrayType) {
                return ArrayType.of(replaceAnyDataTypeWithOutIndex(
                        ((ArrayType) sigType).getItemType(), NullType.INSTANCE));
            } else if (sigType instanceof MapType) {
                return MapType.of(replaceAnyDataTypeWithOutIndex(((MapType) sigType).getKeyType(), NullType.INSTANCE),
                        replaceAnyDataTypeWithOutIndex(((MapType) sigType).getValueType(), NullType.INSTANCE));
            } else if (sigType instanceof StructType) {
                // TODO: do not support struct type now
                // throw new AnalysisException("do not support struct type now");
                return sigType;
            } else {
                if (sigType instanceof AnyDataType
                        && ((AnyDataType) sigType).getIndex() == AnyDataType.INDEX_OF_INSTANCE_WITHOUT_INDEX) {
                    return expressionType;
                }
                return sigType;
            }
        } else if (sigType instanceof ArrayType && expressionType instanceof ArrayType) {
            return ArrayType.of(replaceAnyDataTypeWithOutIndex(
                    ((ArrayType) sigType).getItemType(), ((ArrayType) expressionType).getItemType()));
        } else if (sigType instanceof MapType && expressionType instanceof MapType) {
            return MapType.of(replaceAnyDataTypeWithOutIndex(
                            ((MapType) sigType).getKeyType(), ((MapType) expressionType).getKeyType()),
                    replaceAnyDataTypeWithOutIndex(
                            ((MapType) sigType).getValueType(), ((MapType) expressionType).getValueType()));
        } else if (sigType instanceof StructType && expressionType instanceof StructType) {
            // TODO: do not support struct type now
            // throw new AnalysisException("do not support struct type now");
            return sigType;
        } else {
            if (sigType instanceof AnyDataType
                    && ((AnyDataType) sigType).getIndex() == AnyDataType.INDEX_OF_INSTANCE_WITHOUT_INDEX) {
                return expressionType;
            }
            return sigType;
        }
    }

    private static void collectAnyDataType(DataType sigType, DataType expressionType,
            Map<Integer, List<DataType>> indexToArgumentTypes) {
        if (expressionType instanceof NullType) {
            if (sigType instanceof ArrayType) {
                collectAnyDataType(((ArrayType) sigType).getItemType(), NullType.INSTANCE, indexToArgumentTypes);
            } else if (sigType instanceof MapType) {
                collectAnyDataType(((MapType) sigType).getKeyType(), NullType.INSTANCE, indexToArgumentTypes);
                collectAnyDataType(((MapType) sigType).getValueType(), NullType.INSTANCE, indexToArgumentTypes);
            } else if (sigType instanceof StructType) {
                // TODO: do not support struct type now
                // throw new AnalysisException("do not support struct type now");
            } else {
                if (sigType instanceof AnyDataType && ((AnyDataType) sigType).getIndex() >= 0) {
                    List<DataType> dataTypes = indexToArgumentTypes.computeIfAbsent(
                            ((AnyDataType) sigType).getIndex(), i -> Lists.newArrayList());
                    dataTypes.add(expressionType);
                }
            }
        } else if (sigType instanceof ArrayType && expressionType instanceof ArrayType) {
            collectAnyDataType(((ArrayType) sigType).getItemType(),
                    ((ArrayType) expressionType).getItemType(), indexToArgumentTypes);
        } else if (sigType instanceof MapType && expressionType instanceof MapType) {
            collectAnyDataType(((MapType) sigType).getKeyType(),
                    ((MapType) expressionType).getKeyType(), indexToArgumentTypes);
            collectAnyDataType(((MapType) sigType).getValueType(),
                    ((MapType) expressionType).getValueType(), indexToArgumentTypes);
        } else if (sigType instanceof StructType && expressionType instanceof StructType) {
            // TODO: do not support struct type now
            // throw new AnalysisException("do not support struct type now");
        } else {
            if (sigType instanceof AnyDataType && ((AnyDataType) sigType).getIndex() >= 0) {
                List<DataType> dataTypes = indexToArgumentTypes.computeIfAbsent(
                        ((AnyDataType) sigType).getIndex(), i -> Lists.newArrayList());
                dataTypes.add(expressionType);
            }
        }
    }

    private static void collectFollowToAnyDataType(DataType sigType, DataType expressionType,
            Map<Integer, List<DataType>> indexToArgumentTypes, Set<Integer> allNullTypeIndex) {
        if (expressionType instanceof NullType) {
            if (sigType instanceof ArrayType) {
                collectFollowToAnyDataType(((ArrayType) sigType).getItemType(),
                        NullType.INSTANCE, indexToArgumentTypes, allNullTypeIndex);
            } else if (sigType instanceof MapType) {
                collectFollowToAnyDataType(((MapType) sigType).getKeyType(),
                        NullType.INSTANCE, indexToArgumentTypes, allNullTypeIndex);
                collectFollowToAnyDataType(((MapType) sigType).getValueType(),
                        NullType.INSTANCE, indexToArgumentTypes, allNullTypeIndex);
            } else if (sigType instanceof StructType) {
                // TODO: do not support struct type now
                // throw new AnalysisException("do not support struct type now");
            } else {
                if (sigType instanceof FollowToAnyDataType
                        && allNullTypeIndex.contains(((FollowToAnyDataType) sigType).getIndex())) {
                    List<DataType> dataTypes = indexToArgumentTypes.computeIfAbsent(
                            ((FollowToAnyDataType) sigType).getIndex(), i -> Lists.newArrayList());
                    dataTypes.add(expressionType);
                }
            }
        } else if (sigType instanceof ArrayType && expressionType instanceof ArrayType) {
            collectFollowToAnyDataType(((ArrayType) sigType).getItemType(),
                    ((ArrayType) expressionType).getItemType(), indexToArgumentTypes, allNullTypeIndex);
        } else if (sigType instanceof MapType && expressionType instanceof MapType) {
            collectFollowToAnyDataType(((MapType) sigType).getKeyType(),
                    ((MapType) expressionType).getKeyType(), indexToArgumentTypes, allNullTypeIndex);
            collectFollowToAnyDataType(((MapType) sigType).getValueType(),
                    ((MapType) expressionType).getValueType(), indexToArgumentTypes, allNullTypeIndex);
        } else if (sigType instanceof StructType && expressionType instanceof StructType) {
            // TODO: do not support struct type now
            // throw new AnalysisException("do not support struct type now");
        } else {
            if (sigType instanceof FollowToAnyDataType
                    && allNullTypeIndex.contains(((FollowToAnyDataType) sigType).getIndex())) {
                List<DataType> dataTypes = indexToArgumentTypes.computeIfAbsent(
                        ((FollowToAnyDataType) sigType).getIndex(), i -> Lists.newArrayList());
                dataTypes.add(expressionType);
            }
        }
    }

    private static DataType replaceAnyDataType(DataType dataType,
            Map<Integer, Optional<DataType>> indexToCommonTypes) {
        if (dataType instanceof ArrayType) {
            return ArrayType.of(replaceAnyDataType(((ArrayType) dataType).getItemType(), indexToCommonTypes));
        } else if (dataType instanceof MapType) {
            return MapType.of(replaceAnyDataType(((MapType) dataType).getKeyType(), indexToCommonTypes),
                    replaceAnyDataType(((MapType) dataType).getValueType(), indexToCommonTypes));
        } else if (dataType instanceof StructType) {
            // TODO: do not support struct type now
            // throw new AnalysisException("do not support struct type now");
            return dataType;
        } else {
            if (dataType instanceof AnyDataType && ((AnyDataType) dataType).getIndex() >= 0) {
                Optional<DataType> optionalDataType = indexToCommonTypes.get(((AnyDataType) dataType).getIndex());
                if (optionalDataType != null && optionalDataType.isPresent()) {
                    return optionalDataType.get();
                }
            } else if (dataType instanceof FollowToAnyDataType) {
                Optional<DataType> optionalDataType = indexToCommonTypes.get(
                        ((FollowToAnyDataType) dataType).getIndex());
                if (optionalDataType != null && optionalDataType.isPresent()) {
                    return optionalDataType.get();
                }
            }
            return dataType;
        }
    }

    /** implementFollowToAnyDataType */
    public static FunctionSignature implementAnyDataTypeWithOutIndex(
            FunctionSignature signature, List<Expression> arguments) {
        // collect all any data type with index
        List<DataType> newArgTypes = Lists.newArrayList();
        for (int i = 0; i < arguments.size(); i++) {
            DataType sigType;
            if (i >= signature.argumentsTypes.size()) {
                sigType = signature.getVarArgType().orElseThrow(
                        () -> new AnalysisException("function arity not match with signature"));
            } else {
                sigType = signature.argumentsTypes.get(i);
            }
            DataType expressionType = arguments.get(i).getDataType();
            newArgTypes.add(replaceAnyDataTypeWithOutIndex(sigType, expressionType));
        }
        signature = signature.withArgumentTypes(signature.hasVarArgs, newArgTypes);
        return signature;
    }

    /** implementFollowToAnyDataType */
    public static FunctionSignature implementAnyDataTypeWithIndex(
            FunctionSignature signature, List<Expression> arguments) {
        // collect all any data type with index
        Map<Integer, List<DataType>> indexToArgumentTypes = Maps.newHashMap();
        Map<Integer, Optional<DataType>> indexToCommonTypes = Maps.newHashMap();
        for (int i = 0; i < arguments.size(); i++) {
            DataType sigType;
            if (i >= signature.argumentsTypes.size()) {
                sigType = signature.getVarArgType().orElseThrow(
                        () -> new AnalysisException("function arity not match with signature"));
            } else {
                sigType = signature.argumentsTypes.get(i);
            }
            DataType expressionType = arguments.get(i).getDataType();
            collectAnyDataType(sigType, expressionType, indexToArgumentTypes);
        }
        // if all any data type's expression is NULL, we should use follow to any data type to do type coercion
        Set<Integer> allNullTypeIndex = indexToArgumentTypes.entrySet().stream()
                .filter(entry -> entry.getValue().stream().allMatch(NullType.class::isInstance))
                .map(Entry::getKey)
                .collect(ImmutableSet.toImmutableSet());
        if (!allNullTypeIndex.isEmpty()) {
            for (int i = 0; i < arguments.size(); i++) {
                DataType sigType;
                if (i >= signature.argumentsTypes.size()) {
                    sigType = signature.getVarArgType().orElseThrow(
                            () -> new IllegalStateException("function arity not match with signature"));
                } else {
                    sigType = signature.argumentsTypes.get(i);
                }
                DataType expressionType = arguments.get(i).getDataType();
                collectFollowToAnyDataType(sigType, expressionType, indexToArgumentTypes, allNullTypeIndex);
            }
        }

        // get all common type for any data type
        for (Map.Entry<Integer, List<DataType>> dataTypes : indexToArgumentTypes.entrySet()) {
            // TODO: should use the same common type method of implicitCast
            Optional<DataType> dataType = TypeCoercionUtils.findWiderCommonTypeForComparison(dataTypes.getValue());
            // TODO: should we use tinyint when all any data type's expression is null type?
            // if (dataType.isPresent() && dataType.get() instanceof NullType) {
            //     dataType = Optional.of(TinyIntType.INSTANCE);
            // }
            indexToCommonTypes.put(dataTypes.getKey(), dataType);
        }

        // replace any data type and follow to any data type with real data type
        List<DataType> newArgTypes = Lists.newArrayList();
        for (DataType sigType : signature.argumentsTypes) {
            newArgTypes.add(replaceAnyDataType(sigType, indexToCommonTypes));
        }
        signature = signature.withArgumentTypes(signature.hasVarArgs, newArgTypes);
        DataType returnType = replaceAnyDataType(signature.returnType, indexToCommonTypes);
        signature = signature.withReturnType(returnType);
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
            DataType targetType;
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
        List<DataType> newArgTypes = signature.argumentsTypes.stream().map(t -> {
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
            DataType targetType;
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
            // only process wildcard decimalv3
            if (((DecimalV3Type) targetType).getPrecision() > 0) {
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
            Preconditions.checkState(finalType.isDecimalV3Type(), "decimalv3 precision promotion failed.");
        }
        DataType argType = finalType;
        List<DataType> newArgTypes = signature.argumentsTypes.stream().map(t -> {
            // only process wildcard decimalv3
            if (t instanceof DecimalV3Type && ((DecimalV3Type) t).getPrecision() <= 0) {
                return argType;
            } else {
                return t;
            }
        }).collect(Collectors.toList());
        signature = signature.withArgumentTypes(signature.hasVarArgs, newArgTypes);
        if (signature.returnType instanceof DecimalV3Type
                && ((DecimalV3Type) signature.returnType).getPrecision() <= 0) {
            signature = signature.withReturnType(argType);
        }
        return signature;
    }

    static class ComputeSignatureChain {
        private final ResponsibilityChain<SignatureContext> computeChain;

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
