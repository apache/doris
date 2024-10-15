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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
        return signature.withReturnType(replaceFollowToArgumentReturnType(
                signature.returnType, signature.argumentsTypes));
    }

    private static DataType replaceFollowToArgumentReturnType(DataType returnType, List<DataType> argumentTypes) {
        if (returnType instanceof ArrayType) {
            return ArrayType.of(replaceFollowToArgumentReturnType(
                    ((ArrayType) returnType).getItemType(), argumentTypes));
        } else if (returnType instanceof MapType) {
            return MapType.of(replaceFollowToArgumentReturnType(((MapType) returnType).getKeyType(), argumentTypes),
                    replaceFollowToArgumentReturnType(((MapType) returnType).getValueType(), argumentTypes));
        } else if (returnType instanceof StructType) {
            // TODO: do not support struct type now
            // throw new AnalysisException("do not support struct type now");
            return returnType;
        } else if (returnType instanceof FollowToArgumentType) {
            int argumentIndex = ((FollowToArgumentType) returnType).argumentIndex;
            return argumentTypes.get(argumentIndex);
        } else {
            return returnType;
        }
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
        List<DataType> newArgTypes = Lists.newArrayListWithCapacity(arguments.size());
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
        Set<Integer> allNullTypeIndex = Sets.newHashSetWithExpectedSize(indexToArgumentTypes.size());
        for (Entry<Integer, List<DataType>> entry : indexToArgumentTypes.entrySet()) {
            boolean allIsNullType = true;
            for (DataType dataType : entry.getValue()) {
                if (!(dataType instanceof NullType)) {
                    allIsNullType = false;
                    break;
                }
            }
            if (allIsNullType) {
                allNullTypeIndex.add(entry.getKey());
            }
        }
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
        List<DataType> newArgTypes = Lists.newArrayListWithCapacity(signature.argumentsTypes.size());
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

        boolean hasDateTimeV2Type = false;
        boolean hasDecimalV3Type = false;
        for (DataType argumentsType : signature.argumentsTypes) {
            hasDateTimeV2Type |= TypeCoercionUtils.hasDateTimeV2Type(argumentsType);
            hasDecimalV3Type |= TypeCoercionUtils.hasDecimalV3Type(argumentsType);
        }

        if (hasDateTimeV2Type) {
            signature = defaultDateTimeV2PrecisionPromotion(signature, arguments);
        }
        if (hasDecimalV3Type) {
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
            List<DataType> argTypes = extractArgumentType(DateTimeV2Type.class,
                    targetType, arguments.get(i).getDataType());
            if (argTypes.isEmpty()) {
                continue;
            }

            for (DataType argType : argTypes) {
                Expression arg = arguments.get(i);
                DateTimeV2Type dateTimeV2Type;
                if (arg instanceof StringLikeLiteral) {
                    StringLikeLiteral str = (StringLikeLiteral) arguments.get(i);
                    dateTimeV2Type = DateTimeV2Type.forTypeFromString(str.getStringValue());
                } else {
                    dateTimeV2Type = DateTimeV2Type.forType(argType);
                }
                if (finalType == null) {
                    finalType = dateTimeV2Type;
                } else {
                    finalType = DateTimeV2Type.getWiderDatetimeV2Type(finalType, dateTimeV2Type);
                }
            }
        }
        if (finalType == null) {
            return signature;
        }
        DateTimeV2Type argType = finalType;

        ImmutableList.Builder<DataType> newArgTypesBuilder = ImmutableList.builderWithExpectedSize(signature.arity);
        for (DataType at : signature.argumentsTypes) {
            newArgTypesBuilder.add(TypeCoercionUtils.replaceDateTimeV2WithTarget(at, argType));
        }
        List<DataType> newArgTypes = newArgTypesBuilder.build();
        signature = signature.withArgumentTypes(signature.hasVarArgs, newArgTypes);
        signature = signature.withArgumentTypes(signature.hasVarArgs, newArgTypes);
        if (signature.returnType instanceof DateTimeV2Type) {
            signature = signature.withReturnType(argType);
        }
        return signature;
    }

    private static FunctionSignature defaultDecimalV3PrecisionPromotion(
            FunctionSignature signature, List<Expression> arguments) {
        DecimalV3Type finalType = null;
        for (int i = 0; i < arguments.size(); i++) {
            DataType targetType;
            if (i >= signature.argumentsTypes.size()) {
                Preconditions.checkState(signature.getVarArgType().isPresent(),
                        "argument size larger than signature");
                targetType = signature.getVarArgType().get();
            } else {
                targetType = signature.getArgType(i);
            }
            List<DataType> argTypes = extractArgumentType(DecimalV3Type.class,
                    targetType, arguments.get(i).getDataType());
            if (argTypes.isEmpty()) {
                continue;
            }

            for (DataType argType : argTypes) {
                Expression arg = arguments.get(i);
                DecimalV3Type decimalV3Type;
                if (arg.isLiteral() && arg.getDataType().isIntegralType()) {
                    // create decimalV3 with minimum scale enough to hold the integral literal
                    decimalV3Type = DecimalV3Type.createDecimalV3Type(new BigDecimal(((Literal) arg).getStringValue()));
                } else {
                    decimalV3Type = DecimalV3Type.forType(argType);
                }
                if (finalType == null) {
                    finalType = decimalV3Type;
                } else {
                    finalType = (DecimalV3Type) DecimalV3Type.widerDecimalV3Type(finalType, decimalV3Type, false);
                }
            }
        }
        DecimalV3Type argType = finalType;
        if (finalType == null) {
            return signature;
        }
        List<DataType> newArgTypes = signature.argumentsTypes.stream()
                .map(at -> TypeCoercionUtils.replaceDecimalV3WithTarget(at, argType))
                .collect(Collectors.toList());
        signature = signature.withArgumentTypes(signature.hasVarArgs, newArgTypes);
        if (signature.returnType instanceof DecimalV3Type
                && ((DecimalV3Type) signature.returnType).getPrecision() <= 0) {
            signature = signature.withReturnType(argType);
        }
        return signature;
    }

    private static List<DataType> extractArgumentType(Class<? extends DataType> targetType,
            DataType signatureType, DataType argumentType) {
        if (targetType.isAssignableFrom(signatureType.getClass())) {
            return Lists.newArrayList(argumentType);
        } else if (signatureType instanceof ArrayType) {
            if (argumentType instanceof NullType) {
                return extractArgumentType(targetType, ((ArrayType) signatureType).getItemType(), argumentType);
            } else if (argumentType instanceof ArrayType) {
                return extractArgumentType(targetType,
                        ((ArrayType) signatureType).getItemType(), ((ArrayType) argumentType).getItemType());
            } else {
                return Lists.newArrayList();
            }
        } else if (signatureType instanceof MapType) {
            if (argumentType instanceof NullType) {
                List<DataType> ret = extractArgumentType(targetType,
                        ((MapType) signatureType).getKeyType(), argumentType);
                ret.addAll(extractArgumentType(targetType, ((MapType) signatureType).getValueType(), argumentType));
                return ret;
            } else if (argumentType instanceof MapType) {
                List<DataType> ret = extractArgumentType(targetType,
                        ((MapType) signatureType).getKeyType(), ((MapType) argumentType).getKeyType());
                ret.addAll(extractArgumentType(targetType,
                        ((MapType) signatureType).getValueType(), ((MapType) argumentType).getValueType()));
                return ret;
            } else {
                return Lists.newArrayList();
            }
        } else if (signatureType instanceof StructType) {
            // TODO: do not support struct type now
            return Lists.newArrayList();
        } else {
            return Lists.newArrayList();
        }
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
