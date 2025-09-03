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
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.ComplexDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToArgumentType;
import org.apache.doris.nereids.types.coercion.ScaleTimeType;
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
        boolean hasTimeV2Type = false;
        boolean hasDecimalV3Type = false;
        for (DataType argumentsType : signature.argumentsTypes) {
            hasDateTimeV2Type |= TypeCoercionUtils.hasDateTimeV2Type(argumentsType);
            hasTimeV2Type |= TypeCoercionUtils.hasTimeV2Type(argumentsType);
            hasDecimalV3Type |= TypeCoercionUtils.hasDecimalV3Type(argumentsType);
        }

        if (hasDateTimeV2Type || hasTimeV2Type) {
            signature = defaultTimePrecisionPromotion(signature, arguments);
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

    // for time type with precision(now are DateTimeV2Type and TimeV2Type),
    // we will promote the precision of the type to the maximum precision of all arguments
    private static FunctionSignature defaultTimePrecisionPromotion(FunctionSignature signature,
            List<Expression> arguments) {
        int finalTypeScale = -1;
        for (int i = 0; i < arguments.size(); i++) {
            DataType targetType; // type of signature_args[i]
            if (i >= signature.argumentsTypes.size()) {
                Preconditions.checkState(signature.getVarArgType().isPresent(),
                        "argument size larger than signature");
                targetType = signature.getVarArgType().get();
            } else {
                targetType = signature.getArgType(i);
            }
            // if input type X's slot(targetType) is datetimev2/timev2 or complex of them, get all nested type of X.
            List<DataType> nestedInputTypes = ImmutableList.<DataType>builder()
                    .addAll(extractArgumentTypeBySignature(DateTimeV2Type.class, targetType,
                            arguments.get(i).getDataType()))
                    .addAll(extractArgumentTypeBySignature(TimeV2Type.class, targetType,
                            arguments.get(i).getDataType()))
                    .build();
            // there's DateTimeV2 and TimeV2 at same time, so we need get exact target type when we promote any slot.
            List<DataType> nestedTargetTypes = ImmutableList.<DataType>builder()
                    .addAll(extractSignatureTypes(DateTimeV2Type.class, targetType, arguments.get(i).getDataType()))
                    .addAll(extractSignatureTypes(TimeV2Type.class, targetType, arguments.get(i).getDataType()))
                    .build();
            if (nestedInputTypes.isEmpty()) {
                // if no DateTimeV2Type or TimeV2Type in the argument[i], no precision promotion
                continue;
            }

            // for Map or Struct, we have more than one nested type.
            // targetType may be ScaleTimeType or comlex type(Array, Struct) with ScaleTimeType nested.
            Expression arg = arguments.get(i);
            for (int j = 0; j < nestedInputTypes.size(); j++) {
                // inputType could be any legal input type
                DataType inputType = nestedInputTypes.get(j);
                // corresponding target slot type for inputType
                DataType nestedTargetType = nestedTargetTypes.get(j);
                int targetScale = 0;

                // for string input, try to get the most suitable scale
                if (arg instanceof StringLikeLiteral) {
                    ScaleTimeType timelikeType = (ScaleTimeType) nestedTargetType;
                    targetScale = timelikeType.forTypeFromString((StringLikeLiteral) arg).getScale();
                } else {
                    // for all other input types, get the target scale when cast it to targetType
                    ScaleTimeType targetScaleType = (ScaleTimeType) nestedTargetType;
                    ScaleTimeType promotedType = targetScaleType.scaleTypeForType(inputType);
                    targetScale = promotedType.getScale();
                }

                finalTypeScale = Math.max(finalTypeScale, targetScale); // init value -1 always promotes
            }
        }

        // if no DateTimeV2Type or TimeV2Type in the arguments, no precision promotion
        if (finalTypeScale < 0) {
            return signature;
        }
        // promote the precision of return type
        ImmutableList.Builder<DataType> newArgTypesBuilder = ImmutableList.builderWithExpectedSize(signature.arity);
        for (DataType signatureArgType : signature.argumentsTypes) {
            newArgTypesBuilder.add(TypeCoercionUtils.replaceTimesWithTargetPrecision(signatureArgType, finalTypeScale));
        }
        List<DataType> newArgTypes = newArgTypesBuilder.build();
        signature = signature.withArgumentTypes(signature.hasVarArgs, newArgTypes);
        if (signature.returnType instanceof DateTimeV2Type || signature.returnType instanceof TimeV2Type
                || signature.returnType instanceof ComplexDataType) {
            signature = signature.withReturnType(
                    TypeCoercionUtils.replaceTimesWithTargetPrecision(signature.returnType, finalTypeScale));
        }
        return signature;
    }

    /**
     * Dynamically compute function signature for variant type arguments.
     * This method handles cases where the function signature contains variant types
     * and needs to be adjusted based on the actual argument types.
     *
     * @param signature Original function signature
     * @param arguments List of actual arguments passed to the function
     * @return Updated function signature with resolved variant types
     */
    public static FunctionSignature dynamicComputeVariantArgs(
            FunctionSignature signature, List<Expression> arguments) {

        List<DataType> newArgTypes = Lists.newArrayListWithCapacity(arguments.size());
        boolean findVariantType = false;

        for (int i = 0; i < arguments.size(); i++) {
            // Get signature type for current argument position
            DataType sigType;
            if (i >= signature.argumentsTypes.size()) {
                sigType = signature.getVarArgType().orElseThrow(
                        () -> new AnalysisException("function arity not match with signature"));
            } else {
                sigType = signature.argumentsTypes.get(i);
            }

            // Get actual type of the argument expression
            DataType expressionType = arguments.get(i).getDataType();

            // If both signature type and expression type are variant,
            // use expression type and update return type
            if (sigType instanceof VariantType && expressionType instanceof VariantType) {
                // return type is variant, update return type to expression type
                if (signature.returnType instanceof VariantType) {
                    signature = signature.withReturnType(expressionType);
                    if (findVariantType) {
                        throw new AnalysisException("variant type is not supported in multiple arguments");
                    } else {
                        findVariantType = true;
                    }
                }
                newArgTypes.add(expressionType);
            } else {
                // Otherwise keep original signature type
                newArgTypes.add(sigType);
            }
        }

        // Update signature with new argument types
        signature = signature.withArgumentTypes(signature.hasVarArgs, newArgTypes);
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
            List<DataType> argTypes = extractArgumentTypeBySignature(DecimalV3Type.class, targetType,
                    arguments.get(i).getDataType());
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

    private static List<DataType> extractArgumentTypeBySignature(Class<? extends DataType> targetType,
            DataType signatureType, DataType argumentType) {
        return extractBySignature(targetType, signatureType, argumentType, (sig, arg) -> arg);
    }

    private static List<DataType> extractSignatureTypes(Class<? extends DataType> targetType, DataType signatureType,
            DataType argumentType) {
        return extractBySignature(targetType, signatureType, argumentType, (sig, arg) -> sig);
    }

    // if signatureType is a super type of targetType, then extract corresponding argumentType slot
    private static List<DataType> extractBySignature(Class<? extends DataType> targetType,
            DataType signatureType, DataType argumentType, BiFunction<DataType, DataType, DataType> pick) {
        if (targetType.isAssignableFrom(signatureType.getClass())) {
            return Lists.newArrayList(pick.apply(signatureType, argumentType));
        } else if (signatureType instanceof ArrayType) {
            if (argumentType instanceof NullType) {
                return extractBySignature(targetType, ((ArrayType) signatureType).getItemType(),
                        argumentType, pick);
            } else if (argumentType instanceof ArrayType) {
                return extractBySignature(targetType, ((ArrayType) signatureType).getItemType(),
                        ((ArrayType) argumentType).getItemType(), pick);
            } else {
                return Lists.newArrayList();
            }
        } else if (signatureType instanceof MapType) {
            if (argumentType instanceof NullType) {
                List<DataType> ret = extractBySignature(targetType, ((MapType) signatureType).getKeyType(),
                        argumentType, pick);
                ret.addAll(extractBySignature(targetType, ((MapType) signatureType).getValueType(),
                        argumentType, pick));
                return ret;
            } else if (argumentType instanceof MapType) {
                List<DataType> ret = extractBySignature(targetType, ((MapType) signatureType).getKeyType(),
                        ((MapType) argumentType).getKeyType(), pick);
                ret.addAll(extractBySignature(targetType, ((MapType) signatureType).getValueType(),
                        ((MapType) argumentType).getValueType(), pick));
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
