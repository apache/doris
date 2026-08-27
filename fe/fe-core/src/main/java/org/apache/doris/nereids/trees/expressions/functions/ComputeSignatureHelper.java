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
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.ComplexDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToArgumentType;
import org.apache.doris.nereids.types.coercion.ScaleTimeType;
import org.apache.doris.nereids.util.ResponsibilityChain;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.GlobalVariable;

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

/** ComputeSignatureHelper */
public class ComputeSignatureHelper {

    private static final String MAP_KEY = "key";
    private static final String MAP_VALUE = "value";
    private static final String ARRAY_ITEM = "array";

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
            // Convert legacy datetime/date types to v2 types, keep v2 types as is
            if (sigType.isDateType()) {
                // Legacy DateType -> DateV2Type
                sigType = DateV2Type.INSTANCE;
            } else if (sigType.isDateTimeType()) {
                // Legacy DateTimeType -> DateTimeV2Type
                sigType = DateTimeV2Type.SYSTEM_DEFAULT;
            }
            newArgTypes.add(replaceAnyDataTypeWithOutIndex(sigType, expressionType));
        }
        signature = signature.withArgumentTypes(signature.hasVarArgs, newArgTypes);
        return signature;
    }

    /** implementFollowToAnyDataType without legacy date/datetime V2 promotion */
    public static FunctionSignature implementAnyDataTypeWithOutIndexNoLegacyDateUpgrade(
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
            // SKIP: legacy Date/DateTime type promotion to V2
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
            Optional<DataType> dataType;
            if (GlobalVariable.enableNewTypeCoercionBehavior) {
                dataType = TypeCoercionUtils.findWiderCommonType(dataTypes.getValue(), false, true);
            } else {
                dataType = TypeCoercionUtils.findWiderCommonTypeForComparison(dataTypes.getValue());
            }
            // TODO: should we use tinyint when all any data type's expression is null type?
            // if (dataType.isPresent() && dataType.get() instanceof NullType) {
            //     dataType = Optional.of(TinyIntType.INSTANCE);
            // }
            indexToCommonTypes.put(dataTypes.getKey(), dataType);
        }

        // replace any data type and follow to any data type with real data type
        List<DataType> newArgTypes = Lists.newArrayListWithCapacity(signature.argumentsTypes.size());
        for (DataType sigType : signature.argumentsTypes) {
            // Convert legacy datetime/date types to v2 types, keep v2 types as is
            if (sigType.isDateType()) {
                // Legacy DateType -> DateV2Type
                sigType = DateV2Type.INSTANCE;
            } else if (sigType.isDateTimeType()) {
                // Legacy DateTimeType -> DateTimeV2Type
                sigType = DateTimeV2Type.SYSTEM_DEFAULT;
            }
            newArgTypes.add(replaceAnyDataType(sigType, indexToCommonTypes));
        }
        signature = signature.withArgumentTypes(signature.hasVarArgs, newArgTypes);
        DataType returnType = replaceAnyDataType(signature.returnType, indexToCommonTypes);
        signature = signature.withReturnType(returnType);
        return signature;
    }

    /** implementFollowToAnyDataType without legacy date/datetime V2 promotion */
    public static FunctionSignature implementAnyDataTypeWithIndexNoLegacyDateUpgrade(
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
        // if all any data type's expression is NULL, we should use follow to any data
        // type to do type coercion
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
            Optional<DataType> dataType;
            if (GlobalVariable.enableNewTypeCoercionBehavior) {
                dataType = TypeCoercionUtils.findWiderCommonType(dataTypes.getValue(), false, true);
            } else {
                dataType = TypeCoercionUtils.findWiderCommonTypeForComparison(dataTypes.getValue());
            }
            // TODO: should we use tinyint when all any data type's expression is null type?
            // if (dataType.isPresent() && dataType.get() instanceof NullType) {
            // dataType = Optional.of(TinyIntType.INSTANCE);
            // }
            indexToCommonTypes.put(dataTypes.getKey(), dataType);
        }

        // replace any data type and follow to any data type with real data type
        List<DataType> newArgTypes = Lists.newArrayListWithCapacity(signature.argumentsTypes.size());
        for (DataType sigType : signature.argumentsTypes) {
            // SKIP: legacy Date/DateTime type promotion to V2
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
        boolean hasTimestampTzType = false;
        boolean hasDecimalV3Type = false;
        for (DataType argumentsType : signature.argumentsTypes) {
            hasDateTimeV2Type |= TypeCoercionUtils.hasDateTimeV2Type(argumentsType);
            hasTimeV2Type |= TypeCoercionUtils.hasTimeV2Type(argumentsType);
            hasDecimalV3Type |= TypeCoercionUtils.hasDecimalV3Type(argumentsType);
            hasTimestampTzType |= TypeCoercionUtils.hasTimestampTzType(argumentsType);
        }

        if (hasDateTimeV2Type || hasTimeV2Type || hasTimestampTzType) {
            signature = defaultTimePrecisionPromotion(signature, arguments);
        }
        if (hasDecimalV3Type) {
            // do decimal v3 precision
            signature = defaultDecimalV3PrecisionPromotion(signature, arguments, computeSignature);
        }
        return signature;
    }

    /** ensureNestedNullableOfArray */
    public static FunctionSignature ensureNestedNullableOfArray(FunctionSignature signature,
            List<Expression> arguments) {
        if (!(signature.returnType instanceof ArrayType)) {
            return signature;
        }
        ArrayType arrayType = (ArrayType) signature.returnType;
        return signature.withReturnType(ArrayType.of(arrayType.getItemType()));
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
                    .addAll(extractArgumentTypeBySignature(TimeStampTzType.class, targetType,
                            arguments.get(i).getDataType()))
                    .build();
            // there's DateTimeV2 and TimeV2 at same time, so we need get exact target type when we promote any slot.
            List<DataType> nestedTargetTypes = ImmutableList.<DataType>builder()
                    .addAll(extractSignatureTypes(DateTimeV2Type.class, targetType, arguments.get(i).getDataType()))
                    .addAll(extractSignatureTypes(TimeV2Type.class, targetType, arguments.get(i).getDataType()))
                    .addAll(extractSignatureTypes(TimeStampTzType.class, targetType, arguments.get(i).getDataType()))
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
                || signature.returnType instanceof TimeStampTzType
                || signature.returnType instanceof ComplexDataType) {
            signature = signature.withReturnType(
                    TypeCoercionUtils.replaceTimesWithTargetPrecision(signature.returnType, finalTypeScale));
        }
        return signature;
    }

    private static FunctionSignature defaultDecimalV3PrecisionPromotion(
            FunctionSignature signature, List<Expression> arguments, ComputeSignature computeSignature) {
        // The wider type across all decimal slots, used for decimal slots that are not
        // inside a MAP (keeping the original behavior), for the placeholder return type,
        // and for MAP-nested leaves whose group has no concrete type information.
        DecimalV3Type widerType = null;

        // Decimal leaves inside a MAP are independent type variables: they must keep
        // their own precision/scale instead of being merged into one wider type,
        // otherwise widening one leaf (e.g. the scale of a big integral key) may overflow
        // the other leaf. They are grouped by the full structural path through nested
        // containers (e.g. "key", "value", "value/array", "value/key") and the resolved
        // leaf type, so the leaves of different (or repeated) MAP arguments on the same
        // path aggregate while leaves on different paths stay independent.
        Map<String, DecimalV3Type> groupWider = Maps.newHashMap();

        // The template signature carrying the original Any/Follow slots that the resolved
        // signature was derived from. It lets us link a top-level scalar slot with the MAP
        // leaf it belongs to by the original Any/Follow group identity (the index) instead
        // of the resolved concrete type, which can collide when independent slots resolve
        // to the same type (e.g. the key and the value of a MAP both becoming DECIMAL(10,3)).
        FunctionSignature template = findDecimalV3Template(computeSignature, signature);

        // The outermost MAP leaf group of each Any/Follow index (from the template), used
        // to link a top-level scalar slot (e.g. map_contains_value's probe, element_at's
        // lookup) with the MAP leaf that carries the same index.
        Map<Integer, String> indexToMapLeafGroup = Maps.newHashMap();

        // Fallback used when the template can not be recovered: the outermost MAP leaf
        // group of each resolved type, used to link a top-level scalar slot with the MAP
        // leaf it was resolved from (after Any/Follow resolution both carry the same type).
        Map<DecimalV3Type, String> mapLeafGroupByType = Maps.newHashMap();

        // Top-level scalar decimal leaves with a concrete resolved type, whose promoted
        // type must also be folded into the linked MAP leaf group.
        List<DecimalLeaf> scalarLeaves = Lists.newArrayList();

        // Top-level scalar decimal slots are independent logical type variables
        // (e.g. the key/value of map_agg(k, v) are Any(0) and Any(1)); group them by
        // the resolved type so the slots of one logical group aggregate while the slots
        // of different groups keep their own precision/scale.
        Map<DecimalV3Type, DecimalV3Type> scalarGroupWider = Maps.newHashMap();

        DecimalV3Type[] widerHolder = new DecimalV3Type[1];
        for (int i = 0; i < arguments.size(); i++) {
            DataType targetType = getSignatureArgumentType(signature, i);
            DataType templateType = template == null ? null : getSignatureArgumentType(template, i);
            collectDecimalLeaf(targetType, arguments.get(i).getDataType(), arguments.get(i),
                    "", templateType, indexToMapLeafGroup, mapLeafGroupByType, groupWider,
                    scalarGroupWider, scalarLeaves, widerHolder);
        }
        widerType = widerHolder[0];
        if (widerType == null) {
            return signature;
        }

        // Fold the promoted type of every top-level scalar slot into the MAP leaf group it
        // is linked with (by the original Any/Follow identity when available, otherwise by
        // the resolved type), so the MAP leaf and the scalar slot linked with it are
        // promoted to one type.
        for (DecimalLeaf scalarLeaf : scalarLeaves) {
            String linkedGroup;
            if (scalarLeaf.index >= 0) {
                linkedGroup = indexToMapLeafGroup.get(scalarLeaf.index);
            } else {
                linkedGroup = mapLeafGroupByType.get(scalarLeaf.resolvedType);
            }
            if (linkedGroup != null) {
                groupWider.merge(linkedGroup, scalarLeaf.promotedType,
                        ComputeSignatureHelper::mergeDecimalV3Type);
            }
        }

        List<DataType> newArgTypes = Lists.newArrayListWithCapacity(signature.argumentsTypes.size());
        for (int i = 0; i < signature.argumentsTypes.size(); i++) {
            DataType templateType = template == null ? null : getSignatureArgumentType(template, i);
            newArgTypes.add(replaceDecimalV3Leaf(signature.argumentsTypes.get(i), "", templateType,
                    indexToMapLeafGroup, mapLeafGroupByType, groupWider, scalarGroupWider, widerType));
        }
        signature = signature.withArgumentTypes(signature.hasVarArgs, newArgTypes);
        if (signature.returnType instanceof DecimalV3Type
                && ((DecimalV3Type) signature.returnType).getPrecision() <= 0) {
            signature = signature.withReturnType(widerType);
        }
        return signature;
    }

    private static DataType getSignatureArgumentType(FunctionSignature signature, int index) {
        if (index >= signature.argumentsTypes.size()) {
            Preconditions.checkState(signature.getVarArgType().isPresent(),
                    "argument size larger than signature");
            return signature.getVarArgType().get();
        }
        return signature.getArgType(index);
    }

    /**
     * Compute the promoted DecimalV3Type for one decimal slot from its argument type.
     */
    private static DecimalV3Type promotedDecimalV3Type(Expression arg, DataType argType) {
        if (arg.isLiteral() && arg.getDataType().isIntegralType()) {
            // create decimalV3 with minimum scale enough to hold the integral literal
            return DecimalV3Type.createDecimalV3Type(new BigDecimal(((Literal) arg).getStringValue()));
        }
        return DecimalV3Type.forType(argType);
    }

    /**
     * Collect every decimal leaf of one argument and fold its promoted type into the
     * corresponding group. {@code path} is the full structural path through nested
     * containers (empty for a top-level slot, {@link #MAP_KEY}/{@link #MAP_VALUE} for
     * the key/value of a MAP, {@link #ARRAY_ITEM} for an ARRAY item), so an ARRAY nested
     * in a MAP value (e.g. "value/array") or the key/value of a nested MAP (e.g.
     * "value/key") keep the enclosing group instead of being merged with the outer
     * leaves. {@code templateType} is the corresponding slot of the template signature
     * that still carries the original Any/Follow identity of this leaf. {@code widerHolder}
     * accumulates the wider type across all decimal leaves.
     */
    private static void collectDecimalLeaf(DataType sigType, DataType argType, Expression arg,
            String path, DataType templateType, Map<Integer, String> indexToMapLeafGroup,
            Map<DecimalV3Type, String> mapLeafGroupByType,
            Map<String, DecimalV3Type> groupWider, Map<DecimalV3Type, DecimalV3Type> scalarGroupWider,
            List<DecimalLeaf> scalarLeaves, DecimalV3Type[] widerHolder) {
        if (sigType instanceof DecimalV3Type) {
            DecimalV3Type sigDecimal = (DecimalV3Type) sigType;
            DecimalV3Type promoted = null;
            if (!(argType instanceof NullType)) {
                promoted = promotedDecimalV3Type(arg, argType);
                widerHolder[0] = mergeDecimalV3Type(widerHolder[0], promoted);
            }
            if (path.isEmpty()) {
                // top-level scalar slot: a concrete resolved type may be linked with a
                // MAP leaf below by the original Any/Follow identity, and otherwise the
                // slots of the same resolved type form one logical group (e.g. the two
                // arguments of map_agg) and stay independent from the slots of other groups
                if (promoted != null && sigDecimal.getPrecision() > 0) {
                    scalarLeaves.add(new DecimalLeaf(sigDecimal, promoted, anyFollowIndex(templateType)));
                    scalarGroupWider.merge(sigDecimal, promoted,
                            ComputeSignatureHelper::mergeDecimalV3Type);
                }
            } else if (isMapNested(path) && promoted != null) {
                String groupKey = path + ":" + sigDecimal;
                groupWider.merge(groupKey, promoted, ComputeSignatureHelper::mergeDecimalV3Type);
                int index = anyFollowIndex(templateType);
                if (index >= 0) {
                    // keep the outermost group (shortest path) for linking by the index
                    indexToMapLeafGroup.putIfAbsent(index, groupKey);
                } else {
                    // fallback: keep the outermost group (shortest path, key before value)
                    // for linking by the resolved type
                    mapLeafGroupByType.putIfAbsent(sigDecimal, groupKey);
                }
            }
            // other leaves (e.g. ARRAY items not nested in a MAP) keep the original
            // behavior of the single wider type
            return;
        } else if (sigType instanceof MapType) {
            MapType mapType = (MapType) sigType;
            DataType templateKey = templateType instanceof MapType
                    ? ((MapType) templateType).getKeyType() : null;
            DataType templateValue = templateType instanceof MapType
                    ? ((MapType) templateType).getValueType() : null;
            if (argType instanceof MapType) {
                MapType argMapType = (MapType) argType;
                collectDecimalLeaf(mapType.getKeyType(), argMapType.getKeyType(), arg,
                        appendPath(path, MAP_KEY), templateKey, indexToMapLeafGroup, mapLeafGroupByType,
                        groupWider, scalarGroupWider, scalarLeaves, widerHolder);
                collectDecimalLeaf(mapType.getValueType(), argMapType.getValueType(), arg,
                        appendPath(path, MAP_VALUE), templateValue, indexToMapLeafGroup, mapLeafGroupByType,
                        groupWider, scalarGroupWider, scalarLeaves, widerHolder);
            } else if (argType instanceof NullType) {
                collectDecimalLeaf(mapType.getKeyType(), argType, arg,
                        appendPath(path, MAP_KEY), templateKey, indexToMapLeafGroup, mapLeafGroupByType,
                        groupWider, scalarGroupWider, scalarLeaves, widerHolder);
                collectDecimalLeaf(mapType.getValueType(), argType, arg,
                        appendPath(path, MAP_VALUE), templateValue, indexToMapLeafGroup, mapLeafGroupByType,
                        groupWider, scalarGroupWider, scalarLeaves, widerHolder);
            }
            return;
        } else if (sigType instanceof ArrayType) {
            DataType itemArgType;
            if (argType instanceof ArrayType) {
                itemArgType = ((ArrayType) argType).getItemType();
            } else if (argType instanceof NullType) {
                itemArgType = argType;
            } else {
                return;
            }
            // carry the enclosing MAP path through the ARRAY so items nested in a MAP
            // value stay in the value group
            DataType templateItem = templateType instanceof ArrayType
                    ? ((ArrayType) templateType).getItemType() : null;
            collectDecimalLeaf(((ArrayType) sigType).getItemType(), itemArgType, arg,
                    appendPath(path, ARRAY_ITEM), templateItem, indexToMapLeafGroup, mapLeafGroupByType,
                    groupWider, scalarGroupWider, scalarLeaves, widerHolder);
        }
        // StructType and other types are not supported
    }

    /**
     * Replace every decimal leaf in {@code sigType}: leaves inside a MAP use the wider
     * type of their own structural group, top-level scalar slots use the wider type of
     * their own logical group (slots of the same resolved type), and all other leaves
     * (e.g. ARRAY items not nested in a MAP) keep the original behavior of using the
     * single wider type across all decimal slots.
     */
    private static DataType replaceDecimalV3Leaf(DataType sigType, String path, DataType templateType,
            Map<Integer, String> indexToMapLeafGroup, Map<DecimalV3Type, String> mapLeafGroupByType,
            Map<String, DecimalV3Type> groupWider, Map<DecimalV3Type, DecimalV3Type> scalarGroupWider,
            DecimalV3Type widerType) {
        if (sigType instanceof DecimalV3Type) {
            DecimalV3Type sigDecimal = (DecimalV3Type) sigType;
            if (path.isEmpty()) {
                // a top-level scalar slot linked with a MAP leaf keeps the type of that
                // leaf (e.g. map_contains_value's probe / element_at's lookup must match
                // the MAP value/key type). The link is resolved by the original Any/Follow
                // identity, falling back to the resolved type when the template can not be
                // recovered.
                if (sigDecimal.getPrecision() > 0) {
                    String linkedGroup = null;
                    int index = anyFollowIndex(templateType);
                    if (index >= 0) {
                        linkedGroup = indexToMapLeafGroup.get(index);
                    } else {
                        linkedGroup = mapLeafGroupByType.get(sigDecimal);
                    }
                    if (linkedGroup != null) {
                        DecimalV3Type linkedWider = groupWider.get(linkedGroup);
                        if (linkedWider != null) {
                            return linkedWider;
                        }
                    }
                    // independent logical Any groups (e.g. the key/value arguments of
                    // map_agg) keep their own precision/scale instead of being merged
                    // into one wider type
                    DecimalV3Type scalarWider = scalarGroupWider.get(sigDecimal);
                    if (scalarWider != null) {
                        return scalarWider;
                    }
                }
                return widerType;
            }
            if (isMapNested(path)) {
                DecimalV3Type groupType = groupWider.get(path + ":" + sigDecimal);
                return groupType != null ? groupType : widerType;
            }
            // other leaves (e.g. ARRAY items not nested in a MAP) keep the original
            // behavior of the single wider type
            return widerType;
        } else if (sigType instanceof ArrayType) {
            DataType templateItem = templateType instanceof ArrayType
                    ? ((ArrayType) templateType).getItemType() : null;
            return ArrayType.of(replaceDecimalV3Leaf(((ArrayType) sigType).getItemType(),
                    appendPath(path, ARRAY_ITEM), templateItem, indexToMapLeafGroup, mapLeafGroupByType,
                    groupWider, scalarGroupWider, widerType));
        } else if (sigType instanceof MapType) {
            MapType mapType = (MapType) sigType;
            DataType templateKey = templateType instanceof MapType
                    ? ((MapType) templateType).getKeyType() : null;
            DataType templateValue = templateType instanceof MapType
                    ? ((MapType) templateType).getValueType() : null;
            return MapType.of(
                    replaceDecimalV3Leaf(mapType.getKeyType(), appendPath(path, MAP_KEY),
                            templateKey, indexToMapLeafGroup, mapLeafGroupByType, groupWider,
                            scalarGroupWider, widerType),
                    replaceDecimalV3Leaf(mapType.getValueType(), appendPath(path, MAP_VALUE),
                            templateValue, indexToMapLeafGroup, mapLeafGroupByType, groupWider,
                            scalarGroupWider, widerType));
        }
        return sigType;
    }

    private static String appendPath(String path, String segment) {
        return path.isEmpty() ? segment : path + "/" + segment;
    }

    private static boolean isMapNested(String path) {
        return path.contains(MAP_KEY) || path.contains(MAP_VALUE);
    }

    private static DecimalV3Type mergeDecimalV3Type(DecimalV3Type left, DecimalV3Type right) {
        if (left == null) {
            return right;
        }
        return (DecimalV3Type) DecimalV3Type.widerDecimalV3Type(left, right, false);
    }

    /** A top-level scalar decimal leaf that may be linked with a MAP key/value leaf. */
    private static class DecimalLeaf {
        final DecimalV3Type resolvedType;
        final DecimalV3Type promotedType;
        final int index;

        DecimalLeaf(DecimalV3Type resolvedType, DecimalV3Type promotedType, int index) {
            this.resolvedType = resolvedType;
            this.promotedType = promotedType;
            this.index = index;
        }
    }

    /**
     * The index of the original Any/Follow slot this (template) type carries, or -1 when
     * it is not an Any/Follow slot. {@link AnyDataType#INSTANCE_WITHOUT_INDEX} has index
     * -1, so MAP leaves declared without an index never take part in the scalar linking.
     */
    private static int anyFollowIndex(DataType dataType) {
        if (dataType instanceof AnyDataType) {
            return ((AnyDataType) dataType).getIndex();
        } else if (dataType instanceof FollowToAnyDataType) {
            return ((FollowToAnyDataType) dataType).getIndex();
        }
        return -1;
    }

    /**
     * Recover the original signature (still carrying the Any/Follow slots) that the given
     * resolved {@code signature} was derived from, by matching the arity and the slots
     * that do not contain Any/Follow. Returns null when it can not be recovered, in which
     * case the scalar linking falls back to the resolved concrete type.
     */
    private static FunctionSignature findDecimalV3Template(ComputeSignature computeSignature,
            FunctionSignature signature) {
        List<FunctionSignature> signatures = computeSignature.getSignatures();
        if (signatures == null) {
            return null;
        }
        for (FunctionSignature candidate : signatures) {
            if (candidate.hasVarArgs != signature.hasVarArgs || candidate.arity != signature.arity) {
                continue;
            }
            boolean matched = true;
            for (int i = 0; i < candidate.argumentsTypes.size(); i++) {
                DataType candidateType = candidate.argumentsTypes.get(i);
                if (containsAnyOrFollow(candidateType)) {
                    continue;
                }
                if (!candidateType.equals(signature.argumentsTypes.get(i))) {
                    matched = false;
                    break;
                }
            }
            if (matched) {
                return candidate;
            }
        }
        return null;
    }

    private static boolean containsAnyOrFollow(DataType dataType) {
        if (dataType instanceof AnyDataType || dataType instanceof FollowToAnyDataType) {
            return true;
        } else if (dataType instanceof ArrayType) {
            return containsAnyOrFollow(((ArrayType) dataType).getItemType());
        } else if (dataType instanceof MapType) {
            return containsAnyOrFollow(((MapType) dataType).getKeyType())
                    || containsAnyOrFollow(((MapType) dataType).getValueType());
        }
        return false;
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
