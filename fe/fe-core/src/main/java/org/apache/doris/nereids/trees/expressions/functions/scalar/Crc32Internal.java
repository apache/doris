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
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
        // Manually chain the signature computation steps, skipping date type conversion
        signature = implementAnyDataTypeWithOutIndexSkipDateConversion(signature, getArguments());
        signature = implementAnyDataTypeWithIndexSkipDateConversion(signature, getArguments());
        signature = ComputeSignatureHelper.computePrecision(this, signature, getArguments());
        signature = ComputeSignatureHelper.implementFollowToArgumentReturnType(signature, getArguments());
        signature = ComputeSignatureHelper.normalizeDecimalV2(signature, getArguments());
        signature = ComputeSignatureHelper.ensureNestedNullableOfArray(signature, getArguments());
        signature = ComputeSignatureHelper.dynamicComputeVariantArgs(signature, getArguments());
        return signature;
    }

    /**
     * Custom implementation of implementAnyDataTypeWithOutIndex that skips date
     * type conversion.
     */
    private FunctionSignature implementAnyDataTypeWithOutIndexSkipDateConversion(
            FunctionSignature signature, List<Expression> arguments) {
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
            // Skip date type conversion - directly use replaceAnyDataTypeWithOutIndex
            newArgTypes.add(replaceAnyDataTypeWithOutIndex(sigType, expressionType));
        }
        return signature.withArgumentTypes(signature.hasVarArgs, newArgTypes);
    }

    /**
     * Custom implementation of implementAnyDataTypeWithIndex that skips date type
     * conversion.
     */
    private FunctionSignature implementAnyDataTypeWithIndexSkipDateConversion(
            FunctionSignature signature, List<Expression> arguments) {
        // Collect all any data type with index
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

        // Handle all null type index
        Set<Integer> allNullTypeIndex = Sets.newHashSetWithExpectedSize(indexToArgumentTypes.size());
        for (Map.Entry<Integer, List<DataType>> entry : indexToArgumentTypes.entrySet()) {
            boolean allIsNullType = entry.getValue().stream().allMatch(dt -> dt instanceof NullType);
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

        // Get common types - reuse ComputeSignatureHelper logic
        for (Map.Entry<Integer, List<DataType>> dataTypes : indexToArgumentTypes.entrySet()) {
            Optional<DataType> dataType = org.apache.doris.nereids.util.TypeCoercionUtils
                    .findWiderCommonType(dataTypes.getValue(), false, true);
            indexToCommonTypes.put(dataTypes.getKey(), dataType);
        }

        // Replace any data type - skip date conversion
        List<DataType> newArgTypes = Lists.newArrayListWithCapacity(signature.argumentsTypes.size());
        for (DataType sigType : signature.argumentsTypes) {
            newArgTypes.add(replaceAnyDataType(sigType, indexToCommonTypes));
        }
        signature = signature.withArgumentTypes(signature.hasVarArgs, newArgTypes);
        DataType returnType = replaceAnyDataType(signature.returnType, indexToCommonTypes);
        return signature.withReturnType(returnType);
    }

    // below are copied from ComputeSignatureHelper,the reason to implement here is
    // they are static function in original
    // Helper methods copied from ComputeSignatureHelper
    private static DataType replaceAnyDataTypeWithOutIndex(DataType sigType, DataType expressionType) {
        if (expressionType instanceof NullType) {
            if (sigType instanceof ArrayType) {
                return ArrayType.of(replaceAnyDataTypeWithOutIndex(
                        ((ArrayType) sigType).getItemType(), NullType.INSTANCE));
            } else if (sigType instanceof MapType) {
                return MapType.of(
                        replaceAnyDataTypeWithOutIndex(((MapType) sigType).getKeyType(), NullType.INSTANCE),
                        replaceAnyDataTypeWithOutIndex(((MapType) sigType).getValueType(), NullType.INSTANCE));
            } else if (sigType instanceof AnyDataType
                    && ((AnyDataType) sigType).getIndex() == AnyDataType.INDEX_OF_INSTANCE_WITHOUT_INDEX) {
                return expressionType;
            }
            return sigType;
        } else if (sigType instanceof ArrayType && expressionType instanceof ArrayType) {
            return ArrayType.of(replaceAnyDataTypeWithOutIndex(
                    ((ArrayType) sigType).getItemType(), ((ArrayType) expressionType).getItemType()));
        } else if (sigType instanceof MapType && expressionType instanceof MapType) {
            return MapType.of(
                    replaceAnyDataTypeWithOutIndex(
                            ((MapType) sigType).getKeyType(), ((MapType) expressionType).getKeyType()),
                    replaceAnyDataTypeWithOutIndex(
                            ((MapType) sigType).getValueType(), ((MapType) expressionType).getValueType()));
        } else if (sigType instanceof AnyDataType
                && ((AnyDataType) sigType).getIndex() == AnyDataType.INDEX_OF_INSTANCE_WITHOUT_INDEX) {
            return expressionType;
        }
        return sigType;
    }

    private static void collectAnyDataType(DataType sigType, DataType expressionType,
            Map<Integer, List<DataType>> indexToArgumentTypes) {
        if (expressionType instanceof NullType) {
            if (sigType instanceof ArrayType) {
                collectAnyDataType(((ArrayType) sigType).getItemType(), NullType.INSTANCE, indexToArgumentTypes);
            } else if (sigType instanceof MapType) {
                collectAnyDataType(((MapType) sigType).getKeyType(), NullType.INSTANCE, indexToArgumentTypes);
                collectAnyDataType(((MapType) sigType).getValueType(), NullType.INSTANCE, indexToArgumentTypes);
            } else if (sigType instanceof AnyDataType && ((AnyDataType) sigType).getIndex() >= 0) {
                List<DataType> dataTypes = indexToArgumentTypes.computeIfAbsent(
                        ((AnyDataType) sigType).getIndex(), i -> Lists.newArrayList());
                dataTypes.add(expressionType);
            }
        } else if (sigType instanceof ArrayType && expressionType instanceof ArrayType) {
            collectAnyDataType(((ArrayType) sigType).getItemType(),
                    ((ArrayType) expressionType).getItemType(), indexToArgumentTypes);
        } else if (sigType instanceof MapType && expressionType instanceof MapType) {
            collectAnyDataType(((MapType) sigType).getKeyType(),
                    ((MapType) expressionType).getKeyType(), indexToArgumentTypes);
            collectAnyDataType(((MapType) sigType).getValueType(),
                    ((MapType) expressionType).getValueType(), indexToArgumentTypes);
        } else if (sigType instanceof AnyDataType && ((AnyDataType) sigType).getIndex() >= 0) {
            List<DataType> dataTypes = indexToArgumentTypes.computeIfAbsent(
                    ((AnyDataType) sigType).getIndex(), i -> Lists.newArrayList());
            dataTypes.add(expressionType);
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
            } else if (sigType instanceof FollowToAnyDataType
                    && allNullTypeIndex.contains(((FollowToAnyDataType) sigType).getIndex())) {
                List<DataType> dataTypes = indexToArgumentTypes.computeIfAbsent(
                        ((FollowToAnyDataType) sigType).getIndex(), i -> Lists.newArrayList());
                dataTypes.add(expressionType);
            }
        } else if (sigType instanceof ArrayType && expressionType instanceof ArrayType) {
            collectFollowToAnyDataType(((ArrayType) sigType).getItemType(),
                    ((ArrayType) expressionType).getItemType(), indexToArgumentTypes, allNullTypeIndex);
        } else if (sigType instanceof MapType && expressionType instanceof MapType) {
            collectFollowToAnyDataType(((MapType) sigType).getKeyType(),
                    ((MapType) expressionType).getKeyType(), indexToArgumentTypes, allNullTypeIndex);
            collectFollowToAnyDataType(((MapType) sigType).getValueType(),
                    ((MapType) expressionType).getValueType(), indexToArgumentTypes, allNullTypeIndex);
        } else if (sigType instanceof FollowToAnyDataType
                && allNullTypeIndex.contains(((FollowToAnyDataType) sigType).getIndex())) {
            List<DataType> dataTypes = indexToArgumentTypes.computeIfAbsent(
                    ((FollowToAnyDataType) sigType).getIndex(), i -> Lists.newArrayList());
            dataTypes.add(expressionType);
        }
    }

    private static DataType replaceAnyDataType(DataType dataType,
            Map<Integer, Optional<DataType>> indexToCommonTypes) {
        if (dataType instanceof ArrayType) {
            return ArrayType.of(replaceAnyDataType(((ArrayType) dataType).getItemType(), indexToCommonTypes));
        } else if (dataType instanceof MapType) {
            return MapType.of(
                    replaceAnyDataType(((MapType) dataType).getKeyType(), indexToCommonTypes),
                    replaceAnyDataType(((MapType) dataType).getValueType(), indexToCommonTypes));
        } else if (dataType instanceof AnyDataType && ((AnyDataType) dataType).getIndex() >= 0) {
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
