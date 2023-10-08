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
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.trees.expressions.functions.ComputeSignatureHelper.ComputeSignatureChain;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;
import java.util.function.BiFunction;

/**
 * this class is usage to compute function's return type by the argument's type.
 * in most cases, you should extends BoundFunction and implement some child interfaces of
 * ComputeSignature(usually is ExplicitlyCastableSignature) and supply the signatures.
 */
@Developing
public interface ComputeSignature extends FunctionTrait, ImplicitCastInputTypes {
    ///// current interface's methods /////

    // the signatures which you should supply as compute source
    List<FunctionSignature> getSignatures();

    // this method cache from the searchSignature method and implement by BoundFunction.getSignature().
    // usually, it is cache version of searchSignature().
    FunctionSignature getSignature();

    /**
     * find signature by the arguments. this method will be invoked in the BoundFunction.getSignature(),
     * which BoundFunction instanceof ComputeSignature.
     *
     * @return the matched signature
     */
    FunctionSignature searchSignature(List<FunctionSignature> signatures);

    ///// re-defined other interface's methods, so we can mixin this interfaces like a trait /////

    // get function name, re-define getName method in BoundFunction
    default String getName() {
        return getClass().getSimpleName();
    }

    ///// override expressions trait methods, so we can compute some properties by the signature /////

    /**
     * compute expectedInputTypes from the signature's argumentsTypes
     * @return expectedInputTypes
     */
    @Override
    default List<DataType> expectedInputTypes() {
        FunctionSignature signature = getSignature();
        int arity = arity();
        if (signature.hasVarArgs && arity > signature.arity) {
            Builder<DataType> varTypes = ImmutableList.<DataType>builder()
                    .addAll(signature.argumentsTypes);
            DataType varType = signature.getVarArgType().get();
            for (int i = signature.arity; i < arity; ++i) {
                varTypes.add(varType);
            }
            return varTypes.build();
        }
        return signature.argumentsTypes;
    }

    /**
     * find function's return type by the signature.
     * @return DataType
     */
    @Override
    default DataType getDataType() {
        return getSignature().returnType;
    }

    @Override
    default boolean hasVarArguments() {
        return getSignature().hasVarArgs;
    }

    /** default computeSignature */
    default FunctionSignature computeSignature(FunctionSignature signature) {
        // NOTE:
        // this computed chain only process the common cases.
        // If you want to add some common cases to here, please separate the process code
        // to the other methods and add to this chain.
        // If you want to add some special cases, please override this method in the special
        // function class, like 'If' function and 'Substring' function.
        return ComputeSignatureChain.from(this, signature, getArguments())
                .then(ComputeSignatureHelper::implementAnyDataTypeWithIndex)
                .then(ComputeSignatureHelper::computePrecision)
                .then(ComputeSignatureHelper::implementFollowToArgumentReturnType)
                .then(ComputeSignatureHelper::normalizeDecimalV2)
                .then(ComputeSignatureHelper::dynamicComputePropertiesOfArray)
                .get();
    }

    /** use processor to process computeSignature */
    static boolean processComplexType(DataType signatureType, DataType realType,
            BiFunction<DataType, DataType, Boolean> processor) {

        if (signatureType instanceof ArrayType && realType instanceof ArrayType) {
            return processor.apply(((ArrayType) signatureType).getItemType(),
                    ((ArrayType) realType).getItemType());
        } else if (signatureType instanceof MapType && realType instanceof MapType) {
            return processor.apply(((MapType) signatureType).getKeyType(), ((MapType) realType).getKeyType())
                    && processor.apply(((MapType) signatureType).getValueType(), ((MapType) realType).getValueType());
        } else if (signatureType instanceof StructType && realType instanceof StructType) {
            // TODO: do not support struct type now
            // throw new AnalysisException("do not support struct type now");
            return true;
        } else {
            return processor.apply(signatureType, realType);
        }
    }
}
