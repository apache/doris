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
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;

import java.util.List;

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
    FunctionSignature searchSignature();

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
    default List<AbstractDataType> expectedInputTypes() {
        return (List) getSignature().argumentsTypes;
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
}
