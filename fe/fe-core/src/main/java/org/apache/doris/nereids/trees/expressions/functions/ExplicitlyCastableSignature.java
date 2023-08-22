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
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;

import java.util.List;

/**
 * Explicitly castable signature. This class equals to 'CompareMode.IS_NONSTRICT_SUPERTYPE_OF'.
 *
 * Non-strict supertypes broaden the definition of supertype to accept implicit casts
 * of arguments that may result in loss of precision - e.g. decimal to float.
 */
public interface ExplicitlyCastableSignature extends ComputeSignature {

    static boolean isExplicitlyCastable(DataType signatureType, DataType realType) {
        return ComputeSignature.processComplexType(
                signatureType, realType, ExplicitlyCastableSignature::isPrimitiveExplicitlyCastable);
    }

    /** isExplicitlyCastable */
    static boolean isPrimitiveExplicitlyCastable(DataType signatureType, DataType realType) {
        if (signatureType instanceof AnyDataType
                || signatureType instanceof FollowToAnyDataType
                || signatureType.isAssignableFrom(realType)) {
            return true;
        }
        if (realType instanceof NullType) {
            return true;
        }
        try {
            // TODO: copy canCastTo method to DataType
            return Type.canCastTo(realType.toCatalogDataType(), signatureType.toCatalogDataType());
        } catch (Throwable t) {
            // the signatureType maybe DataType and can not cast to catalog data type.
            return false;
        }
    }

    @Override
    default FunctionSignature searchSignature(List<FunctionSignature> signatures) {
        return SearchSignature.from(this, signatures, getArguments())
                // first round, use identical strategy to find signature
                .orElseSearch(IdenticalSignature::isIdentical)
                // second round: if not found, use nullOrIdentical strategy
                .orElseSearch(NullOrIdenticalSignature::isNullOrIdentical)
                // third round: if second round not found, use implicitlyCastable strategy
                .orElseSearch(ImplicitlyCastableSignature::isImplicitlyCastable)
                // fourth round: if third round not found, use explicitlyCastable strategy
                .orElseSearch(ExplicitlyCastableSignature::isExplicitlyCastable)
                .resultOrException(getName());
    }
}
