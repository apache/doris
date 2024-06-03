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
import org.apache.doris.nereids.analyzer.ComplexDataType;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;
import org.apache.doris.qe.SessionVariable;

import java.util.List;

/**
 * Implicitly castable function signature. This class equals to 'CompareMode.IS_SUPERTYPE_OF'.
 *
 * X is a supertype of Y if Y.arg[i] can be strictly implicitly cast to X.arg[i]. If
 * X has vargs, the remaining arguments of Y must be strictly implicitly castable
 */
public interface ImplicitlyCastableSignature extends ComputeSignature {

    static boolean isImplicitlyCastable(DataType signatureType, DataType realType) {
        return ComputeSignature.processComplexType(
                signatureType, realType, ImplicitlyCastableSignature::isPrimitiveImplicitlyCastable);
    }

    /** isImplicitlyCastable */
    static boolean isPrimitiveImplicitlyCastable(DataType signatureType, DataType realType) {
        if (signatureType instanceof AnyDataType
                || signatureType instanceof FollowToAnyDataType
                || signatureType.isAssignableFrom(realType)) {
            return true;
        }
        if (realType instanceof NullType) {
            return true;
        }
        if (signatureType instanceof ComplexDataType && !(realType instanceof ComplexDataType)) {
            return false;
        }
        try {
            // TODO: copy isImplicitlyCastable method to DataType
            // TODO: resolve AnyDataType invoke toCatalogDataType
            if (signatureType instanceof ArrayType) {
                if (((ArrayType) signatureType).getItemType() instanceof AnyDataType) {
                    return false;
                }
            }
            if (Type.isImplicitlyCastable(realType.toCatalogDataType(), signatureType.toCatalogDataType(), true,
                    SessionVariable.getEnableDecimal256())) {
                return true;
            }
        } catch (Throwable t) {
            // the signatureType maybe DataType and can not cast to catalog data type.
        }
        try {
            List<DataType> allPromotions = realType.getAllPromotions();
            for (DataType promotion : allPromotions) {
                if (isImplicitlyCastable(signatureType, promotion)) {
                    return true;
                }
            }
        } catch (Throwable t) {
            // the signatureType maybe DataType and can not cast to catalog data type.
        }
        return false;
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
                .resultOrException(getName());
    }
}
