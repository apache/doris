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
import org.apache.doris.nereids.analyzer.ComplexDataType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;

import java.util.List;

/**
 * Identical function signature. This class equals to 'CompareMode.IS_IDENTICAL'.
 *
 * Two signatures are identical if the number of arguments and their types match
 * exactly and signature isn't varargs.
 */
public interface IdenticalSignature extends ComputeSignature {
    /** isIdentical */
    static boolean isIdentical(DataType signatureType, DataType realType) {
        return ComputeSignature.processComplexType(
                signatureType, realType, IdenticalSignature::isPrimitiveIdentical);
    }

    /** isIdentical */
    static boolean isPrimitiveIdentical(DataType signatureType, DataType realType) {
        try {
            // TODO: copy matchesType to DataType
            // TODO: resolve AnyDataType invoke toCatalogDataType
            if (signatureType instanceof AnyDataType || signatureType instanceof FollowToAnyDataType) {
                return false;
            }
            if (signatureType instanceof ComplexDataType && !(realType instanceof ComplexDataType)) {
                return false;
            }
            return realType.toCatalogDataType().matchesType(signatureType.toCatalogDataType());
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
                .resultOrException(getName());
    }
}
