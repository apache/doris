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
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;

/** ComputePrecisionForSum */
public interface ComputePrecisionForArrayItemAgg extends ComputePrecision {
    @Override
    default FunctionSignature computePrecision(FunctionSignature signature) {
        if (getArgumentType(0) instanceof ArrayType) {
            DataType itemType = ((ArrayType) getArgument(0).getDataType()).getItemType();
            if (itemType instanceof DecimalV3Type) {
                DecimalV3Type returnType = DecimalV3Type.createDecimalV3Type(
                        DecimalV3Type.MAX_DECIMAL128_PRECISION, ((DecimalV3Type) itemType).getScale());
                if (signature.returnType instanceof ArrayType) {
                    signature = signature.withReturnType(ArrayType.of(returnType));
                } else {
                    signature = signature.withReturnType(returnType);
                }
            }
        }
        return signature;
    }
}
