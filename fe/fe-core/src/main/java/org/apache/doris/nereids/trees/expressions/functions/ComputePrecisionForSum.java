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
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.qe.ConnectContext;

/** ComputePrecisionForSum */
public interface ComputePrecisionForSum extends ComputePrecision {
    @Override
    default FunctionSignature computePrecision(FunctionSignature signature) {
        DataType argumentType = getArgumentType(0);
        if (signature.getArgType(0) instanceof DecimalV3Type) {
            DecimalV3Type decimalV3Type = DecimalV3Type.forType(argumentType);
            boolean enableDecimal256 = false;
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext != null) {
                enableDecimal256 = connectContext.getSessionVariable().enableDecimal256();
            }
            return signature.withArgumentType(0, decimalV3Type)
                    .withReturnType(DecimalV3Type.createDecimalV3Type(
                            enableDecimal256 ? DecimalV3Type.MAX_DECIMAL256_PRECISION
                                    : DecimalV3Type.MAX_DECIMAL128_PRECISION,
                            decimalV3Type.getScale()));
        } else {
            return signature;
        }
    }
}
