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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;

/**
 * When reading an AGG_STATE column created before
 * the result type {@code org.apache.doris.catalog.AggStateType#resultType} was persisted,
 * reconstructs the result type of the nested aggregate as the old BE computed it.
 */
public interface LegacyAggStateResultTypeResolver {
    /**
     * Resolves the missing result type of a legacy AGG_STATE column.
     * @param storedInput the argument type recorded in the column
     * @return the result type the old BE computed for the nested aggregate function
     */
    DataType resolveLegacyAggStateResultType(DataType storedInput);

    /** The legacy BE used the maximum precision of the stored map value's decimal width. */
    static int legacyMapValuePrecision(DecimalV3Type valueType) {
        int precision = valueType.getPrecision();
        if (precision <= DecimalV3Type.MAX_DECIMAL32_PRECISION) {
            return DecimalV3Type.MAX_DECIMAL32_PRECISION;
        }
        if (precision <= DecimalV3Type.MAX_DECIMAL64_PRECISION) {
            return DecimalV3Type.MAX_DECIMAL64_PRECISION;
        }
        if (precision <= DecimalV3Type.MAX_DECIMAL128_PRECISION) {
            return DecimalV3Type.MAX_DECIMAL128_PRECISION;
        }
        return DecimalV3Type.MAX_DECIMAL256_PRECISION;
    }
}
