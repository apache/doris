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
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;

/**
 * use for literal in date arithmetic with interval >= 'day', such as date_sub('2024-05-28', INTERVAL 1 day).
 * if the first argument is string like literal and could cast to legal date literal,
 * then use DateV2 signature rather than DatetimeV2 signature(usually in SIGNATURES).
 * but won't add any signatures to SIGNATURES. normally there shou be a DateV2 signature in SIGNATURES.
 */
@Developing
public interface ComputeSignatureForDateArithmetic extends ComputeSignature {

    @Override
    default FunctionSignature computeSignature(FunctionSignature signature) {
        FunctionSignature ret = ComputeSignature.super.computeSignature(signature);
        if (child(0) instanceof StringLikeLiteral) {
            try {
                String s = ((StringLikeLiteral) child(0)).getStringValue().trim();
                // avoid use date/dateV2 signature for '2020-02-02 00:00:00'
                if (s.length() <= 10) {
                    new DateV2Literal(s); // check legality
                    return FunctionSignature.ret(DateV2Type.INSTANCE).args(DateV2Type.INSTANCE, IntegerType.INSTANCE);
                }
            } catch (Exception e) {
                // string like literal cannot cast to a legal date/dateV2 literal
            }
        }
        return ret;
    }
}
