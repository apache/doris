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
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeV2Literal;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TimeV2Type;

/**
 * This is to ensure second('12:34:56') prioritizes matching time type,
 * ensuring time string literals can correctly handle precision.
 * if the first argument is string like literal and could cast to legal time
 * literal,
 * then use TimeType signature rather than DatetimeV2 signature(usually in
 * SIGNATURES).
 * but won't add any signatures to SIGNATURES. normally there should be a
 * TimeType signature in SIGNATURES.
 */
@Developing
public interface ComputeSignatureForSingleTimeArithmetic extends ComputeSignature {

    @Override
    default FunctionSignature computeSignature(FunctionSignature signature) {
        if (child(0) instanceof StringLikeLiteral) {
            try {
                String s = ((StringLikeLiteral) child(0)).getStringValue().trim();
                if (isTimeFormat(s)) {
                    new TimeV2Literal(s); // check legality
                    TimeV2Type t = TimeV2Type.forTypeFromString(s);
                    int scale = t.getScale();
                    return FunctionSignature.ret(IntegerType.INSTANCE).args(TimeV2Type.of(scale));
                }

            } catch (Exception e) {
                // string like literal cannot cast to a legal time/timeV2 literal
            }
        }
        // Call the parent implementation for non-time formats
        return ComputeSignature.super.computeSignature(signature);
    }

    /**
     * Check if the string is in a valid time format.
     */
    default boolean isTimeFormat(String s) {
        if (s == null || s.isEmpty()) {
            return false;
        }

        s = s.trim();
        if (s.startsWith("+") || s.startsWith("-")) {
            return true;
        }

        if (s.contains("-") || s.contains("/") || s.contains(" ") || s.contains("T")) {
            return false;
        }

        if (s.contains(":")) {
            return isColonTimeFormat(s);
        } else {
            return isNumericTimeFormat(s);
        }
    }

    /**
     * Check if the string is in HH:MM[:SS[.FFFFFF]] format.
     */
    default boolean isColonTimeFormat(String s) {
        String[] parts = s.split("\\.", 2);
        String timePart = parts[0];

        /*
         * Split time part, first part is two length we define it is time type, such as
         * 12:12:12
         * datetime such as 2023:12:12 12:12:12 length is 19,time part length is <= 15
         */
        String[] timeFields = timePart.split(":");
        if ((timeFields[0].length() == 2 && timeFields[1].length() <= 15) || timeFields[0].length() == 3
                || timePart.length() < 8) {
            return true;
        }

        return false;
    }

    /**
     * Check if the string is in numeric format: continuous digits [ .FFFFFF ]
     */
    default boolean isNumericTimeFormat(String s) {
        String[] parts = s.split("\\.", 2);
        String numberPart = parts[0];

        int length = numberPart.length();
        if (length <= 7) {
            return true;
        }
        return false;
    }
}
