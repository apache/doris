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

package org.apache.doris.common;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * Util class for float/double to string.
 */
public class FractionalFormat {

    /**
     * Get the shortest string that round-trips to the given float value.
     *
     * @param value The float value.
     * @return string value.
     */
    public static String getFormatStringValue(float value) {
        if (Float.isNaN(value)) {
            return "NaN";
        }
        if (Float.isInfinite(value)) {
            return value > 0 ? "Infinity" : "-Infinity";
        }
        if (value == 0) {
            return Float.floatToRawIntBits(value) < 0 ? "-0" : "0";
        }
        BigDecimal exactValue = new BigDecimal(value);
        int bits = Float.floatToRawIntBits(value);
        for (int precision = 1; precision < 9; precision++) {
            BigDecimal candidate = exactValue.round(new MathContext(precision, RoundingMode.HALF_EVEN));
            if (Float.floatToRawIntBits(Float.parseFloat(candidate.toString())) == bits) {
                return format(candidate);
            }
        }
        return format(exactValue.round(new MathContext(9, RoundingMode.HALF_EVEN)));
    }

    /**
     * Get the shortest string that round-trips to the given double value.
     *
     * @param value The double value.
     * @return string value.
     */
    public static String getFormatStringValue(double value) {
        if (Double.isNaN(value)) {
            return "NaN";
        }
        if (Double.isInfinite(value)) {
            return value > 0 ? "Infinity" : "-Infinity";
        }
        if (value == 0) {
            return Double.doubleToRawLongBits(value) < 0 ? "-0" : "0";
        }
        BigDecimal exactValue = new BigDecimal(value);
        long bits = Double.doubleToRawLongBits(value);
        for (int precision = 1; precision < 17; precision++) {
            BigDecimal candidate = exactValue.round(new MathContext(precision, RoundingMode.HALF_EVEN));
            if (Double.doubleToRawLongBits(Double.parseDouble(candidate.toString())) == bits) {
                return format(candidate);
            }
        }
        return format(exactValue.round(new MathContext(17, RoundingMode.HALF_EVEN)));
    }

    private static String format(BigDecimal value) {
        BigDecimal normalized = value.stripTrailingZeros();
        int exponent = normalized.precision() - normalized.scale() - 1;
        if (exponent >= -4 && exponent < 16) {
            return normalized.toPlainString();
        }

        String digits = normalized.unscaledValue().abs().toString();
        StringBuilder result = new StringBuilder(digits.length() + 7);
        if (normalized.signum() < 0) {
            result.append('-');
        }
        result.append(digits.charAt(0));
        if (digits.length() > 1) {
            result.append('.').append(digits, 1, digits.length());
        }
        result.append(exponent < 0 ? "e-" : "e+");
        int absoluteExponent = Math.abs(exponent);
        if (absoluteExponent < 10) {
            result.append('0');
        }
        return result.append(absoluteExponent).toString();
    }
}
