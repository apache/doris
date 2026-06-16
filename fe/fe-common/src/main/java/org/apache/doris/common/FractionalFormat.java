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
import java.math.RoundingMode;

/**
 * Util class for float/double to string.
 */
public class FractionalFormat {

    // FLT_DIG + 1; precision <= this value means float-equivalent rendering.
    private static final int FLOAT_MAX_SIGNIFICANT_DIGITS = 7;

    /**
     * Get string of double/float value for cast to string and output to mysql.
     *
     * @param value The double/float value.
     * @param precision precision (use {@code <= FLOAT_MAX_SIGNIFICANT_DIGITS} for float).
     * @param sciFormat format for string with scientific form.
     * @return string value.
     */
    public static String getFormatStringValue(double value, int precision, String sciFormat) {
        if (Double.isNaN(value)) {
            return "NaN";
        }
        if (Double.isInfinite(value)) {
            return value > 0 ? "Infinity" : "-Infinity";
        }
        if (Double.compare(value, 0.0) == 0) {
            return "0";
        }
        if (Double.compare(value, -0.0) == 0) {
            return "-0";
        }
        if (precision <= FLOAT_MAX_SIGNIFICANT_DIGITS) {
            return formatFixedPrecision(value, precision, sciFormat);
        }
        return formatShortest(value, precision);
    }

    private static String formatFixedPrecision(double value, int precision, String sciFormat) {
        int expLower = -4;
        int exponent = (int) Math.floor(Math.log10(Math.abs(value)));
        if (exponent < precision && exponent >= expLower) {
            BigDecimal bd = new BigDecimal(value);
            bd = bd.setScale(precision - bd.precision() + bd.scale(), RoundingMode.HALF_UP);
            String result = bd.toPlainString();
            if (result.contains(".")) {
                result = result.replaceAll("0+$", "");
                if (result.endsWith(".")) {
                    result = result.substring(0, result.length() - 1);
                }
            }
            return result;
        }
        return String.format(sciFormat, value).replaceAll("(\\.\\d*?[1-9])0*E", "$1E")
                .replaceAll("\\.0*E", "E").replaceAll("E", "e");
    }

    private static String formatShortest(double value, int precision) {
        // shortest round-trip; new BigDecimal(double) would capture the exact
        // IEEE-754 value and setScale(16,HALF_UP) would expose its tail,
        // e.g. round(23900/293, 2) -> "81.56999999999999".
        BigDecimal bd = new BigDecimal(Double.toString(value)).stripTrailingZeros();
        int expLower = -4;
        int exponent = (int) Math.floor(Math.log10(Math.abs(value)));
        if (exponent < precision && exponent >= expLower) {
            String result = bd.toPlainString();
            if (result.contains(".")) {
                result = result.replaceAll("0+$", "");
                if (result.endsWith(".")) {
                    result = result.substring(0, result.length() - 1);
                }
            }
            return result;
        }
        return formatScientific(bd);
    }

    /** Format the BigDecimal as "<sig>.<digits>e[+|-]NN" with at least two exponent digits. */
    private static String formatScientific(BigDecimal bd) {
        String unscaled = bd.unscaledValue().abs().toString();
        int sig = unscaled.length();
        int exp = (sig - 1) - bd.scale();
        StringBuilder sb = new StringBuilder(sig + 6);
        if (bd.signum() < 0) {
            sb.append('-');
        }
        sb.append(unscaled.charAt(0));
        if (sig > 1) {
            sb.append('.');
            sb.append(unscaled, 1, sig);
        }
        sb.append('e');
        if (exp >= 0) {
            sb.append('+');
        } else {
            sb.append('-');
            exp = -exp;
        }
        if (exp < 10) {
            sb.append('0');
        }
        sb.append(exp);
        return sb.toString();
    }
}
