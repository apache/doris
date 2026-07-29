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

import com.fasterxml.jackson.core.io.schubfach.DoubleToDecimal;
import com.fasterxml.jackson.core.io.schubfach.FloatToDecimal;

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
        return format(FloatToDecimal.toString(value), Float.floatToRawIntBits(value), true);
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
        return format(DoubleToDecimal.toString(value), Double.doubleToRawLongBits(value), false);
    }

    private static String format(String value, long rawBits, boolean singlePrecision) {
        boolean negative = value.charAt(0) == '-';
        int mantissaStart = negative ? 1 : 0;
        int exponentMarker = value.indexOf('E', mantissaStart);
        int mantissaEnd = exponentMarker < 0 ? value.length() : exponentMarker;
        int decimalPoint = value.indexOf('.', mantissaStart);

        int firstSignificantDigit = mantissaStart;
        while (value.charAt(firstSignificantDigit) == '0'
                || value.charAt(firstSignificantDigit) == '.') {
            firstSignificantDigit++;
        }

        int exponent = exponentMarker < 0
                ? decimalExponent(firstSignificantDigit, decimalPoint)
                : parseExponent(value, exponentMarker + 1);

        StringBuilder digits = new StringBuilder(17);
        for (int i = firstSignificantDigit; i < mantissaEnd; i++) {
            char c = value.charAt(i);
            if (c != '.') {
                digits.append(c);
            }
        }
        while (digits.length() > 1 && digits.charAt(digits.length() - 1) == '0') {
            digits.setLength(digits.length() - 1);
        }
        exponent = shortenToOneDigit(digits, exponent, negative, rawBits, singlePrecision);

        StringBuilder result = new StringBuilder(24);
        if (negative) {
            result.append('-');
        }
        if (exponent >= -4 && exponent < 16) {
            appendPlainString(result, digits, exponent);
        } else {
            appendScientificString(result, digits, exponent);
        }
        return result.toString();
    }

    private static int shortenToOneDigit(StringBuilder digits, int exponent, boolean negative,
            long rawBits, boolean singlePrecision) {
        // Schubfach follows Java's requirement to emit at least two significant digits. fmt
        // instead emits the nearest representation with the minimum number of digits, for
        // example 5e-324 instead of 4.9e-324. Only a two-digit result can therefore become
        // shorter. This compatibility check performs at most one reparse, rather than a
        // precision-search loop for every value.
        if (digits.length() != 2) {
            return exponent;
        }

        int firstDigit = digits.charAt(0) - '0';
        int secondDigit = digits.charAt(1) - '0';
        if (secondDigit > 5 || (secondDigit == 5 && (firstDigit & 1) != 0)) {
            firstDigit++;
        }
        int candidateExponent = exponent;
        if (firstDigit == 10) {
            firstDigit = 1;
            candidateExponent++;
        }

        StringBuilder candidate = new StringBuilder(8);
        if (negative) {
            candidate.append('-');
        }
        candidate.append(firstDigit).append('e').append(candidateExponent);
        boolean roundTrips = singlePrecision
                ? Float.floatToRawIntBits(Float.parseFloat(candidate.toString())) == (int) rawBits
                : Double.doubleToRawLongBits(Double.parseDouble(candidate.toString())) == rawBits;
        if (roundTrips) {
            digits.setLength(1);
            digits.setCharAt(0, (char) ('0' + firstDigit));
            return candidateExponent;
        }
        return exponent;
    }

    private static int decimalExponent(int firstSignificantDigit, int decimalPoint) {
        if (firstSignificantDigit < decimalPoint) {
            return decimalPoint - firstSignificantDigit - 1;
        }
        return decimalPoint - firstSignificantDigit;
    }

    private static int parseExponent(String value, int offset) {
        boolean negative = value.charAt(offset) == '-';
        if (negative || value.charAt(offset) == '+') {
            offset++;
        }
        int exponent = 0;
        for (int i = offset; i < value.length(); i++) {
            exponent = exponent * 10 + value.charAt(i) - '0';
        }
        return negative ? -exponent : exponent;
    }

    private static void appendPlainString(StringBuilder result, StringBuilder digits, int exponent) {
        if (exponent < 0) {
            result.append("0.");
            for (int i = -1; i > exponent; i--) {
                result.append('0');
            }
            result.append(digits);
            return;
        }

        int integralDigits = exponent + 1;
        if (digits.length() <= integralDigits) {
            result.append(digits);
            for (int i = digits.length(); i < integralDigits; i++) {
                result.append('0');
            }
            return;
        }

        result.append(digits, 0, integralDigits)
                .append('.')
                .append(digits, integralDigits, digits.length());
    }

    private static void appendScientificString(StringBuilder result, StringBuilder digits,
            int exponent) {
        result.append(digits.charAt(0));
        if (digits.length() > 1) {
            result.append('.').append(digits, 1, digits.length());
        }
        result.append(exponent < 0 ? "e-" : "e+");
        int absoluteExponent = Math.abs(exponent);
        if (absoluteExponent < 10) {
            result.append('0');
        }
        result.append(absoluteExponent);
    }
}
