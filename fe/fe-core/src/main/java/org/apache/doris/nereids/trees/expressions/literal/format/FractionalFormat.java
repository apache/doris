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

package org.apache.doris.nereids.trees.expressions.literal.format;

import com.fasterxml.jackson.core.io.schubfach.DoubleToDecimal;
import com.fasterxml.jackson.core.io.schubfach.FloatToDecimal;

/**
 * Converts floating-point values to the string representation used by Doris.
 *
 * <p>This is deliberately not just a wrapper around {@link Float#toString(float)} or
 * {@link Double#toString(double)}. FE constant folding and BE expression evaluation must produce
 * the same text. BE uses fmt's shortest-round-trip conversion and Doris keeps values whose decimal
 * exponent is in [-4, 16) in fixed notation. Java uses different notation boundaries and requires
 * at least two significant digits in a few cases.
 *
 * <p>The conversion is therefore split into two stages:
 * <ol>
 *   <li>Jackson's Schubfach implementation computes round-tripping decimal digits with bounded
 *       work and no former BigDecimal precision-search loop. The compatibility step below removes
 *       Java's required second digit when fmt can represent the value with one digit.</li>
 *   <li>{@link #format(String, long, boolean)} normalizes that decimal to the spelling used by BE:
 *       fixed/scientific notation boundaries, lower-case {@code e}, an explicit exponent sign,
 *       at least two exponent digits, and no redundant fractional zeroes.</li>
 * </ol>
 *
 * <p>NaN, infinities, and signed zero are handled before Schubfach because they have Doris-specific
 * spellings and signed zero must retain its raw sign bit.
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
        // Keep the raw bits so format() can verify the only compatibility candidate that
        // Schubfach intentionally does not emit under Java's formatting rules.
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
        // Keep the raw bits so format() can verify the only compatibility candidate that
        // Schubfach intentionally does not emit under Java's formatting rules.
        return format(DoubleToDecimal.toString(value), Double.doubleToRawLongBits(value), false);
    }

    /**
     * Rewrites a Schubfach result into the notation used by the BE formatter.
     *
     * <p>{@code digits} below is the significand with its decimal point and redundant trailing
     * zeroes removed. {@code exponent} is the decimal exponent of its first digit. Consequently,
     * the represented magnitude is:
     *
     * <pre>
     * integer(digits) * 10 ^ (exponent - digits.length() + 1)
     * </pre>
     *
     * <p>For example, both {@code 0.00123} and {@code 1.23E-3} become
     * {@code digits = "123"} and {@code exponent = -3}. Keeping this canonical pair makes the
     * final choice between fixed and scientific notation independent of the notation selected by
     * Jackson.
     *
     * @param value shortest decimal emitted by Jackson's Schubfach converter
     * @param rawBits original IEEE-754 bits, used to validate a possible one-digit representation
     * @param singlePrecision whether {@code rawBits} represents a float rather than a double
     * @return the equivalent decimal using Doris/BE formatting conventions
     */
    private static String format(String value, long rawBits, boolean singlePrecision) {
        // Schubfach emits either [-]d.ddd or [-]d.dddE[+-]dd. Locate the sign, significand,
        // decimal point, and optional exponent without constructing intermediate substrings.
        boolean negative = value.charAt(0) == '-';
        int mantissaStart = negative ? 1 : 0;
        int exponentMarker = value.indexOf('E', mantissaStart);
        int mantissaEnd = exponentMarker < 0 ? value.length() : exponentMarker;
        int decimalPoint = value.indexOf('.', mantissaStart);

        // Plain small values can start with "0.00...". Skip both those leading zeroes and the
        // decimal point so that digits always starts at the first non-zero significant digit.
        int firstSignificantDigit = mantissaStart;
        while (value.charAt(firstSignificantDigit) == '0'
                || value.charAt(firstSignificantDigit) == '.') {
            firstSignificantDigit++;
        }

        // Scientific input already carries the exponent of its first digit. For plain input,
        // derive the same exponent from the positions of the first digit and decimal point.
        int exponent = exponentMarker < 0
                ? decimalExponent(firstSignificantDigit, decimalPoint)
                : parseExponent(value, exponentMarker + 1);

        // Copy only significant digits. Removing trailing zeroes is safe because exponent denotes
        // the position of the first digit, so it changes neither the magnitude nor decimal point.
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

        // Match the BE fmt policy: decimal exponents -4 through 15 use fixed notation; all other
        // exponents use scientific notation. Examples are 0.0001, 1e-05, 10^15, and 1e+16.
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
        /*
         * Jackson's Schubfach implementation follows Java's rule that a finite non-zero result has
         * at least two significant digits. fmt instead minimizes the digit count whenever the
         * shorter decimal still maps to the same binary value. For example:
         *
         *   Jackson: Double.MIN_VALUE -> 4.9E-324
         *   BE fmt:  Double.MIN_VALUE -> 5e-324
         *
         * A result with three or more digits cannot be affected by this minimum-two-digits rule,
         * and a one-digit result is already minimal. Therefore only a two-digit significand needs
         * one compatibility check.
         *
         * First round the two-digit decimal to its nearest one-digit decimal using ties-to-even,
         * which is the IEEE-754 decimal selection rule. Rounding 9x may carry into the exponent
         * (for example, 9.9e-324 becomes 1e-323). The candidate is accepted only when parsing it
         * reproduces the original raw bits. Decimal equality is intentionally insufficient here:
         * distinct decimal numbers are expected to map to the same float or double inside a
         * rounding interval.
         *
         * This performs at most one parse and replaces the former loop that built and parsed up to
         * 16 BigDecimal candidates for every double cell.
         */
        if (digits.length() != 2) {
            return exponent;
        }

        // HALF_EVEN rounding from two significant digits to one significant digit.
        int firstDigit = digits.charAt(0) - '0';
        int secondDigit = digits.charAt(1) - '0';
        if (secondDigit > 5 || (secondDigit == 5 && (firstDigit & 1) != 0)) {
            firstDigit++;
        }
        int candidateExponent = exponent;
        // Normalize the carry from 10 * 10^exponent to 1 * 10^(exponent + 1).
        if (firstDigit == 10) {
            firstDigit = 1;
            candidateExponent++;
        }

        // Scientific notation avoids having to insert a variable number of zeroes. Java's parser
        // accepts an unpadded exponent, so this temporary string is only for the identity check.
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

    /**
     * Returns the exponent of the first significant digit in a plain decimal.
     *
     * <p>When that digit is before the decimal point, the exponent is the number of following
     * integral positions. When it is after the decimal point, the negative distance from the
     * decimal point gives the exponent. For example, {@code 123.4} returns 2 and {@code 0.001234}
     * returns -3.
     */
    private static int decimalExponent(int firstSignificantDigit, int decimalPoint) {
        if (firstSignificantDigit < decimalPoint) {
            return decimalPoint - firstSignificantDigit - 1;
        }
        return decimalPoint - firstSignificantDigit;
    }

    /**
     * Parses the signed exponent of a Schubfach scientific string without allocating a substring.
     */
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

    /**
     * Appends fixed notation from canonical significant digits and their first-digit exponent.
     *
     * <p>The three branches place the decimal point before the digits ({@code 0.00123}), after the
     * digits with appended zeroes ({@code 12300}), or inside the digits ({@code 123.45}).
     */
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

    /**
     * Appends normalized scientific notation such as {@code 1.23e-08} or {@code 1e+16}.
     *
     * <p>Doris follows BE fmt spelling: lower-case {@code e}, an explicit sign for positive
     * exponents, and a leading zero for one-digit exponents.
     */
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
