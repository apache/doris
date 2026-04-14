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

package org.apache.doris.analysis;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

public class DecimalLiteralUtilsTest {

    @Test
    void testNormalDecimal() throws AnalysisException {
        DecimalLiteral lit = DecimalLiteralUtils.create("1.5", false);
        Assertions.assertEquals(new BigDecimal("1.5"), lit.getValue());
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(2, type.getScalarPrecision());
        Assertions.assertEquals(1, type.getScalarScale());
    }

    @Test
    void testInteger() throws AnalysisException {
        DecimalLiteral lit = DecimalLiteralUtils.create("123", false);
        Assertions.assertEquals(0, new BigDecimal("123").compareTo(lit.getValue()));
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(3, type.getScalarPrecision());
        Assertions.assertEquals(0, type.getScalarScale());
    }

    @Test
    void testZero() throws AnalysisException {
        DecimalLiteral lit = DecimalLiteralUtils.create("0", false);
        Assertions.assertEquals(0, new BigDecimal("0").compareTo(lit.getValue()));
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(1, type.getScalarPrecision());
        Assertions.assertEquals(0, type.getScalarScale());
    }

    @Test
    void testZeroWithScale() throws AnalysisException {
        // "0.00" -> BigDecimal precision=1, scale=2
        // getBigDecimalPrecision: max(2, 1) = 2
        // getBigDecimalScale: 2
        DecimalLiteral lit = DecimalLiteralUtils.create("0.00", false);
        Assertions.assertEquals(new BigDecimal("0.00"), lit.getValue());
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(2, type.getScalarPrecision());
        Assertions.assertEquals(2, type.getScalarScale());
    }

    @Test
    void testNegativeDecimal() throws AnalysisException {
        DecimalLiteral lit = DecimalLiteralUtils.create("-1.23", false);
        Assertions.assertEquals(new BigDecimal("-1.23"), lit.getValue());
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(3, type.getScalarPrecision());
        Assertions.assertEquals(2, type.getScalarScale());
    }

    @Test
    void testLeadingFractionalZeros() throws AnalysisException {
        // "0.01234" -> BigDecimal precision=4, scale=5
        // getBigDecimalPrecision: scale > precision, max(5, 4) = 5
        // getBigDecimalScale: 5
        DecimalLiteral lit = DecimalLiteralUtils.create("0.01234", false);
        Assertions.assertEquals(new BigDecimal("0.01234"), lit.getValue());
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(5, type.getScalarPrecision());
        Assertions.assertEquals(5, type.getScalarScale());
    }

    @Test
    void testNegativeJavaScale() throws AnalysisException {
        // "2000" as BigDecimal has precision=1, scale=-3 when created via new BigDecimal("2000")
        // Actually new BigDecimal("2000") has precision=4, scale=0
        // But "2E+3" has precision=1, scale=-3
        // getBigDecimalPrecision: scale < 0, abs(-3) + 1 = 4
        // getBigDecimalScale: max(0, -3) = 0
        DecimalLiteral lit = DecimalLiteralUtils.create("2E+3", false);
        Assertions.assertEquals(0, new BigDecimal("2000").compareTo(lit.getValue()));
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(4, type.getScalarPrecision());
        Assertions.assertEquals(0, type.getScalarScale());
    }

    @Test
    void testScientificNotation() throws AnalysisException {
        // "1.23E+10" -> BigDecimal: unscaledValue=123, scale=-8
        // getBigDecimalPrecision: scale < 0, abs(-8) + 3 = 11
        // getBigDecimalScale: max(0, -8) = 0
        DecimalLiteral lit = DecimalLiteralUtils.create("1.23E+10", false);
        Assertions.assertEquals(0, new BigDecimal("1.23E+10").compareTo(lit.getValue()));
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(11, type.getScalarPrecision());
        Assertions.assertEquals(0, type.getScalarScale());
    }

    @Test
    void testBoundaryPrecision38() throws AnalysisException {
        // 38-digit integer: precision exactly at MAX_DECIMAL128_PRECISION
        String val = "12345678901234567890123456789012345678"; // 38 digits
        DecimalLiteral lit = DecimalLiteralUtils.create(val, false);
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(38, type.getScalarPrecision());
        Assertions.assertEquals(0, type.getScalarScale());
    }

    @Test
    void testBoundaryPrecision38WithScale() throws AnalysisException {
        // 38-digit precision with decimal part
        String val = "1234567890123456789.0123456789012345678"; // 19 integer + 19 decimal = 38 precision
        DecimalLiteral lit = DecimalLiteralUtils.create(val, false);
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(38, type.getScalarPrecision());
        Assertions.assertEquals(19, type.getScalarScale());
    }

    @Test
    void testTrailingZerosStrippedToFit() throws AnalysisException {
        // Precision > 38, but trailing zeros can be stripped to fit within 38
        // "1.230000...0" with 40 decimal digits -> precision=41, scale=40
        // After strip: "1.23" -> precision=3, fits in 38
        // integerPart = 41-40 = 1, newScale = 38-1 = 37
        // setScale(37) -> precision=max(37,38)=38, scale=37
        String val = "1.2300000000000000000000000000000000000000"; // 40 decimal digits
        DecimalLiteral lit = DecimalLiteralUtils.create(val, false);
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(38, type.getScalarPrecision());
        Assertions.assertEquals(37, type.getScalarScale());
    }

    @Test
    void testPrecisionExceeds38CannotStrip() throws AnalysisException {
        // 39-digit integer with no trailing zeros: cannot strip, passes through with precision > 38
        String val = "123456789012345678901234567890123456789"; // 39 digits
        DecimalLiteral lit = DecimalLiteralUtils.create(val, false);
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(39, type.getScalarPrecision());
        Assertions.assertEquals(0, type.getScalarScale());
    }

    @Test
    void testDecimal256EnabledWithinRange() throws AnalysisException {
        // 39-digit integer: exceeds 38 but within 76 with decimal256 enabled
        String val = "123456789012345678901234567890123456789"; // 39 digits
        DecimalLiteral lit = DecimalLiteralUtils.create(val, true);
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(39, type.getScalarPrecision());
        Assertions.assertEquals(0, type.getScalarScale());
    }

    @Test
    void testDecimal256BoundaryPrecision76() throws AnalysisException {
        // 76-digit integer: exactly at MAX_DECIMAL256_PRECISION
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 76; i++) {
            sb.append((i % 9) + 1);
        }
        DecimalLiteral lit = DecimalLiteralUtils.create(sb.toString(), true);
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(76, type.getScalarPrecision());
        Assertions.assertEquals(0, type.getScalarScale());
    }

    @Test
    void testDecimal256TrailingZerosStrippedToFit() throws AnalysisException {
        // With decimal256, precision > 76 but trailing zeros can be stripped
        // Build a number with 77 precision digits that has trailing zeros
        // "1." + 76 zeros except last few are real digits + trailing zeros
        String intPart = "1";
        // 76 decimal digits, last 40 are zeros -> precision = 77, scale = 76
        String decPart = "2345678901234567890123456789012345670000000000000000000000000000000000000000";
        // That's 76 chars. precision = 77, scale = 76
        String val = intPart + "." + decPart;
        DecimalLiteral lit = DecimalLiteralUtils.create(val, true);
        // After strip: "1.234567890123456789012345678901234567"
        // stripped precision = 37, fits in 76
        // integerPart = 77 - 76 = 1, newScale = 76 - 1 = 75
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(76, type.getScalarPrecision());
        Assertions.assertEquals(75, type.getScalarScale());
    }

    @Test
    void testInvalidString() {
        Assertions.assertThrows(AnalysisException.class, () -> {
            DecimalLiteralUtils.create("abc", false);
        });
    }

    @Test
    void testEmptyString() {
        Assertions.assertThrows(AnalysisException.class, () -> {
            DecimalLiteralUtils.create("", false);
        });
    }

    @Test
    void testLargeNegativeDecimal() throws AnalysisException {
        // Large negative number
        String val = "-12345678901234567890123456789012345678"; // 38 digits
        DecimalLiteral lit = DecimalLiteralUtils.create(val, false);
        Assertions.assertTrue(lit.getValue().compareTo(BigDecimal.ZERO) < 0);
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(38, type.getScalarPrecision());
        Assertions.assertEquals(0, type.getScalarScale());
    }

    @Test
    void testVerySmallDecimal() throws AnalysisException {
        // "0.0001" -> precision=4, scale=4
        DecimalLiteral lit = DecimalLiteralUtils.create("0.0001", false);
        Assertions.assertEquals(new BigDecimal("0.0001"), lit.getValue());
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(4, type.getScalarPrecision());
        Assertions.assertEquals(4, type.getScalarScale());
    }

    @Test
    void testCreateFromBigDecimal() {
        BigDecimal v = new BigDecimal("123.456");
        DecimalLiteral lit = DecimalLiteralUtils.create(v, false);
        Assertions.assertEquals(v, lit.getValue());
        ScalarType type = (ScalarType) lit.getType();
        Assertions.assertEquals(6, type.getScalarPrecision());
        Assertions.assertEquals(3, type.getScalarScale());
    }

    @Test
    void testEnableDecimal256FalseVsTrue() throws AnalysisException {
        // A 39-digit number: exceeds 38, within 76
        // With enableDecimal256=false: precision stays 39 (cannot strip, no trailing zeros)
        // With enableDecimal256=true: precision stays 39 (within 76, no stripping needed)
        String val = "123456789012345678901234567890123456789";
        DecimalLiteral litFalse = DecimalLiteralUtils.create(val, false);
        DecimalLiteral litTrue = DecimalLiteralUtils.create(val, true);
        // Values should be the same
        Assertions.assertEquals(0, litFalse.getValue().compareTo(litTrue.getValue()));
        // Both should have precision 39
        Assertions.assertEquals(39, ((ScalarType) litFalse.getType()).getScalarPrecision());
        Assertions.assertEquals(39, ((ScalarType) litTrue.getType()).getScalarPrecision());
    }

    @Test
    void testTrailingZerosWithDifferentDecimal256Settings() throws AnalysisException {
        // A number with precision 40, with trailing zeros, enableDecimal256=false
        // Should strip and fit within 38
        String val = "1234567890.123456789012345678900000000000"; // 10 integer + 30 decimal = precision 40
        DecimalLiteral litFalse = DecimalLiteralUtils.create(val, false);
        ScalarType typeFalse = (ScalarType) litFalse.getType();
        Assertions.assertEquals(38, typeFalse.getScalarPrecision());

        // With enableDecimal256=true, precision 40 is within 76, no stripping needed
        DecimalLiteral litTrue = DecimalLiteralUtils.create(val, true);
        ScalarType typeTrue = (ScalarType) litTrue.getType();
        Assertions.assertEquals(40, typeTrue.getScalarPrecision());
        Assertions.assertEquals(30, typeTrue.getScalarScale());
    }
}
