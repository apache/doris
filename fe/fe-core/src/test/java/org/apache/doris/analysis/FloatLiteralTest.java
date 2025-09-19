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

import org.apache.doris.catalog.Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FloatLiteralTest {

    @Test
    public void testDoubleGetStringValue() {
        Assertions.assertEquals("0", new FloatLiteral(0d, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("0", new FloatLiteral(0.0, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("-0", new FloatLiteral(-0d, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("1", new FloatLiteral(1d, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("1", new FloatLiteral(1.0, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("-1", new FloatLiteral(-1d, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("1.554", new FloatLiteral(1.554, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("0.338", new FloatLiteral(0.338, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("-1", new FloatLiteral(-1.0, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("1e+100", new FloatLiteral(1e100, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("1e-100", new FloatLiteral(1e-100, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("1000000000000000", new FloatLiteral(1.0E15, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("-1000000000000000", new FloatLiteral(-1.0E15, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("1e+16", new FloatLiteral(1.0E16, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("-1e+16", new FloatLiteral(-1.0E16, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("0.0001", new FloatLiteral(0.0001, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("1e-05", new FloatLiteral(0.00001, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("1e+308", new FloatLiteral(1e308, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("-1e+308", new FloatLiteral(-1e308, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("Infinity", new FloatLiteral(Double.POSITIVE_INFINITY, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("-Infinity", new FloatLiteral(Double.NEGATIVE_INFINITY, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("NaN", new FloatLiteral(Double.NaN, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("1234567890123456", new FloatLiteral(1234567890123456.12345, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("1.234567890123457e+16", new FloatLiteral(12345678901234567.12345, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("0.0001234567890123457", new FloatLiteral(0.0001234567890123456789, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("1.234567890123456e-15", new FloatLiteral(0.000000000000001234567890123456, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("123.456", new FloatLiteral(123.456000, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("123", new FloatLiteral(123.000, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("-1234567890123456", new FloatLiteral(-1234567890123456.12345, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("-1.234567890123457e+16", new FloatLiteral(-12345678901234567.12345, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("-0.0001234567890123457", new FloatLiteral(-0.0001234567890123456789, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("-1.234567890123456e-15", new FloatLiteral(-0.000000000000001234567890123456, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("-123.456", new FloatLiteral(-123.456000, Type.DOUBLE).getStringValueForQuery(null));
        Assertions.assertEquals("-123", new FloatLiteral(-123.000, Type.DOUBLE).getStringValueForQuery(null));
    }

    @Test
    public void testFloatGetStringValue() {
        Assertions.assertEquals("0", new FloatLiteral(0d, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("0", new FloatLiteral(0.0, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("-0", new FloatLiteral(-0d, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("1", new FloatLiteral(1d, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("1", new FloatLiteral(1.0d, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("-1", new FloatLiteral(-1d, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("1.554", new FloatLiteral(1.554d, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("0.338", new FloatLiteral(0.338d, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("-1", new FloatLiteral(-1.0d, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("1e+38", new FloatLiteral(1e38, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("1e-38", new FloatLiteral(1e-38, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("1000000", new FloatLiteral(1.0E6).getStringValueForQuery(null));
        Assertions.assertEquals("-1000000", new FloatLiteral(-1.0E6, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("1e+07", new FloatLiteral(1.0E7, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("-1e+07", new FloatLiteral(-1.0E7, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("0.0001", new FloatLiteral(0.0001, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("1e-05", new FloatLiteral(0.00001, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("3.402823e+38", new FloatLiteral((double) Float.MAX_VALUE, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("Infinity", new FloatLiteral(1e39, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("-Infinity", new FloatLiteral(-1e39, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("Infinity", new FloatLiteral(Double.POSITIVE_INFINITY, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("-Infinity", new FloatLiteral(Double.NEGATIVE_INFINITY, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("NaN", new FloatLiteral(Double.NaN, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("123456.1", new FloatLiteral(123456.12345, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("1234567", new FloatLiteral(1234567.12345, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("1.234568e+07", new FloatLiteral(12345678.12345, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("0.0001234568", new FloatLiteral(0.0001234567890123456789, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("1.234568e-15", new FloatLiteral(0.000000000000001234567890123456, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("123.456", new FloatLiteral(123.456000, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("123", new FloatLiteral(123.000, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("-1234567", new FloatLiteral(-1234567.12345, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("-1.234568e+07", new FloatLiteral(-12345678.12345, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("-0.0001234568", new FloatLiteral(-0.0001234567890123456789, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("-1.234568e-15", new FloatLiteral(-0.000000000000001234567890123456, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("-123.456", new FloatLiteral(-123.456000, Type.FLOAT).getStringValueForQuery(null));
        Assertions.assertEquals("-123", new FloatLiteral(-123.000, Type.FLOAT).getStringValueForQuery(null));
    }
}
