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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

class FromUnixtimeTest {
    private final SlotReference timestampSlot = new SlotReference("ts", BigIntType.INSTANCE);
    private ConnectContext previousContext;

    @BeforeEach
    void setUp() {
        previousContext = ConnectContext.get();
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    void testIsMonotonicWithFixedOffsetTimeZone() {
        setTimeZone("+00:00");
        FromUnixtime fromUnixtime = new FromUnixtime(timestampSlot);

        Assertions.assertTrue(fromUnixtime.isMonotonic(
                new BigIntLiteral(1635638400L), new BigIntLiteral(1635642000L)));
    }

    @Test
    void testIsMonotonicWithoutDstTransition() {
        setTimeZone("Europe/Paris");
        FromUnixtime fromUnixtime = new FromUnixtime(timestampSlot);

        Assertions.assertTrue(fromUnixtime.isMonotonic(
                new BigIntLiteral(1610236800L), new BigIntLiteral(1610323200L)));
    }

    @Test
    void testIsMonotonicWithDstFallbackTransition() {
        setTimeZone("Europe/Paris");
        FromUnixtime fromUnixtime = new FromUnixtime(timestampSlot);

        Assertions.assertFalse(fromUnixtime.isMonotonic(
                new BigIntLiteral(1635638400L), new BigIntLiteral(1635642000L)));
    }

    @Test
    void testIsMonotonicWithFormatAndDstFallbackTransition() {
        setTimeZone("Europe/Paris");
        FromUnixtime fromUnixtime = new FromUnixtime(timestampSlot,
                new VarcharLiteral("%Y-%m-%d %H:%i:%s"));

        Assertions.assertFalse(fromUnixtime.isMonotonic(
                new BigIntLiteral(1635638400L), new BigIntLiteral(1635642000L)));
    }

    @Test
    void testIsNotMonotonicWhenMicrosecondRoundingCrossesDstFallback() {
        setTimeZone("Europe/Paris");
        SlotReference decimalSlot = new SlotReference("ts", DecimalV3Type.createDecimalV3Type(21, 9));
        FromUnixtime fromUnixtime = new FromUnixtime(decimalSlot,
                new VarcharLiteral("%Y-%m-%d %H:%i:%s"));

        Assertions.assertFalse(fromUnixtime.isMonotonic(
                DecimalV3Literal.of(new BigDecimal("1635641999.999999000")),
                DecimalV3Literal.of(new BigDecimal("1635641999.999999999"))));
    }

    @Test
    void testRejectMixedFractionFormatSpecifiers() {
        FromUnixtime fromUnixtime = new FromUnixtime(timestampSlot,
                new VarcharLiteral("%s.%f|%n"));

        Assertions.assertThrows(AnalysisException.class,
                fromUnixtime::checkLegalityBeforeTypeCoercion);
        Assertions.assertThrows(AnalysisException.class,
                fromUnixtime::checkLegalityAfterRewrite);
        FromUnixtime escapedNanosecond = new FromUnixtime(timestampSlot,
                new VarcharLiteral("%s.%f|%%n"));
        Assertions.assertDoesNotThrow(escapedNanosecond::checkLegalityAfterRewrite);
    }

    @Test
    void testDecimalTwoArgumentSignatureByScale() {
        DecimalV3Type decimalMicro = DecimalV3Type.createDecimalV3Type(18, 6);
        DecimalV3Type decimalNano = DecimalV3Type.createDecimalV3Type(21, 9);

        assertDecimalTwoArgumentType(decimalMicro, DecimalV3Type.createDecimalV3Type(18, 6), true);
        assertDecimalTwoArgumentType(decimalNano, DecimalV3Type.createDecimalV3Type(21, 9), false);
        assertDecimalTwoArgumentType(decimalMicro, DecimalV3Type.createDecimalV3Type(10, 3), false);
        assertDecimalTwoArgumentType(decimalNano, DecimalV3Type.createDecimalV3Type(18, 9), false);
        assertDecimalTwoArgumentType(decimalNano, DecimalV2Type.createDecimalV2Type(27, 9), false);
    }

    @Test
    void testIsMonotonicWithNonMonotonicFormat() {
        setTimeZone("+00:00");
        FromUnixtime fromUnixtime = new FromUnixtime(timestampSlot, new VarcharLiteral("%W"));

        Assertions.assertFalse(fromUnixtime.isMonotonic(
                new BigIntLiteral(1610236800L), new BigIntLiteral(1610323200L)));
    }

    private void setTimeZone(String timeZone) {
        ConnectContext.get().getSessionVariable().setTimeZone(timeZone);
    }

    private void assertDecimalTwoArgumentType(
            DecimalV3Type expectedType, DataType inputType, boolean nullable) {
        FromUnixtime fromUnixtime = new FromUnixtime(
                new SlotReference("decimal_value", inputType, nullable), new VarcharLiteral("%s.%f"));
        FunctionSignature signature = fromUnixtime.getSignature();

        Assertions.assertEquals(expectedType, signature.getArgType(0));
        Assertions.assertEquals(nullable, fromUnixtime.nullable());
    }
}
