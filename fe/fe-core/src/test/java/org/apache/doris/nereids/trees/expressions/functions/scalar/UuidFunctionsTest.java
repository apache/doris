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

import org.apache.doris.nereids.rules.expression.check.CheckCast;
import org.apache.doris.nereids.trees.expressions.ExpressionEvaluator;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.executable.UuidArithmetic;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.UuidLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.UuidType;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

class UuidFunctionsTest {
    private static final UuidLiteral ZERO = new UuidLiteral("00000000000000000000000000000000");
    private static final UuidLiteral NORMAL = new UuidLiteral("00112233445566778899aabbccddeeff");
    private static final NullLiteral NULL_UUID = new NullLiteral(UuidType.INSTANCE);
    private ConnectContext previousContext;

    @BeforeEach
    void setUp() {
        previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    void parseFallbackAndNullSemantics() {
        for (String text : List.of("", "bad", " 00112233445566778899aabbccddeeff",
                "00112233445566778899aabbccddeeff0", "{00112233-4455-6677-8899-aabbccddeeff}")) {
            StringLiteral input = new StringLiteral(text);
            Assertions.assertEquals(ZERO, UuidArithmetic.toUuidOrZero(input));
            Assertions.assertEquals(NULL_UUID, UuidArithmetic.toUuidOrNull(input));
            Assertions.assertEquals(NORMAL, UuidArithmetic.toUuidOrDefault(input, NORMAL));
        }
        NullLiteral input = new NullLiteral(StringType.INSTANCE);
        Assertions.assertEquals(NULL_UUID, UuidArithmetic.toUuidOrZero(input));
        Assertions.assertEquals(NULL_UUID, UuidArithmetic.toUuidOrNull(input));
        Assertions.assertEquals(ZERO, UuidArithmetic.toUuidOrDefault(input));
        Assertions.assertEquals(NORMAL, UuidArithmetic.toUuidOrDefault(input, NORMAL));
        Assertions.assertEquals(NORMAL, UuidArithmetic.toUuidOrDefault(
                new StringLiteral("00112233445566778899AABBCCDDEEFF"), NULL_UUID));
        Assertions.assertFalse(new ToUuidOrDefault(input, NORMAL).nullable());
        Assertions.assertTrue(new ToUuidOrDefault(input, NULL_UUID).nullable());
    }

    @Test
    void rejectsUuidToLargeIntCast() {
        for (boolean strict : List.of(false, true)) {
            Assertions.assertFalse(CheckCast.check(UuidType.INSTANCE, LargeIntType.INSTANCE, strict));
            Assertions.assertFalse(CheckCast.check(ArrayType.of(UuidType.INSTANCE),
                    ArrayType.of(LargeIntType.INSTANCE), strict));
        }
    }

    @Test
    void decoderVersionTimezoneAndOutOfRange() throws Exception {
        StringLiteral utc = new StringLiteral("UTC");
        Assertions.assertEquals("1970-01-01 00:00:00.000", UuidArithmetic.uuidV7ToDateTime(NORMAL, utc).getStringValue());
        UuidLiteral epochMillis = new UuidLiteral("00000000-0001-7000-0000-000000000000");
        Assertions.assertEquals("1970-01-01 00:00:00.001",
                UuidArithmetic.uuidV7ToDateTime(epochMillis, utc).getStringValue());
        Assertions.assertEquals("1970-01-01 08:00:00.001",
                UuidArithmetic.uuidV7ToDateTime(epochMillis, new StringLiteral("Asia/Shanghai")).getStringValue());
        Assertions.assertInstanceOf(NullLiteral.class, UuidArithmetic.uuidV7ToDateTime(
                new UuidLiteral("ffffffff-ffff-7000-8000-000000000000"), utc));
    }

    @Test
    void deterministicFunctionsActuallyFoldInFe() {
        Assertions.assertEquals(NORMAL, ExpressionEvaluator.INSTANCE.eval(
                new ToUuidOrDefault(new StringLiteral("bad"), NORMAL)));
        Assertions.assertEquals(NULL_UUID, ExpressionEvaluator.INSTANCE.eval(
                new ToUuidOrNull(new StringLiteral("bad"))));
    }

    @Test
    void timestampGeneratorKeepsVolatileIdentity() {
        DateTimeV2Literal time = new DateTimeV2Literal("2026-09-10 12:34:56.789");
        DateTimeToUuidV7 first = new DateTimeToUuidV7(time);
        Assertions.assertFalse(first.foldable());
        Assertions.assertFalse(first.isDeterministic());
        Assertions.assertNotEquals(first, new DateTimeToUuidV7(time));
        Assertions.assertEquals(first, first.withChildren(List.of(time)));
        Assertions.assertEquals(first.getVolatileIdentity(), first.withChildren(List.of(
                new DateTimeV2Literal("1970-01-01 00:00:00"))).getVolatileIdentity());
    }

    @Test
    void rejectsImplicitNumericFunctions() {
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> new Abs(NORMAL).getSignature());
    }

    @Test
    void decoderRequiresConstantTimezone() {
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> new UuidV7ToDateTime(NORMAL, new SlotReference("tz", StringType.INSTANCE))
                        .checkLegalityBeforeTypeCoercion());
    }
}
