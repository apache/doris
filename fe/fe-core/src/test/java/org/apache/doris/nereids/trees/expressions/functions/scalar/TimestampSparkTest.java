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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

/**
 * Unit tests for {@link TimestampSpark}.
 */
public class TimestampSparkTest {

    @Test
    public void testSignaturesCount() {
        Assertions.assertEquals(9, TimestampSpark.SIGNATURES.size());
    }

    @Test
    public void testFunctionName() {
        SlotReference col = new SlotReference("ts_col", VarcharType.SYSTEM_DEFAULT);
        TimestampSpark func = new TimestampSpark(col);
        Assertions.assertEquals("timestamp_spark", func.getName());
    }

    @Test
    public void testSignatureOrderPreservation() {
        List<FunctionSignature> signatures = TimestampSpark.SIGNATURES;

        // Integer signatures: TinyInt, SmallInt, Integer, BigInt
        Assertions.assertEquals(TinyIntType.INSTANCE, signatures.get(0).argumentsTypes.get(0));
        Assertions.assertEquals(SmallIntType.INSTANCE, signatures.get(1).argumentsTypes.get(0));
        Assertions.assertEquals(IntegerType.INSTANCE, signatures.get(2).argumentsTypes.get(0));
        Assertions.assertEquals(BigIntType.INSTANCE, signatures.get(3).argumentsTypes.get(0));

        // Decimal before Float/Double
        Assertions.assertTrue(signatures.get(4).argumentsTypes.get(0) instanceof DecimalV3Type);

        // Float, Double
        Assertions.assertEquals(FloatType.INSTANCE, signatures.get(5).argumentsTypes.get(0));
        Assertions.assertEquals(DoubleType.INSTANCE, signatures.get(6).argumentsTypes.get(0));

        // String signatures
        Assertions.assertEquals(VarcharType.SYSTEM_DEFAULT, signatures.get(7).argumentsTypes.get(0));
        Assertions.assertEquals(StringType.INSTANCE, signatures.get(8).argumentsTypes.get(0));
    }

    @Test
    public void testIntegerSignaturesReturnSystemDefault() {
        List<FunctionSignature> signatures = TimestampSpark.SIGNATURES;
        for (int i = 0; i < 4; i++) {
            Assertions.assertEquals(DateTimeV2Type.SYSTEM_DEFAULT, signatures.get(i).returnType,
                    "Signature " + i + " should return SYSTEM_DEFAULT");
        }
    }

    @Test
    public void testStringSignaturesReturnMax() {
        List<FunctionSignature> signatures = TimestampSpark.SIGNATURES;
        // Varchar and String signatures should return MAX
        Assertions.assertEquals(DateTimeV2Type.MAX, signatures.get(7).returnType);
        Assertions.assertEquals(DateTimeV2Type.MAX, signatures.get(8).returnType);
    }

    @Test
    public void testComputeSignatureWithStringLiteralNoFrac() {
        VarcharLiteral literal = new VarcharLiteral("2024-01-15 14:30:45");
        TimestampSpark func = new TimestampSpark(literal);

        FunctionSignature sig = func.computeSignature(func.getSignatures().get(7));
        Assertions.assertEquals(DateTimeV2Type.of(0), sig.returnType);
    }

    @Test
    public void testComputeSignatureWithStringLiteralScale3() {
        VarcharLiteral literal = new VarcharLiteral("2024-01-15 14:30:45.123");
        TimestampSpark func = new TimestampSpark(literal);

        FunctionSignature sig = func.computeSignature(func.getSignatures().get(7));
        Assertions.assertEquals(DateTimeV2Type.of(3), sig.returnType);
    }

    @Test
    public void testComputeSignatureWithStringLiteralScale6() {
        VarcharLiteral literal = new VarcharLiteral("2024-01-15 14:30:45.123456");
        TimestampSpark func = new TimestampSpark(literal);

        FunctionSignature sig = func.computeSignature(func.getSignatures().get(7));
        Assertions.assertEquals(DateTimeV2Type.of(6), sig.returnType);
    }

    @Test
    public void testComputeSignatureWithStringLiteralTruncateBeyond6() {
        // 7 fractional digits → truncated to 6
        VarcharLiteral literal = new VarcharLiteral("2024-01-15 14:30:45.1234567");
        TimestampSpark func = new TimestampSpark(literal);

        FunctionSignature sig = func.computeSignature(func.getSignatures().get(7));
        Assertions.assertEquals(DateTimeV2Type.of(6), sig.returnType);
    }

    @Test
    public void testComputeSignatureWithStringLiteralTrailingZeros() {
        // ".12300" → scale 3 (trailing zeros trimmed)
        VarcharLiteral literal = new VarcharLiteral("2024-01-15 14:30:45.12300");
        TimestampSpark func = new TimestampSpark(literal);

        FunctionSignature sig = func.computeSignature(func.getSignatures().get(7));
        Assertions.assertEquals(DateTimeV2Type.of(3), sig.returnType);
    }

    @Test
    public void testComputeSignatureWithStringLiteralDateOnly() {
        VarcharLiteral literal = new VarcharLiteral("2024-01-15");
        TimestampSpark func = new TimestampSpark(literal);

        FunctionSignature sig = func.computeSignature(func.getSignatures().get(7));
        Assertions.assertEquals(DateTimeV2Type.of(0), sig.returnType);
    }

    @Test
    public void testComputeSignatureWithDecimalLiteralScale6() {
        DecimalV3Literal literal = new DecimalV3Literal(new BigDecimal("1705308045.999999"));
        TimestampSpark func = new TimestampSpark(literal);

        FunctionSignature sig = func.computeSignature(func.getSignatures().get(4));
        Assertions.assertEquals(DateTimeV2Type.of(6), sig.returnType);
    }

    @Test
    public void testComputeSignatureWithDecimalLiteralScale0() {
        // 1e9 → BigDecimal("1E+9"), scale <= 0 → DateTimeV2(0)
        DecimalV3Literal literal = new DecimalV3Literal(new BigDecimal("1000000000"));
        TimestampSpark func = new TimestampSpark(literal);

        FunctionSignature sig = func.computeSignature(func.getSignatures().get(4));
        Assertions.assertEquals(DateTimeV2Type.of(0), sig.returnType);
    }

    @Test
    public void testComputeSignatureWithNonLiteralString() {
        // Non-literal string → keeps MAX (scale 6)
        SlotReference col = new SlotReference("ts_col", VarcharType.SYSTEM_DEFAULT);
        TimestampSpark func = new TimestampSpark(col);

        FunctionSignature sig = func.computeSignature(func.getSignatures().get(7));
        Assertions.assertEquals(DateTimeV2Type.MAX, sig.returnType);
    }

    @Test
    public void testWithChildren() {
        SlotReference col = new SlotReference("ts_col", VarcharType.SYSTEM_DEFAULT);
        TimestampSpark original = new TimestampSpark(col);

        SlotReference newCol = new SlotReference("new_col", StringType.INSTANCE);
        List<Expression> newChildren = ImmutableList.of(newCol);

        TimestampSpark newFunc = original.withChildren(newChildren);

        Assertions.assertNotSame(original, newFunc);
        Assertions.assertEquals(1, newFunc.children().size());
        Assertions.assertEquals(newCol, newFunc.child(0));
    }

    @Test
    public void testWithChildrenInvalidSize() {
        SlotReference col = new SlotReference("ts_col", VarcharType.SYSTEM_DEFAULT);
        TimestampSpark func = new TimestampSpark(col);

        SlotReference col2 = new SlotReference("col2", StringType.INSTANCE);
        List<Expression> twoChildren = ImmutableList.of(col, col2);
        Assertions.assertThrows(IllegalArgumentException.class, () -> func.withChildren(twoChildren));

        List<Expression> noChildren = ImmutableList.of();
        Assertions.assertThrows(IllegalArgumentException.class, () -> func.withChildren(noChildren));
    }

    @Test
    public void testGetSignatures() {
        SlotReference col = new SlotReference("ts_col", VarcharType.SYSTEM_DEFAULT);
        TimestampSpark func = new TimestampSpark(col);

        List<FunctionSignature> signatures = func.getSignatures();
        Assertions.assertEquals(TimestampSpark.SIGNATURES, signatures);
        Assertions.assertEquals(9, signatures.size());
    }
}
