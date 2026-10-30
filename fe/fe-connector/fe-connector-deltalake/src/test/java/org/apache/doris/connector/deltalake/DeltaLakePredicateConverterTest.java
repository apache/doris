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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorIn;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;

import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.types.TypeChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

class DeltaLakePredicateConverterTest {
    private final ConnectorColumnRef column = new ConnectorColumnRef("mixed", ConnectorType.of("BIGINT"));
    private final DeltaLakePredicateConverter converter = new DeltaLakePredicateConverter(
            new StructType().add("MiXeD", LongType.LONG), ZoneId.of("Asia/Shanghai"));

    @Test
    void preservesLogicalColumnNamesAndEveryComparisonIncludingReversedOperands() {
        for (ConnectorComparison.Operator operator : ConnectorComparison.Operator.values()) {
            Predicate result = converter.convert(new ConnectorComparison(operator, column,
                    ConnectorLiteral.ofLong(1))).orElseThrow();
            Assertions.assertTrue(result.toString().contains("MiXeD"));
            Assertions.assertTrue(converter.convert(new ConnectorComparison(operator,
                    ConnectorLiteral.ofLong(1), column)).isPresent());
        }
        Predicate nullSafe = converter.convert(new ConnectorNot(new ConnectorComparison(
                ConnectorComparison.Operator.EQ_FOR_NULL, column, ConnectorLiteral.ofLong(1)))).orElseThrow();
        Assertions.assertEquals("IS NOT DISTINCT FROM", ((Predicate) nullSafe.getChildren().get(0)).getName());
    }

    @Test
    void onlyDropsUnconvertibleConjunctsOutsideNotAndOr() {
        ConnectorExpression known = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                column, ConnectorLiteral.ofLong(1));
        ConnectorExpression unknown = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                column, ConnectorLiteral.ofString("one"));
        ConnectorAnd and = new ConnectorAnd(List.of(known, unknown));
        Assertions.assertTrue(converter.convert(and).isPresent());
        Assertions.assertTrue(converter.convert(new ConnectorNot(and)).isEmpty());
        Assertions.assertTrue(converter.convert(new ConnectorOr(List.of(known, unknown))).isEmpty());
        Assertions.assertTrue(converter.convert(new ConnectorOr(List.of(known, and))).isEmpty());
        Assertions.assertTrue(converter.convert(new ConnectorNot(new ConnectorOr(List.of(known, known)))).isPresent());
    }

    @Test
    void preservesInNullAndNegationSemantics() {
        for (boolean negated : List.of(false, true)) {
            Assertions.assertTrue(converter.convert(new ConnectorIsNull(column, negated)).isPresent());
            Assertions.assertTrue(converter.convert(new ConnectorIn(column,
                    List.of(ConnectorLiteral.ofLong(1), ConnectorLiteral.ofNull(ConnectorType.of("BIGINT"))),
                    negated)).isPresent());
            Assertions.assertTrue(converter.convert(new ConnectorIn(column,
                    List.of(ConnectorLiteral.ofString("one")), negated)).isEmpty());
        }
    }

    @Test
    void acceptsExactPrimitiveLiteralsAndRejectsLossyOrUnsupportedConversions() {
        Assertions.assertEquals(true, converter.literal(BooleanType.BOOLEAN, ConnectorLiteral.ofBoolean(true)).getValue());
        Assertions.assertEquals((byte) -128, converter.literal(ByteType.BYTE, ConnectorLiteral.ofInt(-128)).getValue());
        Assertions.assertEquals((short) 32767, converter.literal(ShortType.SHORT, ConnectorLiteral.ofInt(32767)).getValue());
        Assertions.assertEquals(7, converter.literal(IntegerType.INTEGER, ConnectorLiteral.ofLong(7)).getValue());
        Assertions.assertEquals(Long.MAX_VALUE, converter.literal(LongType.LONG,
                ConnectorLiteral.ofLong(Long.MAX_VALUE)).getValue());
        Assertions.assertEquals(1.25f, converter.literal(FloatType.FLOAT,
                new ConnectorLiteral(ConnectorType.of("FLOAT"), 1.25)).getValue());
        Assertions.assertEquals(1.25, converter.literal(DoubleType.DOUBLE, ConnectorLiteral.ofDouble(1.25)).getValue());
        Assertions.assertEquals("a/b%+中文", converter.literal(StringType.STRING,
                ConnectorLiteral.ofString("a/b%+中文")).getValue());
        Assertions.assertArrayEquals(new byte[] {0, -1}, (byte[]) converter.literal(BinaryType.BINARY,
                new ConnectorLiteral(ConnectorType.of("VARBINARY"), new byte[] {0, -1})).getValue());
        Assertions.assertEquals(-1, converter.literal(DateType.DATE,
                ConnectorLiteral.ofDate(LocalDate.of(1969, 12, 31))).getValue());
        Assertions.assertEquals(new BigDecimal("1.20"), converter.literal(new DecimalType(10, 2),
                ConnectorLiteral.ofDecimal(new BigDecimal("1.2"), 2, 1)).getValue());
        Assertions.assertNull(converter.literal(ByteType.BYTE, ConnectorLiteral.ofInt(128)));
        Assertions.assertNull(converter.literal(IntegerType.INTEGER, ConnectorLiteral.ofLong(Long.MAX_VALUE)));
        Assertions.assertNull(converter.literal(LongType.LONG, ConnectorLiteral.ofDouble(1.1)));
        Assertions.assertNull(converter.literal(LongType.LONG, ConnectorLiteral.ofDouble(1.0e18)));
        Assertions.assertNull(converter.literal(new DecimalType(20, 0), ConnectorLiteral.ofDouble(1.0e18)));
        Assertions.assertNull(converter.literal(FloatType.FLOAT, ConnectorLiteral.ofDouble(0.1)));
        Assertions.assertNull(converter.literal(DoubleType.DOUBLE, ConnectorLiteral.ofDouble(Double.NaN)));
        Assertions.assertNull(converter.literal(new DecimalType(3, 2),
                ConnectorLiteral.ofDecimal(new BigDecimal("12.345"), 5, 3)));
        Assertions.assertNull(converter.literal(LongType.LONG, ConnectorLiteral.ofString("1")));
    }

    @Test
    void distinguishesUtcTimestampCarriersFromWallClockAndNtz() {
        LocalDateTime epoch = LocalDateTime.of(1970, 1, 1, 0, 0);
        Assertions.assertEquals(-28_800_000_000L, converter.literal(TimestampType.TIMESTAMP,
                ConnectorLiteral.ofDatetime(epoch)).getValue());
        Assertions.assertEquals(0L, converter.literal(TimestampType.TIMESTAMP,
                new ConnectorLiteral(ConnectorType.of("TIMESTAMPTZ", 6, 0), epoch)).getValue());
        Assertions.assertEquals(0L, converter.literal(TimestampNTZType.TIMESTAMP_NTZ,
                ConnectorLiteral.ofDatetime(epoch)).getValue());
        Assertions.assertEquals(-1L, converter.literal(TimestampNTZType.TIMESTAMP_NTZ,
                ConnectorLiteral.ofDatetime(epoch.minusNanos(1000))).getValue());
    }

    @Test
    void leavesDateToNtzPredicatesResidualBecauseOldStatsUseDateStrings() {
        StructField field = new StructField("dt", TimestampNTZType.TIMESTAMP_NTZ, true)
                .withTypeChanges(List.of(new TypeChange(DateType.DATE, TimestampNTZType.TIMESTAMP_NTZ)));
        DeltaLakePredicateConverter widening = new DeltaLakePredicateConverter(
                new StructType().add(field).add("MiXeD", LongType.LONG), ZoneId.of("UTC"));
        ConnectorExpression date = new ConnectorComparison(ConnectorComparison.Operator.GT,
                new ConnectorColumnRef("dt", ConnectorType.of("DATETIMEV2", 6, 0)),
                ConnectorLiteral.ofDatetime(LocalDateTime.of(2024, 2, 29, 0, 0)));
        ConnectorExpression integer = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                column, ConnectorLiteral.ofLong(1));
        Assertions.assertTrue(widening.convert(date).isEmpty());
        Assertions.assertEquals(widening.convert(integer).orElseThrow().toString(),
                widening.convert(new ConnectorAnd(List.of(date, integer))).orElseThrow().toString());
        Assertions.assertTrue(widening.convert(new ConnectorOr(List.of(date, integer))).isEmpty());
    }
}
