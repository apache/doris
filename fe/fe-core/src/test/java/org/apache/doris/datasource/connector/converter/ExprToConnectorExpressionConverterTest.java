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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Literal encoding of {@link ExprToConnectorExpressionConverter}. The literals start as Nereids
 * literals and reach the converter through {@code toLegacyLiteral}, as they do when the scan node
 * translates its conjuncts.
 */
public class ExprToConnectorExpressionConverterTest {

    private static ConnectorLiteral convert(Literal literal) {
        ConnectorExpression converted = ExprToConnectorExpressionConverter.convert(literal.toLegacyLiteral());
        Assertions.assertInstanceOf(ConnectorLiteral.class, converted);
        return (ConnectorLiteral) converted;
    }

    @Test
    public void arrayLiteralCarriesItsElementsRatherThanItsText() {
        // Rendered as text, this array reads "[red, a, b, it's]": three elements or four, and no way
        // to tell which.
        ConnectorLiteral array = convert(new ArrayLiteral(ImmutableList.of(
                new VarcharLiteral("red"), new VarcharLiteral("a, b"), new VarcharLiteral("it's"))));

        Assertions.assertEquals("ARRAY", array.getType().getTypeName());
        Assertions.assertEquals("VARCHAR", array.getType().getChildren().get(0).getTypeName());
        Assertions.assertEquals(Arrays.asList("red", "a, b", "it's"), array.getValue());
    }

    @Test
    public void elementsTakeTheShapeTheyWouldHaveAsLiteralsOfTheirOwn() {
        List<Literal> scalars = ImmutableList.of(
                BooleanLiteral.of(true),
                new IntegerLiteral(7),
                new BigIntLiteral(Long.MIN_VALUE),
                new LargeIntLiteral(new BigInteger("170141183460469231731687303715884105727")),
                new DoubleLiteral(1.5),
                new DecimalV3Literal(new BigDecimal("12.50")),
                new DateV2Literal("2024-01-02"),
                new DateTimeV2Literal(DateTimeV2Type.of(6), "2024-01-02 03:04:05.123456"));
        List<Class<?>> shapes = Arrays.asList(Boolean.class, Long.class, Long.class, String.class,
                Double.class, BigDecimal.class, LocalDate.class, LocalDateTime.class);

        for (int i = 0; i < scalars.size(); i++) {
            Literal scalar = scalars.get(i);
            Object element = ((List<?>) convert(new ArrayLiteral(ImmutableList.of(scalar))).getValue()).get(0);
            Assertions.assertEquals(convert(scalar).getValue(), element, scalar.toString());
            Assertions.assertEquals(shapes.get(i), element.getClass(), scalar.toString());
        }
        Assertions.assertEquals(LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123_456_000),
                ((List<?>) convert(new ArrayLiteral(ImmutableList.of(scalars.get(7)))).getValue()).get(0));
    }

    @Test
    public void nullElementsAndNestedArraysKeepTheirStructure() {
        ArrayType intArray = ArrayType.of(IntegerType.INSTANCE);
        ConnectorLiteral withNull = convert(new ArrayLiteral(ImmutableList.of(
                new VarcharLiteral("red"), new NullLiteral(VarcharType.SYSTEM_DEFAULT))));
        Assertions.assertEquals(Arrays.asList("red", null), withNull.getValue());

        ConnectorLiteral nested = convert(new ArrayLiteral(ImmutableList.of(
                new ArrayLiteral(ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2)), intArray),
                new ArrayLiteral(ImmutableList.of(), intArray),
                new NullLiteral(intArray)), ArrayType.of(intArray)));
        Assertions.assertEquals("ARRAY", nested.getType().getChildren().get(0).getTypeName());
        Assertions.assertEquals(Arrays.asList(Arrays.asList(1L, 2L), Collections.emptyList(), null),
                nested.getValue());
    }

    @Test
    public void elementListIsUnmodifiableAndANullArrayIsStillANullLiteral() {
        List<?> elements = (List<?>) convert(new ArrayLiteral(ImmutableList.of(new IntegerLiteral(1)))).getValue();
        Assertions.assertThrows(UnsupportedOperationException.class, elements::clear);

        ConnectorLiteral nullArray = convert(new NullLiteral(ArrayType.of(IntegerType.INSTANCE)));
        Assertions.assertTrue(nullArray.isNull());
        Assertions.assertEquals("ARRAY", nullArray.getType().getTypeName());
    }
}
