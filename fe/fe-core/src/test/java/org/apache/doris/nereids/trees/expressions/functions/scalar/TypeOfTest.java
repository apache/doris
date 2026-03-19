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

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.VarBinaryType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class TypeOfTest {

    private static final NereidsParser PARSER = new NereidsParser();

    @Test
    void testAnalyzeTimeRewriteForTrinoExamples() {
        assertTypeOfRewrite("typeof(123)", "integer");
        assertTypeOfRewrite("typeof('cat')", "varchar(3)");
        assertTypeOfRewrite("typeof(cast(1 as bigint))", "bigint");
        assertTypeOfRewrite("typeof(cast(1 as varchar))", "varchar");
        assertTypeOfRewrite("typeof(cast(null as varchar(10)))", "varchar(10)");
        assertTypeOfRewrite("typeof(try_cast(null as varchar(10)))", "varchar(10)");
        assertTypeOfRewrite("typeof(cast(null as char(3)))", "char(3)");
        assertTypeOfRewrite("typeof(cast(null as decimal(5,1)))", "decimal(5,1)");
        assertTypeOfRewrite("typeof(cast(null as decimal))", "decimal(38,0)");
        assertTypeOfRewrite("typeof(try_cast(null as decimal))", "decimal(38,0)");
        assertTypeOfRewrite("typeof(concat('ala', 'ma', 'kota'))", "varchar");
        assertTypeOfRewrite("typeof(typeof(123))", "varchar");
        assertTypeOfRewrite("typeof(2 + sin(2) + 2.3)", "double");
    }

    @Test
    void testAnalyzeTimeRewriteForSlotReference() {
        Map<String, Slot> slots = new HashMap<>();
        Expression expression = ExpressionRewriteTestHelper.replaceUnboundSlot(
                PARSER.parseExpression("typeof(Icol)"), slots);
        Expression rewritten = ExpressionRewriteTestHelper.typeCoercion(expression);
        Assertions.assertEquals(new VarcharLiteral("integer", -1), rewritten);
    }

    @Test
    void testDisplayNameForComplexTypes() {
        Assertions.assertEquals("real",
                TypeOfDisplayName.fromDataType(FloatType.INSTANCE));
        Assertions.assertEquals("varbinary",
                TypeOfDisplayName.fromDataType(VarBinaryType.INSTANCE));
        Assertions.assertEquals("date",
                TypeOfDisplayName.fromDataType(DateV2Type.INSTANCE));
        Assertions.assertEquals("time(0)",
                TypeOfDisplayName.fromDataType(TimeV2Type.SYSTEM_DEFAULT));
        Assertions.assertEquals("array(integer)",
                TypeOfDisplayName.fromDataType(ArrayType.of(IntegerType.INSTANCE)));
        Assertions.assertEquals("map(integer, integer)",
                TypeOfDisplayName.fromDataType(
                        MapType.of(IntegerType.INSTANCE, IntegerType.INSTANCE)));
        Assertions.assertEquals("timestamp(3)",
                TypeOfDisplayName.fromDataType(DateTimeV2Type.of(3)));
        Assertions.assertEquals("timestamp(2) with time zone",
                TypeOfDisplayName.fromDataType(TimeStampTzType.of(2)));

        StructType structType = new StructType(ImmutableList.of(
                new StructField("s_id", IntegerType.INSTANCE, true, ""),
                new StructField("scores",
                        ArrayType.of(DecimalV3Type.createDecimalV3Type(5, 1)), true, "")
        ));
        Assertions.assertEquals("row(s_id integer, scores array(decimal(5,1)))",
                TypeOfDisplayName.fromDataType(structType));
    }

    private static void assertTypeOfRewrite(String sqlExpression, String expectedTypeName) {
        Expression expression = ExpressionRewriteTestHelper.typeCoercion(
                PARSER.parseExpression(sqlExpression));
        Assertions.assertEquals(new VarcharLiteral(expectedTypeName, -1), expression);
    }
}
