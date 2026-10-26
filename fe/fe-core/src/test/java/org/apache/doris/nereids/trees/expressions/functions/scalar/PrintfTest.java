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

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PrintfTest extends ExpressionRewriteTestHelper {

    @Test
    public void testFormatWithoutArguments() {
        Expression analyzed = typeCoercion(PARSER.parseExpression("printf('100%%')"));
        Assertions.assertInstanceOf(Printf.class, analyzed);
        Assertions.assertEquals(1, analyzed.arity());
        Assertions.assertEquals(StringType.INSTANCE, analyzed.getDataType());
        Assertions.assertFalse(analyzed.nullable());
    }

    @Test
    public void testMixedArgumentsAndDecimalCoercion() {
        Expression analyzed = typeCoercion(PARSER.parseExpression(
                "printf('%d-%f-%s', cast(100 as int), cast(1.25 as decimal(10,2)), 'test')"));
        Assertions.assertInstanceOf(Printf.class, analyzed);
        Assertions.assertEquals(IntegerType.INSTANCE, analyzed.child(1).getDataType());
        Assertions.assertInstanceOf(Cast.class, analyzed.child(2));
        Assertions.assertEquals(DoubleType.INSTANCE, analyzed.child(2).getDataType());
        Assertions.assertTrue(analyzed.child(3).getDataType().isStringLikeType());
        Assertions.assertEquals(StringType.INSTANCE, analyzed.getDataType());
    }

    @Test
    public void testNullableArguments() {
        Expression nullFormat = typeCoercion(PARSER.parseExpression("printf(null, 1)"));
        Expression nullValue = typeCoercion(PARSER.parseExpression("printf('%s', null)"));
        Assertions.assertTrue(nullFormat.nullable());
        Assertions.assertTrue(nullValue.nullable());
    }
}
