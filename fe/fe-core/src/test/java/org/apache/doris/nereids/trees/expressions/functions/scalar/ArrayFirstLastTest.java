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
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ArrayFirstLastTest extends ExpressionRewriteTestHelper {

    @Test
    public void testArrayFirstLambdaResultCanCastToBoolean() {
        assertLambdaResultCastToBoolean("array_first(x -> x, [0, 1])", 1);
    }

    @Test
    public void testArrayLastLambdaResultCanCastToBoolean() {
        assertLambdaResultCastToBoolean("array_last(x -> x, [0, 1])", -1);
    }

    private void assertLambdaResultCastToBoolean(String sql, long expectedIndex) {
        Expression analyzed = typeCoercion(PARSER.parseExpression(sql));
        Assertions.assertTrue(analyzed instanceof ElementAt);

        ElementAt elementAt = (ElementAt) analyzed;
        Assertions.assertTrue(elementAt.left() instanceof ArrayFilter);
        Assertions.assertEquals(expectedIndex, ((BigIntLiteral) elementAt.right()).getValue());

        ArrayFilter arrayFilter = (ArrayFilter) elementAt.left();
        Expression filterResult = arrayFilter.child(1);
        Assertions.assertTrue(filterResult instanceof Cast);
        Assertions.assertEquals(ArrayType.of(BooleanType.INSTANCE), filterResult.getDataType());
        Assertions.assertTrue(((Cast) filterResult).child() instanceof ArrayMap);
    }
}
