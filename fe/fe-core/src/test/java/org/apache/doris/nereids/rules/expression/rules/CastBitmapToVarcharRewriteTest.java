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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapEmpty;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapFromString;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CastBitmapToVarcharRewriteTest extends ExpressionRewriteTestHelper {
    @Test
    public void testRewrite() {
        executor = new ExpressionRuleExecutor(ExpressionNormalization.NORMALIZE_REWRITE_RULES);
        Assertions.assertEquals(1, new CastBitmapToVarcharRewrite().buildRules().size());
        assertRewriteAfterCastBitmapToVarchar(new Cast(new BitmapEmpty(), VarcharType.MAX_VARCHAR_TYPE),
                "bitmap_to_string(bitmap_empty())", StringType.INSTANCE);
        assertRewriteAfterCastBitmapToVarchar(new Cast(new BitmapFromString(new VarcharLiteral("1,2,3,4,5")),
                VarcharType.MAX_VARCHAR_TYPE), "bitmap_to_string(bitmap_from_string('1,2,3,4,5'))",
                StringType.INSTANCE);
    }

    private void assertRewriteAfterCastBitmapToVarchar(Expression needRewriteExpression, String expected,
            DataType expectedType) {
        Expression rewritten = executor.rewrite(needRewriteExpression, context);
        Expression expectedExpression = PARSER.parseExpression(expected);
        Assertions.assertEquals(expectedExpression.toSql(), rewritten.toSql());
        Assertions.assertEquals(expectedType, rewritten.getDataType());
    }
}
