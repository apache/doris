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

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

class SimplifyCastRuleTest extends ExpressionRewriteTestHelper {

    @Test
    public void testSimplify() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(SimplifyCastRule.INSTANCE));
        assertRewrite(new Cast(new VarcharLiteral("1"), StringType.INSTANCE),
                new StringLiteral("1"));
        assertRewrite(new Cast(new VarcharLiteral("1"), VarcharType.SYSTEM_DEFAULT),
                new VarcharLiteral("1", -1));
        assertRewrite(new Cast(new TinyIntLiteral((byte) 1), DecimalV3Type.SYSTEM_DEFAULT),
                new DecimalV3Literal(DecimalV3Type.SYSTEM_DEFAULT, new BigDecimal("1.000000000")));
        assertRewrite(new Cast(new SmallIntLiteral((short) 1000), DecimalV3Type.SYSTEM_DEFAULT),
                new DecimalV3Literal(DecimalV3Type.SYSTEM_DEFAULT, new BigDecimal("1000.000000000")));
        assertRewrite(new Cast(new VarcharLiteral("1"), VarcharType.SYSTEM_DEFAULT), new VarcharLiteral("1", -1));
        assertRewrite(new Cast(new VarcharLiteral("1"), VarcharType.SYSTEM_DEFAULT), new VarcharLiteral("1", -1));

        Expression decimalV3Literal = new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(5, 3),
                new BigDecimal("12.000"));
        assertRewrite(new Cast(decimalV3Literal, DecimalV3Type.createDecimalV3Type(7, 3)),
                new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(7, 3),
                        new BigDecimal("12.000")));
        assertRewrite(new Cast(decimalV3Literal, DecimalV3Type.createDecimalV3Type(3, 1)),
                new DecimalV3Literal(DecimalV3Type.createDecimalV3Type(3, 1),
                        new BigDecimal("12.0")));
    }
}
