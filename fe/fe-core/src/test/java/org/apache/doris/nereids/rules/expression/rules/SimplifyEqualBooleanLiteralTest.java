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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class SimplifyEqualBooleanLiteralTest extends ExpressionRewriteTestHelper {

    @Test
    void testEqualToTrue() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        SimplifyEqualBooleanLiteral.INSTANCE
                )
        ));

        assertRewriteAfterTypeCoercion("a > 1 = true", "a > 1");
        assertRewriteAfterTypeCoercion("Ba = true", "Ba = true");
    }

    @Test
    void testEqualToFalse() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        SimplifyEqualBooleanLiteral.INSTANCE
                )
        ));

        assertRewriteAfterTypeCoercion("a > 1 = false", "not(a > 1)");
        assertRewriteAfterTypeCoercion("Ba = false", "Ba = false");
    }

    @Test
    void testNullSafeEqualToFalse() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        SimplifyEqualBooleanLiteral.INSTANCE
                )
        ));

        assertRewriteAfterTypeCoercion("a > 1 <=> false", "a > 1 <=> false");
        assertRewriteAfterTypeCoercion("Xa > 1 <=> false", "not(Xa > 1)");
        assertRewriteAfterTypeCoercion("Ba <=> false", "Ba <=> false");
    }

    @Test
    void testNullSafeEqualToTrue() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        SimplifyEqualBooleanLiteral.INSTANCE
                )
        ));

        assertRewriteAfterTypeCoercion("Ba <=> true", "Ba <=> true");
        assertRewriteAfterTypeCoercion("null <=> true", "false");
        assertRewriteAfterTypeCoercion("a > 1 <=> true", "a > 1 and a is not null");
        assertRewriteAfterTypeCoercion("Xa > 1 <=> true", "Xa > 1");
        assertRewriteAfterTypeCoercion("Xa > null <=> true", "Xa > null <=> true");
        assertRewriteAfterTypeCoercion("a + b > c - d <=> true", "a + b > c - d and a is not null and b is not null and c is not null and d is not null");
        assertRewriteAfterTypeCoercion("(a in (1, 2, c)) <=> true", "(a in (1, 2, c)) <=> true");
        assertRewriteAfterTypeCoercion("(a in (1, 2, 3, null)) <=> true", "a is not null and a in (1, 2, 3)");
        assertRewriteAfterTypeCoercion("(a in (null, null, null)) <=> true", "false");
        assertRewriteAfterTypeCoercion("(a + b in (1, 2, 3, null)) <=> true", "a is not null and b is not null and a + b in (1, 2, 3)");
        assertRewriteAfterTypeCoercion("(a > 1 and b > 1 and (c > d or e > 1)) <=> true",
                "a > 1 and a is not null and b > 1 and b is not null and (c > d and c is not null and d is not null or e > 1 and e is not null)");
        assertRewriteAfterTypeCoercion("(a / b > 1) <=> true", "(a / b > 1) <=> true");
        assertRewriteAfterTypeCoercion("(a / b > 1 and c > 1 or (d > 1)) <=> true",
                "(a / b > 1) <=> true and (c > 1 and c is not null) or (d > 1 and d is not null)");
    }
}
