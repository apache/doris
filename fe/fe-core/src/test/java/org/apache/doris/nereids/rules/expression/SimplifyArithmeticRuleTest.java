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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.rules.expression.rules.FoldConstantRule;
import org.apache.doris.nereids.rules.expression.rules.FunctionBinder;
import org.apache.doris.nereids.rules.expression.rules.SimplifyArithmeticComparisonRule;
import org.apache.doris.nereids.rules.expression.rules.SimplifyArithmeticRule;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class SimplifyArithmeticRuleTest extends ExpressionRewriteTestHelper {
    @Test
    public void testSimplifyArithmetic() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                SimplifyArithmeticRule.INSTANCE,
                FunctionBinder.INSTANCE,
                FoldConstantRule.INSTANCE
        ));
        assertRewriteAfterTypeCoercion("IA", "IA");
        assertRewriteAfterTypeCoercion("IA + 1", "IA + 1");
        assertRewriteAfterTypeCoercion("IA + IB", "IA + IB");
        assertRewriteAfterTypeCoercion("1 * 3 / IA", "(3 / cast(IA as DOUBLE))");
        assertRewriteAfterTypeCoercion("1 - IA", "1 - IA");
        assertRewriteAfterTypeCoercion("1 + 1", "2");
        assertRewriteAfterTypeCoercion("IA + 2 - 1", "IA + 1");
        assertRewriteAfterTypeCoercion("IA + 2 - (1 - 1)", "IA + 2");
        assertRewriteAfterTypeCoercion("IA + 2 - ((1 - IB) - (3 + IC))", "IA + IB + IC + 4");
        assertRewriteAfterTypeCoercion("IA * IB + 2 - IC * 2", "(IA * IB) - (IC * 2) + 2");
        assertRewriteAfterTypeCoercion("IA * IB", "IA * IB");
        assertRewriteAfterTypeCoercion("IA * IB / 2 * 2", "cast((IA * IB) as DOUBLE) / 1");
        assertRewriteAfterTypeCoercion("IA * IB / (2 * 2)", "cast((IA * IB) as DOUBLE) / 4");
        assertRewriteAfterTypeCoercion("IA * IB / (2 * 2)", "cast((IA * IB) as DOUBLE) / 4");
        assertRewriteAfterTypeCoercion("IA * (IB / 2) * 2)", "cast(IA as DOUBLE) * cast(IB as DOUBLE) / 1");
        assertRewriteAfterTypeCoercion("IA * (IB / 2) * (IC + 1))", "cast(IA as DOUBLE) * cast(IB as DOUBLE) * cast((IC + 1) as DOUBLE) / 2");
        assertRewriteAfterTypeCoercion("IA * IB / 2 / IC * 2 * ID / 4", "(((cast((IA * IB) as DOUBLE) / cast(IC as DOUBLE)) * cast(ID as DOUBLE)) / 4)");
    }

    @Test
    public void testSimplifyArithmeticComparison() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                SimplifyArithmeticRule.INSTANCE,
                FoldConstantRule.INSTANCE,
                SimplifyArithmeticComparisonRule.INSTANCE,
                SimplifyArithmeticRule.INSTANCE,
                FunctionBinder.INSTANCE,
                FoldConstantRule.INSTANCE
        ));
        assertRewriteAfterTypeCoercion("IA", "IA");
        assertRewriteAfterTypeCoercion("IA > IB", "IA > IB");
        assertRewriteAfterTypeCoercion("IA - 1 > 1", "(cast(IA as BIGINT) > 2)");
        assertRewriteAfterTypeCoercion("IA + 1 - 1 >= 1", "(cast(IA as BIGINT) >= 1)");
        assertRewriteAfterTypeCoercion("IA - 1 - 1 < 1", "(cast(IA as BIGINT) < 3)");
        assertRewriteAfterTypeCoercion("IA - 1 - 1 = 1", "(cast(IA as BIGINT) = 3)");
        assertRewriteAfterTypeCoercion("IA + (1 - 1) >= 1", "(cast(IA as BIGINT) >= 1)");
        assertRewriteAfterTypeCoercion("IA - 1 * 1 = 1", "(cast(IA as BIGINT) = 2)");
        assertRewriteAfterTypeCoercion("IA + 1 + 1 > IB", "(cast(IA as BIGINT) > (cast(IB as BIGINT) - 2))");
        assertRewriteAfterTypeCoercion("IA + 1 > IB", "(cast(IA as BIGINT) > (cast(IB as BIGINT) - 1))");
        assertRewriteAfterTypeCoercion("IA + 1 - (IB - 1) > 1", "IA - IB > -1");
        assertRewriteAfterTypeCoercion("IA + 1 - (IB * IC - 1) > 1", "IA - IB * IC > -1");
        assertRewriteAfterTypeCoercion("IA + 1 - (IB * IC - 1) > 1", "IA - IB * IC > -1");
        assertRewriteAfterTypeCoercion("IA + 1 - (IB * IC - 1) > 1", "((IA - (IB * IC)) > -1)");
        assertRewriteAfterTypeCoercion("(IA - 1) + (IB + 1) - (IC - 1) > 1", "(((IA + IB) - IC) > 0)");
        assertRewriteAfterTypeCoercion("(IA - 1) + (IB + 1) - ((IC - 1) - (ID + 1)) > 1", "((((IA + IB) - IC) + ID) > -1)");
        assertRewriteAfterTypeCoercion("(IA - 1) + (IB + 1) - ((IC - 1) - (ID * IE + 1)) > 1", "((((IA + IB) - IC) + (ID * IE)) > -1)");
        assertRewriteAfterTypeCoercion("IA + 1 - (IB - 1) > IC + 1 - 1 + ID", "((IA - IB) > ((IC + ID) + -2))");
        assertRewriteAfterTypeCoercion("IA + 1 - (IB - 1) > IC - 1", "((IA - IB) > (IC - 3))");
        assertRewriteAfterTypeCoercion("IA + 1 > IB - 1", "(cast(IA as BIGINT) > (IB - 2))");
        assertRewriteAfterTypeCoercion("IA > IB - 1", "cast(IA as bigint) > IB - 1");
        assertRewriteAfterTypeCoercion("IA + 1 > IB", "cast(IA as BIGINT) > (cast(IB as BIGINT) - 1)");
        assertRewriteAfterTypeCoercion("IA + 1 > IB * IC", "cast(IA as BIGINT) > ((IB * IC) - 1)");
        assertRewriteAfterTypeCoercion("IA * ID > IB * IC", "IA * ID > IB * IC");
        assertRewriteAfterTypeCoercion("IA * ID / 2 > IB * IC", "cast((IA * ID) as DOUBLE) > cast((IB * IC) as DOUBLE) * 2");
        assertRewriteAfterTypeCoercion("IA * ID / -2 > IB * IC", "cast((IB * IC) as DOUBLE) * -2 > cast((IA * ID) as DOUBLE)");
        assertRewriteAfterTypeCoercion("1 - IA > 1", "(cast(IA as BIGINT) < 0)");
        assertRewriteAfterTypeCoercion("1 - IA + 1 * 3 - 5 > 1", "(cast(IA as BIGINT) < -2)");
    }

}

