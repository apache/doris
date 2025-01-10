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

class InPredicateExtractNonConstantTest extends ExpressionRewriteTestHelper {
    @Test
    public void testRewrite() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(
                        InPredicateExtractNonConstant.INSTANCE
                )
        ));

        assertRewriteAfterTypeCoercion("TA in (3, 2, 1)", "TA in (3, 2, 1)");
        assertRewriteAfterTypeCoercion("TA in (TB, TC, TB)", "TA = TB or TA = TC");
        assertRewriteAfterTypeCoercion("TA in (3, 2, 1, TB, TC, TB)", "TA in (3, 2, 1) or TA = TB or TA = TC");
        assertRewriteAfterTypeCoercion("TA in (1 + 2, 2 + 3, 3 + TB)", "TA in (1 + 2, 2 + 3) or TA = 3 + TB");
    }
}
