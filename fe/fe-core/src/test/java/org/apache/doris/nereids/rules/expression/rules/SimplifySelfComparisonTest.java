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

import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class SimplifySelfComparisonTest extends ExpressionRewriteTestHelper {

    @Test
    void testRewrite() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                ExpressionRewrite.bottomUp(SimplifySelfComparison.INSTANCE)
        ));

        // foldable, cast
        assertRewriteAfterTypeCoercion("TA + TB = TA + TB", "NOT ((TA + TB) IS NULL) OR NULL");
        assertRewriteAfterTypeCoercion("TA + TB >= TA + TB", "NOT ((TA + TB) IS NULL) OR NULL");
        assertRewriteAfterTypeCoercion("TA + TB <= TA + TB", "NOT ((TA + TB) IS NULL) OR NULL");
        assertRewriteAfterTypeCoercion("TA + TB <=> TA + TB", "TRUE");
        assertRewriteAfterTypeCoercion("TA + TB > TA + TB", "(TA + TB) IS NULL AND NULL");
        assertRewriteAfterTypeCoercion("TA + TB < TA + TB", "(TA + TB) IS NULL AND NULL");
        assertRewriteAfterTypeCoercion("DAYS_ADD(CA, 7) <=> DAYS_ADD(CA, 7)", "TRUE");
        assertRewriteAfterTypeCoercion("USER() = USER()", "TRUE");
        assertRewriteAfterTypeCoercion("CURRENT_TIMESTAMP() = CURRENT_TIMESTAMP()", "TRUE");

        // non-foldable expressions are distinct, the two random will not equals
        assertRewriteAfterTypeCoercion("random(5, 10) = random(5, 10)", "random(5, 10) = random(5, 10)");
        assertRewriteAfterTypeCoercion("random(5, 10) + 100 = random(5, 10) + 100", "random(5, 10) + 100 = random(5, 10) + 100");
    }

}
