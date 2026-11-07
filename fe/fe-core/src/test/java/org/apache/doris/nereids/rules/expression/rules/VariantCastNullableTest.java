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

import org.apache.doris.common.Config;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class VariantCastNullableTest extends ExpressionRewriteTestHelper {
    @Test
    public void testNullPredicatesRetainVariantCast() {
        boolean original = Config.enable_variant_v2;
        try {
            Config.enable_variant_v2 = false;
            executor = new ExpressionRuleExecutor(ImmutableList.of(
                    bottomUp(NullSafeEqualToEqual.INSTANCE)));
            Cast cast = new Cast(new SlotReference("v", VariantType.INSTANCE, false), StringType.INSTANCE);
            assertRewrite(new IsNull(cast), new IsNull(cast));
            assertRewrite(new NullSafeEqual(cast, new NullLiteral(StringType.INSTANCE)), new IsNull(cast));
        } finally {
            Config.enable_variant_v2 = original;
        }
    }

    @Test
    public void testConditionalFunctionsRetainVariantCastFallback() {
        boolean original = Config.enable_variant_v2;
        try {
            Config.enable_variant_v2 = false;
            executor = new ExpressionRuleExecutor(ImmutableList.of(bottomUp(SimplifyConditionalFunction.INSTANCE)));
            Cast cast = new Cast(new SlotReference("v", VariantType.INSTANCE, false), StringType.INSTANCE);
            StringLiteral fallback = new StringLiteral("fallback");
            assertRewrite(new Coalesce(cast, fallback), new Coalesce(cast, fallback));
            assertRewrite(new Nvl(cast, fallback), new Nvl(cast, fallback));
        } finally {
            Config.enable_variant_v2 = original;
        }
    }
}
