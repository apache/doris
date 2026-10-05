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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class LikeToEqualRewriteTest extends ExpressionRewriteTestHelper {
    @Test
    public void testLikeRegularConst() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(LikeToEqualRewrite.INSTANCE)
        ));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        VarcharLiteral str = new VarcharLiteral("abc");
        assertRewrite(new Like(slot, str), new EqualTo(slot, str));
    }

    @Test
    public void testLikeEscapePercentSignConst() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(LikeToEqualRewrite.INSTANCE)
        ));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        VarcharLiteral str = new VarcharLiteral("abc\\%");
        VarcharLiteral strForEqual = new VarcharLiteral("abc%");
        assertRewrite(new Like(slot, str), new EqualTo(slot, strForEqual));
    }

    @Test
    public void testLikeEscapeUnderlineConst() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(LikeToEqualRewrite.INSTANCE)
        ));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        VarcharLiteral str = new VarcharLiteral("abc\\_");
        VarcharLiteral strForEqual = new VarcharLiteral("abc_");
        assertRewrite(new Like(slot, str), new EqualTo(slot, strForEqual));
    }

    @Test
    public void testLikeNotEscapePercentSignConst() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(LikeToEqualRewrite.INSTANCE)
        ));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        VarcharLiteral str = new VarcharLiteral("abc%");
        assertRewrite(new Like(slot, str), new Like(slot, str));
    }

    @Test
    public void testLikeNotEscapeUnderlineConst() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(LikeToEqualRewrite.INSTANCE)
        ));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        VarcharLiteral str = new VarcharLiteral("abc_");
        assertRewrite(new Like(slot, str), new Like(slot, str));
    }

    @Test
    public void testLikeTwoEscape() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(LikeToEqualRewrite.INSTANCE)
        ));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        VarcharLiteral str = new VarcharLiteral("abc\\\\");
        VarcharLiteral strForEqual = new VarcharLiteral("abc\\");
        assertRewrite(new Like(slot, str), new EqualTo(slot, strForEqual));
    }

    @Test
    public void testLikeThreeEscape() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(LikeToEqualRewrite.INSTANCE)
        ));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        VarcharLiteral str = new VarcharLiteral("abc\\\\\\");
        VarcharLiteral strForEqual = new VarcharLiteral("abc\\\\");
        assertRewrite(new Like(slot, str), new EqualTo(slot, strForEqual));
    }

    @Test
    public void testLikeFourEscape() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(LikeToEqualRewrite.INSTANCE)
        ));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        VarcharLiteral str = new VarcharLiteral("abc\\\\\\\\");
        VarcharLiteral strForEqual = new VarcharLiteral("abc\\\\");
        assertRewrite(new Like(slot, str), new EqualTo(slot, strForEqual));
    }

    @Test
    public void testLikeTwoEscapeAndPercentSign() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(LikeToEqualRewrite.INSTANCE)
        ));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        VarcharLiteral str = new VarcharLiteral("abc\\\\%");
        assertRewrite(new Like(slot, str), new Like(slot, str));
    }

    @Test
    public void testLikeThreeEscapeAndPercentSign() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(LikeToEqualRewrite.INSTANCE)
        ));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        VarcharLiteral str = new VarcharLiteral("abc\\\\\\%");
        VarcharLiteral strForEqual = new VarcharLiteral("abc\\%");
        assertRewrite(new Like(slot, str), new EqualTo(slot, strForEqual));
    }

    @Test
    public void testLikeFourEscapeAndPercentSign() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(LikeToEqualRewrite.INSTANCE)
        ));
        SlotReference slot = new SlotReference("a", StringType.INSTANCE, true);
        VarcharLiteral str = new VarcharLiteral("abc\\\\\\\\%");
        assertRewrite(new Like(slot, str), new Like(slot, str));
    }
}
