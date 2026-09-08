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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RegexpExtract;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RegexpReplace;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RegexpReplaceOne;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RegexpFunctionRewriteTest extends ExpressionRewriteTestHelper {
    private SlotReference str;

    @BeforeEach
    public void setup() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(RegexpFunctionRewrite.INSTANCE)
        ));
        str = new SlotReference("str", StringType.INSTANCE, true);
    }

    @Test
    public void testRewriteAnchoredRegexpReplace() {
        RegexpReplace before = new RegexpReplace(str, new VarcharLiteral("^https?://(?:www\\.)?([^/]+)/.*$"),
                new VarcharLiteral("\\1"));
        RegexpReplaceOne expected = new RegexpReplaceOne(str,
                new VarcharLiteral("^https?://(?:www\\.)?([^/]+)/.*$"), new VarcharLiteral("\\1"));
        assertRuleRewrite(before, expected);
    }

    @Test
    public void testRewriteAnchoredRegexpReplaceWithOptions() {
        RegexpReplace before = new RegexpReplace(str, new VarcharLiteral("^abc"), new VarcharLiteral("x"),
                new VarcharLiteral("ignore_invalid_escape"));
        RegexpReplaceOne expected = new RegexpReplaceOne(str, new VarcharLiteral("^abc"), new VarcharLiteral("x"),
                new VarcharLiteral("ignore_invalid_escape"));
        assertRuleRewrite(before, expected);
    }

    @Test
    public void testDoNotRewriteRegexpReplaceWithAlternation() {
        RegexpReplace before = new RegexpReplace(str, new VarcharLiteral("^a|b"), new VarcharLiteral("x"));
        assertRuleNoRewrite(before);
    }

    @Test
    public void testDoNotRewriteRegexpReplaceWithEscapedDollar() {
        RegexpReplace before = new RegexpReplace(str, new VarcharLiteral("a\\$"), new VarcharLiteral("x"));
        assertRuleNoRewrite(before);
    }

    @Test
    public void testDoNotRewriteRegexpReplaceWithInlineMultilineFlag() {
        RegexpReplace before = new RegexpReplace(str, new VarcharLiteral("(?m)a$"), new VarcharLiteral("x"));
        assertRuleNoRewrite(before);
    }

    @Test
    public void testRewriteRegexpExtractTrimSuffix() {
        RegexpExtract before = new RegexpExtract(str, new VarcharLiteral("^.*(abc).*$"), new BigIntLiteral(1));
        RegexpExtract expected = new RegexpExtract(str, new VarcharLiteral("^.*(abc)"), new BigIntLiteral(1));
        assertRuleRewrite(before, expected);
    }

    @Test
    public void testRewriteRegexpExtractTrimSuffixOnly() {
        RegexpExtract before = new RegexpExtract(str, new VarcharLiteral("^([a-z]+).*$"), new BigIntLiteral(1));
        RegexpExtract expected = new RegexpExtract(str, new VarcharLiteral("^([a-z]+)"), new BigIntLiteral(1));
        assertRuleRewrite(before, expected);
    }

    @Test
    public void testDoNotRewriteRegexpExtractGroupZero() {
        RegexpExtract before = new RegexpExtract(str, new VarcharLiteral("^.*(abc).*$"), new BigIntLiteral(0));
        assertRuleNoRewrite(before);
    }

    @Test
    public void testDoNotRewriteRegexpExtractWithoutCapture() {
        RegexpExtract before = new RegexpExtract(str, new VarcharLiteral("^.*abc.*$"), new BigIntLiteral(1));
        assertRuleNoRewrite(before);
    }

    @Test
    public void testRewriteRegexpExtractLazyPrefixSuffixOnly() {
        RegexpExtract before = new RegexpExtract(str, new VarcharLiteral("^.*?(abc).*$"), new BigIntLiteral(1));
        RegexpExtract expected = new RegexpExtract(str, new VarcharLiteral("^.*?(abc)"), new BigIntLiteral(1));
        assertRuleRewrite(before, expected);
    }

    @Test
    public void testDoNotRewriteRegexpExtractEscapedDotSuffix() {
        RegexpExtract before = new RegexpExtract(str, new VarcharLiteral("^(a)\\.*$"), new BigIntLiteral(1));
        assertRuleNoRewrite(before);
    }

    @Test
    public void testDoNotRewriteRegexpExtractWithAlternation() {
        RegexpExtract before = new RegexpExtract(str, new VarcharLiteral("(a)|(b).*$"), new BigIntLiteral(1));
        assertRuleNoRewrite(before);
    }

    @Test
    public void testDoNotRewriteRegexpExtractWithInlineDotFlag() {
        RegexpExtract before = new RegexpExtract(str, new VarcharLiteral("(?-s)^(a).*$"), new BigIntLiteral(1));
        assertRuleNoRewrite(before);
    }

    private void assertRuleRewrite(Expression before, Expression expected) {
        Assertions.assertEquals(expected, executor.rewrite(before, context));
    }

    private void assertRuleNoRewrite(Expression before) {
        Assertions.assertEquals(before, executor.rewrite(before, context));
    }
}
