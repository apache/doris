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

import org.apache.doris.nereids.rules.expression.rules.InPredicateDedup;
import org.apache.doris.nereids.rules.expression.rules.InPredicateToEqualToRule;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class InPredicateDedupTest extends ExpressionRewriteTestHelper {
    // The default expression width limit of 10,000 also counts the compare expression.
    private static final int LARGE_LIST_SIZE = 9_999;

    private final SlotReference key = new SlotReference("k", IntegerType.INSTANCE);
    private final ExpressionRuleExecutor dedupExecutor = new ExpressionRuleExecutor(
            ImmutableList.of(bottomUp(InPredicateDedup.INSTANCE, InPredicateToEqualToRule.INSTANCE)));

    @Test
    void deduplicatesRepeatedLiteralListsOnBothSidesOfTheLimit() {
        IntegerLiteral value = new IntegerLiteral(7);
        for (int size : new int[] {200, 201, LARGE_LIST_SIZE}) {
            InPredicate original = new InPredicate(key, Collections.nCopies(size, value));
            Assertions.assertEquals(new EqualTo(key, value), dedupExecutor.rewrite(original, context),
                    "list size " + size);
        }
    }

    @Test
    void keepsTheFunctionAndTheRestOfTheListInOrder() {
        List<Expression> distinctPrefix = new ArrayList<>();
        for (int value = 100; value > 0; value--) {
            distinctPrefix.add(new IntegerLiteral(value));
        }
        List<Expression> options = new ArrayList<>();
        for (int repeat = 0; repeat < 4; repeat++) {
            options.addAll(distinctPrefix);
        }
        List<Expression> suffix = new ArrayList<>();
        suffix.add(new Abs(new SlotReference("v", IntegerType.INSTANCE)));
        suffix.addAll(Collections.nCopies(100, new IntegerLiteral(7)));
        options.addAll(suffix);
        List<Expression> expected = new ArrayList<>(distinctPrefix);
        expected.addAll(suffix);

        InPredicate original = new InPredicate(key, options);
        InPredicate rewritten = (InPredicate) dedupExecutor.rewrite(original, context);
        Assertions.assertEquals(expected, rewritten.getOptions());
    }

    @Test
    void keepsLargeListsWithMoreThanOneHundredDistinctPrefixValues() {
        List<Expression> options = new ArrayList<>();
        for (int value = 0; value < 101; value++) {
            options.add(new IntegerLiteral(value));
        }
        options.addAll(Collections.nCopies(LARGE_LIST_SIZE - options.size(), new IntegerLiteral(0)));

        InPredicate original = new InPredicate(key, options);
        Assertions.assertSame(original, dedupExecutor.rewrite(original, context));
    }

    @Test
    void retainsOneNullInLargeNotInList() {
        IntegerLiteral value = new IntegerLiteral(7);
        NullLiteral nullValue = new NullLiteral(IntegerType.INSTANCE);
        List<Expression> options = new ArrayList<>(Collections.nCopies(201, value));
        options.add(nullValue);
        options.add(nullValue);

        Not original = new Not(new InPredicate(key, options));
        Not expected = new Not(new InPredicate(key, ImmutableList.of(value, nullValue)));
        Assertions.assertEquals(expected, dedupExecutor.rewrite(original, context));
    }
}
