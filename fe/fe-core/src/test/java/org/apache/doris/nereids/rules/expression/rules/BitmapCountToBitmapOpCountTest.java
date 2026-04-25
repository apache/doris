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
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapAnd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapAndCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapAndNot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapAndNotAlias;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapAndNotCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapNot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapOr;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapOrCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapXor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapXorCount;
import org.apache.doris.nereids.types.BitmapType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BitmapCountToBitmapOpCountTest extends ExpressionRewriteTestHelper {

    private final SlotReference bitmap1 = new SlotReference("bitmap1", BitmapType.INSTANCE, true);
    private final SlotReference bitmap2 = new SlotReference("bitmap2", BitmapType.INSTANCE, true);
    private final SlotReference bitmap3 = new SlotReference("bitmap3", BitmapType.INSTANCE, true);

    @BeforeEach
    void setUp() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(BitmapCountToBitmapOpCount.INSTANCE)
        ));
    }

    @Test
    void testRewriteBitmapAnd() {
        assertRewrite(
                new BitmapCount(new BitmapAnd(bitmap1, bitmap2)),
                new BitmapAndCount(bitmap1, bitmap2)
        );
    }

    @Test
    void testRewriteBitmapAndWithVarArgs() {
        assertRewrite(
                new BitmapCount(new BitmapAnd(bitmap1, bitmap2, bitmap3)),
                new BitmapAndCount(bitmap1, bitmap2, bitmap3)
        );
    }

    @Test
    void testRewriteBitmapOr() {
        assertRewrite(
                new BitmapCount(new BitmapOr(bitmap1, bitmap2, bitmap3)),
                new BitmapOrCount(bitmap1, bitmap2, bitmap3)
        );
    }

    @Test
    void testRewriteBitmapXor() {
        assertRewrite(
                new BitmapCount(new BitmapXor(bitmap1, bitmap2)),
                new BitmapXorCount(bitmap1, bitmap2)
        );
    }

    @Test
    void testRewriteBitmapNot() {
        assertRewrite(
                new BitmapCount(new BitmapNot(bitmap1, bitmap2)),
                new BitmapAndNotCount(bitmap1, bitmap2)
        );
    }

    @Test
    void testRewriteBitmapAndNot() {
        assertRewrite(
                new BitmapCount(new BitmapAndNot(bitmap1, bitmap2)),
                new BitmapAndNotCount(bitmap1, bitmap2)
        );
    }

    @Test
    void testRewriteBitmapAndNotAlias() {
        assertRewrite(
                new BitmapCount(new BitmapAndNotAlias(bitmap1, bitmap2)),
                new BitmapAndNotCount(bitmap1, bitmap2)
        );
    }

    @Test
    void testKeepPlainBitmapCount() {
        assertRewrite(
                new BitmapCount(bitmap1),
                new BitmapCount(bitmap1)
        );
    }
}
