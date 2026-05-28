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
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lower;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LargeInPredicateToRangeRewriterTest extends ExpressionRewriteTestHelper {

    @Test
    public void testLargeInPredicate() {
        int originalThreshold = Config.hive_metastore_partition_pruning_in_predicate_threshold;
        try {
            Config.hive_metastore_partition_pruning_in_predicate_threshold = 2;
            SlotReference dt = new SlotReference("dt", IntegerType.INSTANCE);
            InPredicate inPredicate = new InPredicate(dt, Lists.newArrayList(
                    new IntegerLiteral(3), new IntegerLiteral(1), new NullLiteral(IntegerType.INSTANCE),
                    new IntegerLiteral(2)));

            Pair<Expression, Boolean> rewriteResult =
                    LargeInPredicateToRangeRewriter.rewrite(inPredicate, context.cascadesContext);

            Assertions.assertTrue(rewriteResult.second);
            Assertions.assertEquals(new And(new GreaterThanEqual(dt, new IntegerLiteral(1)),
                    new LessThanEqual(dt, new IntegerLiteral(3))), rewriteResult.first);
        } finally {
            Config.hive_metastore_partition_pruning_in_predicate_threshold = originalThreshold;
        }
    }

    @Test
    public void testSmallInPredicate() {
        int originalThreshold = Config.hive_metastore_partition_pruning_in_predicate_threshold;
        try {
            Config.hive_metastore_partition_pruning_in_predicate_threshold = 10;
            SlotReference dt = new SlotReference("dt", IntegerType.INSTANCE);
            InPredicate inPredicate = new InPredicate(dt, Lists.newArrayList(
                    new IntegerLiteral(1), new IntegerLiteral(2), new IntegerLiteral(3)));

            Pair<Expression, Boolean> rewriteResult =
                    LargeInPredicateToRangeRewriter.rewrite(inPredicate, context.cascadesContext);

            Assertions.assertFalse(rewriteResult.second);
            Assertions.assertEquals(inPredicate, rewriteResult.first);
        } finally {
            Config.hive_metastore_partition_pruning_in_predicate_threshold = originalThreshold;
        }
    }

    @Test
    public void testNonSlotCompareExpr() {
        int originalThreshold = Config.hive_metastore_partition_pruning_in_predicate_threshold;
        try {
            Config.hive_metastore_partition_pruning_in_predicate_threshold = 2;
            SlotReference dt = new SlotReference("dt", IntegerType.INSTANCE);
            InPredicate inPredicate = new InPredicate(new Lower(dt), Lists.newArrayList(
                    new IntegerLiteral(1), new IntegerLiteral(2), new IntegerLiteral(3)));

            Pair<Expression, Boolean> rewriteResult =
                    LargeInPredicateToRangeRewriter.rewrite(inPredicate, context.cascadesContext);

            Assertions.assertFalse(rewriteResult.second);
            Assertions.assertEquals(inPredicate, rewriteResult.first);
        } finally {
            Config.hive_metastore_partition_pruning_in_predicate_threshold = originalThreshold;
        }
    }

    @Test
    public void testNonLiteralOption() {
        int originalThreshold = Config.hive_metastore_partition_pruning_in_predicate_threshold;
        try {
            Config.hive_metastore_partition_pruning_in_predicate_threshold = 1;
            SlotReference dt = new SlotReference("dt", IntegerType.INSTANCE);
            InPredicate inPredicate = new InPredicate(dt, Lists.newArrayList(
                    new IntegerLiteral(1), new SlotReference("other", IntegerType.INSTANCE)));

            Pair<Expression, Boolean> rewriteResult =
                    LargeInPredicateToRangeRewriter.rewrite(inPredicate, context.cascadesContext);

            Assertions.assertFalse(rewriteResult.second);
            Assertions.assertEquals(inPredicate, rewriteResult.first);
        } finally {
            Config.hive_metastore_partition_pruning_in_predicate_threshold = originalThreshold;
        }
    }

    @Test
    public void testOnlyNullOptions() {
        int originalThreshold = Config.hive_metastore_partition_pruning_in_predicate_threshold;
        try {
            Config.hive_metastore_partition_pruning_in_predicate_threshold = 1;
            SlotReference dt = new SlotReference("dt", IntegerType.INSTANCE);
            InPredicate inPredicate = new InPredicate(dt, Lists.newArrayList(
                    new NullLiteral(IntegerType.INSTANCE), new NullLiteral(IntegerType.INSTANCE)));

            Pair<Expression, Boolean> rewriteResult =
                    LargeInPredicateToRangeRewriter.rewrite(inPredicate, context.cascadesContext);

            Assertions.assertFalse(rewriteResult.second);
            Assertions.assertEquals(inPredicate, rewriteResult.first);
        } finally {
            Config.hive_metastore_partition_pruning_in_predicate_threshold = originalThreshold;
        }
    }
}
