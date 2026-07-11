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

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapAnd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapAndCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapAndNotCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapNot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapOr;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapOrCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapXor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapXorCount;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** Rewrite bitmap_count(bitmap_op(...)) to corresponding bitmap_op_count function. */
public class BitmapCountToBitmapOpCount implements ExpressionPatternRuleFactory {
    public static final BitmapCountToBitmapOpCount INSTANCE = new BitmapCountToBitmapOpCount();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(BitmapCount.class).then(BitmapCountToBitmapOpCount::rewrite)
                        .toRule(ExpressionRuleType.BITMAP_COUNT_TO_BITMAP_OP_COUNT)
        );
    }

    private static Expression rewrite(BitmapCount bitmapCount) {
        Expression child = bitmapCount.child(0);
        if (child instanceof BitmapAnd) {
            return new BitmapAndCount(child.child(0), child.child(1), varArgs(child));
        } else if (child instanceof BitmapOr) {
            return new BitmapOrCount(child.child(0), child.child(1), varArgs(child));
        } else if (child instanceof BitmapXor) {
            return new BitmapXorCount(child.child(0), child.child(1), varArgs(child));
        } else if (child instanceof BitmapNot) {
            return new BitmapAndNotCount(child.child(0), child.child(1));
        }
        return bitmapCount;
    }

    private static Expression[] varArgs(Expression expression) {
        return expression.children().subList(2, expression.arity()).toArray(new Expression[0]);
    }
}
