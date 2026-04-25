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
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapAndNot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapAndNotAlias;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapAndNotCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapNot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapOr;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapOrCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapXor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapXorCount;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Rewrite bitmap_count(bitmap_xxx(...)) to bitmap_xxx_count(...).
 */
public class BitmapCountToBitmapOpCount implements ExpressionPatternRuleFactory {

    public static final BitmapCountToBitmapOpCount INSTANCE = new BitmapCountToBitmapOpCount();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(BitmapCount.class)
                        .then(BitmapCountToBitmapOpCount::rewrite)
                        .toRule(ExpressionRuleType.BITMAP_COUNT_TO_BITMAP_OP_COUNT)
        );
    }

    private static Expression rewrite(BitmapCount bitmapCount) {
        Expression child = bitmapCount.child();
        if (child instanceof BitmapAnd) {
            return rewriteBitmapAnd((BitmapAnd) child);
        }
        if (child instanceof BitmapOr) {
            return rewriteBitmapOr((BitmapOr) child);
        }
        if (child instanceof BitmapXor) {
            return rewriteBitmapXor((BitmapXor) child);
        }
        if (child instanceof BitmapNot) {
            BitmapNot bitmapNot = (BitmapNot) child;
            return new BitmapAndNotCount(bitmapNot.child(0), bitmapNot.child(1));
        }
        if (child instanceof BitmapAndNot) {
            BitmapAndNot bitmapAndNot = (BitmapAndNot) child;
            return new BitmapAndNotCount(bitmapAndNot.child(0), bitmapAndNot.child(1));
        }
        if (child instanceof BitmapAndNotAlias) {
            BitmapAndNotAlias bitmapAndNotAlias = (BitmapAndNotAlias) child;
            return new BitmapAndNotCount(bitmapAndNotAlias.child(0), bitmapAndNotAlias.child(1));
        }
        return bitmapCount;
    }

    private static Expression rewriteBitmapAnd(BitmapAnd bitmapAnd) {
        List<Expression> children = bitmapAnd.children();
        return new BitmapAndCount(children.get(0), children.get(1),
                children.subList(2, children.size()).toArray(new Expression[0]));
    }

    private static Expression rewriteBitmapOr(BitmapOr bitmapOr) {
        List<Expression> children = bitmapOr.children();
        return new BitmapOrCount(children.get(0), children.get(1),
                children.subList(2, children.size()).toArray(new Expression[0]));
    }

    private static Expression rewriteBitmapXor(BitmapXor bitmapXor) {
        List<Expression> children = bitmapXor.children();
        return new BitmapXorCount(children.get(0), children.get(1),
                children.subList(2, children.size()).toArray(new Expression[0]));
    }
}
