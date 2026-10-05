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

import org.apache.doris.nereids.rules.exploration.mv.Predicates;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.SplitPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * PredicatesSplitterTest
 */
public class PredicatesSplitterTest extends ExpressionRewriteTestHelper {

    @Test
    public void testSplitPredicates() {
        assetEquals("a = b and (c = d or a = 10) and a > 7 and 10 > d",
                "a = b",
                "a > 7 and 10 > d",
                "c = d or a = 10");
        assetEquals("a = b and c + d = e and a > 7 and 10 > d",
                "a = b",
                "a > 7 and 10 > d",
                "c + d = e");
        assetEquals("a = b and c + d = e or a > 7 and 10 > d",
                "",
                "",
                "a = b and c + d = e or a > 7 and 10 > d");
    }

    private void assetEquals(String expression,
            String expectedEqualExpr,
            String expectedRangeExpr,
            String expectedResidualExpr) {

        Map<String, Slot> mem = Maps.newLinkedHashMap();
        Expression targetExpr = replaceUnboundSlot(PARSER.parseExpression(expression), mem);
        SplitPredicate splitPredicate = Predicates.splitPredicates(targetExpr);

        if (!StringUtils.isEmpty(expectedEqualExpr)) {
            Expression equalExpression = replaceUnboundSlot(PARSER.parseExpression(expectedEqualExpr), mem);
            Assertions.assertEquals(ExpressionUtils.extractConjunctionToSet(equalExpression),
                    splitPredicate.getEqualPredicateMap().keySet());
        } else {
            Assertions.assertTrue(splitPredicate.getEqualPredicateMap().isEmpty());
        }

        if (!StringUtils.isEmpty(expectedRangeExpr)) {
            Expression rangeExpression = replaceUnboundSlot(PARSER.parseExpression(expectedRangeExpr), mem);
            Assertions.assertEquals(ExpressionUtils.extractConjunctionToSet(rangeExpression),
                    splitPredicate.getRangePredicateMap().keySet());
        } else {
            Assertions.assertTrue(splitPredicate.getRangePredicateMap().isEmpty());
        }

        if (!StringUtils.isEmpty(expectedResidualExpr)) {
            Expression residualExpression = replaceUnboundSlot(PARSER.parseExpression(expectedResidualExpr), mem);
            Assertions.assertEquals(
                    ExpressionUtils.extractConjunctionToSet(residualExpression),
                    splitPredicate.getResidualPredicateMap().keySet());
        } else {
            Assertions.assertTrue(splitPredicate.getResidualPredicateMap().isEmpty());
        }
    }
}

