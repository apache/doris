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

package org.apache.doris.nereids.spm.placeholder;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/** Index equality must agree with the existing occurrence-matching contract. */
public class PlaceholderIndexTest {
    @Test
    public void testIndependentOccurrencesDoNotRenderParentSignatures() {
        NereidsParser parser = new NereidsParser();
        Map<Expression, String> signatures = new IdentityHashMap<>();
        Map<PlaceholderExpr.MatchKey, Integer> index = new HashMap<>();
        Expression literal = new IntegerLiteral(1);
        for (int i = 0; i < 512; i++) {
            Expression parent = parser.parseExpression("a = 1");
            PlaceholderExpr.MatchKey key = PlaceholderExpr.matchKey(literal, parent,
                    1, 0L, -1, i, "1", signatures);
            index.put(key, i);
        }
        Assertions.assertEquals(512, index.size());
        Assertions.assertTrue(signatures.isEmpty(), "distinct occurrences need no parent rendering");
    }

    @Test
    public void testLiteralAndInListKeysRetainExpressionEquality() {
        NereidsParser parser = new NereidsParser();
        List<Expression> expressions = List.of(new IntegerLiteral(1), new IntegerLiteral(2),
                new StringLiteral("x"), new VarcharLiteral("x"), NullLiteral.INSTANCE,
                parser.parseExpression("a IN (1, 2)"), parser.parseExpression("a IN (1, 3)"));
        Map<Expression, String> signatures = new IdentityHashMap<>();
        for (Expression left : expressions) {
            PlaceholderExpr record = new PlaceholderExpr(left, left, null, 0, 0L, 0, 1L, "");
            PlaceholderExpr.MatchKey key = PlaceholderExpr.matchKey(left, null, 0, 0L, 0, 1L, "", signatures);
            for (Expression right : expressions) {
                PlaceholderExpr.MatchKey other = PlaceholderExpr.matchKey(right, null,
                        1, 0L, 0, 1L, null, signatures);
                Assertions.assertEquals(record.matches(right, null, 1, 0L, 0, 1L, null), key.equals(other));
                if (key.equals(other)) {
                    Assertions.assertEquals(key.hashCode(), other.hashCode());
                }
            }
        }
    }

    @Test
    public void testKeysMatchLegacyEqualityIncludingParentlessPositions() {
        NereidsParser parser = new NereidsParser();
        List<Expression> parents = Arrays.asList(null, parser.parseExpression("abs(1)"),
                parser.parseExpression("sqrt(1)"), parser.parseExpression("a = 1"),
                parser.parseExpression("b = 1"));
        Map<Expression, String> signatures = new IdentityHashMap<>();
        for (Expression parent : parents) {
            Expression literal = new IntegerLiteral(1);
            PlaceholderExpr record = new PlaceholderExpr(literal, SpmConstVar.of(1, literal),
                    parent, 0, 3L, 2, 7L, "0/1");
            PlaceholderExpr.MatchKey expected = PlaceholderExpr.matchKey(literal, parent,
                    0, 3L, 2, 7L, "0/1", signatures);
            for (Expression candidate : parents) {
                for (int child : List.of(-1, 0, 1)) {
                    for (long block : List.of(3L, 4L)) {
                        for (long root : List.of(7L, 8L)) {
                            boolean matches = record.matches(literal, candidate, child,
                                    block, 2, root, "0/1");
                            PlaceholderExpr.MatchKey key = PlaceholderExpr.matchKey(literal,
                                    candidate, child, block, 2, root, "0/1", signatures);
                            Assertions.assertEquals(matches, expected.equals(key));
                            if (matches) {
                                Assertions.assertEquals(expected.hashCode(), key.hashCode());
                            }
                        }
                    }
                }
            }
        }
    }
}
