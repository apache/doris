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
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
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
import java.util.Objects;

/** Index equality must agree with the existing occurrence-matching contract. */
public class PlaceholderIndexTest {
    @Test
    public void testIndependentOccurrencesDoNotRenderParentSignatures() {
        NereidsParser parser = new NereidsParser();
        Map<Expression, String> signatures = new IdentityHashMap<>();
        Map<PlaceholderKey, Integer> index = new HashMap<>();
        Expression literal = new IntegerLiteral(1);
        for (int i = 0; i < 512; i++) {
            Expression parent = parser.parseExpression("a = 1");
            PlaceholderKey key = PlaceholderKey.of(literal, parent,
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
            LegacyIdentity record = new LegacyIdentity(left, null, 0, 0L, 0, 1L, "");
            PlaceholderKey key = PlaceholderKey.of(left, null, 0, 0L, 0, 1L, "", signatures);
            for (Expression right : expressions) {
                PlaceholderKey other = PlaceholderKey.of(right, null,
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
            LegacyIdentity record = new LegacyIdentity(literal, parent, 0, 3L, 2, 7L, "0/1");
            PlaceholderKey expected = PlaceholderKey.of(literal, parent,
                    0, 3L, 2, 7L, "0/1", signatures);
            for (Expression candidate : parents) {
                for (int child : List.of(-1, 0, 1)) {
                    for (long block : List.of(3L, 4L)) {
                        for (long root : List.of(7L, 8L)) {
                            boolean matches = record.matches(literal, candidate, child,
                                    block, 2, root, "0/1");
                            PlaceholderKey key = PlaceholderKey.of(literal,
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

    private record LegacyIdentity(Expression originalExpr, Expression parentExpr, int childIndex, long blockId, int projectItem,
            long rootOrdinal, String occurrencePath) {
        boolean matches(Expression expr, Expression parent, int childIndex, long blockId,
                int projectItem, long rootOrdinal, String occurrencePath) {
            if (this.blockId != blockId || this.projectItem != projectItem) {
                // different query blocks (outer query vs subquery / derived table) or different
                // SELECT-list items: never share one id, or a user query with different values
                // in the two positions could never match
                return false;
            }
            if (this.rootOrdinal != rootOrdinal
                    || !Objects.equals(this.occurrencePath, occurrencePath == null ? ""
                            : occurrencePath)) {
                // different expression OCCURRENCES (see rootOrdinal / occurrencePath): the two
                // literals are independent slots, and a query varying them separately must still
                // match
                return false;
            }
            if (!originalExpr.equals(expr)) {
                return false;
            }
            // A literal with no parent (top-level expression, e.g. an ORDER BY key that is a
            // bare literal) only reuses the id of another parent-less literal. Comparing a
            // non-null parentExpr against a null parent (or vice versa) must not crash on
            // parentSignature(null) and must not share an id across different nesting levels.
            if (parentExpr == null || parent == null) {
                return parentExpr == null && parent == null;
            }
            return this.childIndex == childIndex
                    && parentSignature(parentExpr).equals(parentSignature(parent));
        }

        private static String parentSignature(Expression expr) {
            if (expr instanceof SubqueryExpr) {
                return "<subquery>";
            }
            List<Expression> children = expr.children();
            if (children.isEmpty()) {
                return expr.getClass().getSimpleName() + ":" + expr.toSql();
            }
            StringBuilder sb = new StringBuilder(expr.getClass().getSimpleName());
            for (Expression child : children) {
                sb.append('(').append(parentSignature(child)).append(')');
            }
            return sb.toString();
        }

    }

}
