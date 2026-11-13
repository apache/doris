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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.rules.OrToIn;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class OrToInTest extends ExpressionRewriteTestHelper {

    @Test
    void testDeDup() {
        // (a=1 and b=1) or (a=2 and c=2) infers "a in (1, 2)"
        // (a=1 and d=1) or (a=2 and e=2) infers "a in (1, 2)" again
        // duplicated one should be removed
        Expression expression = PARSER.parseExpression("((a=1 and b=1) or (a=2 and c=2)) and ((a=1 and d=1) or (a=2 and e=2))");
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("AND[a IN (1, 2),OR[AND[(a = 1),(b = 1)],AND[(a = 2),(c = 2)]],OR[AND[(a = 1),(d = 1)],AND[(a = 2),(e = 2)]]]",
                rewritten.toSql());
    }

    // (a=1 and b=1) or (a=2 and c=2) infers "a in (1, 2)", but "a in (1, 2)" is not in root level,
    // and cannot be push down, and hence, do not extract "a in (1, 2)"
    @Test
    void testExtractNothing() {
        Expression expression = PARSER.parseExpression("((a=1 and b=1) or (a=2 and c=2) or d=1) and e = 2");
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("AND[OR[AND[(a = 1),(b = 1)],AND[(a = 2),(c = 2)],(d = 1)],(e = 2)]",
                rewritten.toSql());
    }

    // replace mode
    @Test
    void test1() {
        String expr = "col1 = 1 or col1 = 2 or col1 = 3 and (col2 = 4)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("AND[col1 IN (1, 2, 3),OR[col1 IN (1, 2),AND[(col1 = 3),(col2 = 4)]]]",
                rewritten.toSql());
        Expression rewritten2 = OrToIn.REPLACE_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("OR[col1 IN (1, 2),AND[(col1 = 3),(col2 = 4)]]", rewritten2.toSql());
    }

    @Test
    void test2() {
        String expr = "col1 = 1 and col1 = 3 and col2 = 3 or col2 = 4";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(col2 = 4)", rewritten.toSql());
    }

    @Test
    void test3() {
        String expr = "(A = 1 or A = 2) and  (B = 3 or B = 4)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("AND[A IN (1, 2),B IN (3, 4)]", rewritten.toSql());
    }

    @Test
    void test4() {
        String expr = "case when col = 1 or col = 2 or col = 3 then 1"
                + "         when col = 4 or col = 5 or col = 6 then 1 else 0 end";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.REPLACE_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("CASE WHEN col IN (1, 2, 3) THEN 1 WHEN col IN (4, 5, 6) THEN 1 ELSE 0 END",
                rewritten.toSql());
    }

    @Test
    void test5() {
        String expr = "col = 1 or (col = 2 and (col = 3 or col = 4 or col = 5))";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(col = 1)", rewritten.toSql());
    }

    @Test
    void test6() {
        String expr = "col = 1 or col = 2 or col in (1, 2, 3)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("col IN (1, 2, 3)", rewritten.toSql());
    }

    @Test
    void test7() {
        String expr = "A = 1 or A = 2 or abs(A)=5 or A in (1, 2, 3) or B = 1 or B = 2 or B in (1, 2, 3) or B+1 in (4, 5, 7)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("OR[A IN (1, 2, 3),(abs(A) = 5),B IN (1, 2, 3),(B + 1) IN (4, 5, 7)]",
                rewritten.toSql());
    }

    @Test
    void testEnsureOrder() {
        // ensure not rewrite to col2 in (1, 2) or  cor 1 in (1, 2)
        String expr = "col1 IN (1, 2) OR col2 IN (1, 2)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("OR[col1 IN (1, 2),col2 IN (1, 2)]", rewritten.toSql());
    }

    @Test
    void test9() {
        String expr = "col1=1 and (col2=1 or col2=2)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("AND[(col1 = 1),col2 IN (1, 2)]", rewritten.toSql());
    }

    @Test
    void test10() {
        // recursive rewrites
        String expr = "col1=1 or (col2 = 2 and (col3=4 or col3=5))";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("OR[(col1 = 1),AND[(col2 = 2),col3 IN (4, 5)]]", rewritten.toSql());
    }

    // replace mode
    @Test
    void test11() {
        // rewrite multi-inPredicates
        String expr = "(a=1 and b=2 and c=3) or (a=2 and b=2 and c=4)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("AND[(b = 2),a IN (1, 2),c IN (3, 4),OR[AND[(a = 1),(c = 3)],AND[(a = 2),(c = 4)]]]",
                rewritten.toSql());
    }

    @Test
    void test12() {
        // no rewrite
        String expr = "a in (1, 2) and a in (3, 4)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("FALSE", rewritten.toSql());
    }

    @Test
    void test13() {
        // no rewrite, because of "a like 'xyz'"
        String expr = "a like 'xyz% or a=1 or a=2'";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(a like 'xyz% or a=1 or a=2')", rewritten.toSql());
    }

    @Test
    void test14() {
        // "a = 1" shares slot a with "f(a) = 2". The OR still implies "a in (1, 3)", so extract mode
        // appends it and keeps the original OR; replace mode must not touch the OR.
        String expr = "(a=1 and f(a)=2) or a=3";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("AND[a IN (1, 3),OR[AND[(a = 1),(f(a) = 2)],(a = 3)]]", rewritten.toSql());
        Expression replaced = OrToIn.REPLACE_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("OR[AND[(a = 1),(f(a) = 2)],(a = 3)]", replaced.toSql());
    }

    @Test
    void test15() {
        // no rewrite, because of "a like 'xyz'"
        String expr = "x=1 or (a=1 and b=2) or (a=2 and c=3)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("OR[(x = 1),AND[(a = 1),(b = 2)],AND[(a = 2),(c = 3)]]", rewritten.toSql());
    }

    @Test
    void test16() {
        String expr = "a=1 or a=1 or a=1";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        Assertions.assertEquals("(a = 1)", rewritten.toSql());
    }

    //replace mode
    @Test
    void test17() {
        String expr = "(a=1 and b=2) or (a in (2, 3) and ((a=2 and c=3) or (a=3 and d=4)))";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression, context);
        System.out.println(rewritten);
    }

    @Test
    void testInferFromDisjunctWithDependentConjuncts() {
        // Same shape as the homepage-rank query on dwd_traffic_events_di:
        //   (spm=S1 and ep=P1 and ev=E1) or (spm=S2 and ep=P2 and ev=E2)
        //   or (f(j)=.. and g(j) in (..) and ((spm=S3 and ep=P1 and ev=E1) or (spm=S4 and ep=P2 and ev=E2)))
        // The inner OR is rewritten first and yields "spm in (3, 4)" etc. next to the kept inner OR, so these
        // In-predicates share slots with their sibling. They must still take part in the outer inference.
        String expr = "(spm=1 and ep=1 and ev=1) or (spm=2 and ep=2 and ev=2)"
                + " or (f(j)=1 and g(j) in (1, 2) and ((spm=3 and ep=1 and ev=1) or (spm=4 and ep=2 and ev=2)))";
        Expression expression = PARSER.parseExpression(expr);
        // InferInPredicateFromOr calls rewriteTree(Expression) on filter predicates
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression);
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(rewritten);
        assertHasInPredicate(conjuncts, "spm", "1", "2", "3", "4");
        assertHasInPredicate(conjuncts, "ep", "1", "2");
        assertHasInPredicate(conjuncts, "ev", "1", "2");
        // exactly 3 inferred In-predicates + the original OR. f(j) / g(j) are not plain slots and
        // share j with each other, so they are never inferred to the top level.
        Assertions.assertEquals(4, conjuncts.size(), conjuncts.toString());
        Assertions.assertEquals(1, conjuncts.stream().filter(Or.class::isInstance).count());

        assertFixedPoint(rewritten);

        // replace mode must keep the original OR because the inferred In-predicates are not equivalent to it
        Expression replaced = OrToIn.REPLACE_MODE_INSTANCE.rewriteTree(expression);
        Assertions.assertTrue(replaced instanceof Or, replaced.toSql());
    }

    @Test
    void testNonSlotCompareExprStillRequiresIndependence() {
        // f(a) is not a plain slot, keep the conservative independence check even in extract mode
        String expr = "(f(a)=1 and a>0) or f(a)=2";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression);
        Assertions.assertEquals("OR[AND[(f(a) = 1),(a > 0)],(f(a) = 2)]", rewritten.toSql());
    }

    @Test
    void testSlotSharedWithOtherPredicate() {
        // "a > 0" and "a is not null" share slot a with "a = 1" / "a in (2, 3)"
        String expr = "(a=1 and a>0) or (a in (2, 3) and a is not null)";
        Expression expression = PARSER.parseExpression(expr);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(expression);
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(rewritten);
        assertHasInPredicate(conjuncts, "a", "1", "2", "3");
        Assertions.assertEquals(2, conjuncts.size(), conjuncts.toString());
        Assertions.assertEquals(1, conjuncts.stream().filter(Or.class::isInstance).count());
        assertFixedPoint(rewritten);
    }

    @Test
    void testDuplicateSlotCandidateOnlyDropsConflictingSlot() {
        // The inner OR is rewritten first and appends "a in (1, 2)" next to it, so the first outer disjunct
        // holds "a = 1" and "a in (1, 2)" on the same slot. That slot is a pattern for SimplifyRange and is
        // dropped, but "b = 10" must still be inferred.
        String expr = "(b=10 and a=1 and ((a=1 and c=100) or (a=2 and c=200))) or (b=20 and a=3)";
        // InferInPredicateFromOr calls rewriteTree(Expression) on filter predicates
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(PARSER.parseExpression(expr));
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(rewritten);
        assertHasInPredicate(conjuncts, "b", "10", "20");
        Assertions.assertEquals(1, conjuncts.stream().filter(Or.class::isInstance).count(), conjuncts.toString());
        // the conflicting slot is dropped, not inferred from a half-collected candidate
        Assertions.assertFalse(conjuncts.stream().anyMatch(c -> c instanceof InPredicate
                && ((InPredicate) c).getCompareExpr().toSql().equals("a")), conjuncts.toString());
        assertFixedPoint(rewritten);
    }

    @Test
    void testConflictingSlotIsNotRevivedByLaterConjunct() {
        // Three conjuncts on slot a: after "a = 1" and "a in (1, 2)" conflict, the later "a in (1, 3)"
        // must not register a again. Each single conjunct is implied by the disjunct, so re-registering
        // would still be sound, but the outcome would then depend on the order the conjuncts are written
        // in. Slot b is unrelated and is still inferred.
        String expr = "(b=10 and a=1 and a in (1, 2) and a in (1, 3)) or (b=20 and a=3)";
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(PARSER.parseExpression(expr));
        List<Expression> conjuncts = ExpressionUtils.extractConjunction(rewritten);
        assertHasInPredicate(conjuncts, "b", "10", "20");
        Assertions.assertFalse(conjuncts.stream().anyMatch(c -> c instanceof InPredicate
                && ((InPredicate) c).getCompareExpr().toSql().equals("a")), conjuncts.toString());
        Assertions.assertEquals(1, conjuncts.stream().filter(Or.class::isInstance).count());
        assertFixedPoint(rewritten);
    }

    @Test
    void testDuplicateSlotCandidateWithoutUnrelatedSlotLeavesOrAlone() {
        // Two conjuncts on slot a and no other candidate slot in the first disjunct: no candidate survives,
        // so the OR is left alone (same end result as the old "clear everything and stop" branch).
        String expr = "(a=1 and a in (1, 2)) or (a=3 and b=1)";
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(PARSER.parseExpression(expr));
        Assertions.assertTrue(rewritten instanceof Or, rewritten.toSql());
    }

    @Test
    void testArrayCompareKeepsIndependenceCheck() {
        // "a = [1]" shares slot a with "size(a) > 0", so only the relaxed extract path could infer
        // "a IN ([1], [2])". ARRAY is not inference-safe (FunctionCollectionIn drops the null map of
        // the left column), so the OR must be left alone.
        SlotReference a = new SlotReference("a", ArrayType.of(IntegerType.INSTANCE), true);
        Expression eq1 = new EqualTo(a, new ArrayLiteral(ImmutableList.of(new IntegerLiteral(1))));
        Expression eq2 = new EqualTo(a, new ArrayLiteral(ImmutableList.of(new IntegerLiteral(2))));
        Expression or = new Or(new And(eq1, new Not(new IsNull(a))), eq2);
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(or);
        Assertions.assertTrue(rewritten instanceof Or, rewritten.toSql());
        Assertions.assertEquals(0, ExpressionUtils.extractConjunction(rewritten).stream()
                .filter(InPredicate.class::isInstance).count(), rewritten.toSql());
    }

    @Test
    void testFloatCompareKeepsIndependenceCheck() {
        // "X = c" and the hash-based IN may disagree on -0.0 / NaN, so floating point is not relaxed.
        SlotReference d = new SlotReference("d", DoubleType.INSTANCE, true);
        Expression or = new Or(
                new And(new EqualTo(d, new DoubleLiteral(1.0)), new Not(new IsNull(d))),
                new EqualTo(d, new DoubleLiteral(2.0)));
        Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(or);
        Assertions.assertTrue(rewritten instanceof Or, rewritten.toSql());
    }

    @Test
    void testScalarTypesStillRelaxed() {
        // 'S' -> STRING, 'I' -> INT, 'C' -> DATEV2, 'M' -> DECIMALV3, 'B' -> BOOLEAN.
        // These keep the relaxation, so the target "spm IN (...)" inference is unaffected.
        for (String expr : Lists.newArrayList("(Sa='x' and Sa>'a') or Sa='y'",
                "(Ia=1 and Ia>0) or Ia=2", "(Ma=1 and Ma>0) or Ma=2")) {
            Expression rewritten = OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(PARSER.parseExpression(expr));
            Assertions.assertFalse(rewritten instanceof Or, expr + " -> " + rewritten.toSql());
        }
    }

    // InferInPredicateFromOr stores the extracted conjuncts as a set and is re-applied by a top-down
    // rewrite job until the filter no longer changes, so a second application must keep the same set.
    private static void assertFixedPoint(Expression firstRewritten) {
        Set<Expression> firstPass = new LinkedHashSet<>(ExpressionUtils.extractConjunction(firstRewritten));
        Expression secondInput = ExpressionUtils.and(Lists.newArrayList(firstPass));
        Set<Expression> secondPass = new LinkedHashSet<>(ExpressionUtils.extractConjunction(
                OrToIn.EXTRACT_MODE_INSTANCE.rewriteTree(secondInput)));
        Assertions.assertEquals(firstPass, secondPass);
    }

    private static void assertHasInPredicate(List<Expression> conjuncts, String slotName, String... values) {
        Set<String> expectedValues = ImmutableSet.copyOf(values);
        boolean found = conjuncts.stream()
                .filter(InPredicate.class::isInstance)
                .map(InPredicate.class::cast)
                .anyMatch(in -> in.getCompareExpr().toSql().equals(slotName)
                        && in.getOptions().size() == expectedValues.size()
                        && in.getOptions().stream().map(Expression::toSql).collect(Collectors.toSet())
                                .equals(expectedValues));
        Assertions.assertTrue(found, "expect " + slotName + " IN " + expectedValues + " in " + conjuncts);
    }
}
