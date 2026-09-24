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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.matcher.SPMAstCheckVisitor;
import org.apache.doris.nereids.spm.matcher.SPMPlaceholderReplacer;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstList;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * M2 milestone test: the placeholder engine (SPMPlaceholderBuilder / SPMAstCheckVisitor /
 * SPMPlaceholderReplacer).
 *
 * Verifies three core phases:
 *
 * 1. Parameterization: literal -> SpmConstVar, IN constant list -> SpmConstList
 * 2. AST match: bind expression vs user expression, structural consistency plus
 *    extracting the actual placeholder values
 * 3. Placeholder replacement: placeholders in planSql -> user actual values
 */
public class SPMPlaceholderEngineTest {

    // ==================== parameterization (SPMPlaceholderBuilder) ====================

    @Test
    public void testParameterizeSingleLiteral() {
        // WHERE a = 100 -> WHERE a = _spm_const_var(1, 100)
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Expression bind = new EqualTo(a, new IntegerLiteral(100));

        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        Expression parameterized = builder.parameterizeExpressions(List.of(bind)).get(0);

        // The EqualTo structure is kept and the right child is replaced with SpmConstVar
        Assertions.assertTrue(parameterized instanceof EqualTo);
        Expression right = ((EqualTo) parameterized).right();
        Assertions.assertTrue(right instanceof SpmConstVar);
        SpmConstVar ph = (SpmConstVar) right;
        Assertions.assertEquals(1L, ph.getId());
        Assertions.assertEquals("_spm_const_var(1, 100)", ph.toSql());
        Assertions.assertEquals(1, builder.getPlaceholderExprs().size());
    }

    @Test
    public void testParameterizeDedupByValueAndParent() {
        // WHERE a = 100 AND a = 100 -> same value AND same parent structure (a = 100)
        // reuse the same placeholder id (SR findPlaceholderExpr Step 1 + Step 2)
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Expression bind = new And(new EqualTo(a, new IntegerLiteral(100)),
                new EqualTo(a, new IntegerLiteral(100)));

        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        Expression parameterized = builder.parameterizeExpressions(List.of(bind)).get(0);

        // Only one placeholder record
        Assertions.assertEquals(1, builder.getPlaceholderExprs().size());
        // Both positions are the same SpmConstVar(1, 100)
        And and = (And) parameterized;
        Expression leftRight = ((EqualTo) and.child(0)).right();
        Expression rightRight = ((EqualTo) and.child(1)).right();
        Assertions.assertEquals(leftRight, rightRight);
        Assertions.assertEquals(1L, ((SpmConstVar) leftRight).getId());
    }

    @Test
    public void testParameterizeNoDedupDifferentParent() {
        // WHERE a = 100 AND b = 100 -> same value but different parent structures
        // (a = 100) vs (b = 100) get different ids; value-only dedup would wrongly share
        // one id and prevent "a = 5 AND b = 7" from matching (design doc 6.3)
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        Expression bind = new And(new EqualTo(a, new IntegerLiteral(100)),
                new EqualTo(b, new IntegerLiteral(100)));

        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        Expression parameterized = builder.parameterizeExpressions(List.of(bind)).get(0);

        Assertions.assertEquals(2, builder.getPlaceholderExprs().size());
        And and = (And) parameterized;
        SpmConstVar leftRight = (SpmConstVar) ((EqualTo) and.child(0)).right();
        SpmConstVar rightRight = (SpmConstVar) ((EqualTo) and.child(1)).right();
        Assertions.assertEquals(1L, leftRight.getId());
        Assertions.assertEquals(2L, rightRight.getId());
        // A user query "a = 5 AND b = 7" extracts distinct values per id and matches
        Map<Long, Expression> values = new HashMap<>();
        Expression user = new And(new EqualTo(a, new IntegerLiteral(5)),
                new EqualTo(b, new IntegerLiteral(7)));
        Assertions.assertTrue(new SPMAstCheckVisitor().checkExpression(parameterized, user, values));
        Assertions.assertEquals(new IntegerLiteral(5), values.get(1L));
        Assertions.assertEquals(new IntegerLiteral(7), values.get(2L));
    }

    @Test
    public void testParameterizeNoDedupSiblingLiterals() {
        // WHERE a = (1 + 1): the two operands are SIBLINGS - same value, same parent
        // structure, different positions. They must get distinct ids, otherwise a
        // similar query "a = (2 + 1)" could never match (every occurrence of one id
        // must resolve to the same user value).
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Expression bind = new EqualTo(a, new Add(new IntegerLiteral(1), new IntegerLiteral(1)));

        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        Expression parameterized = builder.parameterizeExpressions(List.of(bind)).get(0);

        Assertions.assertEquals(2, builder.getPlaceholderExprs().size());
        Add add = (Add) ((EqualTo) parameterized).right();
        SpmConstVar left = (SpmConstVar) add.child(0);
        SpmConstVar right = (SpmConstVar) add.child(1);
        Assertions.assertNotEquals(left.getId(), right.getId());
        // the similar query with different operand values still matches and extracts
        // each operand separately
        Map<Long, Expression> values = new HashMap<>();
        Expression user = new EqualTo(a, new Add(new IntegerLiteral(2), new IntegerLiteral(1)));
        Assertions.assertTrue(new SPMAstCheckVisitor().checkExpression(parameterized, user, values));
        Assertions.assertEquals(new IntegerLiteral(2), values.get(left.getId()));
        Assertions.assertEquals(new IntegerLiteral(1), values.get(right.getId()));
    }

    @Test
    public void testParameterizeInList() {
        // WHERE b IN (2, 3) -> WHERE b IN (_spm_const_list(1, 2, 3))
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        Expression bind = new InPredicate(b, List.of(new IntegerLiteral(2), new IntegerLiteral(3)));

        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        Expression parameterized = builder.parameterizeExpressions(List.of(bind)).get(0);

        Assertions.assertTrue(parameterized instanceof InPredicate);
        InPredicate in = (InPredicate) parameterized;
        Assertions.assertEquals(1, in.getOptions().size());
        Assertions.assertTrue(in.getOptions().get(0) instanceof SpmConstList);
        SpmConstList list = (SpmConstList) in.getOptions().get(0);
        Assertions.assertEquals(1L, list.getId());
        Assertions.assertEquals("_spm_const_list(1, 2, 3)", list.toSql());
    }

    // ==================== AST match + value extraction (SPMAstCheckVisitor) ====================

    @Test
    public void testCheckMatchAndExtractValue() {
        // bind: WHERE a = _spm_const_var(1, 100); user: WHERE a = 42 -> match, extract 42
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Expression bind = new EqualTo(a, new SpmConstVar(1L, new IntegerLiteral(100)));
        Expression user = new EqualTo(a, new IntegerLiteral(42));

        Map<Long, Expression> values = new HashMap<>();
        boolean matched = new SPMAstCheckVisitor()
                .checkExpression(bind, user, values);

        Assertions.assertTrue(matched);
        Assertions.assertEquals(new IntegerLiteral(42), values.get(1L));
    }

    @Test
    public void testCheckMismatchType() {
        // bind: a = _spm_const_var; user: a > 42 -> type mismatch, no match
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Expression bind = new EqualTo(a, new SpmConstVar(1L, new IntegerLiteral(100)));
        Expression user = new org.apache.doris.nereids.trees.expressions.GreaterThan(
                a, new IntegerLiteral(42));

        Map<Long, Expression> values = new HashMap<>();
        boolean matched = new SPMAstCheckVisitor()
                .checkExpression(bind, user, values);
        Assertions.assertFalse(matched);
    }

    @Test
    public void testCheckDuplicatePlaceholderValue() {
        // The same placeholder appears twice in bind; the user values must be consistent
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SpmConstVar ph = new SpmConstVar(1L, new IntegerLiteral(100));
        Expression bind = new And(new EqualTo(a, ph), new EqualTo(b, ph));
        // The user side has inconsistent values -> no match
        Expression user = new And(new EqualTo(a, new IntegerLiteral(42)),
                new EqualTo(b, new IntegerLiteral(43)));

        Map<Long, Expression> values = new HashMap<>();
        boolean matched = new SPMAstCheckVisitor()
                .checkExpression(bind, user, values);
        Assertions.assertFalse(matched);
    }

    @Test
    public void testCheckInListMatch() {
        // bind: b IN (_spm_const_list(1, 2, 3)); user: b IN (9, 10) -> match
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        Expression bind = new InPredicate(b, List.of(new SpmConstList(1L,
                List.of(new IntegerLiteral(2), new IntegerLiteral(3)))));
        Expression user = new InPredicate(b, List.of(new IntegerLiteral(9), new IntegerLiteral(10)));

        Map<Long, Expression> values = new HashMap<>();
        boolean matched = new SPMAstCheckVisitor()
                .checkExpression(bind, user, values);
        Assertions.assertTrue(matched);
        // The extracted value is the whole user-side IN predicate
        Assertions.assertTrue(values.get(1L) instanceof InPredicate);
    }

    // ==================== placeholder replacement (SPMPlaceholderReplacer) ====================

    @Test
    public void testPlaceholderNotFoldableNotConstant() {
        // M1 (SR alignment): a placeholder must survive the SPM CREATE optimization
        // (analyze + rewrite + CBO) so the decompiled frozen planSql can keep its
        // placeholder id and the rewrite can substitute the user value later.
        // -> SpmConstVar / SpmConstList must report foldable() == false and
        //    isConstant() == false (never folded / never treated as a plain constant /
        //    never constant-propagated or moved by constant-collecting rules).
        SpmConstVar scalar = new SpmConstVar(1L, new IntegerLiteral(100));
        Assertions.assertFalse(scalar.foldable(),
                "SpmConstVar must not be foldable (would bake the captured value)");
        Assertions.assertFalse(scalar.isConstant(),
                "SpmConstVar must not be treated as a constant");

        SpmConstList list = new SpmConstList(2L,
                List.of(new IntegerLiteral(1), new IntegerLiteral(2)));
        Assertions.assertFalse(list.foldable(),
                "SpmConstList must not be foldable");
        Assertions.assertFalse(list.isConstant(),
                "SpmConstList must not be treated as a constant");

        // FoldConstantRuleOnFE folds a comparison only when every argument is a real
        // Literal (isAllLiteral). A placeholder is NOT a Literal, so "a = _spm_const_
        // var(1, 100)" can never be folded into "a = 100" during the baseline CREATE
        // optimization - the placeholder marker survives to the decompiled planSql.
        Assertions.assertFalse(scalar.isLiteral(),
                "SpmConstVar is not a Literal (folding gate isAllLiteral rejects it)");
        Assertions.assertFalse(list.isLiteral(),
                "SpmConstList is not a Literal (folding gate isAllLiteral rejects it)");

        // A comparison wrapping a placeholder keeps its default foldable() = true (the
        // node itself is a fold candidate), but folding is gated on all-Literal args,
        // which a placeholder child makes impossible - assert the gate semantics.
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Expression comparison = new EqualTo(a, scalar);
        Assertions.assertFalse(org.apache.doris.nereids.util.ExpressionUtils.isAllLiteral(
                comparison.getArguments()),
                "a comparison wrapping a placeholder must not be all-Literal");
    }

    @Test
    public void testReplaceSinglePlaceholder() {
        // planSql: a = _spm_const_var(1, 100); user value: 42 -> a = 42
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Expression planExpr = new EqualTo(a, new SpmConstVar(1L, new IntegerLiteral(100)));
        Map<Long, Expression> values = Map.of(1L, new IntegerLiteral(42));

        Expression replaced = new SPMPlaceholderReplacer().replace(planExpr, values);

        Assertions.assertTrue(replaced instanceof EqualTo);
        Assertions.assertEquals(new IntegerLiteral(42), ((EqualTo) replaced).right());
    }

    @Test
    public void testReplaceInList() {
        // planSql: b IN (_spm_const_list(1, 2, 3)); user value: IN (9, 10) -> b IN (9, 10)
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        Expression planExpr = new InPredicate(b, List.of(new SpmConstList(1L,
                List.of(new IntegerLiteral(2), new IntegerLiteral(3)))));
        InPredicate userIn = new InPredicate(b, List.of(new IntegerLiteral(9), new IntegerLiteral(10)));
        Map<Long, Expression> values = Map.of(1L, userIn);

        Expression replaced = new SPMPlaceholderReplacer().replace(planExpr, values);

        Assertions.assertTrue(replaced instanceof InPredicate);
        InPredicate in = (InPredicate) replaced;
        Assertions.assertEquals(2, in.getOptions().size());
        Assertions.assertEquals(new IntegerLiteral(9), in.getOptions().get(0));
        Assertions.assertEquals(new IntegerLiteral(10), in.getOptions().get(1));
        // The compareExpr keeps the planSql side (b)
        Assertions.assertEquals(b, in.getCompareExpr());
    }

    @Test
    public void testToSpmDigestFullQueryValueIndependent() {
        // bindSqlDigest must be the FULL parameterized SQL (design doc 6.14): SELECT..
        // FROM.. WHERE.. with every literal normalized to "?" (value-independent).
        String sql = "select ca_zip, sum(cs_sales_price) from catalog_sales "
                + "where cs_bill_customer_sk = c_customer_sk and d_year = 2001 "
                + "and substr(ca_zip, 1, 5) in ('85669', '86197') group by ca_zip order by ca_zip limit 100";
        String digest = new NereidsParser().parseSingle(sql).toSpmDigest();

        Assertions.assertTrue(digest.startsWith("SELECT "), "full SQL digest expected: " + digest);
        Assertions.assertTrue(digest.contains("FROM catalog_sales"), digest);
        Assertions.assertTrue(digest.contains("d_year = ?"), "literal must be normalized: " + digest);
        Assertions.assertTrue(digest.contains("SUBSTR(ca_zip, ?, ?)"), digest);
        Assertions.assertFalse(digest.contains("2001"), "digest must not embed values: " + digest);
        Assertions.assertFalse(digest.contains("85669"), "digest must not embed IN-list values: " + digest);

        // value-independent: same structure with different literals -> identical digest
        String digest2 = new NereidsParser().parseSingle(
                "select ca_zip, sum(cs_sales_price) from catalog_sales "
                        + "where cs_bill_customer_sk = c_customer_sk and d_year = 1999 "
                        + "and substr(ca_zip, 1, 5) in ('11111') group by ca_zip order by ca_zip limit 10")
                .toSpmDigest();
        Assertions.assertEquals(digest, digest2,
                "structurally identical queries must share the SPM digest");
    }

    @Test
    public void testToSpmDigestWhitespaceInsensitive() {
        // The digest is rendered from the PARSED plan tree, never from the SQL text:
        // spaces / newlines / TABs / carriage returns are lexical separators consumed
        // by the parser and can never reach the digest, so a query differing only in
        // whitespace still produces the same digest / hash and matches the baseline.
        String canonical = "select k1, k2, sum(v) from ws_t "
                + "where k1 = 1 and k2 = 2 group by k1, k2 order by k1, k2";
        String spaced = "SELECT\n\tk1,\tk2,\n  SUM(v)\nFROM\t ws_t\n"
                + "WHERE\tk1\t=\t1\n   AND k2\t\t=   2\nGROUP BY\n k1,k2\n"
                + "ORDER BY\n k1 ,\tk2";
        String withComments = "select k1, k2, sum(v) /* agg */ from ws_t "
                + "where k1 = 1 -- first key\n and k2 = 2 group by k1, k2 "
                + "order by k1, k2";

        String d1 = new NereidsParser().parseSingle(canonical).toSpmDigest();
        String d2 = new NereidsParser().parseSingle(spaced).toSpmDigest();
        String d3 = new NereidsParser().parseSingle(withComments).toSpmDigest();
        Assertions.assertEquals(d1, d2,
                "whitespace-only differences must not change the SPM digest");
        Assertions.assertEquals(d1, d3,
                "comments must not change the SPM digest");
        // the Level 1 bucket key is derived from the digest -> identical too
        Assertions.assertEquals(SPMUtils.hashOf(d1), SPMUtils.hashOf(d2));
    }

    @Test
    public void testDigestValueIndependentInsideUnboundFunction() {
        // query15 scenario: substr(ca_zip, 1, 5) IN ('85669', '86197'). The function
        // arguments 1 and 5 are literals parameterized into _spm_const_var; the digest
        // must render them as the id-only placeholder (value-independent) instead of
        // leaking the embedded values via the toSql() fallback.
        SlotReference zip = new SlotReference("ca_zip", StringType.INSTANCE);
        Expression substr = new UnboundFunction("substr",
                List.of(zip, new IntegerLiteral(1), new IntegerLiteral(5)));
        Expression bind = new InPredicate(substr,
                List.of(new StringLiteral("85669"), new StringLiteral("86197")));

        Expression parameterized = new SPMPlaceholderBuilder()
                .parameterizeExpressions(List.of(bind)).get(0);
        String digest = SPMUtils.digest(parameterized);

        Assertions.assertTrue(
                digest.contains("substr(ca_zip, _spm_const_var(1), _spm_const_var(2))"),
                "digest must keep function-argument placeholders value-independent: " + digest);
        Assertions.assertTrue(digest.contains("_spm_const_list(3)"),
                "digest must keep the IN-list placeholder size-independent: " + digest);
        Assertions.assertFalse(digest.contains("_spm_const_var(1, "),
                "digest must not embed the captured values: " + digest);
    }

    @Test
    public void testReplaceInListReplacesCompareExprPlaceholders() {
        // Regression for the query15 unbound-placeholder bug: an IN list placeholder
        // whose compareExpr itself contains placeholders, e.g.
        //   substr(ca_zip, _spm_const_var(1, 1), _spm_const_var(2, 5)) IN (_spm_const_list(3, ...))
        // The replacer used to keep the compareExpr untouched, leaking _spm_const_var(1, 1)
        // and _spm_const_var(2, 5) into the rewritten query.
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        Expression compare = new Add(a, new SpmConstVar(1L, new IntegerLiteral(1)));
        Expression planExpr = new InPredicate(compare, List.of(new SpmConstList(2L,
                List.of(new StringLiteral("x"), new StringLiteral("y")))));
        InPredicate userIn = new InPredicate(new Add(a, new IntegerLiteral(5)),
                List.of(new StringLiteral("p"), new StringLiteral("q")));
        Map<Long, Expression> values = Map.of(
                1L, new IntegerLiteral(5),
                2L, userIn);

        Expression replaced = new SPMPlaceholderReplacer().replace(planExpr, values);

        Assertions.assertTrue(replaced instanceof InPredicate);
        InPredicate in = (InPredicate) replaced;
        // The compareExpr placeholder is replaced: (a + 5), not (a + _spm_const_var(1, 1))
        Assertions.assertEquals(new Add(a, new IntegerLiteral(5)), in.getCompareExpr());
        // The IN list is replaced with the user values
        Assertions.assertEquals(2, in.getOptions().size());
        Assertions.assertEquals(new StringLiteral("p"), in.getOptions().get(0));
        Assertions.assertEquals(new StringLiteral("q"), in.getOptions().get(1));
        // No placeholder may remain in the rewritten expression
        Assertions.assertFalse(containsPlaceholder(in));
    }

    // ==================== whole-tree subquery parameterization ====================

    @Test
    public void testParameterizeSubqueryConstant() throws Exception {
        // WHERE a = 100 AND t1.c IN (SELECT t2.c FROM t2 WHERE t2.b = 1)
        // the constant 1 inside the IN-subquery must be parameterized too:
        //   ... t2.b = _spm_const_var(2, 1)
        LogicalPlan bindPlan = parse(
                "SELECT * FROM t1 WHERE a = 100 "
                        + "AND t1.c IN (SELECT t2.c FROM t2 WHERE t2.b = 1)");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan parameterized = SPMPlanTreeSupport.transform(
                bindPlan, expr -> expr.accept(builder, null));

        // a = 100 -> id 1 (top-level), the subquery constant 1 -> id 2
        Assertions.assertEquals(2, builder.getPlaceholderExprs().size());
        String treeSql = allExprSqls(parameterized);
        Assertions.assertTrue(treeSql.contains("_spm_const_var(1, 100)"), treeSql);
        Assertions.assertTrue(treeSql.contains("_spm_const_var(2, 1)"),
                "the subquery constant must be parameterized: " + treeSql);
    }

    @Test
    public void testMatchSubqueryConstantExtractsValue() throws Exception {
        String bindSql = "SELECT * FROM t1 WHERE a = 100 "
                + "AND t1.c IN (SELECT t2.c FROM t2 WHERE t2.b = 1)";
        LogicalPlan parameterized = SPMPlanTreeSupport.transform(
                parse(bindSql), expr -> expr.accept(new SPMPlaceholderBuilder(), null));
        LogicalPlan userPlan = parse("SELECT * FROM t1 WHERE a = 42 "
                + "AND t1.c IN (SELECT t2.c FROM t2 WHERE t2.b = 2)");

        Map<Long, Expression> values = new HashMap<>();
        boolean matched = SPMPlanTreeSupport.check(parameterized, userPlan, values);
        Assertions.assertTrue(matched);
        // both the top-level (a) and the subquery (b) values are extracted (the parsed
        // literals may be TinyInt/Byte; compare numerically)
        Assertions.assertEquals(42,
                ((Number) ((org.apache.doris.nereids.trees.expressions.literal.Literal)
                        values.get(1L)).getValue()).intValue());
        Assertions.assertEquals(2,
                ((Number) ((org.apache.doris.nereids.trees.expressions.literal.Literal)
                        values.get(2L)).getValue()).intValue());
    }

    @Test
    public void testReplaceSubqueryConstant() throws Exception {
        String bindSql = "SELECT * FROM t1 WHERE a = 100 "
                + "AND t1.c IN (SELECT t2.c FROM t2 WHERE t2.b = 1)";
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan parameterized = SPMPlanTreeSupport.transform(
                parse(bindSql), expr -> expr.accept(builder, null));

        // user values: a = 42, b = 2
        Map<Long, Expression> values = new HashMap<>();
        values.put(1L, new IntegerLiteral(42));
        values.put(2L, new IntegerLiteral(2));
        LogicalPlan replaced = SPMPlanTreeSupport.transform(
                parameterized, expr -> expr.accept(new SPMPlaceholderReplacer(), values));

        // both the top-level and the subquery constants are substituted, no placeholder
        // may remain anywhere in the tree
        Assertions.assertFalse(SPMPlanTreeSupport.containsPlaceholder(replaced),
                "no placeholder may remain: " + allExprSqls(replaced));
        String treeSql = allExprSqls(replaced);
        Assertions.assertTrue(treeSql.contains("= 42"), treeSql);
        Assertions.assertTrue(treeSql.contains("= 2"), treeSql);
    }

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** Concatenates the SQL text of every expression of the tree (for assertions). */
    private static String allExprSqls(LogicalPlan plan) {
        StringBuilder sb = new StringBuilder();
        collectExprSqls(plan, sb);
        return sb.toString();
    }

    private static void collectExprSqls(org.apache.doris.nereids.trees.plans.Plan plan, StringBuilder sb) {
        for (Expression expr : plan.getExpressions()) {
            sb.append(expr.toSql()).append('\n');
            collectSubqueryPlanExprSqls(expr, sb);
        }
        for (org.apache.doris.nereids.trees.plans.Plan child : plan.children()) {
            collectExprSqls(child, sb);
        }
    }

    /** Recurses into every subquery plan reachable from an expression tree. */
    private static void collectSubqueryPlanExprSqls(Expression expr, StringBuilder sb) {
        if (expr instanceof InSubquery || expr instanceof org.apache.doris.nereids.trees.expressions.SubqueryExpr) {
            collectExprSqls(((org.apache.doris.nereids.trees.expressions.SubqueryExpr) expr)
                    .getQueryPlan(), sb);
        }
        for (Expression child : expr.children()) {
            collectSubqueryPlanExprSqls(child, sb);
        }
    }

    private static boolean containsPlaceholder(Expression expr) {
        if (expr instanceof SpmConstVar || expr instanceof SpmConstList) {
            return true;
        }
        for (Expression child : expr.children()) {
            if (containsPlaceholder(child)) {
                return true;
            }
        }
        return false;
    }
}
