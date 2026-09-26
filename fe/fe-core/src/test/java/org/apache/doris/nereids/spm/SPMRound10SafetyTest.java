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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.spm.matcher.SPMFrozenTreeReplacer;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.statistics.repository.ResultRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tenth review round: provenance / parser-mode / placeholder-forgery tests.
 *
 * Covered here:
 *  - a QUALIFIED user UDF call shaped like the internal placeholder marker
 *    ({@code db._spm_const_var(1)}) is never classified as (or replaced like) a frozen
 *    placeholder, for both marker names (plan_frozen provenance + the namespace check);
 *  - the persisted planSql mode is honoured when the plan tree is rebuilt: a raw
 *    fallback text is re-parsed with the CREATOR's mode, the decompiled rendering with
 *    MODE_DEFAULT (a PIPES_AS_CONCAT fallback would otherwise rebuild as a boolean Or);
 *  - the in-memory build entry records the parse mode with the provenance fields;
 *  - legacy rows that froze a temporary table's internal name fail closed on load;
 *  - the state-sensitive rule whitelist exclusions cover SPLIT_LIMIT (execution-only
 *    two-phase LIMIT) and PUSH_DOWN_AGG_THROUGH_JOIN_ON_PKFK (mutable PK/FK constraint).
 */
public class SPMRound10SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    private static LogicalPlan transform(LogicalPlan plan, Map<Long, Expression> values) {
        return SPMPlanTreeSupport.transform(plan,
                expr -> expr.accept(new SPMFrozenTreeReplacer(), values));
    }

    // ==================== #10: qualified marker-like calls are real UDFs ====================

    @Test
    public void testQualifiedPlaceholderCallIsNotFrozen() {
        UnboundFunction unqualified = new UnboundFunction(
                SPMFrozenTreeReplacer.CONST_VAR_FUNC, List.of(new IntegerLiteral(1)));
        UnboundFunction qualified = new UnboundFunction("mydb",
                SPMFrozenTreeReplacer.CONST_VAR_FUNC, List.of(new IntegerLiteral(1)));
        Assertions.assertTrue(SPMFrozenTreeReplacer.isUnsubstitutedPlaceholder(unqualified),
                "the unqualified marker call is SPM's own placeholder");
        Assertions.assertFalse(SPMFrozenTreeReplacer.isUnsubstitutedPlaceholder(qualified),
                "a QUALIFIED call of the same name is a real user UDF reference");
        // both marker names must be namespace-checked
        Assertions.assertFalse(SPMFrozenTreeReplacer.isUnsubstitutedPlaceholder(
                new UnboundFunction("mydb", SPMFrozenTreeReplacer.CONST_LIST_FUNC,
                        List.of(new IntegerLiteral(1)))));

        // parsed trees: the classifier must not forge a frozen tree out of the real call
        Assertions.assertTrue(SPMPlanTreeSupport.containsFrozenPlaceholder(parse(
                "SELECT _spm_const_var(1) AS v FROM t1")));
        Assertions.assertFalse(SPMPlanTreeSupport.containsFrozenPlaceholder(parse(
                "SELECT mydb._spm_const_var(1) AS v FROM t1")),
                "db._spm_const_var(1) is an ordinary UDF call, not a placeholder");

        // and the replacer must leave the qualified call untouched while substituting the
        // unqualified one
        LogicalPlan replaced = transform(parse("SELECT _spm_const_var(1) AS v FROM t1"),
                Map.of(1L, new IntegerLiteral(42)));
        Assertions.assertTrue(replaced.treeString().contains("42"),
                "the unqualified placeholder must be substituted: " + replaced.treeString());
        LogicalPlan kept = transform(parse("SELECT mydb._spm_const_var(1) AS v FROM t1"),
                Map.of(1L, new IntegerLiteral(42)));
        Assertions.assertFalse(kept.treeString().contains("42"),
                "the qualified UDF call must never be replaced by the user's literal: "
                        + kept.treeString());
    }

    // ==================== #10 / #4: persisted provenance classification ====================

    @Test
    public void testPersistedFrozenProvenanceOverridesNameDetection() {
        String qualifiedText = "SELECT mydb._spm_const_var(1) AS v FROM t1";
        Assertions.assertFalse(SPMPlanner.isFrozenPlanSql(qualifiedText, Boolean.FALSE),
                "an explicit raw-fallback provenance must win over the text shape");
        Assertions.assertFalse(SPMPlanner.isFrozenPlanSql(qualifiedText, null),
                "legacy rows classify by parsing: the qualified UDF is not a placeholder");

        String frozenText = "SELECT _spm_const_var(1) AS v FROM t1";
        Assertions.assertTrue(SPMPlanner.isFrozenPlanSql(frozenText, Boolean.TRUE));
        Assertions.assertTrue(SPMPlanner.isFrozenPlanSql(frozenText, null),
                "legacy frozen texts keep being detected by parsing");
        Assertions.assertFalse(SPMPlanner.isFrozenPlanSql(frozenText, Boolean.FALSE),
                "a persisted FALSE provenance must suppress the parse-based detection");
    }

    // ==================== #4: raw fallback plan text keeps the creator's mode ====================

    @Test
    public void testRawFallbackPlanTextKeepsCreatorParserMode() {
        String bindSql = "SELECT 1";
        String planSql = "SELECT a || b FROM t1";
        Pair<LogicalPlan, LogicalPlan> creatorMode = SPMPlanner.rebuildParameterizedTrees(
                bindSql, planSql, SqlModeHelper.MODE_DEFAULT,
                SqlModeHelper.MODE_PIPES_AS_CONCAT);
        Assertions.assertNotNull(creatorMode.second);
        Assertions.assertTrue(
                creatorMode.second.treeString().toLowerCase().contains("concat"),
                "the raw fallback planSql must be re-parsed with the CREATOR's mode: "
                        + creatorMode.second.treeString());

        Pair<LogicalPlan, LogicalPlan> decompiledDefault = SPMPlanner.rebuildParameterizedTrees(
                bindSql, planSql, SqlModeHelper.MODE_DEFAULT, SqlModeHelper.MODE_DEFAULT);
        Assertions.assertNotNull(decompiledDefault.second);
        Assertions.assertFalse(
                decompiledDefault.second.treeString().toLowerCase().contains("concat"),
                "the decompiled rendering stays pinned to the default mode: "
                        + decompiledDefault.second.treeString());
    }

    // ==================== #7: the in-memory build records mode + provenance ====================

    @Test
    public void testInMemoryBuildCapturesModeBeforeParseAndProvenance() throws Exception {
        BaselinePlan[] holder = new BaselinePlan[1];
        SqlModeHelper.withSqlMode(SqlModeHelper.MODE_PIPES_AS_CONCAT, () -> {
            try {
                holder[0] = new SPMPlanner().buildBaseline("SELECT a || b FROM t1",
                        "SELECT a || b FROM t1");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        BaselinePlan baseline = holder[0];
        Assertions.assertEquals(SqlModeHelper.MODE_PIPES_AS_CONCAT, baseline.getCreatorSqlMode(),
                "the creator mode must describe the parse that produced the bind tree");
        Assertions.assertEquals(SqlModeHelper.MODE_PIPES_AS_CONCAT,
                baseline.getPlanSqlMode().longValue(),
                "the in-memory plan text is ordinary user text: it reloads with the"
                        + " creator's mode");
        Assertions.assertEquals(Boolean.FALSE, baseline.getPlanFrozen(),
                "the in-memory engine never produces a frozen (decompiled) plan text");
        Assertions.assertEquals("", baseline.getSchemaFingerprint(),
                "without a statement context no schema identity can be bound");
    }

    // ==================== #5: legacy temporary-table rows fail closed ====================

    @Test
    public void testLegacyTemporaryTableRowIsRejectedOnLoad() {
        // the frozen text of a legacy GLOBAL temp-table baseline carries the creator
        // session's internal name
        ResultRow tempRow = row("select k from db.t_#TEMP#_x", "select k from db.t_#TEMP#_x");
        RuntimeException error = Assertions.assertThrows(RuntimeException.class,
                () -> BaselineManager.parsePersistedRowForTest(tempRow));
        Assertions.assertTrue(error.getMessage().contains("temporary table"), error.getMessage());

        // the same row without the marker loads normally (control)
        ResultRow plainRow = row("select k from db.t", "select k from db.t");
        Assertions.assertDoesNotThrow(() -> BaselineManager.parsePersistedRowForTest(plainRow));
    }

    /** One spm_baselines row in the SELECT_ALL_SQL column order (16 columns). */
    private static ResultRow row(String bindSql, String planSql) {
        List<String> values = new ArrayList<>();
        values.add("1");                                  // 0 id
        values.add(bindSql);                              // 1 bind_sql
        values.add("digest");                             // 2 bind_sql_digest
        values.add("7");                                  // 3 bind_sql_hash
        values.add(planSql);                              // 4 plan_sql
        values.add("qid");                                // 5 query_id
        values.add("0.0");                                // 6 cost
        values.add("-1");                                 // 7 query_time_ms
        values.add("USER");                               // 8 source
        values.add("ENABLED");                            // 9 status
        values.add("2026-01-01 00:00:00");                // 10 create_time
        values.add("2026-01-01 00:00:00");                // 11 update_time
        values.add(String.valueOf(SqlModeHelper.MODE_DEFAULT)); // 12 sql_mode
        values.add(String.valueOf(SqlModeHelper.MODE_DEFAULT)); // 13 plan_sql_mode
        values.add("false");                              // 14 plan_frozen
        values.add("");                                   // 15 schema_fingerprint
        return new ResultRow(values);
    }

    // ==================== #6 / #8: the whitelist excludes the two state-sensitive rules ===

    @Test
    public void testExcludedRulesCoverSplitLimitAndPkFkAggPushDown() {
        Assertions.assertTrue(SPMOptimizer.SPM_EXCLUDED_RULE_NAMES.contains("SPLIT_LIMIT"),
                "the two-phase LIMIT split must be excluded from frozen-plan creation");
        Assertions.assertTrue(SPMOptimizer.SPM_EXCLUDED_RULE_NAMES
                .contains("PUSH_DOWN_AGG_THROUGH_JOIN_ON_PKFK"),
                "the PK/FK aggregate push down must be excluded (mutable constraint state)");
        // and both names must stay parseable by the whitelist builder (RuleType.valueOf)
        Assertions.assertEquals(RuleType.SPLIT_LIMIT, RuleType.valueOf("SPLIT_LIMIT"));
        Assertions.assertEquals(RuleType.PUSH_DOWN_AGG_THROUGH_JOIN_ON_PKFK,
                RuleType.valueOf("PUSH_DOWN_AGG_THROUGH_JOIN_ON_PKFK"));
    }

    // ==================== #11: fingerprint needs a session ====================

    @Test
    public void testSchemaFingerprintIsEmptyWithoutContext() {
        Assertions.assertEquals("", SPMPlanTreeSupport.schemaFingerprint(null,
                parse("SELECT * FROM t1 WHERE k = 1")),
                "without a session no table can be resolved: the check must stay disabled");
    }

    // ==================== #6: top-level LIMIT / OFFSET are outside the digest ====================

    @Test
    public void testTopLevelLimitOffsetAreDigestIndependent() {
        LogicalPlan ten = parse("SELECT k FROM t1 ORDER BY k LIMIT 10");
        LogicalPlan twenty = parse("SELECT k FROM t1 ORDER BY k LIMIT 20");
        LogicalPlan twentyOffset = parse("SELECT k FROM t1 ORDER BY k LIMIT 20 OFFSET 5");
        String key = SPMPlanTreeSupport.canonicalSpmDigest(ten.toSpmDigest());
        Assertions.assertEquals(key,
                SPMPlanTreeSupport.canonicalSpmDigest(twenty.toSpmDigest()),
                "the LIMIT value must not be part of the matching key");
        Assertions.assertNotEquals(key, twentyOffset.toSpmDigest(),
                "the raw digest keeps the ' OFFSET ?' suffix: this is exactly why the key"
                        + " must be canonicalized");
        Assertions.assertEquals(key,
                SPMPlanTreeSupport.canonicalSpmDigest(twentyOffset.toSpmDigest()),
                "the canonical key must be OFFSET-independent (the inherited renderer emits"
                        + " ' OFFSET ?' only for a non-zero offset, so an OFFSET query could"
                        + " never reach the structural match that adopts its values)");

        // and the structural match accepts the value mismatch (top-level limits are
        // adopted from the user query), while a subquery limit stays exact
        Assertions.assertTrue(SPMPlanTreeSupport.check(ten, twentyOffset,
                new java.util.HashMap<>()),
                "a top-level LIMIT / OFFSET difference must pass Level 3");
        LogicalPlan subTen = parse(
                "SELECT k FROM t1 WHERE k IN (SELECT k FROM t2 LIMIT 2)");
        LogicalPlan subTwenty = parse(
                "SELECT k FROM t1 WHERE k IN (SELECT k FROM t2 LIMIT 5)");
        Assertions.assertFalse(SPMPlanTreeSupport.check(subTen, subTwenty,
                new java.util.HashMap<>()),
                "a LIMIT inside a subquery plan is not reachable by the positional merge"
                        + " and must stay exact-matched");
    }
}
