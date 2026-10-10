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

import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Review fixes of the third comment round that live in the digest / output-label layer:
 *
 * - comment 5: UnboundResultSink defaulted to toDigest(), which intentionally DROPS a
 *   LogicalSelectHint (Doris's generic fingerprint ignores hints). Two CREATEs differing
 *   only in `*+ SET_VAR(...) *` produced the SAME bind digest / hash, so the second CREATE
 *   was confirmed as a duplicate of the first and returned its id - while the frozen replay
 *   pinned the OTHER variant's settings. The sink now forwards toSpmDigest to its child.
 * - comment 2: a star over a LATERAL VIEW (Generate) or a WITH alias fell to the
 *   unknown-star fallback, so the caller's derived `v + 2` kept reporting the captured
 *   `v + 1` header; a recursive self-reference must decline instead of recursing.
 * - comment 3: a payload star (`* EXCEPT/REPLACE`) whose expansion has an OPEN tail
 *   (a base table / join side) dropped the DERIVED prefix, so
 *   `SELECT * EXCEPT(v) FROM (SELECT a + 1 ...) d CROSS JOIN u` kept the captured
 *   `a + 1` header although the value was substituted. The derived prefix now realigns
 *   while an EXCEPT / REPLACE entry addressing the underivable tail is ignored.
 */
public class SPMReviewRound3SafetyTest {

    private static LogicalPlan parse(String sql) {
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }

    /** Every project item's own label (explicit alias, derived text or column name). */
    private static void collectProjectLabels(Plan plan, List<String> labels) {
        if (plan instanceof LogicalProject) {
            for (NamedExpression item : ((LogicalProject<?>) plan).getProjects()) {
                if (item instanceof UnboundAlias) {
                    labels.add((String) ((UnboundAlias) item).getAlias().orElse(null));
                } else if (item instanceof Slot) {
                    labels.add(((Slot) item).getName());
                } else {
                    labels.add(String.valueOf(item));
                }
            }
        }
        for (Plan child : plan.children()) {
            collectProjectLabels(child, labels);
        }
    }

    // ==================== comment 5: the sink must not drop the hints ====================

    /**
     * The parse-time sink sits ABOVE the LogicalSelectHint, so its SPM identity must
     * forward to toSpmDigest - the default toDigest() intentionally skips the hint and
     * collapsed two statements that differ only in their SET_VAR settings.
     */
    @Test
    public void testSinkDigestKeepsTheStatementHint() {
        LogicalPlan one = parse("SELECT /*+ SET_VAR(parallel_pipeline_task_num=1) */ k FROM t");
        LogicalPlan two = parse("SELECT /*+ SET_VAR(parallel_pipeline_task_num=2) */ k FROM t");
        Assertions.assertNotEquals(one.toSpmDigest(), two.toSpmDigest(),
                "the sink must forward the SPM digest: the two SET_VAR variants collided"
                        + " when it fell back to toDigest()");
        Assertions.assertEquals(one.toSpmDigest(),
                parse("SELECT /*+ SET_VAR(parallel_pipeline_task_num=1) */ k FROM t")
                        .toSpmDigest(),
                "the digest is deterministic");
        LogicalPlan nestedOne = parse(
                "SELECT k FROM (SELECT /*+ SET_VAR(parallel_pipeline_task_num=1) */ k FROM t) s");
        LogicalPlan nestedTwo = parse(
                "SELECT k FROM (SELECT /*+ SET_VAR(parallel_pipeline_task_num=2) */ k FROM t) s");
        Assertions.assertNotEquals(nestedOne.toSpmDigest(), nestedTwo.toSpmDigest(),
                "a nested SET_VAR block writes the same statement-scoped variable and must"
                        + " change the digest too");
    }

    /**
     * End to end over the CREATE seam: the bind digest / hash ARE the identity the durable
     * duplicate check compares, so the two hint variants must build baselines under
     * DIFFERENT identities - otherwise the second CREATE is answered with the first
     * baseline's id while its own SET_VAR settings were never captured.
     */
    @Test
    public void testBindDigestOfHintVariantsDoesNotCollide() throws Exception {
        String one = "SELECT /*+ SET_VAR(parallel_pipeline_task_num=1) */ k FROM t";
        String two = "SELECT /*+ SET_VAR(parallel_pipeline_task_num=2) */ k FROM t";
        BaselinePlan first = new SPMPlanner().buildBaseline(one, one);
        BaselinePlan second = new SPMPlanner().buildBaseline(two, two);
        Assertions.assertNotEquals(first.getBindSqlDigest(), second.getBindSqlDigest(),
                "two CREATEs differing only in SET_VAR must not share one identity: "
                        + first.getBindSqlDigest());
        Assertions.assertNotEquals(first.getBindSqlHash(), second.getBindSqlHash());
    }

    // ==================== comment 2: LATERAL VIEW / WITH stars ====================

    /**
     * A root star over a LATERAL VIEW expands to the derived subquery's labels PLUS the
     * generator's output column: the derived `v + 2` must realign even though the star's
     * relation is a Generate, not a plain alias.
     */
    @Test
    public void testStarOverLateralViewRealignsTheDerivedHeader() {
        LogicalPlan rendered = parse(
                "SELECT (arr) AS `arr`, (v) AS `v + 1`, (x) AS `x` FROM t");
        LogicalPlan user = parse("SELECT * FROM (SELECT arr, v + 2 FROM t) s"
                + " LATERAL VIEW explode(s.arr) lv AS x");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rendered, user);
        List<String> labels = new ArrayList<>();
        collectProjectLabels(aligned, labels);
        Assertions.assertTrue(labels.contains("v + 2"),
                "the LATERAL VIEW star must expose the caller's own derived label: "
                        + labels);
        Assertions.assertFalse(labels.contains("v + 1"),
                "the captured label must be replaced: " + labels);
        Assertions.assertTrue(labels.contains("x"),
                "the generator's output column keeps its own name: " + labels);
    }

    /**
     * A star naming a WITH alias resolves through the CTE's own query: `SELECT * FROM c`
     * over `WITH c AS (SELECT v + 1 FROM t)` reported the captured `v + 1` header for the
     * caller's `v + 2` because the relation could not reach its alias query.
     */
    @Test
    public void testStarOverCteRealignsTheDerivedHeader() {
        LogicalPlan rendered = parse(
                "WITH c AS (SELECT v + 1 FROM t) SELECT (v) AS `v + 1` FROM c");
        LogicalPlan user = parse("WITH c AS (SELECT v + 2 FROM t) SELECT * FROM c");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rendered, user);
        List<String> labels = new ArrayList<>();
        collectProjectLabels(aligned, labels);
        Assertions.assertTrue(labels.contains("v + 2"),
                "the WITH-alias star must expose the caller's derived label: " + labels);
        Assertions.assertFalse(labels.contains("v + 1"),
                "the captured label must be replaced: " + labels);
    }

    /**
     * A recursive self-reference must DECLINE instead of recursing forever: the caller's
     * headers cannot be positioned, so the alignment fails loudly (the replay keeps its
     * frozen output list) rather than blowing the stack.
     */
    @Test
    public void testRecursiveCteStarDeclines() {
        LogicalPlan rendered = parse(
                "WITH r AS (SELECT n FROM t) SELECT (n) AS `n` FROM r");
        LogicalPlan user = parse("WITH r AS (SELECT * FROM r) SELECT * FROM r");
        SPMPlanTreeSupport.UnalignableOutputLabelsException failure = Assertions.assertThrows(
                SPMPlanTreeSupport.UnalignableOutputLabelsException.class,
                () -> SPMPlanTreeSupport.alignRootOutputLabels(rendered, user));
        Assertions.assertTrue(failure.getMessage().contains("cannot be expanded (recursive)"),
                failure.getMessage());
    }

    // ==================== comment 3: payload stars over an open tail ====================

    /**
     * `* EXCEPT(v)` over a CROSS JOIN: the star's expansion has an UNDERIVABLE tail (u's
     * real column names) but its DERIVED prefix (the subquery's `a + 2`) must still take
     * part in the realignment; dropping it kept the captured `a + 1` header on the replay.
     */
    @Test
    public void testExceptPayloadOverAnOpenTailKeepsTheDerivedPrefix() {
        LogicalPlan rendered = parse("SELECT (a + 1) AS `a + 1`, (u1) AS `u1` FROM t");
        LogicalPlan user = parse(
                "SELECT * EXCEPT(v) FROM (SELECT a + 2, v FROM t) d CROSS JOIN u");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rendered, user);
        List<String> labels = new ArrayList<>();
        collectProjectLabels(aligned, labels);
        Assertions.assertTrue(labels.contains("a + 2"),
                "the derived prefix must realign under the payload star: " + labels);
        Assertions.assertFalse(labels.contains("a + 1"),
                "the captured label must be replaced: " + labels);
        Assertions.assertTrue(labels.contains("u1"),
                "the underivable tail keeps the frozen (real column) names: " + labels);
    }

    /**
     * An EXCEPT name addressing the UNDERIVABLE TAIL (a real base-table column) removes
     * frozen-name positions only - nothing to realign there - so it is ignored instead of
     * declining the whole rewrite, and the derived prefix still realigns.
     */
    @Test
    public void testExceptPayloadOverTheUnderivableTailStillRealigns() {
        LogicalPlan rendered = parse("SELECT (a + 1) AS `a + 1`, (u1) AS `u1` FROM t");
        LogicalPlan user = parse(
                "SELECT * EXCEPT(u1) FROM (SELECT a + 2 FROM t) d CROSS JOIN u");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rendered, user);
        List<String> labels = new ArrayList<>();
        collectProjectLabels(aligned, labels);
        Assertions.assertTrue(labels.contains("a + 2"),
                "an EXCEPT over the underivable tail must still realign the prefix: "
                        + labels);
    }

    /**
     * A REPLACE alias naming the UNDERIVABLE tail position swaps a VALUE there (the
     * header stays the real column name), so it is ignored and the derived prefix still
     * realigns.
     */
    @Test
    public void testReplacePayloadAddressingTheTailKeepsTheDerivedPrefix() {
        LogicalPlan rendered = parse("SELECT (a + 1) AS `a + 1`, (u1) AS `u1` FROM t");
        LogicalPlan user = parse(
                "SELECT * REPLACE(1 AS u1) FROM (SELECT a + 2 FROM t) d CROSS JOIN u");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rendered, user);
        List<String> labels = new ArrayList<>();
        collectProjectLabels(aligned, labels);
        Assertions.assertTrue(labels.contains("a + 2"),
                "the derived prefix realigns while the tail REPLACE is ignored: " + labels);
    }

    /**
     * A payload star whose open tail is FOLLOWED by further items cannot be positioned
     * from the caller's own list (the tail's width is unknown): the shape declines loudly
     * instead of half-renaming the frozen list.
     */
    @Test
    public void testPayloadStarBeforeFurtherItemsDeclines() {
        LogicalPlan rendered = parse(
                "SELECT (a + 1) AS `a + 1`, (u1) AS `u1`, (c + 1) AS `c + 2` FROM t");
        LogicalPlan user = parse("SELECT * EXCEPT(v), c + 3 FROM"
                + " (SELECT a + 2, v, c FROM t) d CROSS JOIN u");
        Assertions.assertThrows(SPMPlanTreeSupport.UnalignableOutputLabelsException.class,
                () -> SPMPlanTreeSupport.alignRootOutputLabels(rendered, user),
                "an open tail of unknown width cannot be split around a trailing item");
    }

    /**
     * The NO-payload form of the same shape IS positionable: the trailing item aligns
     * from the END of the frozen list while the star contributes its derived prefix and
     * the unknown-width middle keeps the real column names.
     */
    @Test
    public void testPlainStarBeforeFurtherItemsRealignsFromTheEnd() {
        LogicalPlan rendered = parse("SELECT (a + 1) AS `a + 1`, (v) AS `v`, (c) AS `c`,"
                + " (u1) AS `u1`, (c + 1) AS `c + 2` FROM t");
        LogicalPlan user = parse("SELECT *, c + 3 FROM"
                + " (SELECT a + 2, v, c FROM t) d CROSS JOIN u");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(rendered, user);
        List<String> labels = new ArrayList<>();
        collectProjectLabels(aligned, labels);
        Assertions.assertTrue(labels.contains("a + 2"),
                "the star's derived prefix realigns: " + labels);
        Assertions.assertTrue(labels.contains("c + 3"),
                "the trailing item realigns from the END of the frozen list: " + labels);
        Assertions.assertTrue(labels.contains("u1"),
                "the unknown-width middle keeps the real column names: " + labels);
        Assertions.assertFalse(labels.contains("c + 2"),
                "the captured label of the trailing item must be replaced: " + labels);
        Assertions.assertFalse(labels.contains("a + 1"),
                "the captured label of the derived prefix must be replaced: " + labels);
    }
}
