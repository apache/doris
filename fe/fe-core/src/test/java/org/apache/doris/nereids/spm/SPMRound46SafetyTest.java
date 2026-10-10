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

import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * The payload checks the first relation-level proof still left open:
 *
 * - a TABLE FUNCTION was not compared at all: numbers("number"="10") bound to
 *   numbers("number"="20") passed the relation-set check, the projection texts and
 *   every other guard - the stored plan generates a different number of rows for
 *   the same caller text;
 * - a MARK join exposes the same JoinType and the same conjunct texts as the plain
 *   semi/anti join it was built from, but it also emits a three-valued mark column,
 *   and the MARK_SLOT name is the column the caller resolves;
 * - an ASOF ... USING join (the LogicalUsingJoin shape) carries its MATCH_CONDITION
 *   outside the compared conjunct lists: >= bound to > pairs different row sets;
 * - Aggregate / Window / Generate output lists were compared label-stripped and
 *   unordered (or not at all): swapping MIN/MAX, row_number/rank or LATERAL VIEW's
 *   generated column names keeps every text in place while changing what the
 *   operator (and the caller's references) compute;
 * - a nested projection's NAME-to-expression mapping is what a parent query
 *   resolves through (outside the alignable root list): k AS x, v AS y bound to
 *   v AS x, k AS y keeps both orderings ("k", "v") equal while returning the other
 *   column under every caller-visible name.
 */
public class SPMRound46SafetyTest {

    private static RuntimeException buildFails(String bindSql, String planSql) {
        return Assertions.assertThrows(RuntimeException.class,
                () -> new SPMPlanner().buildBaseline(bindSql, planSql));
    }

    private static void assertRejection(RuntimeException failure, String... needles) {
        Assertions.assertNotNull(failure.getMessage());
        for (String needle : needles) {
            if (failure.getMessage().contains(needle)) {
                return;
            }
        }
        Assertions.fail("the rejection must mention one of " + Arrays.toString(needles)
                + ", but was: " + failure.getMessage());
    }

    // ==================== table functions ====================

    /** The TVF's properties decide which rows it generates; the name alone is not it. */
    @Test
    public void testChangedTableFunctionPropertyIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT number FROM numbers(\"number\" = \"10\")",
                "SELECT number FROM numbers(\"number\" = \"20\")");
        assertRejection(failure, "table function");
    }

    // ==================== MARK joins ====================

    /** A MARK join also emits the mark column the plain semi join does not have. */
    @Test
    public void testPlainSemiJoinBoundToAMarkJoinIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT t.k FROM t LEFT SEMI MARK JOIN u MARK_SLOT m ON t.k = u.k",
                "SELECT t.k FROM t LEFT SEMI JOIN u ON t.k = u.k");
        assertRejection(failure, "MARK");
    }

    /** The MARK_SLOT name is the caller-visible column: m and n are not interchangeable. */
    @Test
    public void testRenamedMarkSlotIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT t.k FROM t LEFT SEMI MARK JOIN u MARK_SLOT m ON t.k = u.k",
                "SELECT t.k FROM t LEFT SEMI MARK JOIN u MARK_SLOT n ON t.k = u.k");
        assertRejection(failure, "mark slot");
    }

    // ==================== ASOF ... USING ====================

    /** The ASOF USING join's MATCH_CONDITION is outside every compared conjunct list. */
    @Test
    public void testChangedUsingJoinMatchConditionIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT a.k FROM t a ASOF LEFT JOIN u b "
                        + "MATCH_CONDITION(a.d >= b.d) USING (k)",
                "SELECT a.k FROM t a ASOF LEFT JOIN u b "
                        + "MATCH_CONDITION(a.d > b.d) USING (k)");
        assertRejection(failure, "match condition");
    }

    // ==================== output lists of row-forming operators ====================

    /** MIN/MAX (Aggregate outputs) and the names a parent resolves them through. */
    @Test
    public void testSwappedAggregateOutputsAreRejected() {
        RuntimeException failure = buildFails(
                "SELECT x FROM (SELECT MIN(k) AS x, MAX(k) AS y FROM t) s",
                "SELECT x FROM (SELECT MAX(k) AS x, MIN(k) AS y FROM t) s");
        assertRejection(failure, "aggregated output expressions", "projected expressions");
    }

    /** row_number and rank are different window functions, in a different order. */
    @Test
    public void testSwappedWindowOutputsAreRejected() {
        RuntimeException failure = buildFails(
                "SELECT x FROM (SELECT row_number() OVER (ORDER BY k) AS x,"
                        + " rank() OVER (ORDER BY k) AS y FROM t) s",
                "SELECT x FROM (SELECT rank() OVER (ORDER BY k) AS x,"
                        + " row_number() OVER (ORDER BY k) AS y FROM t) s");
        assertRejection(failure, "window expressions", "projected expressions");
    }

    // ==================== nested name-to-expression mappings ====================

    /** k AS x, v AS y vs v AS x, k AS y: the ORDERINGS stay ("k","v")/("v","k")... */
    @Test
    public void testNestedProjectionLabelMappingIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT x FROM (SELECT k AS x, v AS y FROM t) s",
                "SELECT x FROM (SELECT v AS x, k AS y FROM t) s");
        assertRejection(failure, "projected expressions");
    }

    /**
     * The control: the ALIGNABLE root output list stays label-relaxed (SELECT k
     * bound to k AS kk is the documented bare-column alignment).
     */
    @Test
    public void testRootLabelOnlyDifferenceStaysAccepted() throws Exception {
        BaselinePlan built = new SPMPlanner().buildBaseline(
                "SELECT k FROM t LIMIT 3", "SELECT k AS kk FROM t LIMIT 3");
        Assertions.assertNotNull(built);
    }

    // ==================== LATERAL VIEW generated columns ====================

    /**
     * AS x and AS y are new names the caller resolves through (a star keeps both).
     * explode is a return-many-column generator, so the names live in the AS list.
     */
    @Test
    public void testRenamedGeneratedColumnsAreRejected() {
        RuntimeException failure = buildFails(
                "SELECT * FROM t LATERAL VIEW explode(arr) lv AS x",
                "SELECT * FROM t LATERAL VIEW explode(arr) lv AS y");
        assertRejection(failure, "generated column aliases");
    }

    /** A single-name generator keeps the alias on its output slot. */
    @Test
    public void testRenamedSingleColumnGeneratedColumnIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT e FROM t LATERAL VIEW explode_numbers(n) lv AS e",
                "SELECT e FROM t LATERAL VIEW explode_numbers(n) lv AS f");
        assertRejection(failure, "generated column names", "projected expressions");
    }

    // ==================== the forwarded duplicate CREATE's identity ====================

    /**
     * The digest SPMPlanner#canonicalBindDigest precomputes on the follower must be
     * the very identity buildBaseline stores: the manager confirms an idempotent
     * duplicate CREATE by that digest (see BaselineManager.ForwardedDdlExpectation).
     */
    @Test
    public void testCanonicalBindDigestMatchesTheStoredDigest() throws Exception {
        String sql = "SELECT k FROM t WHERE k = 1";
        BaselinePlan built = new SPMPlanner().buildBaseline(sql, sql);
        ConnectContext ctx = ConnectContext.get();
        Assertions.assertEquals(built.getBindSqlDigest(),
                SPMPlanner.canonicalBindDigest(ctx, sql),
                "the confirmation digest must equal the stored one");
    }
}
