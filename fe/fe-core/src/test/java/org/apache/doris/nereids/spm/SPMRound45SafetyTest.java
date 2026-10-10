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

import org.apache.doris.nereids.spm.manager.BaselineManager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * CREATE-time relational-topology proof and the id-watermark confirmation:
 *
 * - the one-way source check accepted a plan that OMITTED a relation of the bind text
 *   (a caller matching the u-reading bind was answered from t alone);
 * - a plan could change the JOIN type / ON condition (an unmatched left row survives a
 *   LEFT join but not an INNER one), drop a DISTINCT (t={1,1}: one caller row, two
 *   replay rows), change GROUP BY keys / the set-operation kind / a window function, or
 *   add a WHERE predicate (SELECT k FROM t bound to ... WHERE k IS NOT NULL silently
 *   drops the caller's NULL rows). The plan is stored and replayed as-is, so every such
 *   divergence is a silent result change - the topology is now proven node by node;
 * - the compact id high-water mark alone cannot prove no newer id was reserved: the
 *   record, the sequence reservation and the baseline row are separate writes, so a
 *   visible reservation beyond the record must raise the watermark;
 * - reserve_time has only SECOND precision, so the pending-create lookup must select the
 *   NEWEST identity first (a same-second tombstone of id N used to sort before N+1's
 *   reservation).
 */
public class SPMRound45SafetyTest {

    private static RuntimeException buildFails(String bindSql, String planSql) {
        return Assertions.assertThrows(RuntimeException.class,
                () -> new SPMPlanner().buildBaseline(bindSql, planSql));
    }

    // ==================== relation occurrences ====================

    /** A plan may not read FEWER relations than the bind text. */
    @Test
    public void testPlanOmittingABindRelationIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT t.k FROM t JOIN u ON t.k = u.k",
                "SELECT t.k FROM t");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("does not read"),
                failure.getMessage());
    }

    /** The second occurrence of a self-joined table cannot collapse into one scan. */
    @Test
    public void testPlanOmittingASelfJoinOccurrenceIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT a.k FROM t a JOIN t b ON a.k = b.k",
                "SELECT a.k FROM t a");
        Assertions.assertTrue(failure.getMessage() != null
                        && (failure.getMessage().contains("scan selectors")
                                || failure.getMessage().contains("join")),
                failure.getMessage());
    }

    /** Renaming a relation alias is rejected: the join / filter texts resolve through it. */
    @Test
    public void testRelationAliasRenameIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT k FROM t a", "SELECT k FROM t x");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("alias"),
                failure.getMessage());
    }

    // ==================== joins: type and ON condition ====================

    /** An unmatched t row survives a LEFT join but is dropped by an INNER one. */
    @Test
    public void testChangedJoinTypeIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT t.k FROM t LEFT JOIN u ON t.k = u.k",
                "SELECT t.k FROM t JOIN u ON t.k = u.k");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("join type"),
                failure.getMessage());
    }

    /** The same join type with another ON condition pairs row sets the caller never paired. */
    @Test
    public void testChangedJoinConditionIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT t.k FROM t JOIN u ON t.k = u.k",
                "SELECT t.k FROM t JOIN u ON t.k = u.v");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("ON condition"),
                failure.getMessage());
    }

    // ==================== row-forming operators ====================

    /** SELECT DISTINCT bound to a plain projection: t={1,1} yields one row vs two. */
    @Test
    public void testDroppedDistinctIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT DISTINCT k FROM t", "SELECT k FROM t");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("DISTINCT"),
                failure.getMessage());
    }

    /** A changed GROUP BY key changes which groups the replay returns. */
    @Test
    public void testChangedGroupByKeyIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT v, count(*) FROM t GROUP BY v",
                "SELECT v, count(*) FROM t GROUP BY k");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("GROUP BY"),
                failure.getMessage());
    }

    /** UNION collapses duplicate rows, UNION ALL keeps them. */
    @Test
    public void testUnionAllVersusUnionIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT k FROM t1 UNION SELECT k FROM t2",
                "SELECT k FROM t1 UNION ALL SELECT k FROM t2");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("set operation"),
                failure.getMessage());
    }

    /** A changed window function beneath an outer projection changes the replayed value. */
    @Test
    public void testChangedWindowFunctionIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT k FROM (SELECT k, row_number() OVER (PARTITION BY v) AS rn FROM t) x",
                "SELECT k FROM (SELECT k, row_number() OVER (PARTITION BY k) AS rn FROM t) x");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("PARTITION BY v"),
                failure.getMessage());
    }

    /**
     * The WITH definitions are the CTE node's AUXILIARY plans (they are not regular
     * inputs): a plan whose CTE body carries a cap / sort the bind body has not changes
     * every consumer's rows and must be compared too.
     */
    @Test
    public void testChangedCteBodyIsRejected() {
        RuntimeException failure = buildFails(
                "WITH c AS (SELECT v FROM t) SELECT v FROM c ORDER BY v LIMIT 1",
                "WITH c AS (SELECT v FROM t ORDER BY v LIMIT 1)"
                        + " SELECT v FROM c ORDER BY v LIMIT 1");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("auxiliary plan"),
                failure.getMessage());
    }

    // ==================== filters: equality and attachment ====================

    /** A plan-ADDED predicate silently drops the caller's NULL rows. */
    @Test
    public void testPlanAddedFilterIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT k FROM t", "SELECT k FROM t WHERE k IS NOT NULL");
        Assertions.assertTrue(failure.getMessage() != null
                        && (failure.getMessage().contains("row filter")
                                || failure.getMessage().contains("row-filter")),
                failure.getMessage());
    }

    /** The SAME predicate moved after a LEFT JOIN into its ON condition skips the filter. */
    @Test
    public void testFilterMovedIntoJoinOnIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT t.k FROM t LEFT JOIN u ON t.k = u.k WHERE u.v IS NOT NULL",
                "SELECT t.k FROM t LEFT JOIN u ON t.k = u.k AND u.v IS NOT NULL");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("filter"),
                failure.getMessage());
    }

    /** An equal-shaped plan whose predicate tests ANOTHER input is not the same filter. */
    @Test
    public void testFilterOnAnotherInputIsRejected() {
        RuntimeException failure = buildFails(
                "SELECT a.k FROM t a JOIN t b ON a.k = b.k WHERE b.v IS NOT NULL",
                "SELECT a.k FROM t a JOIN t b ON a.k = b.k WHERE a.v IS NOT NULL");
        Assertions.assertTrue(failure.getMessage() != null
                        && failure.getMessage().contains("filter"),
                failure.getMessage());
    }

    // ==================== equivalent trees stay accepted ====================

    /** Case / formatting / LABEL differences with an identical topology stay accepted. */
    @Test
    public void testEquivalentTreeWithAnotherLabelStaysAccepted() throws Exception {
        BaselinePlan baseline = new SPMPlanner().buildBaseline(
                "SELECT k FROM t ORDER BY k LIMIT 3",
                "SELECT k AS kk FROM t ORDER BY k LIMIT 3");
        Assertions.assertEquals("SELECT k FROM t ORDER BY k LIMIT 3", baseline.getBindSql());
    }

    // ==================== the id watermark confirmation ====================

    /**
     * A record N-1 must not hide a reservation N that is already VISIBLE: handing N to
     * another key collides with the identity whose baseline row may still publish.
     */
    @Test
    public void testStaleCompactWatermarkIsConfirmedAgainstTheSequenceTail() {
        BaselineManager.hwmRecordReadForTest = () -> 7L;
        try {
            BaselineManager.seqTailReadForTest = () -> 9L;
            Assertions.assertEquals(9L, BaselineManager.compactIdWatermarkForTest(),
                    "the visible reservation must raise the watermark");
            BaselineManager.seqTailReadForTest = () -> 0L;
            Assertions.assertEquals(7L, BaselineManager.compactIdWatermarkForTest(),
                    "without a newer reservation the record decides");
            BaselineManager.hwmRecordReadForTest = () -> 0L;
            Assertions.assertEquals(0L, BaselineManager.compactIdWatermarkForTest(),
                    "no record (pre-upgrade cluster): the seam answers zero");
        } finally {
            BaselineManager.hwmRecordReadForTest = null;
            BaselineManager.seqTailReadForTest = null;
        }
    }

    /**
     * reserve_time has only SECOND precision: the pending-create lookup must select the
     * NEWEST identity (highest last_id) FIRST, with the tombstone / marker priority applied
     * within that identity - N's same-second tombstone used to sort before N+1's
     * reservation, so a retry read the key as resolved and skipped N+1's pending fence.
     */
    @Test
    public void testPendingLookupOrdersTheNewestIdentityFirst() {
        String sql = BaselineManager.pendingSeqLookupSqlForTest();
        String orderBy = sql.substring(sql.lastIndexOf("ORDER BY"));
        Assertions.assertTrue(orderBy.startsWith(
                        "ORDER BY `last_id` DESC, `reserve_time` DESC,"),
                "the NEWEST identity must win first: " + orderBy);
        Assertions.assertTrue(orderBy.contains("`dropped` DESC, `unconfirmed` DESC"),
                "within one identity the tombstone / marker priority stays: " + orderBy);
    }
}
