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

package org.apache.doris.nereids.spm.manager;

import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.BaselineStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;

/**
 * Deterministic resolution of DUPLICATE durable rows (same baseline id).
 *
 * A three-step ALTER (insert new status row, delete the old status row, compensate on
 * failure) can leave both an ENABLED and a DISABLED row for one id in the
 * DUPLICATE KEY table: SELECT_ALL_SQL has no ordering, so snapshot loading / master
 * refresh must pick ONE deterministic winner instead of "whichever row is read last".
 * The rule: the largest update_time wins; at equal time the DISABLED row wins (the
 * user's last intent was to disable); a full tie keeps the first read row (stable).
 */
public class BaselinePlanDurableWinnerTest {

    private static BaselinePlan row(long id, long updateTime, BaselineStatus status) {
        BaselinePlan plan = new BaselinePlan();
        plan.setId(id);
        plan.setUpdateTime(updateTime);
        plan.setStatus(status);
        return plan;
    }

    @Test
    public void testNewerRowWins() {
        BaselinePlan oldRow = row(1, 100, BaselineStatus.DISABLED);
        BaselinePlan newRow = row(1, 200, BaselineStatus.ENABLED);
        Assertions.assertSame(newRow, BaselineManager.pickDurableWinner(oldRow, newRow),
                "the more recent durable row must win");
        Assertions.assertSame(newRow, BaselineManager.pickDurableWinner(newRow, oldRow),
                "the winner must not depend on the order the rows were read in");
    }

    @Test
    public void testDisabledWinsAtEqualTime() {
        // ENABLED / DISABLED rows of the failed three-step ALTER share the update time:
        // the disable intent must win, otherwise a restart can re-enable the baseline
        // the user tried to disable (and ALTER already reported failure for).
        BaselinePlan enabled = row(7, 500, BaselineStatus.ENABLED);
        BaselinePlan disabled = row(7, 500, BaselineStatus.DISABLED);
        Assertions.assertSame(disabled, BaselineManager.pickDurableWinner(enabled, disabled));
        Assertions.assertSame(disabled, BaselineManager.pickDurableWinner(disabled, enabled));
    }

    @Test
    public void testFullTieKeepsFirstReadRow() {
        BaselinePlan first = row(9, 300, BaselineStatus.ENABLED);
        BaselinePlan second = row(9, 300, BaselineStatus.ENABLED);
        Assertions.assertSame(first, BaselineManager.pickDurableWinner(first, second),
                "a full tie must be stable: the first row read stays the winner");
        Assertions.assertSame(second, BaselineManager.pickDurableWinner(second, first));
    }

    /**
     * The persisted create_time / update_time are zone-free DATETIME, so they
     * must be written and read as ABSOLUTE (UTC) instants. Rendering them in the host zone
     * made the stored value depend on the WRITER: an ENABLED row written at 12:00 UTC on a
     * UTC FE and the newer DISABLED row at 12:01 UTC on a UTC-8 successor stored "12:00"
     * and "04:01", and the reader (any zone) ranked the ENABLED row higher - an
     * interrupted status flip was then undone by the recovery that picks the later row.
     * A DST fall-back on one FE inverted the order of two writes the same way.
     */
    @Test
    public void testUpdateTimeIsAbsoluteAcrossHostZones() {
        java.util.TimeZone original = java.util.TimeZone.getDefault();
        try {
            // 2026-11-01 12:00 UTC, i.e. inside the Americas fall-back day
            long instant = java.time.LocalDateTime.of(2026, 11, 1, 12, 0)
                    .toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));
            Assertions.assertEquals("2026-11-01 12:00:00", BaselineManager.toTs(instant));
            Assertions.assertEquals(instant, BaselineManager.fromTs("2026-11-01 12:00:00"));

            java.util.TimeZone.setDefault(
                    java.util.TimeZone.getTimeZone("America/Los_Angeles"));
            Assertions.assertEquals("2026-11-01 12:00:00", BaselineManager.toTs(instant),
                    "the SAME instant must render identically on a host in another zone");
            Assertions.assertEquals(instant,
                    BaselineManager.fromTs(BaselineManager.toTs(instant)),
                    "and it must round-trip to the same instant");

            // the recovery rule over two rows written across the fall-back: the later
            // INSTANT (09:10Z, 01:10 PST) must beat the earlier one (08:30Z, 01:30 PDT),
            // which a host-local rendering would have ordered the other way round
            long beforeFallBack = java.time.LocalDateTime.of(2026, 11, 1, 8, 30)
                    .toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
            long afterFallBack = java.time.LocalDateTime.of(2026, 11, 1, 9, 10)
                    .toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
            BaselinePlan enabled = row(11, BaselineManager.fromTs(
                    BaselineManager.toTs(beforeFallBack)), BaselineStatus.ENABLED);
            BaselinePlan disabled = row(11, BaselineManager.fromTs(
                    BaselineManager.toTs(afterFallBack)), BaselineStatus.DISABLED);
            Assertions.assertSame(disabled,
                    BaselineManager.pickDurableWinner(enabled, disabled),
                    "the later status change must win regardless of the host zone");
            Assertions.assertSame(disabled,
                    BaselineManager.pickDurableWinner(disabled, enabled));
        } finally {
            java.util.TimeZone.setDefault(original);
        }
    }

    @Test
    public void testWinnerIsIdempotentAcrossRepeatedFolds() {
        // snapshot loading folds the candidate rows pairwise: folding the winner again
        // must never flip it
        BaselinePlan enabled = row(3, 400, BaselineStatus.ENABLED);
        BaselinePlan disabled = row(3, 400, BaselineStatus.DISABLED);
        BaselinePlan winner = BaselineManager.pickDurableWinner(enabled, disabled);
        Assertions.assertSame(winner, BaselineManager.pickDurableWinner(winner, enabled));
        Assertions.assertSame(winner, BaselineManager.pickDurableWinner(winner, disabled));
        Assertions.assertSame(winner, BaselineManager.pickDurableWinner(winner, winner));
    }

    /**
     * Fully tied timestamps + status must still resolve DETERMINISTICALLY
     * when the CONTENT differs. Two masters can leave two DIFFERENT rows of one id with
     * the same stored second (DATETIME has second precision), and an arbitrary pick made
     * refresh / restart / SHOW / the paginated read disagree on which row is
     * authoritative. The content tie-breakers are total and order-independent.
     */
    @Test
    public void testContentTieBreakIsOrderIndependent() {
        BaselinePlan digestZ = row(9, 300, BaselineStatus.ENABLED);
        digestZ.setBindSqlDigest("z-digest");
        digestZ.setPlanSql("plan-a");
        digestZ.setBindSql("bind-a");
        BaselinePlan digestA = row(9, 300, BaselineStatus.ENABLED);
        digestA.setBindSqlDigest("a-digest");
        digestA.setPlanSql("plan-z");
        digestA.setBindSql("bind-z");
        Assertions.assertSame(digestA, BaselineManager.pickDurableWinner(digestZ, digestA),
                "the lexicographically smaller digest wins regardless of the read order");
        Assertions.assertSame(digestA, BaselineManager.pickDurableWinner(digestA, digestZ));

        // same digest: the PLAN text breaks the tie (again order-independent)
        BaselinePlan planB = row(9, 300, BaselineStatus.ENABLED);
        planB.setBindSqlDigest("same-digest");
        planB.setPlanSql("plan-b");
        BaselinePlan planA = row(9, 300, BaselineStatus.ENABLED);
        planA.setBindSqlDigest("same-digest");
        planA.setPlanSql("plan-a");
        Assertions.assertSame(planA, BaselineManager.pickDurableWinner(planB, planA),
                "the plan text must break a digest tie");
        Assertions.assertSame(planA, BaselineManager.pickDurableWinner(planA, planB));

        // ... then the BIND text; a fully identical pair stays stable on the first read
        BaselinePlan bindB = row(9, 300, BaselineStatus.ENABLED);
        bindB.setBindSqlDigest("same-digest");
        bindB.setPlanSql("same-plan");
        bindB.setBindSql("bind-b");
        BaselinePlan bindA = row(9, 300, BaselineStatus.ENABLED);
        bindA.setBindSqlDigest("same-digest");
        bindA.setPlanSql("same-plan");
        bindA.setBindSql("bind-a");
        Assertions.assertSame(bindA, BaselineManager.pickDurableWinner(bindB, bindA),
                "the bind text must break a plan tie");
        Assertions.assertSame(bindA, BaselineManager.pickDurableWinner(bindA, bindB));
        Assertions.assertSame(bindA, BaselineManager.pickDurableWinner(bindA, bindA),
                "a fully identical pair stays interchangeable");
    }

    /** Seeds one ENABLED in-memory baseline and returns its id. */
    private static long seedEnabledBaseline(BaselineManager manager) {
        BaselinePlan plan = new BaselinePlan();
        plan.setBindSql("select 1");
        plan.setBindSqlDigest("d");
        plan.setBindSqlHash(1);
        plan.setPlanSql("select 1");
        plan.setStatus(BaselineStatus.ENABLED);
        long id = manager.createBaseline(plan);
        Assertions.assertTrue(id > 0);
        return id;
    }

    /**
     * An ambiguous status-change failure - the old-row DELETE committed but reported
     * KV_TXN_MAYBE_COMMITTED - must be reconciled, not blindly compensated: deleting the
     * freshly inserted row would leave NO durable baseline for the next refresh / restart.
     */
    @Test
    public void testStatusUpdateReconcilesCommittedDeleteFailure() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        long id = seedEnabledBaseline(manager);

        final EnumSet<BaselineStatus> durable = EnumSet.of(BaselineStatus.ENABLED);
        BaselineManager.statusProtocolStoreForTest = new BaselineManager.StatusProtocolStoreForTest() {
            @Override
            public void insert(BaselinePlan inserted) {
                durable.add(inserted.getStatus());
            }

            @Override
            public void deleteByIdAndStatus(long rowId, BaselineStatus status) {
                if (status == BaselineStatus.ENABLED) {
                    // the DELETE commits, then reports the ambiguous error
                    durable.remove(BaselineStatus.ENABLED);
                    throw new RuntimeException("KV_TXN_MAYBE_COMMITTED");
                }
                durable.remove(status);
            }

            @Override
            public int countByIdAndStatus(long rowId, BaselineStatus status) {
                return durable.contains(status) ? 1 : 0;
            }
        };
        try {
            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED),
                    "a committed old-row delete must be reconciled as success");
            Assertions.assertEquals(BaselineStatus.DISABLED,
                    manager.getBaseline(id).getStatus());
            Assertions.assertTrue(durable.contains(BaselineStatus.DISABLED),
                    "the new-status row must survive as the durable version");
            Assertions.assertFalse(durable.contains(BaselineStatus.ENABLED));
        } finally {
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * A delete failure that did NOT commit keeps BOTH versions, and the CONFIRMED new row
     * decides the outcome ( semantics, refined by): the failed
     * ALTER must not compensate the freshly inserted row away (when the delete had
     * actually committed and only its PUBLICATION lagged, deleting the new row would
     * leave the baseline with no durable version at all), and because the new row carries
     * a strictly later updateTime it IS the durable winner - the cache follows it instead
     * of keeping the OLD status replayable until the next refresh. Duplicate rows are
     * resolved deterministically by every load path (pickDurableWinner).
     */
    @Test
    public void testStatusUpdateKeepsBothRowsWhenTheDeleteOutcomeIsUnknown() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        long id = seedEnabledBaseline(manager);

        final EnumSet<BaselineStatus> durable = EnumSet.of(BaselineStatus.ENABLED);
        BaselineManager.statusProtocolStoreForTest = new BaselineManager.StatusProtocolStoreForTest() {
            @Override
            public void insert(BaselinePlan inserted) {
                durable.add(inserted.getStatus());
            }

            @Override
            public void deleteByIdAndStatus(long rowId, BaselineStatus status) {
                if (status == BaselineStatus.ENABLED) {
                    // the DELETE did NOT commit: the old row is still durable
                    throw new RuntimeException("KV_TXN_MAYBE_COMMITTED");
                }
                durable.remove(status);
            }

            @Override
            public int countByIdAndStatus(long rowId, BaselineStatus status) {
                return durable.contains(status) ? 1 : 0;
            }
        };
        try {
            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED),
                    "the confirmed new-status row is the durable winner: the ALTER landed");
            Assertions.assertEquals(BaselineStatus.DISABLED,
                    manager.getBaseline(id).getStatus(),
                    "the cache must follow the durable winner - keeping the OLD status let"
                            + " queries replay a baseline the durable table already flipped");
            Assertions.assertTrue(durable.contains(BaselineStatus.ENABLED),
                    "the old-status row must stay durable");
            Assertions.assertTrue(durable.contains(BaselineStatus.DISABLED),
                    "an UNKNOWN delete outcome must keep the new-status row as well - a"
                            + " rollback would leave NOTHING behind when the delete had"
                            + " actually committed and only its publication lagged");
        } finally {
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }
}
