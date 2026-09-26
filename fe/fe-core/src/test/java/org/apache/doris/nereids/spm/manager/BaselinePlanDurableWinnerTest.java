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
     * A delete failure that did NOT commit rolls the freshly inserted row back and keeps
     * the old-status version - the update reports the failure and memory reverts.
     */
    @Test
    public void testStatusUpdateRollsBackUncommittedDeleteFailure() {
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
            Assertions.assertThrows(RuntimeException.class,
                    () -> manager.updateStatus(id, BaselineStatus.DISABLED));
            Assertions.assertEquals(BaselineStatus.ENABLED,
                    manager.getBaseline(id).getStatus(),
                    "the in-memory flip must be reverted");
            Assertions.assertTrue(durable.contains(BaselineStatus.ENABLED),
                    "the old-status row must stay durable");
            Assertions.assertFalse(durable.contains(BaselineStatus.DISABLED),
                    "the rollback must delete the freshly inserted row");
        } finally {
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }
}
