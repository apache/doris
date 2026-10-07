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
 * The relation-payload checks and the bounded identity read:
 *
 * - an inline VALUES relation carries its row BOUNDARIES: two statements with
 *   the same width and the same cell texts but a different number of rows are
 *   different queries (the frozen plan emits another row count), and the digest
 *   masks all of it;
 * - the durable pending-create read must seek the compact per-identity key
 *   prefix instead of scanning the append-only history of every identity ever
 *   created, while keeping the newest-identity-first ordering.
 */
public class SPMRound47SafetyTest {

    private static RuntimeException buildFails(String bindSql, String planSql) {
        return Assertions.assertThrows(RuntimeException.class,
                () -> new SPMPlanner().buildBaseline(bindSql, planSql));
    }

    /** The row boundaries of an inline VALUES relation are part of the query. */
    @Test
    public void testValuesRowBoundariesArePartOfThePlanIdentity() {
        RuntimeException failure = buildFails(
                "SELECT a, b FROM (VALUES (1, 2), (3, 4)) t(a, b)",
                "SELECT a, b FROM (VALUES (1, 2)) t(a, b)");
        Assertions.assertNotNull(failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("VALUES")
                        || failure.getMessage().contains("align"),
                "the rejection must describe the VALUES rows: " + failure.getMessage());
    }

    /** The identity read is bounded by the compact key and keeps the new order. */
    @Test
    public void testPendingLookupIsBoundedByTheIdentityKey() {
        String sql = BaselineManager.pendingSeqLookupSqlForTest();
        Assertions.assertTrue(sql.contains("`id` = ${compactId}"),
                "the identity read must seek the compact key prefix: " + sql);
        String orderBy = sql.substring(sql.lastIndexOf("ORDER BY"));
        Assertions.assertTrue(orderBy.startsWith(
                        "ORDER BY `last_id` DESC, `reserve_time` DESC,"),
                "the NEWEST identity must still win first: " + orderBy);
        Assertions.assertTrue(orderBy.contains("`dropped` DESC, `unconfirmed` DESC"),
                "the tombstone / marker priority stays within one identity: " + orderBy);
    }
}
