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

package org.apache.doris.connector.iceberg;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * FIX-COUNT-NPE (upstream 32a2651f66b, #64648) — pins that
 * {@link IcebergScanPlanProvider#getCountFromSummary} is null-safe.
 *
 * <p>WHY: the COUNT(*)-pushdown row count is read from the iceberg snapshot summary's {@code total-records}
 * / {@code total-position-deletes} / {@code total-equality-deletes}. A compaction / replace / overwrite
 * snapshot can OMIT one of those counters, and the pre-fix code (a faithful hand-port of the legacy,
 * null-unsafe {@code IcebergScanNode.getCountFromSnapshot}) NPE-d on {@code summary.get(...).equals("0")}
 * / {@code Long.parseLong(null)} — crashing the whole query instead of just declining the pushdown. The fix
 * returns the {@code -1} "not pushable / unknown" sentinel (callers gate on {@code >= 0}) when any counter
 * is absent. This is the connector-module analog of fe-core {@code IcebergCountPushDownTest}; the SPI
 * migration copied the pre-fix logic, so fe-core carrying the fix did not protect the live path here.
 */
public class IcebergCountFromSummaryTest {

    // The three iceberg snapshot-summary counter keys (org.apache.iceberg.SnapshotSummary constants).
    private static final String TOTAL_EQUALITY_DELETES = "total-equality-deletes";
    private static final String TOTAL_POSITION_DELETES = "total-position-deletes";
    private static final String TOTAL_RECORDS = "total-records";

    /** Build a snapshot summary; a {@code null} arg OMITS that key — the exact absence the fix guards. */
    private static Map<String, String> summary(String equalityDeletes, String positionDeletes,
            String totalRecords) {
        Map<String, String> m = new HashMap<>();
        if (equalityDeletes != null) {
            m.put(TOTAL_EQUALITY_DELETES, equalityDeletes);
        }
        if (positionDeletes != null) {
            m.put(TOTAL_POSITION_DELETES, positionDeletes);
        }
        if (totalRecords != null) {
            m.put(TOTAL_RECORDS, totalRecords);
        }
        return m;
    }

    @Test
    public void missingAnyCounterReturnsMinusOneInsteadOfNpe() {
        // The regression: pre-fix each of these threw NPE (get(...).equals / parseLong(null)). Assert for
        // BOTH dangling-delete flag values so the guard is proven independent of that branch.
        for (boolean ignore : new boolean[] {false, true}) {
            Assertions.assertEquals(-1L,
                    IcebergScanPlanProvider.getCountFromSummary(summary(null, "0", "100"), ignore),
                    "absent total-equality-deletes must decline pushdown, not NPE");
            Assertions.assertEquals(-1L,
                    IcebergScanPlanProvider.getCountFromSummary(summary("0", null, "100"), ignore),
                    "absent total-position-deletes must decline pushdown, not NPE");
            Assertions.assertEquals(-1L,
                    IcebergScanPlanProvider.getCountFromSummary(summary("0", "0", null), ignore),
                    "absent total-records must decline pushdown, not NPE");
            Assertions.assertEquals(-1L,
                    IcebergScanPlanProvider.getCountFromSummary(Collections.emptyMap(), ignore),
                    "empty summary must decline pushdown, not NPE");
        }
    }

    @Test
    public void noDeletesPushesTotalRecords() {
        Assertions.assertEquals(100L,
                IcebergScanPlanProvider.getCountFromSummary(summary("0", "0", "100"), false));
    }

    @Test
    public void equalityDeletesNotPushable() {
        // Equality deletes re-project at read time; the summary cannot net them out -> not pushable.
        Assertions.assertEquals(-1L,
                IcebergScanPlanProvider.getCountFromSummary(summary("3", "0", "100"), false));
        Assertions.assertEquals(-1L,
                IcebergScanPlanProvider.getCountFromSummary(summary("3", "0", "100"), true));
    }

    @Test
    public void positionDeletesHonorDanglingFlag() {
        // ignore dangling deletes -> netted count (total - deletes) is pushable; otherwise not pushable.
        Assertions.assertEquals(90L,
                IcebergScanPlanProvider.getCountFromSummary(summary("0", "10", "100"), true));
        Assertions.assertEquals(-1L,
                IcebergScanPlanProvider.getCountFromSummary(summary("0", "10", "100"), false));
    }

    @Test
    public void allRowsDeletedNetsToZeroNotSentinel() {
        // 100 records, 100 position deletes, ignore=true -> genuine 0. Must NOT collapse to the -1 sentinel
        // (a real count of 0 is still a valid, pushable answer).
        Assertions.assertEquals(0L,
                IcebergScanPlanProvider.getCountFromSummary(summary("0", "100", "100"), true));
    }
}
