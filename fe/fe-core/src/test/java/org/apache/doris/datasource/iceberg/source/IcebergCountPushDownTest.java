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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.datasource.iceberg.IcebergUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

public class IcebergCountPushDownTest {

    private static Map<String, String> summary(String equalityDeletes, String positionDeletes, String totalRecords) {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (equalityDeletes != null) {
            builder.put(IcebergUtils.TOTAL_EQUALITY_DELETES, equalityDeletes);
        }
        if (positionDeletes != null) {
            builder.put(IcebergUtils.TOTAL_POSITION_DELETES, positionDeletes);
        }
        if (totalRecords != null) {
            builder.put(IcebergUtils.TOTAL_RECORDS, totalRecords);
        }
        return builder.build();
    }

    @Test
    public void testMissingCounterFallsBackToScan() {
        // Snapshots written by compaction/replace (and some writers) may omit
        // total-* counters. The pushdown previously NPE'd on the missing key;
        // it must now fall back to a normal scan (return -1).
        Assertions.assertEquals(-1L, IcebergUtils.getCountFromSummary(summary(null, "0", "100"), false));
        Assertions.assertEquals(-1L, IcebergUtils.getCountFromSummary(summary("0", null, "100"), false));
        Assertions.assertEquals(-1L, IcebergUtils.getCountFromSummary(summary("0", "0", null), false));
        Assertions.assertEquals(-1L, IcebergUtils.getCountFromSummary(Collections.emptyMap(), false));
    }

    @Test
    public void testUtilityMissingCounterReturnsUnknownCount() {
        Assertions.assertEquals(-1L, IcebergUtils.getCountFromSummary(summary("0", null, "100"), true));
    }

    @Test
    public void testNoDeletesPushesDownTotalRecords() {
        Assertions.assertEquals(100L, IcebergUtils.getCountFromSummary(summary("0", "0", "100"), false));
    }

    @Test
    public void testEqualityDeletesCannotPushDown() {
        Assertions.assertEquals(-1L, IcebergUtils.getCountFromSummary(summary("3", "0", "100"), false));
    }

    @Test
    public void testPositionDeletesRespectIgnoreDangling() {
        // ignoreDanglingDelete = true -> total-records minus position-deletes
        Assertions.assertEquals(90L, IcebergUtils.getCountFromSummary(summary("0", "10", "100"), true));
        // ignoreDanglingDelete = false -> cannot push down (fall back to scan)
        Assertions.assertEquals(-1L, IcebergUtils.getCountFromSummary(summary("0", "10", "100"), false));
    }
}
