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

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Verifies that capture uses the bound scan rather than all partitions of its table. */
class SPMMetadataCaptureTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("spm_metadata_db");
        connectContext.setDatabase("spm_metadata_db");
        createTable("CREATE TABLE t (k INT, v INT) DUPLICATE KEY(k)"
                + " PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN ('10'),"
                + " PARTITION p2 VALUES LESS THAN ('20'), PARTITION p3 VALUES LESS THAN ('30'))"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        connectContext.getSessionVariable().setEnableSpmFallback(false);
    }

    @Test
    void testCaptureOnlySnapshotsExplicitPartitionsWhenFreezing() throws Exception {
        Assertions.assertTrue(capture("SELECT k FROM t WHERE k = 5", true).getPartitionNames().isEmpty());
        String pinned = "SELECT k FROM t PARTITION(p1, p2) WHERE k = 5";
        Map<Long, Map<Long, SPMPlanTreeSupport.PartitionPin>> pins = capture(pinned, true).getPartitionNames();
        Assertions.assertEquals(1, pins.size());
        Assertions.assertEquals(Set.of("p1", "p2"), pins.values().stream()
                .flatMap(map -> map.values().stream())
                .map(SPMPlanTreeSupport.PartitionPin::getName)
                .collect(Collectors.toSet()));
        Assertions.assertTrue(capture(pinned, false).getPartitionNames().isEmpty());

        String cte = "WITH c AS (SELECT k FROM t PARTITION(p2)) SELECT k FROM c";
        Assertions.assertEquals(Set.of("p2"), capture(cte, true).getPartitionNames().values().stream()
                .flatMap(map -> map.values().stream())
                .map(SPMPlanTreeSupport.PartitionPin::getName)
                .collect(Collectors.toSet()));
    }

    @Test
    void testHiddenColumnVisibilityChangesFingerprintWithoutDdl() throws Exception {
        boolean original = connectContext.getSessionVariable().showHiddenColumns();
        try {
            connectContext.getSessionVariable().setShowHiddenColumns(false);
            Map<String, String> visible = capture("SELECT k FROM t", true).getTableFingerprints();
            connectContext.getSessionVariable().setShowHiddenColumns(true);
            Map<String, String> hidden = capture("SELECT k FROM t", true).getTableFingerprints();
            Assertions.assertNotEquals(visible.get("t"), hidden.get("t"));
            Assertions.assertTrue(hidden.get("t").endsWith(":hidden"));
        } finally {
            connectContext.getSessionVariable().setShowHiddenColumns(original);
        }
    }

    private SPMOptimizer.OptimizeResult capture(String sql, boolean freeze) throws Exception {
        createStatementCtx(sql);
        return SPMOptimizer.optimize(connectContext, (LogicalPlan) new NereidsParser().parseSingle(sql), sql, freeze);
    }
}
