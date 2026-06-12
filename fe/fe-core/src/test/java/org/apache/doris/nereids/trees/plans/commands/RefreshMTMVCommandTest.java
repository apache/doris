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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

/**
 * Tests for REFRESH MATERIALIZED VIEW command parsing with INCREMENTAL/PARTITIONS modes.
 */
public class RefreshMTMVCommandTest {

    private final NereidsParser parser = new NereidsParser();

    private RefreshMTMVInfo extractRefreshInfo(String sql) throws Exception {
        StatementBase statementBase = parser.parseSQL(sql).get(0);
        Assertions.assertTrue(statementBase instanceof LogicalPlanAdapter,
                "Parsed statement should be LogicalPlanAdapter");
        LogicalPlan plan = ((LogicalPlanAdapter) statementBase).getLogicalPlan();
        Assertions.assertTrue(plan instanceof RefreshMTMVCommand,
                "Parsed plan should be RefreshMTMVCommand");
        Field field = RefreshMTMVCommand.class.getDeclaredField("refreshMTMVInfo");
        field.setAccessible(true);
        return (RefreshMTMVInfo) field.get(plan);
    }

    private void assertParseFails(String sql) {
        try {
            parser.parseSQL(sql);
            Assertions.fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }
    }

    // TC-8-1: REFRESH MV ... INCREMENTAL can be parsed
    @Test
    public void testParseRefreshIncremental() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        Assertions.assertEquals(RefreshMode.INCREMENTAL, info.getRefreshMode());
        Assertions.assertTrue(info.getPartitions().isEmpty());
    }

    // TC-8-2: REFRESH MV ... AUTO still parseable
    @Test
    public void testParseRefreshAuto() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 AUTO");
        Assertions.assertEquals(RefreshMode.AUTO, info.getRefreshMode());
    }

    // TC-8-3: REFRESH MV ... COMPLETE still parseable
    @Test
    public void testParseRefreshComplete() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 COMPLETE");
        Assertions.assertEquals(RefreshMode.COMPLETE, info.getRefreshMode());
        Assertions.assertTrue(info.isComplete());
    }

    // TC-8-4: REFRESH MV ... PARTITIONS parsed as new refresh mode
    @Test
    public void testParseRefreshPartitionsMode() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 PARTITIONS");
        Assertions.assertEquals(RefreshMode.PARTITIONS, info.getRefreshMode());
        Assertions.assertTrue(info.getPartitions().isEmpty());
    }

    // TC-8-extra: REFRESH MV without mode should be rejected (syntax error)
    @Test
    public void testParseRefreshWithoutModeFails() throws Exception {
        assertParseFails("REFRESH MATERIALIZED VIEW db1.mv1");
    }

    // TC-8-extra: REFRESH MV with old-style partitionSpec still works
    @Test
    public void testParseRefreshWithPartitionSpec() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 PARTITIONS (p1, p2)");
        Assertions.assertEquals(RefreshMode.PARTITIONS, info.getRefreshMode());
        Assertions.assertEquals(2, info.getPartitions().size());
        Assertions.assertFalse(info.allowFallback());
    }

    @Test
    public void testParseRefreshWithFallback() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL FALLBACK");
        Assertions.assertEquals(RefreshMode.INCREMENTAL, info.getRefreshMode());
        Assertions.assertTrue(info.allowFallback());
    }

    @Test
    public void testParseRefreshPartitionsFallback() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 PARTITIONS FALLBACK");
        Assertions.assertEquals(RefreshMode.PARTITIONS, info.getRefreshMode());
        Assertions.assertTrue(info.allowFallback());
    }

    @Test
    public void testParseRefreshCompleteFallback() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 COMPLETE FALLBACK");
        Assertions.assertEquals(RefreshMode.COMPLETE, info.getRefreshMode());
        Assertions.assertTrue(info.allowFallback());
    }

    @Test
    public void testParsePartitionSpecFallbackFails() {
        assertParseFails("REFRESH MATERIALIZED VIEW db1.mv1 PARTITIONS (p1) FALLBACK");
    }

    // TC-8-extra: isComplete() backward compat — only true for COMPLETE mode
    @Test
    public void testIsCompleteBackwardCompat() throws Exception {
        RefreshMTMVInfo completeInfo = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 COMPLETE");
        Assertions.assertTrue(completeInfo.isComplete());

        RefreshMTMVInfo incrementalInfo = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        Assertions.assertFalse(incrementalInfo.isComplete());

        RefreshMTMVInfo autoInfo = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 AUTO");
        Assertions.assertFalse(autoInfo.isComplete());
    }
}
