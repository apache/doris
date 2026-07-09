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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Tests that timeTravelTimestampMs is preserved through ALL with* optimizer rewrites.
 *
 * The time travel timestamp is set during FE analysis (BindRelation) and must survive
 * the full optimizer pipeline — partition pruning, tablet pruning, pre-agg status changes,
 * MV rewrite, virtual column injection — before reaching PhysicalPlanTranslator.
 *
 * Each with* method is tested individually (regression guard for the 6-method bug where
 * post-construction assignment was forgotten) and also in a chained sequence that exercises
 * the full optimizer path.
 */
public class LogicalOlapScanTimeTravelTest {

    private static OlapTable testTable;
    private static final IdGenerator<RelationId> ID_GEN = RelationId.createGenerator();
    private static final long TS = 1_700_000_000_000L;

    @BeforeAll
    public static void setup() {
        testTable = new OlapTable(100L, "orders",
                ImmutableList.of(
                        new Column("id", Type.BIGINT, true, AggregateType.NONE, "0", ""),
                        new Column("amount", Type.DOUBLE, false, AggregateType.NONE, "0", "")),
                KeysType.DUP_KEYS, new PartitionInfo(), null);
        testTable.setIndexMeta(-1, "orders", testTable.getFullSchema(),
                0, 0, (short) 1,
                org.apache.doris.thrift.TStorageType.COLUMN, KeysType.DUP_KEYS);
    }

    private static LogicalOlapScan newScan() {
        return new LogicalOlapScan(ID_GEN.getNextId(), testTable, ImmutableList.of("db"));
    }

    // -------------------------------------------------------------------------
    // Basic field behaviour
    // -------------------------------------------------------------------------

    @Test
    public void testNoTimeTravelByDefault() {
        LogicalOlapScan scan = newScan();
        Assertions.assertFalse(scan.hasTimeTravelTimestampMs(),
                "new scan must not have time travel set");
        Assertions.assertEquals(-1L, scan.getTimeTravelTimestampMs());
    }

    @Test
    public void testWithTimeTravelTimestampMs_setsAndReads() {
        LogicalOlapScan scan = newScan().withTimeTravelTimestampMs(TS);
        Assertions.assertTrue(scan.hasTimeTravelTimestampMs());
        Assertions.assertEquals(TS, scan.getTimeTravelTimestampMs());
    }

    @Test
    public void testWithTimeTravelTimestampMs_zeroIsValid() {
        // Epoch 0 is a valid historical timestamp (in the far past)
        LogicalOlapScan scan = newScan().withTimeTravelTimestampMs(0L);
        Assertions.assertTrue(scan.hasTimeTravelTimestampMs());
        Assertions.assertEquals(0L, scan.getTimeTravelTimestampMs());
    }

    @Test
    public void testWithTimeTravelTimestampMs_returnsNewInstance() {
        // withTimeTravelTimestampMs must not mutate the original
        LogicalOlapScan original = newScan();
        LogicalOlapScan withTs = original.withTimeTravelTimestampMs(TS);
        Assertions.assertFalse(original.hasTimeTravelTimestampMs(),
                "original scan must be unchanged");
        Assertions.assertTrue(withTs.hasTimeTravelTimestampMs());
    }

    // -------------------------------------------------------------------------
    // Preservation through each with* rewrite — individual regression guards.
    // Each test covers one method that previously had the missing copy bug.
    // -------------------------------------------------------------------------

    @Test
    public void testWithPreAggStatus_preservesTimeTravel() {
        LogicalOlapScan after = newScan().withTimeTravelTimestampMs(TS)
                .withPreAggStatus(PreAggStatus.off("test"));
        Assertions.assertEquals(TS, after.getTimeTravelTimestampMs(),
                "withPreAggStatus must preserve timeTravelTimestampMs");
    }

    @Test
    public void testWithSelectedPartitionIds_preservesTimeTravel() {
        LogicalOlapScan after = newScan().withTimeTravelTimestampMs(TS)
                .withSelectedPartitionIds(ImmutableList.of(1L, 2L));
        Assertions.assertEquals(TS, after.getTimeTravelTimestampMs(),
                "withSelectedPartitionIds must preserve timeTravelTimestampMs");
    }

    @Test
    public void testWithSelectedPartitionIds_withPredicate_preservesTimeTravel() {
        // Second overload: withSelectedPartitionIds(ids, hasPartitionPredicate)
        LogicalOlapScan after = newScan().withTimeTravelTimestampMs(TS)
                .withSelectedPartitionIds(ImmutableList.of(1L), true);
        Assertions.assertEquals(TS, after.getTimeTravelTimestampMs(),
                "withSelectedPartitionIds(ids, bool) must preserve timeTravelTimestampMs");
    }

    @Test
    public void testWithSelectedTabletIds_preservesTimeTravel() {
        LogicalOlapScan after = newScan().withTimeTravelTimestampMs(TS)
                .withSelectedTabletIds(ImmutableList.of(10L, 20L));
        Assertions.assertEquals(TS, after.getTimeTravelTimestampMs(),
                "withSelectedTabletIds must preserve timeTravelTimestampMs");
    }

    @Test
    public void testWithTableScanParams_preservesTimeTravel() {
        TableScanParams params = new TableScanParams("incr", ImmutableMap.of(), ImmutableList.of());
        LogicalOlapScan after = newScan().withTimeTravelTimestampMs(TS)
                .withTableScanParams(params);
        Assertions.assertEquals(TS, after.getTimeTravelTimestampMs(),
                "withTableScanParams must preserve timeTravelTimestampMs");
    }

    @Test
    public void testWithManuallySpecifiedTabletIds_preservesTimeTravel() {
        LogicalOlapScan after = newScan().withTimeTravelTimestampMs(TS)
                .withManuallySpecifiedTabletIds(ImmutableList.of(5L));
        Assertions.assertEquals(TS, after.getTimeTravelTimestampMs(),
                "withManuallySpecifiedTabletIds must preserve timeTravelTimestampMs");
    }

    @Test
    public void testWithGroupExprLogicalPropChildren_preservesTimeTravel() {
        // withGroupExprLogicalPropChildren is called on every GroupExpression memo reuse.
        // Previously the bug: it was the only with* method that used return directly
        // without the post-construction timeTravelTimestampMs copy.
        LogicalOlapScan base = newScan().withTimeTravelTimestampMs(TS);
        LogicalOlapScan after = (LogicalOlapScan) base.withGroupExprLogicalPropChildren(
                java.util.Optional.empty(),
                java.util.Optional.of(base.getLogicalProperties()),
                ImmutableList.of());
        Assertions.assertEquals(TS, after.getTimeTravelTimestampMs(),
                "withGroupExprLogicalPropChildren must preserve timeTravelTimestampMs");
    }

    @Test
    public void testWithVirtualColumns_preservesTimeTravel() {
        LogicalOlapScan after = newScan().withTimeTravelTimestampMs(TS)
                .withVirtualColumns(ImmutableList.of());
        Assertions.assertEquals(TS, after.getTimeTravelTimestampMs(),
                "withVirtualColumns must preserve timeTravelTimestampMs");
    }

    @Test
    public void testAppendVirtualColumns_preservesTimeTravel() {
        LogicalOlapScan after = newScan().withTimeTravelTimestampMs(TS)
                .appendVirtualColumns(ImmutableList.of());
        Assertions.assertEquals(TS, after.getTimeTravelTimestampMs(),
                "appendVirtualColumns must preserve timeTravelTimestampMs");
    }

    @Test
    public void testWithOperativeSlots_preservesTimeTravel() {
        LogicalOlapScan after = (LogicalOlapScan) newScan().withTimeTravelTimestampMs(TS)
                .withOperativeSlots(Collections.emptyList());
        Assertions.assertEquals(TS, after.getTimeTravelTimestampMs(),
                "withOperativeSlots must preserve timeTravelTimestampMs");
    }

    // -------------------------------------------------------------------------
    // Non-regression: normal scans (no time travel) must not be affected
    // -------------------------------------------------------------------------

    @Test
    public void testNormalScan_unaffectedByWithPreAggStatus() {
        LogicalOlapScan after = newScan().withPreAggStatus(PreAggStatus.off("no-agg"));
        Assertions.assertFalse(after.hasTimeTravelTimestampMs(),
                "non-TT scan must remain non-TT after withPreAggStatus");
        Assertions.assertEquals(-1L, after.getTimeTravelTimestampMs());
    }

    @Test
    public void testNormalScan_unaffectedByWithSelectedPartitionIds() {
        LogicalOlapScan after = newScan().withSelectedPartitionIds(ImmutableList.of(1L));
        Assertions.assertFalse(after.hasTimeTravelTimestampMs(),
                "non-TT scan must remain non-TT after partition pruning");
    }

    @Test
    public void testNormalScan_unaffectedByWithVirtualColumns() {
        LogicalOlapScan after = newScan().withVirtualColumns(ImmutableList.of());
        Assertions.assertFalse(after.hasTimeTravelTimestampMs(),
                "non-TT scan must remain non-TT after withVirtualColumns");
    }

    // -------------------------------------------------------------------------
    // Full optimizer chain — simulates the real Nereids pipeline path:
    // BindRelation → partition pruning → MV rewrite (virtual cols) → index selection
    // -------------------------------------------------------------------------

    @Test
    public void testFullOptimizerChain_preservesTimeTravel() {
        LogicalOlapScan step = newScan()
                .withTimeTravelTimestampMs(TS)
                .withPreAggStatus(PreAggStatus.off("test"))
                .withSelectedPartitionIds(ImmutableList.of(1L, 2L))
                .withSelectedTabletIds(ImmutableList.of(10L, 20L))
                .withVirtualColumns(ImmutableList.of());
        LogicalOlapScan after = (LogicalOlapScan) step.withOperativeSlots(Collections.emptyList());
        Assertions.assertEquals(TS, after.getTimeTravelTimestampMs(),
                "full optimizer chain must preserve timeTravelTimestampMs end-to-end");
    }

    @Test
    public void testFullOptimizerChain_nonTT_remainsNonTT() {
        // Verifies normal queries are not accidentally assigned a timestamp
        LogicalOlapScan after = newScan()
                .withPreAggStatus(PreAggStatus.off("test"))
                .withSelectedPartitionIds(ImmutableList.of(1L))
                .withSelectedTabletIds(ImmutableList.of(10L));
        Assertions.assertFalse(after.hasTimeTravelTimestampMs(),
                "non-TT scan must stay non-TT through the full optimizer chain");
    }
}
