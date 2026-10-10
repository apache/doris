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

package org.apache.doris.planner;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Drives {@link OlapScanNode#applyGlobalPointIndexPrune} with a real conjunct and a real GLOBAL_POINT
 * index. The partition is a plain (non-cloud) one, so the probe keeps every tablet without an RPC;
 * the tests check the gating and that the predicate reaches the probe.
 */
public class GlobalPointIndexPlanTimePruneTest {
    private static final long PARTITION_ID = 1L;
    private static final long INDEX_ID = 1001L;
    private static final String COLUMN_NAME = "event_id";

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        Config.enable_global_point_index_prune = true;
    }

    private static OlapTable createTable() {
        List<Column> columns = Lists.newArrayList(new Column(COLUMN_NAME, ScalarType.createVarchar(64), false));
        RandomDistributionInfo distInfo = new RandomDistributionInfo(1);
        OlapTable table = new OlapTable(100L, "gp_table", columns, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), distInfo);
        table.setIndexes(Lists.newArrayList(new Index(1L, "idx_gp", Lists.newArrayList(COLUMN_NAME),
                IndexType.GLOBAL_POINT, new HashMap<>(), "")));
        Deencapsulation.setField(table, "baseIndexId", INDEX_ID);
        table.addPartition(new Partition(PARTITION_ID, "p1", new MaterializedIndex(INDEX_ID, IndexState.NORMAL),
                distInfo));
        return table;
    }

    private static OlapScanNode createScanNode(OlapTable table, boolean withPredicate) {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(1));
        desc.setTable(table);
        SlotDescriptor slotDesc = new SlotDescriptor(new SlotId(1), desc.getId());
        slotDesc.setColumn(table.getColumn(COLUMN_NAME));
        desc.addSlot(slotDesc);

        OlapScanNode scanNode = new OlapScanNode(new PlanNodeId(1), desc, "gp_scan", ScanContext.EMPTY);
        scanNode.setSelectedIndexInfo(INDEX_ID, true, null);
        scanNode.setSelectedPartitionIds(Lists.newArrayList(PARTITION_ID));
        if (withPredicate) {
            Expr predicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(slotDesc),
                    new StringLiteral("abc"));
            scanNode.conjuncts = Lists.newArrayList(predicate);
        }
        return scanNode;
    }

    private static void setUpConnectContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        ctx.getSessionVariable().enableGlobalPointIndexPrune = true;
        ctx.getSessionVariable().setEnableQueryCache(false);
    }

    private static GlobalPointIndexPruner.Result pruneResult(OlapScanNode scanNode) {
        return Deencapsulation.getField(scanNode, "globalPointPruneResult");
    }

    @Test
    public void testEqualPredicateReachesProbe() {
        setUpConnectContext();
        OlapScanNode scanNode = createScanNode(createTable(), true);
        scanNode.applyGlobalPointIndexPrune(Collections.emptyMap());

        Assertions.assertTrue(scanNode.columnFilters.containsKey(COLUMN_NAME));
        GlobalPointIndexPruner.Result result = pruneResult(scanNode);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(COLUMN_NAME, result.columnName);
        Assertions.assertEquals(1, result.probeValueCount);
        // Not a cloud partition: nothing can be checked, so nothing is pruned.
        Assertions.assertEquals(result.tabletsBefore, result.tabletsAfter);
        Assertions.assertEquals(result.tabletsBefore, result.degradedTablets);
    }

    @Test
    public void testSkippedWithoutPredicate() {
        setUpConnectContext();
        OlapScanNode scanNode = createScanNode(createTable(), false);
        scanNode.applyGlobalPointIndexPrune(Collections.emptyMap());
        Assertions.assertNull(pruneResult(scanNode));
    }

    @Test
    public void testSkippedWhenConfigDisabled() {
        setUpConnectContext();
        Config.enable_global_point_index_prune = false;
        OlapScanNode scanNode = createScanNode(createTable(), true);
        scanNode.applyGlobalPointIndexPrune(Collections.emptyMap());
        Assertions.assertTrue(scanNode.columnFilters.isEmpty());
        Assertions.assertNull(pruneResult(scanNode));
    }

    @Test
    public void testSkippedWhenSessionVariableDisabled() {
        setUpConnectContext();
        ConnectContext.get().getSessionVariable().enableGlobalPointIndexPrune = false;
        OlapScanNode scanNode = createScanNode(createTable(), true);
        scanNode.applyGlobalPointIndexPrune(Collections.emptyMap());
        Assertions.assertNull(pruneResult(scanNode));
    }

    @Test
    public void testNeverThrowsWithoutConnectContext() {
        OlapScanNode scanNode = createScanNode(createTable(), true);
        Assertions.assertDoesNotThrow(() -> scanNode.applyGlobalPointIndexPrune(Collections.emptyMap()));
        Assertions.assertNull(pruneResult(scanNode));
    }
}
