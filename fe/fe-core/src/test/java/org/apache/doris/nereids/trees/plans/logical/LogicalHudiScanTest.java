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

import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Unit tests for LogicalHudiScan.
 * These tests focus on verifying that operativeSlots are correctly preserved
 * through with* methods to prevent type slicing issues.
 */
public class LogicalHudiScanTest {

    @Mock
    private ExternalTable mockTable;

    @Mock
    private HMSExternalTable mockHmsTable;

    @Mock
    private LogicalProperties mockLogicalProperties;

    private RelationId relationId;
    private List<String> qualifier;
    private SlotReference slot1;
    private SlotReference slot2;
    private Collection<Slot> operativeSlots;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        relationId = new RelationId(1);
        qualifier = ImmutableList.of("catalog", "db", "table");
        slot1 = new SlotReference("col1", IntegerType.INSTANCE);
        slot2 = new SlotReference("col2", StringType.INSTANCE);
        operativeSlots = ImmutableList.of(slot1, slot2);

        Mockito.when(mockTable.getName()).thenReturn("test_hudi_table");
        Mockito.when(mockHmsTable.getName()).thenReturn("test_hudi_table");
        Mockito.when(mockHmsTable.getDlaType()).thenReturn(HMSExternalTable.DLAType.HUDI);
        Mockito.when(mockHmsTable.getPartitionColumns()).thenReturn(ImmutableList.of());
    }

    private LogicalHudiScan newHudiScan() {
        return new LogicalHudiScan(relationId, mockHmsTable, qualifier, ImmutableList.of(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     * Test that withOperativeSlots returns LogicalHudiScan (not LogicalFileScan).
     */
    @Test
    public void testWithOperativeSlotsReturnsLogicalHudiScan() {
        LogicalHudiScan hudiScan = newHudiScan();

        LogicalFileScan result = hudiScan.withOperativeSlots(operativeSlots);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result instanceof LogicalHudiScan,
                "withOperativeSlots should return LogicalHudiScan, not LogicalFileScan");
        Assertions.assertEquals(operativeSlots.size(), result.getOperativeSlots().size());
    }

    /**
     * Test that operativeSlots are correctly set by withOperativeSlots.
     */
    @Test
    public void testWithOperativeSlotsPreservesSlots() {
        LogicalHudiScan hudiScan = newHudiScan();

        LogicalFileScan result = hudiScan.withOperativeSlots(operativeSlots);

        List<Slot> resultSlots = result.getOperativeSlots();
        Assertions.assertEquals(2, resultSlots.size());
        Assertions.assertTrue(resultSlots.contains(slot1));
        Assertions.assertTrue(resultSlots.contains(slot2));
    }

    /**
     * Test that withGroupExpression preserves operativeSlots.
     */
    @Test
    public void testWithGroupExpressionPreservesOperativeSlots() {
        LogicalHudiScan hudiScan = createHudiScanWithOperativeSlots();

        LogicalHudiScan result = hudiScan.withGroupExpression(Optional.empty());

        Assertions.assertTrue(result instanceof LogicalHudiScan);
        Assertions.assertEquals(operativeSlots.size(), result.getOperativeSlots().size());
    }

    /**
     * Test that withGroupExprLogicalPropChildren preserves operativeSlots.
     */
    @Test
    public void testWithGroupExprLogicalPropChildrenPreservesOperativeSlots() {
        LogicalHudiScan hudiScan = createHudiScanWithOperativeSlots();

        LogicalHudiScan result = (LogicalHudiScan) hudiScan.withGroupExprLogicalPropChildren(
                Optional.empty(), Optional.of(mockLogicalProperties), ImmutableList.of());

        Assertions.assertTrue(result instanceof LogicalHudiScan);
        Assertions.assertEquals(operativeSlots.size(), result.getOperativeSlots().size());
    }

    /**
     * Test that withSelectedPartitions preserves operativeSlots.
     */
    @Test
    public void testWithSelectedPartitionsPreservesOperativeSlots() {
        LogicalHudiScan hudiScan = createHudiScanWithOperativeSlots();

        LogicalHudiScan result = hudiScan.withSelectedPartitions(SelectedPartitions.NOT_PRUNED);

        Assertions.assertTrue(result instanceof LogicalHudiScan);
        Assertions.assertEquals(operativeSlots.size(), result.getOperativeSlots().size());
    }

    /**
     * Test that withRelationId preserves operativeSlots.
     */
    @Test
    public void testWithRelationIdPreservesOperativeSlots() {
        LogicalHudiScan hudiScan = createHudiScanWithOperativeSlots();
        RelationId newRelationId = new RelationId(2);

        LogicalHudiScan result = hudiScan.withRelationId(newRelationId);

        Assertions.assertTrue(result instanceof LogicalHudiScan);
        Assertions.assertEquals(operativeSlots.size(), result.getOperativeSlots().size());
        Assertions.assertEquals(newRelationId, result.getRelationId());
    }

    /**
     * Test that empty operativeSlots works correctly.
     */
    @Test
    public void testEmptyOperativeSlots() {
        LogicalHudiScan hudiScan = newHudiScan();

        Assertions.assertTrue(hudiScan.getOperativeSlots().isEmpty());

        LogicalFileScan result = hudiScan.withOperativeSlots(ImmutableList.of());
        Assertions.assertTrue(result.getOperativeSlots().isEmpty());
    }

    /**
     * Test that scanParams and incrementalRelation are preserved through withOperativeSlots.
     */
    @Test
    public void testWithOperativeSlotsPreservesScanParams() {
        LogicalHudiScan hudiScan = newHudiScan();

        LogicalFileScan result = hudiScan.withOperativeSlots(operativeSlots);

        Assertions.assertEquals(hudiScan.getScanParams(), result.getScanParams());
        Assertions.assertEquals(hudiScan.getIncrementalRelation(), ((LogicalHudiScan) result).getIncrementalRelation());
    }

    /**
     * Test that withOperativeSlots returns a new instance (immutability).
     */
    @Test
    public void testWithOperativeSlotsReturnsNewInstance() {
        LogicalHudiScan hudiScan = newHudiScan();

        LogicalFileScan result = hudiScan.withOperativeSlots(operativeSlots);

        Assertions.assertNotSame(hudiScan, result,
                "withOperativeSlots should return a new instance");
    }

    /**
     * Helper method to create a LogicalHudiScan with operativeSlots set.
     */
    private LogicalHudiScan createHudiScanWithOperativeSlots() {
        return (LogicalHudiScan) newHudiScan().withOperativeSlots(operativeSlots);
    }

    /**
     * Test full constructor path via subclass (same package) for memo/props wiring.
     */
    @Test
    public void testFullConstructorInitializesEmptyOperativeSlots() {
        LogicalHudiScan hudiScan = TestableLogicalHudiScan.createFull(
                relationId, mockHmsTable, qualifier,
                Optional.empty(), Optional.of(mockLogicalProperties),
                SelectedPartitions.NOT_PRUNED,
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());

        Assertions.assertNotNull(hudiScan);
        Assertions.assertTrue(hudiScan.getOperativeSlots().isEmpty());
        Assertions.assertEquals(relationId, hudiScan.getRelationId());
    }

    /**
     * Test that withScanParams with empty Optional preserves operativeSlots.
     */
    @Test
    public void testWithScanParamsEmptyPreservesOperativeSlots() {
        LogicalHudiScan hudiScan = createHudiScanWithOperativeSlots();

        LogicalHudiScan result = hudiScan.withScanParams(mockHmsTable, Optional.empty());

        Assertions.assertTrue(result instanceof LogicalHudiScan);
        Assertions.assertEquals(operativeSlots.size(), result.getOperativeSlots().size());
        Assertions.assertFalse(result.getScanParams().isPresent());
    }

    /**
     * Subclass to invoke the protected full constructor (tests live in the same package).
     */
    static class TestableLogicalHudiScan extends LogicalHudiScan {

        TestableLogicalHudiScan(
                org.apache.doris.nereids.trees.plans.RelationId id,
                org.apache.doris.datasource.ExternalTable table,
                List<String> qualifier,
                SelectedPartitions selectedPartitions,
                Optional<org.apache.doris.nereids.trees.TableSample> tableSample,
                Optional<org.apache.doris.analysis.TableSnapshot> tableSnapshot,
                Optional<org.apache.doris.analysis.TableScanParams> scanParams,
                Optional<org.apache.doris.datasource.hudi.source.IncrementalRelation> incrementalRelation,
                Collection<org.apache.doris.nereids.trees.expressions.Slot> operativeSlots,
                List<org.apache.doris.nereids.trees.expressions.NamedExpression> virtualColumns,
                Optional<GroupExpression> groupExpression,
                Optional<LogicalProperties> logicalProperties,
                Optional<List<org.apache.doris.nereids.trees.expressions.Slot>> cachedOutputs) {
            super(id, table, qualifier, selectedPartitions, tableSample, tableSnapshot, scanParams,
                    incrementalRelation, operativeSlots, virtualColumns, groupExpression, logicalProperties,
                    cachedOutputs);
        }

        static LogicalHudiScan createFull(
                RelationId id,
                HMSExternalTable table,
                List<String> qualifier,
                Optional<GroupExpression> groupExpression,
                Optional<LogicalProperties> logicalProperties,
                SelectedPartitions selectedPartitions,
                Optional<org.apache.doris.nereids.trees.TableSample> tableSample,
                Optional<org.apache.doris.analysis.TableSnapshot> tableSnapshot,
                Optional<org.apache.doris.analysis.TableScanParams> scanParams,
                Optional<org.apache.doris.datasource.hudi.source.IncrementalRelation> incrementalRelation) {
            return new TestableLogicalHudiScan(id, table, qualifier, selectedPartitions, tableSample,
                    tableSnapshot, scanParams, incrementalRelation, ImmutableList.of(), ImmutableList.of(),
                    groupExpression, logicalProperties, Optional.empty());
        }
    }
}
