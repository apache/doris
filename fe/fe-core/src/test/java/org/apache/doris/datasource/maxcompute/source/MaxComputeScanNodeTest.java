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

package org.apache.doris.datasource.maxcompute.source;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalCatalog;
import org.apache.doris.datasource.maxcompute.MaxComputeExternalTable;
import org.apache.doris.datasource.maxcompute.source.MaxComputeSplit.SplitType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;

import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class MaxComputeScanNodeTest {

    @Mock
    private MaxComputeExternalTable table;

    @Mock
    private MaxComputeExternalCatalog catalog;

    @Mock
    private com.aliyun.odps.Table odpsTable;

    private SessionVariable sv;
    private TupleDescriptor desc;
    private MaxComputeScanNode node;

    private List<Column> partitionColumns;

    @Before
    public void setUp() {
        partitionColumns = Arrays.asList(
                new Column("dt", PrimitiveType.VARCHAR),
                new Column("hr", PrimitiveType.VARCHAR)
        );
        Mockito.when(table.getPartitionColumns()).thenReturn(partitionColumns);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getOdpsTable()).thenReturn(odpsTable);

        desc = Mockito.mock(TupleDescriptor.class);
        Mockito.when(desc.getTable()).thenReturn(table);
        Mockito.when(desc.getId()).thenReturn(new TupleId(0));
        Mockito.when(desc.getSlots()).thenReturn(new ArrayList<>());

        sv = new SessionVariable();
        node = new MaxComputeScanNode(new PlanNodeId(0), desc,
                SelectedPartitions.NOT_PRUNED, false, sv);
    }

    // ==================== Reflection Helpers ====================

    private void setConjuncts(PlanNode target, List<Expr> conjuncts) throws Exception {
        Field f = PlanNode.class.getDeclaredField("conjuncts");
        f.setAccessible(true);
        f.set(target, conjuncts);
    }

    private void setLimit(PlanNode target, long limit) throws Exception {
        Field f = PlanNode.class.getDeclaredField("limit");
        f.setAccessible(true);
        f.setLong(target, limit);
    }

    private void setOnlyPartitionEqualityPredicate(MaxComputeScanNode target, boolean value) throws Exception {
        Field f = MaxComputeScanNode.class.getDeclaredField("onlyPartitionEqualityPredicate");
        f.setAccessible(true);
        f.setBoolean(target, value);
    }

    private boolean invokeCheckOnlyPartitionEqualityPredicate(MaxComputeScanNode target) throws Exception {
        Method m = MaxComputeScanNode.class.getDeclaredMethod("checkOnlyPartitionEqualityPredicate");
        m.setAccessible(true);
        return (boolean) m.invoke(target);
    }

    // ==================== Group 1: checkOnlyPartitionEqualityPredicate ====================

    @Test
    public void testCheckOnlyPartEq_emptyConjuncts() throws Exception {
        setConjuncts(node, new ArrayList<>());
        Assert.assertTrue(invokeCheckOnlyPartitionEqualityPredicate(node));
    }

    @Test
    public void testCheckOnlyPartEq_singlePartitionEquality() throws Exception {
        SlotRef dtSlot = new SlotRef(null, "dt");
        StringLiteral val = new StringLiteral("2026-02-26");
        BinaryPredicate eq = new BinaryPredicate(BinaryPredicate.Operator.EQ, dtSlot, val);
        setConjuncts(node, Lists.newArrayList(eq));
        Assert.assertTrue(invokeCheckOnlyPartitionEqualityPredicate(node));
    }

    @Test
    public void testCheckOnlyPartEq_multiPartitionEquality() throws Exception {
        SlotRef dtSlot = new SlotRef(null, "dt");
        SlotRef hrSlot = new SlotRef(null, "hr");
        BinaryPredicate eq1 = new BinaryPredicate(BinaryPredicate.Operator.EQ, dtSlot, new StringLiteral("x"));
        BinaryPredicate eq2 = new BinaryPredicate(BinaryPredicate.Operator.EQ, hrSlot, new StringLiteral("10"));
        setConjuncts(node, Lists.newArrayList(eq1, eq2));
        Assert.assertTrue(invokeCheckOnlyPartitionEqualityPredicate(node));
    }

    @Test
    public void testCheckOnlyPartEq_nonPartitionColumn() throws Exception {
        SlotRef statusSlot = new SlotRef(null, "status");
        BinaryPredicate eq = new BinaryPredicate(BinaryPredicate.Operator.EQ, statusSlot, new StringLiteral("active"));
        setConjuncts(node, Lists.newArrayList(eq));
        Assert.assertFalse(invokeCheckOnlyPartitionEqualityPredicate(node));
    }

    @Test
    public void testCheckOnlyPartEq_nonEqOperator() throws Exception {
        SlotRef dtSlot = new SlotRef(null, "dt");
        BinaryPredicate gt = new BinaryPredicate(BinaryPredicate.Operator.GT, dtSlot, new StringLiteral("2026-01-01"));
        setConjuncts(node, Lists.newArrayList(gt));
        Assert.assertFalse(invokeCheckOnlyPartitionEqualityPredicate(node));
    }

    @Test
    public void testCheckOnlyPartEq_inPredicateOnPartitionColumn() throws Exception {
        SlotRef dtSlot = new SlotRef(null, "dt");
        List<Expr> inList = Lists.newArrayList(new StringLiteral("a"), new StringLiteral("b"));
        InPredicate inPred = new InPredicate(dtSlot, inList, false);
        setConjuncts(node, Lists.newArrayList(inPred));
        Assert.assertTrue(invokeCheckOnlyPartitionEqualityPredicate(node));
    }

    @Test
    public void testCheckOnlyPartEq_notInPredicate() throws Exception {
        SlotRef dtSlot = new SlotRef(null, "dt");
        List<Expr> inList = Lists.newArrayList(new StringLiteral("a"), new StringLiteral("b"));
        InPredicate notInPred = new InPredicate(dtSlot, inList, true);
        setConjuncts(node, Lists.newArrayList(notInPred));
        Assert.assertFalse(invokeCheckOnlyPartitionEqualityPredicate(node));
    }

    @Test
    public void testCheckOnlyPartEq_inPredicateOnNonPartitionColumn() throws Exception {
        SlotRef statusSlot = new SlotRef(null, "status");
        List<Expr> inList = Lists.newArrayList(new StringLiteral("a"), new StringLiteral("b"));
        InPredicate inPred = new InPredicate(statusSlot, inList, false);
        setConjuncts(node, Lists.newArrayList(inPred));
        Assert.assertFalse(invokeCheckOnlyPartitionEqualityPredicate(node));
    }

    @Test
    public void testCheckOnlyPartEq_inPredicateWithNonLiteralValue() throws Exception {
        SlotRef dtSlot = new SlotRef(null, "dt");
        SlotRef hrSlot = new SlotRef(null, "hr");
        List<Expr> inList = Lists.newArrayList(hrSlot);
        InPredicate inPred = new InPredicate(dtSlot, inList, false);
        setConjuncts(node, Lists.newArrayList(inPred));
        Assert.assertFalse(invokeCheckOnlyPartitionEqualityPredicate(node));
    }

    @Test
    public void testCheckOnlyPartEq_mixedEqAndInOnPartitionColumns() throws Exception {
        SlotRef dtSlot = new SlotRef(null, "dt");
        BinaryPredicate eq = new BinaryPredicate(BinaryPredicate.Operator.EQ, dtSlot, new StringLiteral("2026-01-01"));

        SlotRef hrSlot = new SlotRef(null, "hr");
        List<Expr> inList = Lists.newArrayList(new StringLiteral("10"), new StringLiteral("11"));
        InPredicate inPred = new InPredicate(hrSlot, inList, false);

        setConjuncts(node, Lists.newArrayList(eq, inPred));
        Assert.assertTrue(invokeCheckOnlyPartitionEqualityPredicate(node));
    }

    @Test
    public void testCheckOnlyPartEq_leftSideNotSlotRef() throws Exception {
        StringLiteral left = new StringLiteral("x");
        StringLiteral right = new StringLiteral("x");
        BinaryPredicate eq = new BinaryPredicate(BinaryPredicate.Operator.EQ, left, right);
        setConjuncts(node, Lists.newArrayList(eq));
        Assert.assertFalse(invokeCheckOnlyPartitionEqualityPredicate(node));
    }

    @Test
    public void testCheckOnlyPartEq_rightSideNotLiteral() throws Exception {
        SlotRef dtSlot = new SlotRef(null, "dt");
        SlotRef hrSlot = new SlotRef(null, "hr");
        BinaryPredicate eq = new BinaryPredicate(BinaryPredicate.Operator.EQ, dtSlot, hrSlot);
        setConjuncts(node, Lists.newArrayList(eq));
        Assert.assertFalse(invokeCheckOnlyPartitionEqualityPredicate(node));
    }

    // ==================== Serializable Stub for TableBatchReadSession ====================

    private static class StubTableBatchReadSession implements TableBatchReadSession {
        private static final long serialVersionUID = 1L;
        private transient InputSplitAssigner assigner;

        StubTableBatchReadSession(InputSplitAssigner assigner) {
            this.assigner = assigner;
        }

        @Override
        public InputSplitAssigner getInputSplitAssigner() throws IOException {
            return assigner;
        }

        @Override
        public DataSchema readSchema() {
            return null;
        }

        @Override
        public boolean supportsDataFormat(DataFormat dataFormat) {
            return false;
        }

        @Override
        public String getId() {
            return "stub-session";
        }

        @Override
        public TableIdentifier getTableIdentifier() {
            return null;
        }

        @Override
        public SessionStatus getStatus() {
            return SessionStatus.NORMAL;
        }

        @Override
        public String toJson() {
            return "{}";
        }
    }

    // ==================== Mock Session Helper ====================

    private MaxComputeScanNode createSpyNodeWithMockSession(long totalRowCount) throws Exception {
        MaxComputeScanNode spyNode = Mockito.spy(node);

        InputSplitAssigner mockAssigner = Mockito.mock(InputSplitAssigner.class);
        com.aliyun.odps.table.read.split.InputSplit mockInputSplit =
                Mockito.mock(com.aliyun.odps.table.read.split.InputSplit.class);

        Mockito.when(mockAssigner.getTotalRowCount()).thenReturn(totalRowCount);
        Mockito.when(mockAssigner.getSplitByRowOffset(Mockito.anyLong(), Mockito.anyLong()))
                .thenReturn(mockInputSplit);
        Mockito.when(mockInputSplit.getSessionId()).thenReturn("test-session-id");

        StubTableBatchReadSession stubSession = new StubTableBatchReadSession(mockAssigner);

        Mockito.doReturn(stubSession).when(spyNode)
                .createTableBatchReadSession(Mockito.anyList(), Mockito.any(
                        com.aliyun.odps.table.configuration.SplitOptions.class));
        Mockito.doReturn(stubSession).when(spyNode)
                .createTableBatchReadSession(Mockito.anyList());

        Mockito.when(odpsTable.getLastDataModifiedTime()).thenReturn(new Date(1000L));

        return spyNode;
    }

    // ==================== Group 2: getSplitsWithLimitOptimization ====================

    private List<Split> invokeGetSplitsWithLimitOptimization(
            MaxComputeScanNode target) throws Exception {
        Method m = MaxComputeScanNode.class.getDeclaredMethod(
                "getSplitsWithLimitOptimization", List.class);
        m.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<Split> result = (List<Split>) m.invoke(target, Collections.emptyList());
        return result;
    }

    @Test
    public void testLimitOpt_limitLessThanTotal() throws Exception {
        MaxComputeScanNode spyNode = createSpyNodeWithMockSession(10000L);
        setLimit(spyNode, 100L);

        List<Split> result = invokeGetSplitsWithLimitOptimization(spyNode);

        Assert.assertEquals(1, result.size());
        MaxComputeSplit split = (MaxComputeSplit) result.get(0);
        Assert.assertEquals(SplitType.ROW_OFFSET, split.splitType);
        Assert.assertEquals(100L, split.getLength());
    }

    @Test
    public void testLimitOpt_limitGreaterThanTotal() throws Exception {
        MaxComputeScanNode spyNode = createSpyNodeWithMockSession(200L);
        setLimit(spyNode, 50000L);

        List<Split> result = invokeGetSplitsWithLimitOptimization(spyNode);

        Assert.assertEquals(1, result.size());
        MaxComputeSplit split = (MaxComputeSplit) result.get(0);
        Assert.assertEquals(SplitType.ROW_OFFSET, split.splitType);
        Assert.assertEquals(200L, split.getLength());
    }

    @Test
    public void testLimitOpt_totalRowCountZero() throws Exception {
        MaxComputeScanNode spyNode = createSpyNodeWithMockSession(0L);
        setLimit(spyNode, 100L);

        List<Split> result = invokeGetSplitsWithLimitOptimization(spyNode);

        Assert.assertTrue(result.isEmpty());
    }

    // ==================== Group 3: getSplits gating conditions ====================

    private MaxComputeScanNode createSpyNodeForGetSplits(long totalRowCount) throws Exception {
        // Need non-empty slots so getSplits doesn't return early
        SlotDescriptor mockSlotDesc = Mockito.mock(SlotDescriptor.class);
        Column dataCol = new Column("value", PrimitiveType.VARCHAR);
        Mockito.when(mockSlotDesc.getColumn()).thenReturn(dataCol);
        Mockito.when(desc.getSlots()).thenReturn(Lists.newArrayList(mockSlotDesc));

        // Need fileNum > 0
        Mockito.when(odpsTable.getFileNum()).thenReturn(10L);

        // For normal path: use row_count strategy
        Mockito.when(catalog.getSplitStrategy()).thenReturn("row_count");
        Mockito.when(catalog.getSplitRowCount()).thenReturn(totalRowCount);

        // Need table.getColumns() for createRequiredColumns()
        List<Column> allColumns = Lists.newArrayList(
                new Column("dt", PrimitiveType.VARCHAR),
                new Column("hr", PrimitiveType.VARCHAR),
                new Column("value", PrimitiveType.VARCHAR)
        );
        Mockito.when(table.getColumns()).thenReturn(allColumns);

        return createSpyNodeWithMockSession(totalRowCount);
    }

    @Test
    public void testGetSplits_allConditionsMet_optimizationPath() throws Exception {
        MaxComputeScanNode spyNode = createSpyNodeForGetSplits(10000L);
        sv.enableMcLimitSplitOptimization = true;
        setOnlyPartitionEqualityPredicate(spyNode, true);
        setLimit(spyNode, 100L);

        List<Split> result = spyNode.getSplits(1);

        Assert.assertEquals(1, result.size());
        MaxComputeSplit split = (MaxComputeSplit) result.get(0);
        Assert.assertEquals(SplitType.ROW_OFFSET, split.splitType);
        Assert.assertEquals(100L, split.getLength());
    }

    @Test
    public void testGetSplits_optimizationDisabled_normalPath() throws Exception {
        MaxComputeScanNode spyNode = createSpyNodeForGetSplits(1000L);
        sv.enableMcLimitSplitOptimization = false;
        setOnlyPartitionEqualityPredicate(spyNode, true);
        setLimit(spyNode, 100L);

        List<Split> result = spyNode.getSplits(1);

        // Normal path with row_count strategy: totalRowCount=1000, splitRowCount=1000 → 1 split
        // but the split length equals splitRowCount, not limit
        Assert.assertFalse(result.isEmpty());
    }

    @Test
    public void testGetSplits_nonPartitionPredicate_normalPath() throws Exception {
        MaxComputeScanNode spyNode = createSpyNodeForGetSplits(1000L);
        sv.enableMcLimitSplitOptimization = true;
        setOnlyPartitionEqualityPredicate(spyNode, false);
        setLimit(spyNode, 100L);

        List<Split> result = spyNode.getSplits(1);

        Assert.assertFalse(result.isEmpty());
    }

    @Test
    public void testGetSplits_noLimit_normalPath() throws Exception {
        MaxComputeScanNode spyNode = createSpyNodeForGetSplits(1000L);
        sv.enableMcLimitSplitOptimization = true;
        setOnlyPartitionEqualityPredicate(spyNode, true);
        // limit defaults to -1 (no limit), don't set it

        List<Split> result = spyNode.getSplits(1);

        Assert.assertFalse(result.isEmpty());
    }
}
