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

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.PlanningSplitMetadata;
import org.apache.doris.datasource.PlanningSplitProducer;
import org.apache.doris.datasource.SplitSink;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileRangeDesc;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IcebergScanNodeTest {
    @Test
    public void testGetSplitsUsesProducerAndSyncsMetadata() throws Exception {
        IcebergSplitPlanningSupport support = Mockito.mock(IcebergSplitPlanningSupport.class);
        Split split = Mockito.mock(Split.class);
        StaticPlanningSplitProducer producer = new StaticPlanningSplitProducer(
                Collections.singletonList(split),
                new IcebergPlanningMetadata(support));
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable(), producer);

        Mockito.when(support.getSelectedPartitionNum()).thenReturn(4);
        Mockito.when(support.hasTableLevelPushDownCount()).thenReturn(true);
        Mockito.when(support.getCountFromSnapshot()).thenReturn(33L);

        List<Split> splits = node.getSplits(3);

        Assert.assertEquals(Collections.singletonList(split), splits);
        Assert.assertEquals(3, producer.lastNumBackends);
        Assert.assertEquals(4L, node.getSelectedPartitionNum());
        Assert.assertEquals(33L, node.getPushDownCountForTest());
    }

    @Test
    public void testGetNodeExplainStringIncludesPushdownPredicatesFromMetadata() {
        IcebergSplitPlanningSupport support = Mockito.mock(IcebergSplitPlanningSupport.class);
        Mockito.when(support.getPushdownIcebergPredicates())
                .thenReturn(Arrays.asList("id > 1", "dt = '2026-03-25'"));
        StaticPlanningSplitProducer producer = new StaticPlanningSplitProducer(
                Collections.emptyList(),
                new IcebergPlanningMetadata(support));
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable(), producer);

        String explain = node.getNodeExplainString("  ", TExplainLevel.NORMAL);

        Assert.assertTrue(explain.contains("icebergPredicatePushdown="));
        Assert.assertTrue(explain.contains("id > 1"));
        Assert.assertTrue(explain.contains("dt = '2026-03-25'"));
    }

    @Test
    public void testGetSplitsRunsIcebergProducerOnCurrentThread() throws Exception {
        IcebergSplitPlanningSupport support = Mockito.mock(IcebergSplitPlanningSupport.class);
        Split split = Mockito.mock(Split.class);
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        ctx.setStatementContext(new StatementContext());

        Mockito.when(support.isBatchMode()).thenReturn(false);
        ContextAwarePlanningSplitProducer producer =
                new ContextAwarePlanningSplitProducer(support, split, ctx);
        TestIcebergScanNode node = new TestIcebergScanNode(new SessionVariable(), producer);

        List<Split> splits = node.getSplits(1);

        Assert.assertEquals(Collections.singletonList(split), splits);
        Assert.assertTrue(producer.contextVisibleDuringProduce);
    }

    @Test
    public void testSetScanParamsUsesSplitTableLevelRowCount() throws Exception {
        StaticPlanningSplitProducer producer = new StaticPlanningSplitProducer(
                Collections.emptyList(),
                PlanningSplitMetadata.EMPTY);
        TestIcebergScanNode node = new TestIcebergScanNode(
                new SessionVariable(),
                producer);
        setField(node, "formatVersion", IcebergScanNode.MIN_DELETE_FILE_SUPPORT_VERSION);

        IcebergSplit split = createIcebergSplit("hdfs://warehouse/db/table/data.parquet");
        split.setTableFormatType(TableFormatType.ICEBERG);
        split.setTableLevelRowCount(99L);
        TFileRangeDesc rangeDesc = new TFileRangeDesc();

        node.applyScanParams(rangeDesc, split);

        Assert.assertEquals(99L, rangeDesc.getTableFormatParams().getTableLevelRowCount());
        Assert.assertEquals("hdfs://warehouse/db/table/data.parquet",
                rangeDesc.getTableFormatParams().getIcebergParams().getOriginalFilePath());
    }

    @Test
    public void testSetScanParamsDefaultsTableLevelRowCountToMinusOne() throws Exception {
        StaticPlanningSplitProducer producer = new StaticPlanningSplitProducer(
                Collections.emptyList(),
                PlanningSplitMetadata.EMPTY);
        TestIcebergScanNode node = new TestIcebergScanNode(
                new SessionVariable(),
                producer);
        setField(node, "formatVersion", IcebergScanNode.MIN_DELETE_FILE_SUPPORT_VERSION);

        IcebergSplit split = createIcebergSplit("hdfs://warehouse/db/table/no_count.parquet");
        split.setTableFormatType(TableFormatType.ICEBERG);
        TFileRangeDesc rangeDesc = new TFileRangeDesc();

        node.applyScanParams(rangeDesc, split);

        Assert.assertEquals(-1L, rangeDesc.getTableFormatParams().getTableLevelRowCount());
    }

    private static IcebergSplit createIcebergSplit(String path) {
        return new IcebergSplit(LocationPath.of(path), 0, 128, 128, new String[0],
                IcebergScanNode.MIN_DELETE_FILE_SUPPORT_VERSION, Collections.emptyMap(),
                Collections.emptyList(), path);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = IcebergScanNode.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class TestIcebergScanNode extends IcebergScanNode {
        private final PlanningSplitProducer producer;

        TestIcebergScanNode(SessionVariable sv, PlanningSplitProducer producer) {
            super(new PlanNodeId(0), createTupleDescriptor(), sv, ScanContext.EMPTY);
            this.producer = producer;
        }

        @Override
        PlanningSplitProducer createPlanningSplitProducer() {
            return producer;
        }

        void applyScanParams(TFileRangeDesc rangeDesc, Split split) {
            setScanParams(rangeDesc, split);
        }

        long getPushDownCountForTest() {
            return tableLevelRowCount;
        }

        private static TupleDescriptor createTupleDescriptor() {
            TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
            TableIf table = Mockito.mock(TableIf.class);
            Mockito.when(table.getName()).thenReturn("tbl");
            Mockito.when(table.getNameWithFullQualifiers()).thenReturn("ctl.db.tbl");
            desc.setTable(table);
            return desc;
        }
    }

    private static class StaticPlanningSplitProducer implements PlanningSplitProducer {
        private final List<Split> plannedSplits;
        private final PlanningSplitMetadata planningMetadata;
        private int lastNumBackends = -1;

        StaticPlanningSplitProducer(
                List<Split> plannedSplits,
                PlanningSplitMetadata planningMetadata) {
            this.plannedSplits = new ArrayList<>(plannedSplits);
            this.planningMetadata = planningMetadata;
        }

        @Override
        public PlanningSplitMetadata getPlanningMetadata() {
            return planningMetadata;
        }

        @Override
        public void start(int numBackends, SplitSink splitSink)
                throws org.apache.doris.common.UserException {
            lastNumBackends = numBackends;
            if (!plannedSplits.isEmpty()) {
                splitSink.addBatch(plannedSplits);
            }
            splitSink.finish();
        }
    }

    private static class ContextAwarePlanningSplitProducer extends AbstractIcebergPlanningSplitProducer {
        private final Split split;
        private final ConnectContext expectedContext;
        private boolean contextVisibleDuringProduce;

        ContextAwarePlanningSplitProducer(
                IcebergSplitPlanningSupport planningSupport,
                Split split,
                ConnectContext expectedContext) {
            super(planningSupport, Runnable::run);
            this.split = split;
            this.expectedContext = expectedContext;
        }

        @Override
        protected void doProduce(int numBackends, SplitSink splitSink) throws Exception {
            contextVisibleDuringProduce = ConnectContext.get() == expectedContext
                    && ConnectContext.get().getStatementContext() != null;
            splitSink.addBatch(Collections.singletonList(split));
        }
    }
}
