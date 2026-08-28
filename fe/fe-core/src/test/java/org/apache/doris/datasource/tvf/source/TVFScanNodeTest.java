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

package org.apache.doris.datasource.tvf.source;

import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.lance.LanceFragmentInfo;
import org.apache.doris.datasource.lance.source.LanceSplit;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TVFScanNodeTest {
    private static final long MB = 1024L * 1024L;

    @Test
    public void testCountColumnKeepsNormalFileSplitting() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.parallelExecInstanceNum = 1;
        sv.setFileSplitSize(32 * MB);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        FunctionGenTable table = Mockito.mock(FunctionGenTable.class);
        ExternalFileTableValuedFunction tvf = Mockito.mock(ExternalFileTableValuedFunction.class);
        Mockito.when(table.getTvf()).thenReturn(tvf);
        Mockito.when(tvf.getTFileType()).thenReturn(TFileType.FILE_LOCAL);
        Mockito.when(tvf.getFileStatuses()).thenReturn(ImmutableList.of(
                splittableFile("file:///tmp/count_col_1.parquet", 128 * MB),
                splittableFile("file:///tmp/count_col_2.parquet", 128 * MB)));
        desc.setTable(table);

        TVFScanNode countColumnNode = new TVFScanNode(
                new PlanNodeId(0), desc, false, sv, ScanContext.EMPTY);
        setFileSplitter(countColumnNode, new FileSplitter(32 * MB, 32 * MB, 0));
        countColumnNode.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        countColumnNode.setPushDownCountSlotIds(Collections.singletonList(new SlotId(7)));

        List<Split> countColumnSplits = countColumnNode.getSplits(1);
        Assert.assertEquals(8, countColumnSplits.size());

        TVFScanNode countStarNode = new TVFScanNode(
                new PlanNodeId(1), desc, false, sv, ScanContext.EMPTY);
        setFileSplitter(countStarNode, new FileSplitter(32 * MB, 32 * MB, 0));
        countStarNode.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        countStarNode.setPushDownCountSlotIds(Collections.emptyList());

        List<Split> countStarSplits = countStarNode.getSplits(1);
        Assert.assertEquals(2, countStarSplits.size());
    }

    @Test
    public void testDetermineTargetFileSplitSizeHonorsMaxFileSplitNum() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        FunctionGenTable table = Mockito.mock(FunctionGenTable.class);
        ExternalFileTableValuedFunction tvf = Mockito.mock(ExternalFileTableValuedFunction.class);
        Mockito.when(table.getTvf()).thenReturn(tvf);
        desc.setTable(table);
        TVFScanNode node = new TVFScanNode(new PlanNodeId(0), desc, false, sv, ScanContext.EMPTY);

        TBrokerFileStatus status = new TBrokerFileStatus();
        status.setSize(10_000L * MB);
        List<TBrokerFileStatus> statuses = Collections.singletonList(status);

        Method method = TVFScanNode.class.getDeclaredMethod("determineTargetFileSplitSize", List.class);
        method.setAccessible(true);
        long target = (long) method.invoke(node, statuses);
        Assert.assertEquals(100 * MB, target);
    }

    @Test
    public void testDetermineTargetFileSplitSizeUsesCoarseSizeForParquet() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setFileSplitSizeOnFe(512 * MB);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        FunctionGenTable table = Mockito.mock(FunctionGenTable.class);
        ExternalFileTableValuedFunction tvf = Mockito.mock(ExternalFileTableValuedFunction.class);
        Mockito.when(table.getTvf()).thenReturn(tvf);
        Mockito.when(tvf.getTFileFormatType()).thenReturn(TFileFormatType.FORMAT_PARQUET);
        desc.setTable(table);
        TVFScanNode node = new TVFScanNode(new PlanNodeId(0), desc, false, sv, ScanContext.EMPTY);

        TBrokerFileStatus status = new TBrokerFileStatus();
        status.setSize(10_000L * MB);
        Method method = TVFScanNode.class.getDeclaredMethod("determineTargetFileSplitSize", List.class);
        method.setAccessible(true);

        Assert.assertEquals(512 * MB,
                (long) method.invoke(node, Collections.singletonList(status)));
    }

    private static TBrokerFileStatus splittableFile(String path, long size) {
        TBrokerFileStatus status = new TBrokerFileStatus();
        status.setPath(path);
        status.setSize(size);
        status.setModificationTime(0);
        status.setIsSplitable(true);
        return status;
    }

    private static void setFileSplitter(TVFScanNode node, FileSplitter splitter) throws Exception {
        java.lang.reflect.Field field = FileQueryScanNode.class.getDeclaredField("fileSplitter");
        field.setAccessible(true);
        field.set(node, splitter);
    }

    @Test
    public void testLanceSplitsPinVersionAndFragments() throws Exception {
        SessionVariable sv = new SessionVariable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        FunctionGenTable table = Mockito.mock(FunctionGenTable.class);
        ExternalFileTableValuedFunction tvf = Mockito.mock(ExternalFileTableValuedFunction.class);
        Mockito.when(table.getTvf()).thenReturn(tvf);
        Mockito.when(tvf.isLanceFormat()).thenReturn(true);
        Mockito.when(tvf.getFilePath()).thenReturn("s3://bucket/table.lance");
        Mockito.when(tvf.getLanceDatasetVersion()).thenReturn(42L);
        Mockito.when(tvf.getLanceFragments()).thenReturn(Arrays.asList(
                new LanceFragmentInfo(7, 1000, 1000),
                new LanceFragmentInfo(11, 250, 250)));
        desc.setTable(table);

        TVFScanNode node = new TVFScanNode(new PlanNodeId(0), desc, false, sv, ScanContext.EMPTY);
        List<Split> splits = node.getSplits(3);
        Assert.assertEquals(2, splits.size());

        LanceSplit first = (LanceSplit) splits.get(0);
        Assert.assertEquals("s3://bucket/table.lance", first.getDatasetUri());
        Assert.assertEquals(42L, first.getVersion());
        Assert.assertEquals(Collections.singletonList(7L), first.getFragmentIds());
        // The S3/file TVF path must weight fragments by physical rows, in sync with LanceScanNode.
        Assert.assertEquals(100L, first.getSplitWeight().getRawValue());
        Assert.assertEquals(25L, ((LanceSplit) splits.get(1)).getSplitWeight().getRawValue());

        TFileRangeDesc range = new TFileRangeDesc();
        node.setScanParams(range, first);
        Assert.assertTrue(range.isSetTableFormatParams());
        Assert.assertEquals("lance", range.getTableFormatParams().getTableFormatType());
        Assert.assertEquals("s3://bucket/table.lance",
                range.getTableFormatParams().getLanceParams().getDatasetUri());
        Assert.assertEquals(42L, range.getTableFormatParams().getLanceParams().getVersion());
        Assert.assertEquals(Collections.singletonList(7L),
                range.getTableFormatParams().getLanceParams().getFragmentIds());
    }

    @Test
    public void testLocalLanceUsesOneLatestWholeDatasetSplit() throws Exception {
        SessionVariable sv = new SessionVariable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        FunctionGenTable table = Mockito.mock(FunctionGenTable.class);
        ExternalFileTableValuedFunction tvf = Mockito.mock(ExternalFileTableValuedFunction.class);
        Mockito.when(table.getTvf()).thenReturn(tvf);
        Mockito.when(tvf.isLanceFormat()).thenReturn(true);
        Mockito.when(tvf.getTFileType()).thenReturn(TFileType.FILE_LOCAL);
        Mockito.when(tvf.getFilePath()).thenReturn("/data/table.lance");
        desc.setTable(table);

        TVFScanNode node = new TVFScanNode(new PlanNodeId(0), desc, false, sv, ScanContext.EMPTY);
        List<Split> splits = node.getSplits(3);
        Assert.assertEquals(1, splits.size());

        LanceSplit split = (LanceSplit) splits.get(0);
        Assert.assertEquals("/data/table.lance", split.getDatasetUri());
        Assert.assertEquals(0L, split.getVersion());
        Assert.assertFalse(split.hasFragmentIds());

        TFileRangeDesc range = new TFileRangeDesc();
        node.setScanParams(range, split);
        Assert.assertEquals(0L, range.getTableFormatParams().getLanceParams().getVersion());
        Assert.assertFalse(range.getTableFormatParams().getLanceParams().isSetFragmentIds());
    }
}
