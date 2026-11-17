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
import org.apache.doris.datasource.scan.FileQueryScanNode;
import org.apache.doris.datasource.split.FileSplitter;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPushAggOp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

public class TVFScanNodeTest {
    private static final long MB = 1024L * 1024L;

    @Test
    public void testCoarseRangesHonorMaxFileSplitNum() throws Exception {
        for (TFileFormatType format : List.of(TFileFormatType.FORMAT_PARQUET, TFileFormatType.FORMAT_ORC)) {
            SessionVariable sv = new SessionVariable();
            sv.setMaxFileSplitNum(1);
            TBrokerFileStatus file = splittableFile("s3://bucket/large", 2048 * MB);
            file.setModificationTime(17);
            TVFScanNode node = fileScanNode(sv, TFileType.FILE_S3, format, file);
            List<Split> splits = node.getSplits(1);
            Assertions.assertEquals(1, splits.size());
            Assertions.assertEquals(2048 * MB, splits.get(0).getLength());
        }
    }

    @Test
    public void testHttpRetainsFineRangesWhileStableStorageUsesCoarseRanges() throws Exception {
        for (TFileFormatType format : List.of(TFileFormatType.FORMAT_PARQUET, TFileFormatType.FORMAT_ORC)) {
            for (long explicitSize : new long[] {0, 16 * MB}) {
                SessionVariable sv = new SessionVariable();
                sv.setFileSplitSize(explicitSize);
                TBrokerFileStatus httpFile = splittableFile("http://example.test/data", 256 * MB);
                TVFScanNode http = fileScanNode(sv, TFileType.FILE_HTTP, format, httpFile);
                Assertions.assertEquals(explicitSize > 0 ? 16 : 8, http.getSplits(1).size());
                for (TFileType type : List.of(TFileType.FILE_S3, TFileType.FILE_HDFS)) {
                    String path = type == TFileType.FILE_S3 ? "s3://bucket/data" : "hdfs://host/data";
                    TBrokerFileStatus stableFile = splittableFile(path, 256 * MB);
                    stableFile.setModificationTime(17);
                    TVFScanNode stable = fileScanNode(sv, type, format, stableFile);
                    Assertions.assertEquals(1, stable.getSplits(1).size());
                }
            }
        }
    }

    private static TVFScanNode fileScanNode(SessionVariable sv, TFileType type,
            TFileFormatType format, TBrokerFileStatus file) throws Exception {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        FunctionGenTable table = Mockito.mock(FunctionGenTable.class);
        ExternalFileTableValuedFunction tvf = Mockito.mock(ExternalFileTableValuedFunction.class);
        Mockito.when(table.getTvf()).thenReturn(tvf);
        Mockito.when(tvf.getTFileType()).thenReturn(type);
        Mockito.when(tvf.getTFileFormatType()).thenReturn(format);
        Mockito.when(tvf.getFileStatuses()).thenReturn(List.of(file));
        desc.setTable(table);
        TVFScanNode node = new TVFScanNode(new PlanNodeId(0), desc, false, sv, ScanContext.EMPTY);
        setFileSplitter(node, new FileSplitter(32 * MB, 32 * MB, 0));
        return node;
    }

    @Test
    public void testDetermineTargetFileSplitSizeUsesCoarseSizeForParquet() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setFileSplitSize(16 * MB);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        FunctionGenTable table = Mockito.mock(FunctionGenTable.class);
        ExternalFileTableValuedFunction tvf = Mockito.mock(ExternalFileTableValuedFunction.class);
        Mockito.when(table.getTvf()).thenReturn(tvf);
        Mockito.when(tvf.getTFileFormatType()).thenReturn(org.apache.doris.thrift.TFileFormatType.FORMAT_PARQUET);
        desc.setTable(table);
        TVFScanNode node = new TVFScanNode(new PlanNodeId(0), desc, false, sv, ScanContext.EMPTY);
        Method method = TVFScanNode.class.getDeclaredMethod("determineTargetFileSplitSize", List.class);
        method.setAccessible(true);
        List<TBrokerFileStatus> statuses = List.of(splittableFile("s3://bucket/large.parquet", 1024 * MB));
        Assertions.assertEquals(512 * MB, (long) method.invoke(node, statuses));
        sv.maxFileScannersConcurrency = 1;
        Assertions.assertEquals(16 * MB, (long) method.invoke(node, statuses));
    }

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
        Mockito.when(tvf.getFileStatuses()).thenReturn(List.of(
                splittableFile("file:///tmp/count_col_1.parquet", 128 * MB),
                splittableFile("file:///tmp/count_col_2.parquet", 128 * MB)));
        desc.setTable(table);

        TVFScanNode countColumnNode = new TVFScanNode(
                new PlanNodeId(0), desc, false, sv, ScanContext.EMPTY);
        setFileSplitter(countColumnNode, new FileSplitter(32 * MB, 32 * MB, 0));
        countColumnNode.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        countColumnNode.setPushDownCountSlotIds(Collections.singletonList(new SlotId(7)));

        List<Split> countColumnSplits = countColumnNode.getSplits(1);
        Assertions.assertEquals(8, countColumnSplits.size());

        TVFScanNode countStarNode = new TVFScanNode(
                new PlanNodeId(1), desc, false, sv, ScanContext.EMPTY);
        setFileSplitter(countStarNode, new FileSplitter(32 * MB, 32 * MB, 0));
        countStarNode.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        countStarNode.setPushDownCountSlotIds(Collections.emptyList());

        List<Split> countStarSplits = countStarNode.getSplits(1);
        Assertions.assertEquals(2, countStarSplits.size());
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
        Assertions.assertEquals(100 * MB, target);
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
}
