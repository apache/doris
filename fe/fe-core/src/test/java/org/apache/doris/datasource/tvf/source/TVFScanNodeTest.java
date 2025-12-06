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

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileType;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class TVFScanNodeTest {
    @Mock
    private SessionVariable sv;

    @Mock
    private FunctionGenTable functionGenTable;

    @Mock
    private ExternalFileTableValuedFunction tableValuedFunction;

    @Test
    public void testMaxFileSplitsNum() throws UserException {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(3));
        Mockito.when(functionGenTable.getTvf()).thenReturn(tableValuedFunction);
        desc.setTable(functionGenTable);
        Mockito.when(tableValuedFunction.getTFileType()).thenReturn(TFileType.FILE_LOCAL);

        TVFScanNode tvfScanNode = new TVFScanNode(new PlanNodeId(1), desc, false, sv);

        // Create large files that would generate many splits with small file_split_size
        // Total size: 1GB = 1024 * 1024 * 1024 bytes
        long totalFileSize = 1024L * 1024 * 1024;
        long fileSize1 = 400L * 1024 * 1024; // 400MB
        long fileSize2 = 300L * 1024 * 1024; // 300MB
        long fileSize3 = 324L * 1024 * 1024; // 324MB

        TBrokerFileStatus fileStatus1 = new TBrokerFileStatus();
        fileStatus1.setPath("file:///test/f1.parquet");
        fileStatus1.setSize(fileSize1);
        fileStatus1.setBlockSize(128L * 1024 * 1024); // 128MB block size
        fileStatus1.setModificationTime(System.currentTimeMillis());
        fileStatus1.setIsSplitable(true);

        TBrokerFileStatus fileStatus2 = new TBrokerFileStatus();
        fileStatus2.setPath("file:///test/f2.parquet");
        fileStatus2.setSize(fileSize2);
        fileStatus2.setBlockSize(128L * 1024 * 1024);
        fileStatus2.setModificationTime(System.currentTimeMillis());
        fileStatus2.setIsSplitable(true);

        TBrokerFileStatus fileStatus3 = new TBrokerFileStatus();
        fileStatus3.setPath("file:///test/f3.parquet");
        fileStatus3.setSize(fileSize3);
        fileStatus3.setBlockSize(128L * 1024 * 1024);
        fileStatus3.setModificationTime(System.currentTimeMillis());
        fileStatus3.setIsSplitable(true);

        List<TBrokerFileStatus> fileStatuses = new ArrayList<>();
        fileStatuses.add(fileStatus1);
        fileStatuses.add(fileStatus2);
        fileStatuses.add(fileStatus3);

        Mockito.when(tableValuedFunction.getFileStatuses()).thenReturn(fileStatuses);

        // Base split size for testing
        long baseSplitSize = 10L * 1024 * 1024; // 10MB, small split size
        Mockito.when(sv.getFileSplitSize()).thenReturn(baseSplitSize);

        // Test case 1: max_file_splits_num = 50 (should limit split count)
        Mockito.when(sv.getMaxFileSplitsNum()).thenReturn(50);
        List<Split> splits1 = tvfScanNode.getSplits(1);
        // With 1GB total and 10MB split size, would generate ~102 splits without limit
        // With limit of 50, should generate at most 50 splits (allow small tolerance due to split algorithm)
        Assert.assertTrue("Split count should be limited to around 50, actual: " + splits1.size(),
                splits1.size() <= 52); // Allow small tolerance for split algorithm boundary cases
        // Verify split size was adjusted: minSplitSize = ceil(1GB / 50) = ~20MB
        long minExpectedSplitSize1 = (totalFileSize + 50 - 1) / 50;
        for (Split split : splits1) {
            FileSplit fileSplit = (FileSplit) split;
            Assert.assertNotNull("Split should have target split size", fileSplit.getTargetSplitSize());
            // Adjusted split size should be at least ceil(totalFileSize / maxFileSplitsNum)
            Assert.assertTrue("Split size should be adjusted to limit split count. Expected at least: "
                            + minExpectedSplitSize1 + ", got: " + fileSplit.getTargetSplitSize(),
                    fileSplit.getTargetSplitSize() >= minExpectedSplitSize1);
        }

        // Test case 2: max_file_splits_num = 20 (should further limit split count)
        Mockito.when(sv.getMaxFileSplitsNum()).thenReturn(20);
        List<Split> splits2 = tvfScanNode.getSplits(1);
        Assert.assertTrue("Split count should be limited to around 20, actual: " + splits2.size(),
                splits2.size() <= 22); // Allow small tolerance for split algorithm boundary cases
        // Adjusted split size should be at least ceil(1GB / 20) = ~50MB
        long minExpectedSplitSize2 = (totalFileSize + 20 - 1) / 20;
        for (Split split : splits2) {
            FileSplit fileSplit = (FileSplit) split;
            Assert.assertTrue("Split size should be adjusted to limit split count. Expected at least: "
                            + minExpectedSplitSize2 + ", got: " + fileSplit.getTargetSplitSize(),
                    fileSplit.getTargetSplitSize() >= minExpectedSplitSize2);
        }

        // Test case 3: max_file_splits_num = 0 (no limit)
        Mockito.when(sv.getMaxFileSplitsNum()).thenReturn(0);
        List<Split> splits3 = tvfScanNode.getSplits(1);
        // Without limit, should generate splits based on file_split_size (10MB)
        // Expected: approximately ceil(1GB / 10MB) = 102 splits
        long expectedSplitsWithoutLimit = (totalFileSize + baseSplitSize - 1) / baseSplitSize;
        Assert.assertTrue("Without limit, should generate more splits. Expected around: "
                        + expectedSplitsWithoutLimit + ", got: " + splits3.size(),
                splits3.size() >= expectedSplitsWithoutLimit - 5); // Allow some tolerance

        // Test case 4: max_file_splits_num = 200 (large limit, should not adjust)
        Mockito.when(sv.getMaxFileSplitsNum()).thenReturn(200);
        List<Split> splits4 = tvfScanNode.getSplits(1);
        // With large limit, should use original split size (10MB)
        // Should generate approximately the same number of splits as case 3
        Assert.assertTrue("With large limit, should generate similar number of splits. Expected around: "
                        + expectedSplitsWithoutLimit + ", got: " + splits4.size(),
                splits4.size() >= expectedSplitsWithoutLimit - 5);
    }
}
