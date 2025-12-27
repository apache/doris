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

package org.apache.doris.datasource.hive.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.fs.DirectoryLister;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class HiveScanNodeTest {
    @Mock
    private SessionVariable sv;

    @Mock
    private HMSExternalTable hmsTable;

    @Mock
    private DirectoryLister directoryLister;

    @Mock
    private HiveMetaStoreCache cache;

    @Test
    public void testMaxFileSplitsNum() throws UserException, IOException {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(3));
        // Use reflection to set the table field
        try {
            java.lang.reflect.Field tableField = TupleDescriptor.class.getDeclaredField("table");
            tableField.setAccessible(true);
            tableField.set(desc, hmsTable);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        // Mock required for constructor
        org.apache.doris.datasource.hive.HMSExternalCatalog mockCatalog = Mockito.mock(org.apache.doris.datasource.hive.HMSExternalCatalog.class);
        Mockito.when(hmsTable.getCatalog()).thenReturn(mockCatalog);
        Mockito.when(mockCatalog.bindBrokerName()).thenReturn(null);

        HiveScanNode hiveScanNode = new HiveScanNode(new PlanNodeId(1), desc, false, sv, directoryLister);

        // Create large files that would generate many splits with small file_split_size
        // Total size: 1GB = 1024 * 1024 * 1024 bytes
        long totalFileSize = 1024L * 1024 * 1024;
        long fileSize1 = 400L * 1024 * 1024; // 400MB
        long fileSize2 = 300L * 1024 * 1024; // 300MB
        long fileSize3 = 324L * 1024 * 1024; // 324MB

        HiveMetaStoreCache.HiveFileStatus fileStatus1 = new HiveMetaStoreCache.HiveFileStatus();
        fileStatus1.setPath(LocationPath.of("file:///test/f1.parquet"));
        fileStatus1.setLength(fileSize1);
        fileStatus1.setBlockSize(128L * 1024 * 1024); // 128MB block size
        fileStatus1.setModificationTime(System.currentTimeMillis());
        fileStatus1.setSplittable(true);
        fileStatus1.setBlockLocations(null);

        HiveMetaStoreCache.HiveFileStatus fileStatus2 = new HiveMetaStoreCache.HiveFileStatus();
        fileStatus2.setPath(LocationPath.of("file:///test/f2.parquet"));
        fileStatus2.setLength(fileSize2);
        fileStatus2.setBlockSize(128L * 1024 * 1024);
        fileStatus2.setModificationTime(System.currentTimeMillis());
        fileStatus2.setSplittable(true);
        fileStatus2.setBlockLocations(null);

        HiveMetaStoreCache.HiveFileStatus fileStatus3 = new HiveMetaStoreCache.HiveFileStatus();
        fileStatus3.setPath(LocationPath.of("file:///test/f3.parquet"));
        fileStatus3.setLength(fileSize3);
        fileStatus3.setBlockSize(128L * 1024 * 1024);
        fileStatus3.setModificationTime(System.currentTimeMillis());
        fileStatus3.setSplittable(true);
        fileStatus3.setBlockLocations(null);

        HiveMetaStoreCache.FileCacheValue fileCacheValue = new HiveMetaStoreCache.FileCacheValue();
        fileCacheValue.getFiles().add(fileStatus1);
        fileCacheValue.getFiles().add(fileStatus2);
        fileCacheValue.getFiles().add(fileStatus3);
        fileCacheValue.setSplittable(true);
        fileCacheValue.setPartitionValues(Collections.emptyList());

        List<HiveMetaStoreCache.FileCacheValue> fileCaches = new ArrayList<>();
        fileCaches.add(fileCacheValue);

        // Base split size for testing
        long baseSplitSize = 10L * 1024 * 1024; // 10MB, small split size

        // Use reflection to access protected method adjustSplitSizeForTotalLimit
        HiveScanNode spyHiveScanNode = Mockito.spy(hiveScanNode);
        java.lang.reflect.Method adjustMethod;
        try {
            adjustMethod = org.apache.doris.datasource.FileQueryScanNode.class
                    .getDeclaredMethod("adjustSplitSizeForTotalLimit", List.class, long.class);
            adjustMethod.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        // Test case 1: max_file_splits_num = 50 (should limit split count)
        Mockito.when(sv.getMaxFileSplitsNum()).thenReturn(50);
        // Manually call the adjustment logic to test
        List<Long> fileSizes = new ArrayList<>();
        fileSizes.add(fileSize1);
        fileSizes.add(fileSize2);
        fileSizes.add(fileSize3);
        long minExpectedSplitSize1 = (totalFileSize + 50 - 1) / 50;
        long adjustedSplitSize1;
        try {
            adjustedSplitSize1 = (Long) adjustMethod.invoke(spyHiveScanNode, fileSizes, baseSplitSize);
        } catch (IllegalAccessException | java.lang.reflect.InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue("Split size should be adjusted to limit split count. Expected at least: "
                        + minExpectedSplitSize1 + ", got: " + adjustedSplitSize1,
                adjustedSplitSize1 >= minExpectedSplitSize1);

        // Test case 2: max_file_splits_num = 20 (should further limit split count)
        Mockito.when(sv.getMaxFileSplitsNum()).thenReturn(20);
        long minExpectedSplitSize2 = (totalFileSize + 20 - 1) / 20;
        long adjustedSplitSize2;
        try {
            adjustedSplitSize2 = (Long) adjustMethod.invoke(spyHiveScanNode, fileSizes, baseSplitSize);
        } catch (IllegalAccessException | java.lang.reflect.InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue("Split size should be adjusted to limit split count. Expected at least: "
                        + minExpectedSplitSize2 + ", got: " + adjustedSplitSize2,
                adjustedSplitSize2 >= minExpectedSplitSize2);

        // Test case 3: max_file_splits_num = 0 (no limit)
        Mockito.when(sv.getMaxFileSplitsNum()).thenReturn(0);
        long adjustedSplitSize3;
        try {
            adjustedSplitSize3 = (Long) adjustMethod.invoke(spyHiveScanNode, fileSizes, baseSplitSize);
        } catch (IllegalAccessException | java.lang.reflect.InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals("Without limit, should use base split size",
                baseSplitSize, adjustedSplitSize3);

        // Test case 4: max_file_splits_num = 200 (large limit, should not adjust)
        Mockito.when(sv.getMaxFileSplitsNum()).thenReturn(200);
        long adjustedSplitSize4;
        try {
            adjustedSplitSize4 = (Long) adjustMethod.invoke(spyHiveScanNode, fileSizes, baseSplitSize);
        } catch (IllegalAccessException | java.lang.reflect.InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        // With large limit, should use original split size
        Assert.assertEquals("With large limit, should use base split size",
                baseSplitSize, adjustedSplitSize4);
    }
}
