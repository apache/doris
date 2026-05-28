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
import org.apache.doris.datasource.FileScanNode;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

public class HiveScanNodeTest {
    private static final long GB = 1024L * 1024L * 1024L;

    @Test
    public void testDetermineTargetFileSplitSizeTinySplit() throws Exception {
        // File size 10GB <= 20GB threshold -> TINY_SPLIT_FILE_SIZE (2MB)
        SessionVariable sv = new SessionVariable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.bindBrokerName()).thenReturn("");
        desc.setTable(table);
        HiveScanNode node = new HiveScanNode(new PlanNodeId(0), desc, false, sv, null, ScanContext.EMPTY);

        HiveMetaStoreCache.FileCacheValue fileCacheValue = new HiveMetaStoreCache.FileCacheValue();
        HiveMetaStoreCache.HiveFileStatus status = new HiveMetaStoreCache.HiveFileStatus();
        status.setLength(10L * GB);
        fileCacheValue.getFiles().add(status);
        List<HiveMetaStoreCache.FileCacheValue> caches = Collections.singletonList(fileCacheValue);

        Method method = HiveScanNode.class.getDeclaredMethod(
                "determineTargetFileSplitSize", List.class, boolean.class);
        method.setAccessible(true);
        long target = (long) method.invoke(node, caches, false);
        Assert.assertEquals(FileScanNode.TINY_SPLIT_FILE_SIZE, target);
    }

    @Test
    public void testDetermineTargetFileSplitSizeHugeSplit() throws Exception {
        // File size 200GB <= 320GB threshold -> HUGE_SPLIT_FILE_SIZE (32MB)
        SessionVariable sv = new SessionVariable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.bindBrokerName()).thenReturn("");
        desc.setTable(table);
        HiveScanNode node = new HiveScanNode(new PlanNodeId(0), desc, false, sv, null, ScanContext.EMPTY);

        HiveMetaStoreCache.FileCacheValue fileCacheValue = new HiveMetaStoreCache.FileCacheValue();
        HiveMetaStoreCache.HiveFileStatus status = new HiveMetaStoreCache.HiveFileStatus();
        status.setLength(200L * GB);
        fileCacheValue.getFiles().add(status);
        List<HiveMetaStoreCache.FileCacheValue> caches = Collections.singletonList(fileCacheValue);

        Method method = HiveScanNode.class.getDeclaredMethod(
                "determineTargetFileSplitSize", List.class, boolean.class);
        method.setAccessible(true);
        long target = (long) method.invoke(node, caches, false);
        Assert.assertEquals(FileScanNode.HUGE_SPLIT_FILE_SIZE, target);
    }

    @Test
    public void testGetSelectedFileSize() throws Exception {
        SessionVariable sv = new SessionVariable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.bindBrokerName()).thenReturn("");
        Mockito.when(table.getDbName()).thenReturn("test_db");
        Mockito.when(table.getName()).thenReturn("test_table");
        desc.setTable(table);
        HiveScanNode node = new HiveScanNode(new PlanNodeId(0), desc, false, sv, null, ScanContext.EMPTY);

        HiveMetaStoreCache.FileCacheValue fileCacheValue = new HiveMetaStoreCache.FileCacheValue();
        HiveMetaStoreCache.HiveFileStatus status = new HiveMetaStoreCache.HiveFileStatus();
        status.setLength(1000L);
        fileCacheValue.getFiles().add(status);
        HiveMetaStoreCache.HiveFileStatus status2 = new HiveMetaStoreCache.HiveFileStatus();
        status2.setLength(2000L);
        fileCacheValue.getFiles().add(status2);
        List<HiveMetaStoreCache.FileCacheValue> caches = Collections.singletonList(fileCacheValue);

        Method method = HiveScanNode.class.getDeclaredMethod("getSelectedFileSize", List.class);
        method.setAccessible(true);
        long selectedFileSize = (long) method.invoke(node, caches);
        Assert.assertEquals(3000L, selectedFileSize);
    }

    @Test
    public void testGetSelectedFileSizeExceedsTotalScanBytes() throws Exception {
        // When ConnectContext.get() is null, the cross-table limit check is skipped.
        // This test verifies that getSelectedFileSize works correctly without ConnectContext.
        SessionVariable sv = new SessionVariable();
        sv.maxSelectedTotalScanBytes = 2000L;
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.bindBrokerName()).thenReturn("");
        Mockito.when(table.getDbName()).thenReturn("test_db");
        Mockito.when(table.getName()).thenReturn("test_table");
        desc.setTable(table);
        HiveScanNode node = new HiveScanNode(new PlanNodeId(0), desc, false, sv, null, ScanContext.EMPTY);

        HiveMetaStoreCache.FileCacheValue fileCacheValue = new HiveMetaStoreCache.FileCacheValue();
        HiveMetaStoreCache.HiveFileStatus status = new HiveMetaStoreCache.HiveFileStatus();
        status.setLength(3000L);
        fileCacheValue.getFiles().add(status);
        List<HiveMetaStoreCache.FileCacheValue> caches = Collections.singletonList(fileCacheValue);

        Method method = HiveScanNode.class.getDeclaredMethod("getSelectedFileSize", List.class);
        method.setAccessible(true);
        // Without ConnectContext, no exception should be thrown even if file size exceeds limit
        long selectedFileSize = (long) method.invoke(node, caches);
        Assert.assertEquals(3000L, selectedFileSize);
    }
}
