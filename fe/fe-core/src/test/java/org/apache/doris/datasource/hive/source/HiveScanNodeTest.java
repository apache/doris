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
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveExternalMetaCache;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileTextScanRangeParams;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

public class HiveScanNodeTest {
    private static final long MB = 1024L * 1024L;

    @Test
    public void testDetermineTargetFileSplitSizeHonorsMaxFileSplitNum() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.bindBrokerName()).thenReturn("");
        desc.setTable(table);
        HiveScanNode node = new HiveScanNode(new PlanNodeId(0), desc, false, sv, null, ScanContext.EMPTY);

        HiveExternalMetaCache.FileCacheValue fileCacheValue = new HiveExternalMetaCache.FileCacheValue();
        HiveExternalMetaCache.HiveFileStatus status = new HiveExternalMetaCache.HiveFileStatus();
        status.setLength(10_000L * MB);
        fileCacheValue.getFiles().add(status);
        List<HiveExternalMetaCache.FileCacheValue> caches = Collections.singletonList(fileCacheValue);

        Method method = HiveScanNode.class.getDeclaredMethod(
                "determineTargetFileSplitSize", List.class, boolean.class);
        method.setAccessible(true);
        long target = (long) method.invoke(node, caches, false);
        Assert.assertEquals(100 * MB, target);
    }

    @Test
    public void testDetermineTargetFileSplitSizeKeepsInitialSize() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.bindBrokerName()).thenReturn("");
        desc.setTable(table);
        HiveScanNode node = new HiveScanNode(new PlanNodeId(0), desc, false, sv, null, ScanContext.EMPTY);

        HiveExternalMetaCache.FileCacheValue fileCacheValue = new HiveExternalMetaCache.FileCacheValue();
        HiveExternalMetaCache.HiveFileStatus status = new HiveExternalMetaCache.HiveFileStatus();
        status.setLength(500L * MB);
        fileCacheValue.getFiles().add(status);
        List<HiveExternalMetaCache.FileCacheValue> caches = Collections.singletonList(fileCacheValue);

        Method method = HiveScanNode.class.getDeclaredMethod(
                "determineTargetFileSplitSize", List.class, boolean.class);
        method.setAccessible(true);
        long target = (long) method.invoke(node, caches, false);
        Assert.assertEquals(32 * MB, target);
    }

    @Test
    public void testSelectedPartitionsCarryPartitionPredicateFlag() {
        SelectedPartitions selectedPartitions = new SelectedPartitions(3, ImmutableMap.of(), true, true);
        Assert.assertTrue(selectedPartitions.hasPartitionPredicate);
    }

    @Test
    public void testHiveScanNodeExposePartitionPredicateFlag() {
        HiveScanNode node = createHiveScanNode();
        node.setSelectedPartitions(new SelectedPartitions(3, ImmutableMap.of(), true, true));
        Assert.assertTrue(node.hasPartitionPredicate());
    }

    @Test
    public void testHiveScanNodeExposePartitionedTableFlag() {
        HiveScanNode node = createHiveScanNode(true);
        Assert.assertTrue(node.isPartitionedTable());
    }

    @Test
    public void testHiveScanNodeExposeMissingPartitionPredicateFlag() {
        HiveScanNode node = createHiveScanNode();
        node.setSelectedPartitions(new SelectedPartitions(3, ImmutableMap.of(), true, false));
        Assert.assertFalse(node.hasPartitionPredicate());
    }

    @Test
    public void testMarkTransactionalHiveScanParams() {
        TFileScanRangeParams scanParams = new TFileScanRangeParams();
        HiveScanNode.markTransactionalHiveScanParams(scanParams);

        Assert.assertTrue(scanParams.isSetTableFormatParams());
        Assert.assertEquals(TableFormatType.TRANSACTIONAL_HIVE.value(),
                scanParams.getTableFormatParams().getTableFormatType());
    }

    @Test
    public void testTrimDoubleQuotesOnlyForDoubleQuoteEnclose() {
        TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
        textParams.setEnclose((byte) '"');
        Assert.assertTrue(HiveScanNode.shouldTrimDoubleQuotes(textParams));

        textParams.setEnclose((byte) '\'');
        Assert.assertFalse(HiveScanNode.shouldTrimDoubleQuotes(textParams));
    }

    @Test
    public void testGetFileSplitBumpsUpdateTimeWhenFileMtimeNewer() throws Exception {
        long oldUpdateTime = 1_000L;
        long newMtime = 5_000L;
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HiveScanNode node = newHiveScanNodeWithFileSplitter(table);
        Mockito.when(table.getUpdateTime()).thenReturn(oldUpdateTime);

        HiveExternalMetaCache cache = Mockito.mock(HiveExternalMetaCache.class);
        Mockito.when(cache.getFilesByPartitions(
                Mockito.anyList(), Mockito.anyBoolean(), Mockito.anyBoolean(),
                Mockito.any(), Mockito.any()))
                .thenReturn(Collections.singletonList(buildFileCacheValue(newMtime)));

        List<Split> allFiles = Lists.newArrayList();
        invokeGetFileSplitByPartitions(node, cache, allFiles);

        Mockito.verify(table).setUpdateTime(newMtime);
        Assert.assertFalse("splitter should have produced splits for a non-empty file", allFiles.isEmpty());
    }

    @Test
    public void testGetFileSplitDoesNotBumpUpdateTimeWhenFileMtimeNotNewer() throws Exception {
        long oldUpdateTime = 5_000L;
        long olderMtime = 1_000L;
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HiveScanNode node = newHiveScanNodeWithFileSplitter(table);
        Mockito.when(table.getUpdateTime()).thenReturn(oldUpdateTime);

        HiveExternalMetaCache cache = Mockito.mock(HiveExternalMetaCache.class);
        Mockito.when(cache.getFilesByPartitions(
                Mockito.anyList(), Mockito.anyBoolean(), Mockito.anyBoolean(),
                Mockito.any(), Mockito.any()))
                .thenReturn(Collections.singletonList(buildFileCacheValue(olderMtime)));

        List<Split> allFiles = Lists.newArrayList();
        invokeGetFileSplitByPartitions(node, cache, allFiles);

        Mockito.verify(table, Mockito.never()).setUpdateTime(Mockito.anyLong());
    }

    private static HiveExternalMetaCache.FileCacheValue buildFileCacheValue(long modificationTime) {
        HiveExternalMetaCache.FileCacheValue fileCacheValue = new HiveExternalMetaCache.FileCacheValue();
        HiveExternalMetaCache.HiveFileStatus status = new HiveExternalMetaCache.HiveFileStatus();
        status.setPath(LocationPath.of("hdfs://test-host:9000/warehouse/tbl/f1"));
        status.setLength(10_000L * MB);
        status.setModificationTime(modificationTime);
        fileCacheValue.setSplittable(false);
        fileCacheValue.getFiles().add(status);
        return fileCacheValue;
    }

    private static HiveScanNode newHiveScanNodeWithFileSplitter(HMSExternalTable table) throws Exception {
        SessionVariable sv = new SessionVariable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.bindBrokerName()).thenReturn("");
        desc.setTable(table);
        HiveScanNode node = new HiveScanNode(
                new PlanNodeId(0), desc, false, sv, null, ScanContext.EMPTY);
        // The fileSplitter field is normally initialized in FileQueryScanNode.doInitialize(),
        // which requires a fully wired external table / schema; for this test we only need a
        // non-null splitter so the loop body can call splitFile(...). A real FileSplitter works
        // because the crafted file is non-splittable, so splitFile takes its early single-split
        // branch and never reaches filesystem APIs.
        Field fileSplitterField = node.getClass().getSuperclass().getDeclaredField("fileSplitter");
        fileSplitterField.setAccessible(true);
        fileSplitterField.set(node, new FileSplitter(32 * MB, 32 * MB, 100));
        return node;
    }

    private static void invokeGetFileSplitByPartitions(
            HiveScanNode node, HiveExternalMetaCache cache, List<Split> allFiles) throws Exception {
        Method method = HiveScanNode.class.getDeclaredMethod(
                "getFileSplitByPartitions",
                HiveExternalMetaCache.class, List.class, List.class,
                String.class, int.class, boolean.class);
        method.setAccessible(true);
        method.invoke(node, cache, Collections.emptyList(), allFiles, "", 1, false);
    }

    private HiveScanNode createHiveScanNode() {
        return createHiveScanNode(false);
    }

    private HiveScanNode createHiveScanNode(boolean partitioned) {
        SessionVariable sv = new SessionVariable();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.bindBrokerName()).thenReturn("");
        Mockito.when(table.isPartitionedTable()).thenReturn(partitioned);
        desc.setTable(table);
        return new HiveScanNode(new PlanNodeId(0), desc, false, sv, null, ScanContext.EMPTY);
    }
}
