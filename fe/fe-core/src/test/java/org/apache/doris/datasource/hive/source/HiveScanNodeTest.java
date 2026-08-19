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

import org.apache.doris.analysis.TableSample;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.hive.HMSCachedClient;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveExternalMetaCache;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.datasource.hive.HiveTransaction;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileTextScanRangeParams;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

public class HiveScanNodeTest {
    private static final long MB = 1024L * 1024L;

    @Test
    public void testStatementCacheReusesListingOnlyForSamePartitionIdentity() throws Exception {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext(context, null);
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
            HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
            Mockito.when(table.getCatalog()).thenReturn(catalog);
            Mockito.when(table.getId()).thenReturn(2L);
            Mockito.when(catalog.getId()).thenReturn(1L);
            Mockito.when(catalog.bindBrokerName()).thenReturn("");
            HiveScanNode firstNode = createHiveScanNode(0, table);
            HiveScanNode secondNode = createHiveScanNode(1, table);
            HiveExternalMetaCache cache = Mockito.mock(HiveExternalMetaCache.class);
            Mockito.when(cache.getFilesByPartitions(
                    Mockito.anyList(), Mockito.anyBoolean(), Mockito.anyBoolean(),
                    Mockito.isNull(), Mockito.eq(table))).thenReturn(Collections.emptyList());
            List<HivePartition> firstPartitions = Collections.singletonList(new HivePartition(
                    null, false, "parquet", "hdfs://warehouse/t/p=1",
                    Collections.singletonList("1"), Collections.emptyMap()));
            List<HivePartition> equivalentPartitions = Collections.singletonList(new HivePartition(
                    null, false, "parquet", "hdfs://warehouse/t/p=1",
                    Collections.singletonList("1"), Collections.emptyMap()));
            List<HivePartition> differentPartitions = Collections.singletonList(new HivePartition(
                    null, false, "parquet", "hdfs://warehouse/t/p=2",
                    Collections.singletonList("2"), Collections.emptyMap()));

            invokeGetFileSplitByPartitions(firstNode, cache, firstPartitions);
            invokeGetFileSplitByPartitions(secondNode, cache, equivalentPartitions);
            invokeGetFileSplitByPartitions(secondNode, cache, differentPartitions);

            Mockito.verify(cache, Mockito.times(3)).getFilesByPartitions(
                    Mockito.anyList(), Mockito.eq(true), Mockito.eq(false),
                    Mockito.isNull(), Mockito.eq(table));
            Mockito.verify(cache, Mockito.times(3)).getFileCacheInvalidationGeneration(1L);
            Mockito.verifyNoMoreInteractions(cache);
        } finally {
            statementContext.close();
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testStatementCacheSeparatesTableAndPartitionFileInvalidations() throws Exception {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext(context, null);
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        ThreadPoolExecutor executor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "refresh", 1, false);
        ThreadPoolExecutor listExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                1, 1, "file", 1, false);
        try {
            HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
            HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
            Mockito.when(table.getCatalog()).thenReturn(catalog);
            Mockito.when(table.getId()).thenReturn(2L);
            Mockito.when(catalog.getId()).thenReturn(0L);
            Mockito.when(catalog.bindBrokerName()).thenReturn("");

            HiveExternalMetaCache realCache = new HiveExternalMetaCache(executor, listExecutor);
            realCache.initCatalog(0L, new HashMap<>());
            HiveExternalMetaCache cache = Mockito.spy(realCache);
            Mockito.doReturn(Collections.emptyList()).when(cache).getFilesByPartitions(
                    Mockito.anyList(), Mockito.eq(true), Mockito.anyBoolean(),
                    Mockito.isNull(), Mockito.eq(table));

            List<HivePartition> partitions = Collections.singletonList(new HivePartition(
                    null, false, "parquet", "hdfs://warehouse/t/p=1",
                    Collections.singletonList("1"), Collections.emptyMap()));

            invokeGetFileSplitByPartitions(createHiveScanNode(0, table), cache, partitions);
            invokeGetFileSplitByPartitions(createHiveScanNode(1, table), cache, partitions);
            Mockito.verify(cache, Mockito.times(2)).getFilesByPartitions(
                    Mockito.same(partitions), Mockito.eq(true), Mockito.eq(false),
                    Mockito.isNull(), Mockito.eq(table));

            NameMapping nameMapping = NameMapping.createForTest("db", "table");
            cache.invalidateTable(0L, "db", "table");
            invokeGetFileSplitByPartitions(createHiveScanNode(2, table), cache, partitions);

            cache.invalidatePartitionCache(nameMapping, "p=1");
            invokeGetFileSplitByPartitions(createHiveScanNode(3, table), cache, partitions);

            Mockito.verify(cache, Mockito.times(4)).getFilesByPartitions(
                    Mockito.same(partitions), Mockito.eq(true), Mockito.eq(false),
                    Mockito.isNull(), Mockito.eq(table));
            Assert.assertEquals(2L, cache.getFileCacheInvalidationGeneration(0L));
        } finally {
            executor.shutdownNow();
            listExecutor.shutdownNow();
            statementContext.close();
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testStatementCacheSeparatesAutomaticFileCacheReplacement() throws Exception {
        HivePartition partition = new HivePartition(null, false, "parquet", "hdfs://warehouse/t/p=1",
                Collections.singletonList("1"), Collections.emptyMap());
        HiveExternalMetaCache.FileCacheValue firstValue = new HiveExternalMetaCache.FileCacheValue();
        firstValue.setCacheGeneration(1L);
        HiveExternalMetaCache.FileCacheValue replacementValue = new HiveExternalMetaCache.FileCacheValue();
        replacementValue.setCacheGeneration(2L);

        Object firstKey = newHiveFileScanTaskCacheKey(partition, firstValue);
        Object sameKey = newHiveFileScanTaskCacheKey(partition, firstValue);
        Object replacementKey = newHiveFileScanTaskCacheKey(partition, replacementValue);

        Assert.assertEquals(firstKey, sameKey);
        Assert.assertEquals(firstKey.hashCode(), sameKey.hashCode());
        Assert.assertNotEquals(firstKey, replacementKey);
    }

    @Test
    public void testBatchListingBypassesStatementCache() throws Exception {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext(context, null);
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
            HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
            Mockito.when(table.getCatalog()).thenReturn(catalog);
            Mockito.when(table.getId()).thenReturn(2L);
            Mockito.when(catalog.getId()).thenReturn(1L);
            Mockito.when(catalog.bindBrokerName()).thenReturn("");
            HiveScanNode firstNode = createHiveScanNode(0, table);
            HiveScanNode secondNode = createHiveScanNode(1, table);
            HiveExternalMetaCache cache = Mockito.mock(HiveExternalMetaCache.class);
            Mockito.when(cache.getFilesByPartitions(
                    Mockito.anyList(), Mockito.anyBoolean(), Mockito.anyBoolean(),
                    Mockito.isNull(), Mockito.eq(table))).thenReturn(Collections.emptyList());
            List<HivePartition> partitions = Collections.singletonList(new HivePartition(
                    null, false, "parquet", "hdfs://warehouse/t/p=1",
                    Collections.singletonList("1"), Collections.emptyMap()));

            invokeGetFileSplitByPartitions(firstNode, cache, partitions, true);
            invokeGetFileSplitByPartitions(secondNode, cache, partitions, true);

            Mockito.verify(cache, Mockito.times(2)).getFilesByPartitions(
                    Mockito.same(partitions), Mockito.anyBoolean(), Mockito.eq(false),
                    Mockito.isNull(), Mockito.eq(table));
        } finally {
            statementContext.close();
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testDisabledGlobalFileCacheBypassesStatementCache() throws Exception {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext(context, null);
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        long previousMaxFileCacheNum = Config.max_external_file_cache_num;
        try {
            Config.max_external_file_cache_num = 0;
            HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
            HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
            Mockito.when(table.getCatalog()).thenReturn(catalog);
            Mockito.when(table.getId()).thenReturn(2L);
            Mockito.when(catalog.getId()).thenReturn(1L);
            Mockito.when(catalog.bindBrokerName()).thenReturn("");
            HiveScanNode firstNode = createHiveScanNode(0, table);
            HiveScanNode secondNode = createHiveScanNode(1, table);
            HiveExternalMetaCache cache = Mockito.mock(HiveExternalMetaCache.class);
            Mockito.when(cache.getFilesByPartitions(
                    Mockito.anyList(), Mockito.eq(false), Mockito.anyBoolean(),
                    Mockito.isNull(), Mockito.eq(table))).thenReturn(Collections.emptyList());
            List<HivePartition> partitions = Collections.singletonList(new HivePartition(
                    null, false, "parquet", "hdfs://warehouse/t",
                    Collections.emptyList(), Collections.emptyMap()));

            invokeGetFileSplitByPartitions(firstNode, cache, partitions);
            invokeGetFileSplitByPartitions(secondNode, cache, partitions);

            Mockito.verify(cache, Mockito.times(2)).getFilesByPartitions(
                    Mockito.same(partitions), Mockito.eq(false), Mockito.eq(false),
                    Mockito.isNull(), Mockito.eq(table));
        } finally {
            Config.max_external_file_cache_num = previousMaxFileCacheNum;
            statementContext.close();
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testHiveRetentionWeightIncludesEmptyPartitions() throws Exception {
        HiveScanNode node = createHiveScanNode();
        Field maxTasks = FileQueryScanNode.class.getDeclaredField("maxRetainedExternalScanTasks");
        maxTasks.setAccessible(true);
        maxTasks.set(node, 10L);
        List<HiveExternalMetaCache.FileCacheValue> fileCaches = Arrays.asList(
                new HiveExternalMetaCache.FileCacheValue(),
                new HiveExternalMetaCache.FileCacheValue());
        Method method = HiveScanNode.class.getDeclaredMethod("retainedHiveFileCount", List.class);
        method.setAccessible(true);

        Assert.assertEquals(2L, method.invoke(node, fileCaches));

        HiveExternalMetaCache.HiveFileStatus status = new HiveExternalMetaCache.HiveFileStatus();
        fileCaches.get(0).getFiles().add(status);
        Assert.assertEquals(3L, method.invoke(node, fileCaches));
    }

    @Test
    public void testTableSampleDoesNotMutateCachedFileStatus() throws Exception {
        HiveScanNode node = createHiveScanNode();
        node.setTableSample(new TableSample(true, 100L, 0L));
        HiveExternalMetaCache.HiveFileStatus cachedStatus =
                new HiveExternalMetaCache.HiveFileStatus();
        cachedStatus.setLength(10L);
        HiveExternalMetaCache.FileCacheValue cacheValue =
                new HiveExternalMetaCache.FileCacheValue();
        cacheValue.setSplittable(true);
        cacheValue.setPartitionValues(Arrays.asList("2026", "08"));
        cacheValue.getFiles().add(cachedStatus);

        Method method = HiveScanNode.class.getDeclaredMethod("selectFiles", List.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<HiveExternalMetaCache.HiveFileStatus> sampled =
                (List<HiveExternalMetaCache.HiveFileStatus>) method.invoke(
                        node, Collections.singletonList(cacheValue));

        Assert.assertEquals(1, sampled.size());
        Assert.assertNotSame(cachedStatus, sampled.get(0));
        Assert.assertTrue(sampled.get(0).isSplittable());
        Assert.assertEquals(Arrays.asList("2026", "08"), sampled.get(0).getPartitionValues());
        Assert.assertFalse(cachedStatus.isSplittable());
        Assert.assertNull(cachedStatus.getPartitionValues());
    }

    @Test
    public void testSmallTableSampleCopiesOnlySelectedFiles() throws Exception {
        HiveScanNode node = createHiveScanNode();
        node.setTableSample(new TableSample(true, 1L, 0L));
        HiveExternalMetaCache.FileCacheValue cacheValue =
                new HiveExternalMetaCache.FileCacheValue();
        cacheValue.setSplittable(true);
        for (int index = 0; index < 1_000; index++) {
            HiveExternalMetaCache.HiveFileStatus cachedStatus =
                    new HiveExternalMetaCache.HiveFileStatus();
            cachedStatus.setLength(1L);
            cacheValue.getFiles().add(cachedStatus);
        }

        Method method = HiveScanNode.class.getDeclaredMethod("selectFiles", List.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<HiveExternalMetaCache.HiveFileStatus> sampled =
                (List<HiveExternalMetaCache.HiveFileStatus>) method.invoke(
                        node, Collections.singletonList(cacheValue));

        Assert.assertEquals(10, sampled.size());
        Assert.assertEquals(ArrayList.class, sampled.getClass());
        for (HiveExternalMetaCache.HiveFileStatus sampledStatus : sampled) {
            Assert.assertTrue(sampledStatus.isSplittable());
            Assert.assertFalse(cacheValue.getFiles().contains(sampledStatus));
        }
        for (HiveExternalMetaCache.HiveFileStatus cachedStatus : cacheValue.getFiles()) {
            Assert.assertFalse(cachedStatus.isSplittable());
        }
    }

    @Test
    public void testTransactionalListingBypassesStatementCachePath() throws Exception {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext(context, null);
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();
        try {
            HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
            HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
            Mockito.when(table.getCatalog()).thenReturn(catalog);
            Mockito.when(catalog.bindBrokerName()).thenReturn("");
            Mockito.when(catalog.getClient()).thenReturn(Mockito.mock(HMSCachedClient.class));
            HiveScanNode node = createHiveScanNode(0, table);
            HiveTransaction transaction = Mockito.mock(HiveTransaction.class);
            Map<String, String> validWriteIds =
                    Collections.singletonMap("db.table", "valid-write-ids");
            Mockito.when(transaction.getValidWriteIds(Mockito.any())).thenReturn(validWriteIds);
            Mockito.when(transaction.isFullAcid()).thenReturn(true);
            Field transactionField = HiveScanNode.class.getDeclaredField("hiveTransaction");
            transactionField.setAccessible(true);
            transactionField.set(node, transaction);

            HiveExternalMetaCache cache = Mockito.mock(HiveExternalMetaCache.class);
            Mockito.when(cache.getFilesByTransaction(
                    Collections.emptyList(), validWriteIds, true, null))
                    .thenReturn(Collections.emptyList());

            invokeGetFileSplitByPartitions(node, cache, Collections.emptyList());
            invokeGetFileSplitByPartitions(node, cache, Collections.emptyList());

            Mockito.verify(cache, Mockito.times(2)).getFilesByTransaction(
                    Collections.emptyList(), validWriteIds, true, null);
            Mockito.verifyNoMoreInteractions(cache);
        } finally {
            statementContext.close();
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

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
    public void testDetermineTargetFileSplitSizeUsesCoarseSizeForParquet() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setFileSplitSize(31 * MB);
        sv.setFileSplitSizeOnFe(512 * MB);
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getFileFormatType(sv)).thenReturn(TFileFormatType.FORMAT_PARQUET);
        Mockito.when(catalog.bindBrokerName()).thenReturn("");
        desc.setTable(table);
        HiveScanNode node = new HiveScanNode(new PlanNodeId(0), desc, false, sv, null, ScanContext.EMPTY);

        HiveExternalMetaCache.FileCacheValue fileCacheValue = new HiveExternalMetaCache.FileCacheValue();
        HiveExternalMetaCache.HiveFileStatus status = new HiveExternalMetaCache.HiveFileStatus();
        status.setLength(10_000L * MB);
        fileCacheValue.getFiles().add(status);

        Method method = HiveScanNode.class.getDeclaredMethod(
                "determineTargetFileSplitSize", List.class, boolean.class);
        method.setAccessible(true);
        long target = (long) method.invoke(node, Collections.singletonList(fileCacheValue), false);
        Assert.assertEquals(512 * MB, target);

        Mockito.when(table.isHiveTransactionalTable()).thenReturn(true);
        target = (long) method.invoke(node, Collections.singletonList(fileCacheValue), false);
        Assert.assertEquals(31 * MB, target);
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

    private HiveScanNode createHiveScanNode(int id, HMSExternalTable table) {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(id));
        desc.setTable(table);
        return new HiveScanNode(
                new PlanNodeId(id), desc, false, new SessionVariable(), null, ScanContext.EMPTY);
    }

    private void invokeGetFileSplitByPartitions(
            HiveScanNode node, HiveExternalMetaCache cache, List<HivePartition> partitions)
            throws Exception {
        invokeGetFileSplitByPartitions(node, cache, partitions, false);
    }

    private void invokeGetFileSplitByPartitions(
            HiveScanNode node, HiveExternalMetaCache cache, List<HivePartition> partitions,
            boolean isBatchMode) throws Exception {
        Method method = HiveScanNode.class.getDeclaredMethod(
                "getFileSplitByPartitions", HiveExternalMetaCache.class, List.class,
                List.class, String.class, int.class, boolean.class);
        method.setAccessible(true);
        method.invoke(node, cache, partitions, new ArrayList<>(), null, 1, isBatchMode);
    }

    private Object newHiveFileScanTaskCacheKey(
            HivePartition partition, HiveExternalMetaCache.FileCacheValue fileCacheValue) throws Exception {
        Class<?> keyClass = Class.forName(HiveScanNode.class.getName() + "$HiveFileScanTaskCacheKey");
        Constructor<?> constructor = keyClass.getDeclaredConstructor(
                long.class, long.class, List.class, long.class, List.class);
        constructor.setAccessible(true);
        return constructor.newInstance(1L, 2L, Collections.singletonList(partition), 0L,
                Collections.singletonList(fileCacheValue));
    }
}
