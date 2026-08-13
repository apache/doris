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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalScanTaskCacheKey;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.datasource.hive.source.HiveScanNode;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;

import com.google.common.collect.ImmutableMap;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.storage.StoragePath;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class HudiScanNodeTest {

    @Test
    public void testCopyHudiSplitIsolatesMutableState() {
        HudiSplit source = new HudiSplit(
                LocationPath.of("hdfs://host/table/file.parquet"),
                1, 2, 3, new String[] {"host-1"}, new ArrayList<>(Collections.singletonList("p1")));
        source.setModificationTime(4);
        source.setTableFormatType(TableFormatType.HUDI);
        source.setAlternativeHosts(new ArrayList<>(Collections.singletonList("host-2")));
        source.setSelfSplitWeight(5L);
        source.setTargetSplitSize(6L);
        source.setHudiDeltaLogs(new ArrayList<>(Collections.singletonList("log-1")));
        source.setHudiColumnNames(new ArrayList<>(Collections.singletonList("column-1")));
        source.setHudiColumnTypes(new ArrayList<>(Collections.singletonList("type-1")));
        source.setNestedFields(new ArrayList<>(Collections.singletonList("nested-1")));
        source.setHudiPartitionValues(new java.util.HashMap<>(ImmutableMap.of("key", "value")));

        HudiSplit copy = HudiScanNode.copyHudiSplit(source);
        copy.getHosts()[0] = "changed-host";
        copy.getPartitionValues().set(0, "changed-partition");
        copy.getAlternativeHosts().set(0, "changed-alternative-host");
        copy.getHudiDeltaLogs().set(0, "changed-log");
        copy.getHudiColumnNames().set(0, "changed-column");
        copy.getHudiColumnTypes().set(0, "changed-type");
        copy.getNestedFields().set(0, "changed-nested");
        copy.getHudiPartitionValues().put("key", "changed-value");
        copy.setTargetSplitSize(7L);

        Assertions.assertNotSame(source.getHosts(), copy.getHosts());
        Assertions.assertEquals(Arrays.asList("host-1"), Arrays.asList(source.getHosts()));
        Assertions.assertEquals(Collections.singletonList("p1"), source.getPartitionValues());
        Assertions.assertEquals(Collections.singletonList("host-2"), source.getAlternativeHosts());
        Assertions.assertEquals(Collections.singletonList("log-1"), source.getHudiDeltaLogs());
        Assertions.assertEquals(Collections.singletonList("column-1"), source.getHudiColumnNames());
        Assertions.assertEquals(Collections.singletonList("type-1"), source.getHudiColumnTypes());
        Assertions.assertEquals(Collections.singletonList("nested-1"), source.getNestedFields());
        Assertions.assertEquals(ImmutableMap.of("key", "value"), source.getHudiPartitionValues());
        Assertions.assertEquals(6L, source.getTargetSplitSize());
    }

    @Test
    public void testPartitionPlanningCacheHitsAndReturnsIndependentCopies() throws Exception {
        StatementContext.ExternalScanTaskCache cache = new StatementContext.ExternalScanTaskCache();
        HivePartition partition = partition("file:///table/p=1", Collections.singletonList("1"));
        HoodieTableFileSystemView firstView = fileSystemView("file:///table/p=1/file.parquet");
        HoodieTableFileSystemView duplicateView = fileSystemView("file:///should-not-be-planned.parquet");
        HudiScanNode firstNode = partitionScanNode(cache, firstView, "100", true, false);
        HudiScanNode duplicateNode = partitionScanNode(cache, duplicateView, "100", true, false);

        List<Split> first = invokeGetPartitionSplits(firstNode, partition);
        List<Split> duplicate = invokeGetPartitionSplits(duplicateNode, partition);

        Mockito.verify(firstView, Mockito.times(1)).getLatestBaseFilesBeforeOrOn("p=1", "100");
        Mockito.verify(duplicateView, Mockito.never()).getLatestBaseFilesBeforeOrOn(Mockito.any(), Mockito.any());
        Assertions.assertEquals(1, first.size());
        Assertions.assertEquals(1, duplicate.size());
        Assertions.assertNotSame(first.get(0), duplicate.get(0));
        Assertions.assertEquals(first.get(0).getPathString(), duplicate.get(0).getPathString());

        HudiSplit firstSplit = (HudiSplit) first.get(0);
        HudiSplit duplicateSplit = (HudiSplit) duplicate.get(0);
        firstSplit.getPartitionValues().set(0, "changed");
        firstSplit.setTargetSplitSize(123L);
        Assertions.assertEquals(Collections.singletonList("1"), duplicateSplit.getPartitionValues());
        Assertions.assertNull(duplicateSplit.getTargetSplitSize());
    }

    @Test
    public void testPartitionPlanningCacheMissesForInstantAndPartition() throws Exception {
        StatementContext.ExternalScanTaskCache cache = new StatementContext.ExternalScanTaskCache();
        HoodieTableFileSystemView firstView = fileSystemView("file:///table/p=1/first.parquet");
        HoodieTableFileSystemView instantView = fileSystemView("file:///table/p=1/instant.parquet");
        HoodieTableFileSystemView partitionView = fileSystemView("file:///table/p=2/partition.parquet");
        HivePartition firstPartition = partition("file:///table/p=1", Collections.singletonList("1"));
        HivePartition secondPartition = partition("file:///table/p=2", Collections.singletonList("2"));

        invokeGetPartitionSplits(partitionScanNode(cache, firstView, "100", true, false), firstPartition);
        invokeGetPartitionSplits(partitionScanNode(cache, instantView, "101", true, false), firstPartition);
        invokeGetPartitionSplits(partitionScanNode(cache, partitionView, "100", true, false), secondPartition);

        Mockito.verify(firstView, Mockito.times(1)).getLatestBaseFilesBeforeOrOn("p=1", "100");
        Mockito.verify(instantView, Mockito.times(1)).getLatestBaseFilesBeforeOrOn("p=1", "101");
        Mockito.verify(partitionView, Mockito.times(1)).getLatestBaseFilesBeforeOrOn("p=2", "100");
    }

    @Test
    public void testBatchPartitionPlanningBypassesStatementCache() throws Exception {
        StatementContext.ExternalScanTaskCache cache = new StatementContext.ExternalScanTaskCache();
        HivePartition partition = partition("file:///table/p=1", Collections.singletonList("1"));
        HoodieTableFileSystemView firstView = fileSystemView("file:///table/p=1/first.parquet");
        HoodieTableFileSystemView secondView = fileSystemView("file:///table/p=1/second.parquet");

        invokeGetPartitionSplits(partitionScanNode(cache, firstView, "100", true, false), partition, false);
        invokeGetPartitionSplits(partitionScanNode(cache, secondView, "100", true, false), partition, false);

        Mockito.verify(firstView).getLatestBaseFilesBeforeOrOn("p=1", "100");
        Mockito.verify(secondView).getLatestBaseFilesBeforeOrOn("p=1", "100");
    }

    @Test
    public void testOversizedSnapshotPlanIsNotRetained() throws Exception {
        StatementContext.ExternalScanTaskCache cache = new StatementContext.ExternalScanTaskCache();
        HivePartition partition = partition("file:///table/p=1", Collections.singletonList("1"));
        HoodieTableFileSystemView firstView = fileSystemView("file:///table/p=1/first.parquet");
        HoodieTableFileSystemView secondView = fileSystemView("file:///table/p=1/second.parquet");
        HudiScanNode firstNode = partitionScanNode(cache, firstView, "100", true, false);
        HudiScanNode secondNode = partitionScanNode(cache, secondView, "100", true, false);
        setField(firstNode, FileQueryScanNode.class, "maxRetainedExternalScanTasks", 0L);
        setField(secondNode, FileQueryScanNode.class, "maxRetainedExternalScanTasks", 0L);

        invokeGetPartitionSplits(firstNode, partition);
        invokeGetPartitionSplits(secondNode, partition);

        Mockito.verify(firstView).getLatestBaseFilesBeforeOrOn("p=1", "100");
        Mockito.verify(secondView).getLatestBaseFilesBeforeOrOn("p=1", "100");
    }

    @Test
    public void testPartitionCacheKeySeparatesReaderAndRuntimePruneModes() throws Exception {
        HivePartition partition = partition("file:///table/p=1", Collections.singletonList("1"));
        Object nativeKey = newPartitionCacheKey("100", true, false, partition);
        Object sameKey = newPartitionCacheKey("100", true, false, partition);
        Object jniKey = newPartitionCacheKey("100", false, false, partition);
        Object runtimePruneKey = newPartitionCacheKey("100", true, true, partition);

        assertCacheHitsOnlyEquivalentKeys(nativeKey, sameKey, jniKey, runtimePruneKey);
    }

    @Test
    public void testIncrementalPlanningBypassesStatementCache() throws Exception {
        StatementContext.ExternalScanTaskCache cache = new StatementContext.ExternalScanTaskCache();
        Map<String, String> options = ImmutableMap.of("hoodie.datasource.query.type", "incremental");
        AtomicInteger firstLoads = new AtomicInteger();
        AtomicInteger duplicateLoads = new AtomicInteger();
        AtomicInteger differentStartLoads = new AtomicInteger();
        AtomicInteger differentOptionsLoads = new AtomicInteger();
        IncrementalRelation firstRelation =
                incrementalRelation("10", "20", options, firstLoads, "first.parquet");
        IncrementalRelation duplicateRelation =
                incrementalRelation("10", "20", new HashMap<>(options), duplicateLoads, "duplicate.parquet");
        IncrementalRelation differentStartRelation =
                incrementalRelation("11", "20", options, differentStartLoads, "different-start.parquet");
        IncrementalRelation differentOptionsRelation = incrementalRelation(
                "10", "20", ImmutableMap.of("hoodie.datasource.query.type", "snapshot"),
                differentOptionsLoads, "different-options.parquet");

        List<Split> first = invokeGetIncrementalSplits(
                incrementalScanNode(cache, firstRelation, true));
        List<Split> duplicate = invokeGetIncrementalSplits(
                incrementalScanNode(cache, duplicateRelation, true));
        List<Split> differentStart = invokeGetIncrementalSplits(
                incrementalScanNode(cache, differentStartRelation, true));
        List<Split> differentOptions = invokeGetIncrementalSplits(
                incrementalScanNode(cache, differentOptionsRelation, true));

        Assertions.assertEquals(1, firstLoads.get());
        Assertions.assertEquals(1, duplicateLoads.get());
        Assertions.assertEquals(1, differentStartLoads.get());
        Assertions.assertEquals(1, differentOptionsLoads.get());
        Assertions.assertEquals(1, first.size());
        Assertions.assertEquals(1, duplicate.size());
        Assertions.assertEquals(1, differentStart.size());
        Assertions.assertEquals(1, differentOptions.size());
        Assertions.assertNotSame(first.get(0), duplicate.get(0));
        Assertions.assertNotEquals(first.get(0).getPathString(), duplicate.get(0).getPathString());

        HudiSplit firstSplit = (HudiSplit) first.get(0);
        HudiSplit duplicateSplit = (HudiSplit) duplicate.get(0);
        firstSplit.getPartitionValues().set(0, "changed");
        Assertions.assertEquals(Collections.singletonList("p=1"), duplicateSplit.getPartitionValues());
    }

    private static HudiScanNode partitionScanNode(
            StatementContext.ExternalScanTaskCache cache, HoodieTableFileSystemView fsView,
            String queryInstant, boolean nativeReader, boolean runtimePrune) throws Exception {
        HudiScanNode node = Mockito.mock(HudiScanNode.class, Answers.CALLS_REAL_METHODS);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class, Answers.RETURNS_DEEP_STUBS);
        Mockito.when(table.getCatalog().getId()).thenReturn(1L);
        Mockito.when(table.getId()).thenReturn(2L);
        Mockito.when(table.getStoragePropertiesMap()).thenReturn(Collections.emptyMap());
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setForceJniScanner(!nativeReader);
        sessionVariable.setEnableRuntimeFilterPartitionPrune(runtimePrune);
        HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
        Mockito.when(metaClient.getBasePath()).thenReturn(new StoragePath("file:///table"));

        setField(node, FileQueryScanNode.class, "externalScanTaskCache", cache);
        setField(node, FileQueryScanNode.class, "maxRetainedExternalScanTasks",
                StatementContext.ExternalScanTaskCache.MAX_RETAINED_TASK_COUNT);
        setField(node, HiveScanNode.class, "hmsTable", table);
        setField(node, FileQueryScanNode.class, "sessionVariable", sessionVariable);
        setField(node, HudiScanNode.class, "isCowTable", true);
        setField(node, HudiScanNode.class, "queryInstant", queryInstant);
        setField(node, HudiScanNode.class, "hudiClient", metaClient);
        setField(node, HudiScanNode.class, "fsView", fsView);
        setField(node, HudiScanNode.class, "noLogsSplitNum", new AtomicLong());
        return node;
    }

    private static HoodieTableFileSystemView fileSystemView(String filePath) {
        HoodieBaseFile baseFile = Mockito.mock(HoodieBaseFile.class);
        Mockito.when(baseFile.getPath()).thenReturn(filePath);
        Mockito.when(baseFile.getFileSize()).thenReturn(10L);
        HoodieTableFileSystemView fsView = Mockito.mock(HoodieTableFileSystemView.class);
        Mockito.when(fsView.getLatestBaseFilesBeforeOrOn(Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> Stream.of(baseFile));
        return fsView;
    }

    private static HudiScanNode incrementalScanNode(
            StatementContext.ExternalScanTaskCache cache, IncrementalRelation relation,
            boolean nativeReader) throws Exception {
        HudiScanNode node = Mockito.mock(HudiScanNode.class, Answers.CALLS_REAL_METHODS);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class, Answers.RETURNS_DEEP_STUBS);
        Mockito.when(table.getCatalog().getId()).thenReturn(1L);
        Mockito.when(table.getId()).thenReturn(2L);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setForceJniScanner(!nativeReader);

        setField(node, FileQueryScanNode.class, "externalScanTaskCache", cache);
        setField(node, HiveScanNode.class, "hmsTable", table);
        setField(node, FileQueryScanNode.class, "sessionVariable", sessionVariable);
        setField(node, FileQueryScanNode.class, "summaryProfile", Mockito.mock(SummaryProfile.class));
        setField(node, HudiScanNode.class, "isCowTable", true);
        setField(node, HudiScanNode.class, "incrementalRelation", relation);
        setField(node, HudiScanNode.class, "noLogsSplitNum", new AtomicLong());
        return node;
    }

    private static IncrementalRelation incrementalRelation(
            String start, String end, Map<String, String> options,
            AtomicInteger loads, String plannedPath) {
        IncrementalRelation relation = Mockito.mock(IncrementalRelation.class);
        Mockito.when(relation.getStartTs()).thenReturn(start);
        Mockito.when(relation.getEndTs()).thenReturn(end);
        Mockito.when(relation.getHoodieParams()).thenReturn(options);
        Mockito.when(relation.collectSplits()).thenAnswer(invocation -> {
            loads.incrementAndGet();
            HudiSplit split = new HudiSplit(
                    LocationPath.of("file:///table/" + plannedPath),
                    0, 10, 10, new String[0],
                    new ArrayList<>(Collections.singletonList("p=1")));
            split.setHudiDeltaLogs(Collections.emptyList());
            return Collections.singletonList(split);
        });
        return relation;
    }

    private static HivePartition partition(String path, List<String> values) {
        return new HivePartition(null, false, "parquet", path, new ArrayList<>(values), Collections.emptyMap());
    }

    @SuppressWarnings("unchecked")
    private static List<Split> invokeGetPartitionSplits(HudiScanNode node, HivePartition partition)
            throws Exception {
        return invokeGetPartitionSplits(node, partition, true);
    }

    @SuppressWarnings("unchecked")
    private static List<Split> invokeGetPartitionSplits(
            HudiScanNode node, HivePartition partition, boolean useStatementCache) throws Exception {
        Method method = HudiScanNode.class.getDeclaredMethod(
                "getPartitionSplits", HivePartition.class, List.class, boolean.class);
        method.setAccessible(true);
        List<Split> splits = new ArrayList<>();
        method.invoke(node, partition, splits, useStatementCache);
        return splits;
    }

    @SuppressWarnings("unchecked")
    private static List<Split> invokeGetIncrementalSplits(HudiScanNode node) throws Exception {
        Method method = HudiScanNode.class.getDeclaredMethod("getIncrementalSplits");
        method.setAccessible(true);
        return (List<Split>) method.invoke(node);
    }

    private static Object newPartitionCacheKey(
            String instant, boolean nativeReader, boolean runtimePrune, HivePartition partition)
            throws Exception {
        Class<?> keyClass = Class.forName(HudiScanNode.class.getName() + "$HudiFileScanTaskCacheKey");
        Constructor<?> constructor = keyClass.getDeclaredConstructor(
                long.class, long.class, String.class, boolean.class, boolean.class, HivePartition.class);
        constructor.setAccessible(true);
        return constructor.newInstance(1L, 2L, instant, nativeReader, runtimePrune, partition);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void assertCacheHitsOnlyEquivalentKeys(Object first, Object same, Object... different)
            throws Exception {
        StatementContext.ExternalScanTaskCache cache = new StatementContext.ExternalScanTaskCache();
        AtomicInteger loads = new AtomicInteger();
        List<String> firstResult = cache.getOrLoad((ExternalScanTaskCacheKey) first,
                () -> Collections.singletonList("load-" + loads.incrementAndGet()));
        List<String> sameResult = cache.getOrLoad((ExternalScanTaskCacheKey) same,
                () -> Collections.singletonList("load-" + loads.incrementAndGet()));
        Assertions.assertSame(firstResult, sameResult);
        Assertions.assertEquals(1, loads.get());
        for (Object key : different) {
            cache.getOrLoad((ExternalScanTaskCacheKey) key,
                    () -> Collections.singletonList("load-" + loads.incrementAndGet()));
        }
        Assertions.assertEquals(1 + different.length, loads.get());
    }

    private static void setField(Object target, Class<?> owner, String name, Object value) throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
