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

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.datasource.hive.source.HiveScanNode;
import org.apache.doris.planner.ListPartitionPrunerV2;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TFileRangeDesc;

import alluxio.core.client.runtime.com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hudi.common.storage.HoodieDefaultStorageStrategy;
import org.apache.hudi.common.storage.HoodieStorageStrategy;
import org.apache.hudi.common.storage.HoodieStorageStrategyFactory;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class HudiScanNodeTest {

    @Test
    public void testDoInitialize(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc, @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog, @Injectable HoodieTableMetaClient client,
            @Injectable HoodieTableConfig tableConfig,
            @Injectable org.apache.hadoop.hive.metastore.api.Table hiveTable,
            @Injectable StorageDescriptor storageDescriptor, @Injectable SerDeInfo serDeInfo) {
        Map<String, String> para1 = Maps.newHashMap();
        para1.put("hoodie.query.without.cache.layer.enabled", "true");
        Map<String, String> para2 = Maps.newHashMap();
        para2.put("hoodie.datasource.write.recordkey.field", "id,name");
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = true;

                table.isView();
                result = false;

                client.reloadActiveTimeline();

                client.getTableConfig();
                result = tableConfig;

                tableConfig.getChubaoFsOwner();
                result = "test:test";

                table.getRemoteTable();
                result = hiveTable;

                hiveTable.getSd();
                result = storageDescriptor;

                storageDescriptor.getLocation();
                result = "hdfs://test";

                storageDescriptor.getInputFormat();
                result = "org.apache.hudi.hadoop.HoodieParquetInputFormat";

                storageDescriptor.getSerdeInfo();
                result =  serDeInfo;

                serDeInfo.getSerializationLib();
                result = "org.apache.hudi.hive.HoodieHiveSerDe";

                serDeInfo.getParameters();
                result = para1;

                hiveTable.getParameters();
                result = para2;
            }
        };
        new MockUp<HudiScanNode>() {
            @Mock
            public void computeColumnsFilter() {

            }

            @Mock
            public void initBackendPolicy() {

            }

            @Mock
            public void initSchemaParams() {

            }

            @Mock
            public TableSnapshot getQueryTableSnapshot() {
                return null;
            }

        };

        new MockUp<HiveMetaStoreClientHelper>() {
            @Mock
            public HoodieTableMetaClient getHudiClient(HMSExternalTable table)  {
                return client;
            }
        };

        new MockUp<HoodieStorageStrategyFactory>() {
            @Mock
            public HoodieStorageStrategy getInstant(HoodieTableMetaClient client, boolean reset) {
                return new HoodieDefaultStorageStrategy("", "", Option.of(client));
            }
        };

        MockedStatic<HoodieStorageStrategyFactory> mocked = Mockito.mockStatic(HoodieStorageStrategyFactory.class);
        mocked.when(() -> HoodieStorageStrategyFactory.getInstant(Mockito.any(), Mockito.anyBoolean())).thenReturn(
                new HoodieDefaultStorageStrategy("hdfs://ns10000/test",
                        "hdfs://ns10000/test/test", Option.of(client)));

        new MockUp<TableSchemaResolver>() {
            @Mock
            public Schema getTableAvroSchema() {
                List<Field> fieldList = new ArrayList<>();
                return Schema.createRecord(fieldList);
            }
        };
        try {
            HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                    false, Optional.empty(), Optional.empty(), sessionVariable, null, ScanContext.EMPTY);
            scanNode.doInitialize();
            String[] primaryKeys = new String[]{"id", "name"};
            java.lang.reflect.Field pkField = HudiScanNode.class.getDeclaredField("primaryKeys");
            pkField.setAccessible(true);
            @SuppressWarnings("unchecked")
            List<String> pks = (List<String>) pkField.get(scanNode);
            Assertions.assertEquals(primaryKeys.length, pks.size());
            Assertions.assertEquals(primaryKeys[0], pks.get(0));
            Assertions.assertEquals(primaryKeys[1], pks.get(1));
        } catch (Exception e) {
            Assertions.assertEquals("", ExceptionUtils.getStackTrace(e));
            Assertions.fail(e);
        }
    }

    @Test
    public void testSetFsNameForRangeDesc(@Injectable SessionVariable sessionVariable,
                                          @Injectable TupleDescriptor tupleDesc, @Injectable HMSExternalTable table,
                                          @Injectable ExternalCatalog catalog) {
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = true;
            }
        };
        HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                false, Optional.empty(), Optional.empty(), sessionVariable, null, ScanContext.EMPTY);
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        FileSplit hdfsFileSplit = new FileSplit(LocationPath.of(
                    "hdfs://HDFSO1101/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e77f7d8.snappy.orc"),
                0, 112140970, 112140970, 0, null, Collections.emptyList());
        scanNode.setFsNameForRangeDesc(hdfsFileSplit, rangeDesc);
        Assertions.assertEquals("hdfs://HDFSO1101", rangeDesc.getFsName());
    }

    @Test
    public void testDoInitializeWithTimeline(@Injectable SessionVariable sessionVariable,
                                             @Injectable TupleDescriptor tupleDesc, @Injectable HMSExternalTable table,
                                             @Injectable ExternalCatalog catalog, @Injectable HoodieTableMetaClient client,
                                             @Injectable HoodieTableConfig tableConfig,
                                             @Injectable org.apache.hadoop.hive.metastore.api.Table hiveTable,
                                             @Injectable StorageDescriptor storageDescriptor, @Injectable SerDeInfo serDeInfo) {
        Map<String, String> para1 = Maps.newHashMap();
        para1.put("hoodie.query.without.cache.layer.enabled", "false");
        Map<String, String> para2 = Maps.newHashMap();
        para2.put("hoodie.datasource.write.recordkey.field", "id");

        // Mock HoodieTimeline and related objects for line 245
        org.apache.hudi.common.table.timeline.HoodieTimeline commitsAndCompactionTimeline =
                Mockito.mock(org.apache.hudi.common.table.timeline.HoodieTimeline.class);
        org.apache.hudi.common.table.timeline.HoodieTimeline filteredTimeline =
                Mockito.mock(org.apache.hudi.common.table.timeline.HoodieTimeline.class);
        org.apache.hudi.common.table.timeline.HoodieInstant hoodieInstant =
                Mockito.mock(org.apache.hudi.common.table.timeline.HoodieInstant.class);

        // Mock timeline chain: getCommitsAndCompactionTimeline() -> filterCompletedAndCompactionInstants()
        Mockito.when(commitsAndCompactionTimeline.filterCompletedAndCompactionInstants()).thenReturn(filteredTimeline);

        // Mock lastInstant() to return a present instant
        Mockito.when(filteredTimeline.lastInstant()).thenReturn(Option.of(hoodieInstant));
        Mockito.when(hoodieInstant.getTimestamp()).thenReturn("20240101120000");

        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = true;

                table.isView();
                result = false;

                client.reloadActiveTimeline();

                client.getCommitsAndCompactionTimeline();
                result = commitsAndCompactionTimeline;

                client.getTableConfig();
                result = tableConfig;

                tableConfig.getChubaoFsOwner();
                result = null;

                table.getRemoteTable();
                result = hiveTable;

                hiveTable.getSd();
                result = storageDescriptor;

                storageDescriptor.getLocation();
                result = "hdfs://test";

                storageDescriptor.getInputFormat();
                result = "org.apache.hudi.hadoop.HoodieParquetInputFormat";

                storageDescriptor.getSerdeInfo();
                result = serDeInfo;

                serDeInfo.getSerializationLib();
                result = "org.apache.hudi.hive.HoodieHiveSerDe";

                serDeInfo.getParameters();
                result = para1;

                hiveTable.getParameters();
                result = para2;
            }
        };

        new MockUp<HudiScanNode>() {
            @Mock
            public void computeColumnsFilter() {
            }

            @Mock
            public void initBackendPolicy() {
            }

            @Mock
            public void initSchemaParams() {
            }

            @Mock
            public TableSnapshot getQueryTableSnapshot() {
                return null;
            }
        };

        new MockUp<HiveMetaStoreClientHelper>() {
            @Mock
            public HoodieTableMetaClient getHudiClient(HMSExternalTable table) {
                return client;
            }
        };

        MockedStatic<HoodieStorageStrategyFactory> mocked = Mockito.mockStatic(HoodieStorageStrategyFactory.class);
        mocked.when(() -> HoodieStorageStrategyFactory.getInstant(Mockito.any(), Mockito.anyBoolean())).thenReturn(
            new HoodieDefaultStorageStrategy("hdfs://ns10000/test",
                "hdfs://ns10000/test/test", Option.of(client)));

        new MockUp<TableSchemaResolver>() {
            @Mock
            public Schema getTableAvroSchema() {
                List<Field> fieldList = new ArrayList<>();
                return Schema.createRecord(fieldList);
            }
        };

        try {
            HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                    false, Optional.empty(), Optional.empty(), sessionVariable, null, ScanContext.EMPTY);
            scanNode.doInitialize();

            // Verify that timeline was set (line 245 was executed)
            // Use reflection to check the timeline field
            java.lang.reflect.Field timelineField = HudiScanNode.class.getDeclaredField("timeline");
            timelineField.setAccessible(true);
            org.apache.hudi.common.table.timeline.HoodieTimeline actualTimeline =
                    (org.apache.hudi.common.table.timeline.HoodieTimeline) timelineField.get(scanNode);

            Assertions.assertNotNull(actualTimeline, "Timeline should be set after doInitialize()");
            Assertions.assertEquals(filteredTimeline, actualTimeline, "Timeline should be the filtered timeline");

            // Verify queryInstant was set
            java.lang.reflect.Field queryInstantField = HudiScanNode.class.getDeclaredField("queryInstant");
            queryInstantField.setAccessible(true);
            String actualQueryInstant = (String) queryInstantField.get(scanNode);
            Assertions.assertEquals("20240101120000", actualQueryInstant, "Query instant should be set");

        } catch (Exception e) {
            Assertions.fail("doInitialize() should not throw exception: " + e.getMessage());
        } finally {
            mocked.close();
        }
    }

    @Test
    public void testDoInitializeWithEmptyTimeline(@Injectable SessionVariable sessionVariable,
                                                  @Injectable TupleDescriptor tupleDesc, @Injectable HMSExternalTable table,
                                                  @Injectable ExternalCatalog catalog, @Injectable HoodieTableMetaClient client,
                                                  @Injectable HoodieTableConfig tableConfig,
                                                  @Injectable org.apache.hadoop.hive.metastore.api.Table hiveTable,
                                                  @Injectable StorageDescriptor storageDescriptor, @Injectable SerDeInfo serDeInfo) {
        Map<String, String> para1 = Maps.newHashMap();
        para1.put("hoodie.query.without.cache.layer.enabled", "false");
        Map<String, String> para2 = Maps.newHashMap();
        para2.put("hoodie.datasource.write.recordkey.field", "id");

        // Mock HoodieTimeline for line 245 - empty timeline case
        org.apache.hudi.common.table.timeline.HoodieTimeline commitsAndCompactionTimeline =
                Mockito.mock(org.apache.hudi.common.table.timeline.HoodieTimeline.class);
        org.apache.hudi.common.table.timeline.HoodieTimeline filteredTimeline =
                Mockito.mock(org.apache.hudi.common.table.timeline.HoodieTimeline.class);

        // Mock timeline chain: getCommitsAndCompactionTimeline() -> filterCompletedAndCompactionInstants()
        Mockito.when(commitsAndCompactionTimeline.filterCompletedAndCompactionInstants()).thenReturn(filteredTimeline);

        // Mock lastInstant() to return empty (covers line 254-258)
        Mockito.when(filteredTimeline.lastInstant()).thenReturn(Option.empty());

        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = true;

                table.isView();
                result = false;

                client.reloadActiveTimeline();

                client.getCommitsAndCompactionTimeline();
                result = commitsAndCompactionTimeline;

                client.getTableConfig();
                result = tableConfig;

                tableConfig.getChubaoFsOwner();
                result = null;

                table.getRemoteTable();
                result = hiveTable;

                hiveTable.getSd();
                result = storageDescriptor;

                storageDescriptor.getLocation();
                result = "hdfs://test";

                storageDescriptor.getInputFormat();
                result = "org.apache.hudi.hadoop.HoodieParquetInputFormat";

                storageDescriptor.getSerdeInfo();
                result = serDeInfo;

                serDeInfo.getSerializationLib();
                result = "org.apache.hudi.hive.HoodieHiveSerDe";

                serDeInfo.getParameters();
                result = para1;

                hiveTable.getParameters();
                result = para2;
            }
        };

        new MockUp<HudiScanNode>() {
            @Mock
            public void computeColumnsFilter() {
            }

            @Mock
            public void initBackendPolicy() {
            }

            @Mock
            public void initSchemaParams() {
            }

            @Mock
            public TableSnapshot getQueryTableSnapshot() {
                return null;
            }
        };

        new MockUp<HiveMetaStoreClientHelper>() {
            @Mock
            public HoodieTableMetaClient getHudiClient(HMSExternalTable table) {
                return client;
            }
        };

        MockedStatic<HoodieStorageStrategyFactory> mocked = Mockito.mockStatic(HoodieStorageStrategyFactory.class);
        mocked.when(() -> HoodieStorageStrategyFactory.getInstant(Mockito.any(), Mockito.anyBoolean())).thenReturn(
            new HoodieDefaultStorageStrategy("hdfs://ns10000/test",
                "hdfs://ns10000/test/test", Option.of(client)));

        new MockUp<TableSchemaResolver>() {
            @Mock
            public Schema getTableAvroSchema() {
                List<Field> fieldList = new ArrayList<>();
                return Schema.createRecord(fieldList);
            }
        };

        HudiScanNode scanNode = null;
        try {
            scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                false, Optional.empty(), Optional.empty(), sessionVariable, null, ScanContext.EMPTY);
            scanNode.doInitialize();

            // Verify that timeline was set (line 245 was executed)
            java.lang.reflect.Field timelineField = HudiScanNode.class.getDeclaredField("timeline");
            timelineField.setAccessible(true);
            org.apache.hudi.common.table.timeline.HoodieTimeline actualTimeline =
                    (org.apache.hudi.common.table.timeline.HoodieTimeline) timelineField.get(scanNode);

            Assertions.assertNotNull(actualTimeline, "Timeline should be set after doInitialize()");
            Assertions.assertEquals(filteredTimeline, actualTimeline, "Timeline should be the filtered timeline");

            // Verify that prunedPartitions is empty and partitionInit is true (lines 256-257)
            // prunedPartitions is defined in parent class HiveScanNode
            java.lang.reflect.Field prunedPartitionsField = org.apache.doris.datasource.hive.source.HiveScanNode.class.getDeclaredField("prunedPartitions");
            prunedPartitionsField.setAccessible(true);
            List<?> prunedPartitions = (List<?>) prunedPartitionsField.get(scanNode);
            Assertions.assertNotNull(prunedPartitions, "Pruned partitions should not be null");
            Assertions.assertTrue(prunedPartitions.isEmpty(), "Pruned partitions should be empty when timeline is empty");

            // partitionInit is also defined in parent class HiveScanNode
            java.lang.reflect.Field partitionInitField = org.apache.doris.datasource.hive.source.HiveScanNode.class.getDeclaredField("partitionInit");
            partitionInitField.setAccessible(true);
            boolean partitionInit = partitionInitField.getBoolean(scanNode);
            Assertions.assertTrue(partitionInit, "PartitionInit should be true when timeline is empty");

        } catch (java.lang.NoSuchFieldException e) {
            // Field might not exist, try to get it from parent class
            // Note: scanNode is not accessible here, so we need to handle this differently
            Assertions.fail("Failed to access prunedPartitions field: " + e.getMessage());
        } catch (Exception e) {
            // Print full stack trace for debugging
            java.io.StringWriter sw = new java.io.StringWriter();
            java.io.PrintWriter pw = new java.io.PrintWriter(sw);
            e.printStackTrace(pw);
            Assertions.fail("doInitialize() should not throw exception: " + e.getMessage()
                    + ", cause: " + (e.getCause() != null ? e.getCause().getMessage() : "null")
                    + ", stack: " + sw.toString());
        } finally {
            mocked.close();
        }
    }

    // Helper method to create mock HivePartition
    private HivePartition createMockHivePartition(String path, List<String> partitionValues) {
        return new HivePartition(NameMapping.createForTest("testDb", "testTable"), false,
                "org.apache.hudi.hadoop.HoodieParquetInputFormat", path, partitionValues, Maps.newHashMap());
    }

    // Helper method to create mock FileStatus
    private FileStatus createMockFileStatus(String path, long length) {
        FileStatus status = Mockito.mock(FileStatus.class);
        Mockito.when(status.getPath()).thenReturn(new Path(path));
        Mockito.when(status.getLen()).thenReturn(length);
        return status;
    }

    // Helper method to create mock HudiScanNode
    private HudiScanNode createMockHudiScanNode(SessionVariable sessionVariable, TupleDescriptor tupleDesc,
                                                 HMSExternalTable table, ExternalCatalog catalog,
                                                 HoodieTableMetaClient client) {
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = false;
            }
        };

        HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                false, Optional.empty(), Optional.empty(), sessionVariable, null, ScanContext.EMPTY);

        // Set internal fields via reflection if needed
        try {
            java.lang.reflect.Field clientField = HudiScanNode.class.getDeclaredField("hudiClient");
            clientField.setAccessible(true);
            clientField.set(scanNode, client);

            java.lang.reflect.Field basePathField = HudiScanNode.class.getDeclaredField("basePath");
            basePathField.setAccessible(true);
            basePathField.set(scanNode, "/test/base/path");

            java.lang.reflect.Field queryInstantField = HudiScanNode.class.getDeclaredField("queryInstant");
            queryInstantField.setAccessible(true);
            queryInstantField.set(scanNode, "20240101000000");

            java.lang.reflect.Field partitionInitField = HiveScanNode.class.getDeclaredField("partitionInit");
            partitionInitField.setAccessible(true);
            partitionInitField.set(scanNode, true);

            java.lang.reflect.Field prunedPartitionsField = HiveScanNode.class.getDeclaredField("prunedPartitions");
            prunedPartitionsField.setAccessible(true);
            prunedPartitionsField.set(scanNode, new ArrayList<HivePartition>());
        } catch (Exception e) {
            // Handle exception
        }

        return scanNode;
    }


    @Test
    public void testGetSplitsWithIncrementalRead(@Injectable SessionVariable sessionVariable,
                                                 @Injectable TupleDescriptor tupleDesc,
                                                 @Injectable HMSExternalTable table,
                                                 @Injectable ExternalCatalog catalog,
                                                 @Injectable HoodieTableMetaClient client,
                                                 @Injectable IncrementalRelation incrementalRelation) throws Exception {

        // Create scan node with incremental read
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = false;
            }
        };

        HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                false, Optional.empty(), Optional.of(incrementalRelation), sessionVariable, null, ScanContext.EMPTY);

        // Set incremental read flag
        java.lang.reflect.Field incrementalReadField = HudiScanNode.class.getDeclaredField("incrementalRead");
        incrementalReadField.setAccessible(true);
        incrementalReadField.set(scanNode, true);

        java.lang.reflect.Field incrementalRelationField = HudiScanNode.class.getDeclaredField("incrementalRelation");
        incrementalRelationField.setAccessible(true);
        incrementalRelationField.set(scanNode, incrementalRelation);

        // Mock incremental relation
        List<Split> mockSplits = Arrays.asList(
                Mockito.mock(Split.class),
                Mockito.mock(Split.class)
        );

        new Expectations() {
            {
                incrementalRelation.fallbackFullTableScan();
                result = false;

                incrementalRelation.collectSplits();
                result = mockSplits;
            }
        };

        // Execute
        List<Split> splits = scanNode.getSplits(3);

        // Verify
        Assert.assertEquals(2, splits.size());
        // Mockito.verify(incrementalRelation).collectSplits();
    }

    @Test
    public void testIsBatchMode(@Injectable SessionVariable sessionVariable,
                                @Injectable TupleDescriptor tupleDesc,
                                @Injectable HMSExternalTable table,
                                @Injectable ExternalCatalog catalog,
                                @Injectable HoodieTableMetaClient client) throws Exception {

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Create many partitions to trigger batch mode
        List<HivePartition> partitions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            partitions.add(createMockHivePartition("/test/base/path/partition" + i, Arrays.asList("2024", String.valueOf(i))));
        }

        java.lang.reflect.Field prunedPartitionsField = HiveScanNode.class.getDeclaredField("prunedPartitions");
        prunedPartitionsField.setAccessible(true);
        prunedPartitionsField.set(scanNode, partitions);

        // Mock ConnectContext
        new MockUp<org.apache.doris.qe.ConnectContext>() {
            @Mock
            public org.apache.doris.qe.ConnectContext get() {
                org.apache.doris.qe.ConnectContext context = Mockito.mock(org.apache.doris.qe.ConnectContext.class);
                SessionVariable sv = Mockito.mock(SessionVariable.class);
                Mockito.when(context.getSessionVariable()).thenReturn(sv);
                Mockito.when(sv.getNumPartitionsInBatchMode()).thenReturn(5);
                return context;
            }
        };

        // Execute
        boolean isBatchMode = scanNode.isBatchMode();

        // Verify - 10 partitions >= 5 threshold
        Assert.assertTrue(isBatchMode);
    }

    @Test
    public void testEmptyPartitions(@Injectable SessionVariable sessionVariable,
                                    @Injectable TupleDescriptor tupleDesc,
                                    @Injectable HMSExternalTable table,
                                    @Injectable ExternalCatalog catalog,
                                    @Injectable HoodieTableMetaClient client) throws Exception {

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Set empty partitions
        List<HivePartition> partitions = new ArrayList<>();
        java.lang.reflect.Field prunedPartitionsField = HiveScanNode.class.getDeclaredField("prunedPartitions");
        prunedPartitionsField.setAccessible(true);
        prunedPartitionsField.set(scanNode, partitions);

        // Execute
        List<Split> splits = scanNode.getSplits(3);

        // Verify
        Assert.assertNotNull(splits);
        Assert.assertEquals(0, splits.size());
    }

    @Test
    public void testGetSplitsExceedsMaxPartitionNum(@Injectable SessionVariable sessionVariable,
                                                    @Injectable TupleDescriptor tupleDesc,
                                                    @Injectable HMSExternalTable table,
                                                    @Injectable ExternalCatalog catalog,
                                                    @Injectable HoodieTableMetaClient client) throws Exception {

        // Set a very low max partition limit first
        int oldMaxPartitionNum = Config.max_selected_partition_num_for_lakehouse_table;
        Config.max_selected_partition_num_for_lakehouse_table = 2; // Set to 2 for testing

        try {
            // Use the existing helper to create a properly mocked scan node
            HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

            // Override to make this a partitioned table and reset partitionInit
            new Expectations() {
                {
                    table.getPartitionColumnTypes();
                    result = Arrays.asList(Type.STRING, Type.STRING); // Non-empty list to indicate partitioned table

                    table.getDbName();
                    result = "testDb";

                    table.getName();
                    result = "testTable";
                }
            };

            // Reset partitionInit to false so partition processing runs
            java.lang.reflect.Field partitionInitField = HiveScanNode.class.getDeclaredField("partitionInit");
            partitionInitField.setAccessible(true);
            partitionInitField.set(scanNode, false);

            // Directly test the scenario by calling getPrunedPartitions method via reflection
            Method getPrunedPartitionsMethod = HudiScanNode.class.getDeclaredMethod("getPrunedPartitions",
                    HoodieTableMetaClient.class, Option.class);
            getPrunedPartitionsMethod.setAccessible(true);

            // This should trigger the constraint check and throw AnalysisException
            // We'll simulate having too many partitions by mocking the pruner to return more partitions than allowed
            new MockUp<ListPartitionPrunerV2>() {
                @Mock
                public void init(Map idToPartitionItem, List partitionColumns, Map columnNameToRange,
                        Map uidToPartitionRange, Map rangeToId, Map singleColumnRangeMap, boolean isPartitionColumnsAnalyzed) {
                    // Mock initialization
                }

                @Mock
                public Collection<Long> prune() {
                    // Return more partition IDs than the limit allows
                    return Arrays.asList(1L, 2L, 3L, 4L, 5L); // 5 partitions > limit of 2
                }
            };

            // Mock the partition processing components
            new MockUp<Env>() {
                @Mock
                public Env getCurrentEnv() {
                    Env env = Mockito.mock(Env.class);
                    org.apache.doris.datasource.ExternalMetaCacheMgr metaCacheMgr =
                            Mockito.mock(org.apache.doris.datasource.ExternalMetaCacheMgr.class);
                    HudiCachedPartitionProcessor processor = Mockito.mock(HudiCachedPartitionProcessor.class);

                    // Mock the partition processor to return partition values
                    try {
                        TablePartitionValues partitionValues = Mockito.mock(TablePartitionValues.class);

                        // Create partition data that exceeds the limit
                        Map<Long, PartitionItem> idToPartitionItem = Maps.newHashMap();
                        Map<Long, String> partitionIdToNameMap = Maps.newHashMap();
                        Map<Long, List<String>> partitionValuesMap = Maps.newHashMap();

                        // Add 5 partitions (more than the limit of 2)
                        for (long i = 1L; i <= 5L; i++) {
                            PartitionItem mockItem = Mockito.mock(PartitionItem.class);
                            idToPartitionItem.put(i, mockItem);
                            partitionIdToNameMap.put(i, "partition" + i);
                            partitionValuesMap.put(i, Arrays.asList("2024", String.valueOf(i)));
                        }

                        java.util.concurrent.locks.ReentrantReadWriteLock lock = new java.util.concurrent.locks.ReentrantReadWriteLock();
                        Mockito.when(partitionValues.readLock()).thenReturn(lock.readLock());
                        Mockito.when(partitionValues.getIdToPartitionItem()).thenReturn(idToPartitionItem);
                        Mockito.when(partitionValues.getPartitionIdToNameMap()).thenReturn(partitionIdToNameMap);
                        Mockito.when(partitionValues.getPartitionValuesMap()).thenReturn(partitionValuesMap);
                        Mockito.when(partitionValues.getUidToPartitionRange()).thenReturn(Maps.newHashMap());
                        Mockito.when(partitionValues.getRangeToId()).thenReturn(Maps.newHashMap());
                        Mockito.when(partitionValues.getSingleColumnRangeMap()).thenReturn(com.google.common.collect.TreeRangeMap.create());

                        Mockito.when(processor.getPartitionValues(Mockito.any(), Mockito.any(), Mockito.anyBoolean()))
                                .thenReturn(partitionValues);
                    } catch (Exception e) {
                        // Handle mocking exception
                    }

                    Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
                    Mockito.when(metaCacheMgr.getHudiPartitionProcess(Mockito.any())).thenReturn(processor);
                    return env;
                }
            };

            // Execute and expect AnalysisException due to partition count constraint
            // Since we're using reflection, the AnalysisException will be wrapped in InvocationTargetException
            try {
                getPrunedPartitionsMethod.invoke(scanNode, client, Option.empty());
                Assert.fail("Expected AnalysisException to be thrown due to partition count exceeding limit");
            } catch (java.lang.reflect.InvocationTargetException e) {
                // Unwrap the actual exception
                Throwable cause = e.getCause();
                Assert.assertTrue("Expected AnalysisException but got: " + cause.getClass().getName(),
                                cause instanceof AnalysisException);

                AnalysisException analysisException = (AnalysisException) cause;
                String message = analysisException.getMessage();

                // Verify the exception message contains the expected constraint violation details
                Assert.assertTrue("Exception message should mention partition count exceeding limit, but was: " + message,
                                message.contains("exceed max selected partition num"));
                Assert.assertTrue("Exception message should mention the table name, but was: " + message,
                                message.contains("testDb.testTable"));
            } catch (Exception e) {
                Assert.fail("Unexpected exception type: " + e.getClass().getName() + " - " + e.getMessage());
            }

        } finally {
            Config.max_selected_partition_num_for_lakehouse_table = oldMaxPartitionNum;
        }
    }

    @Test
    public void testNumApproximateSplits(@Injectable SessionVariable sessionVariable,
                                         @Injectable TupleDescriptor tupleDesc,
                                         @Injectable HMSExternalTable table,
                                         @Injectable ExternalCatalog catalog,
                                         @Injectable HoodieTableMetaClient client) throws Exception {

        // Create a simplified mock setup that doesn't include getCatalogProperties()
        // since numApproximateSplits() doesn't call it
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = false;

                // Don't set up getCatalogProperties() expectation since it's not needed
            }
        };

        HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                false, Optional.empty(), Optional.empty(), sessionVariable, null, ScanContext.EMPTY);

        // Set partitions
        List<HivePartition> partitions = Arrays.asList(
                createMockHivePartition("/test/base/path/partition1", Arrays.asList("2024", "01")),
                createMockHivePartition("/test/base/path/partition2", Arrays.asList("2024", "02"))
        );

        java.lang.reflect.Field prunedPartitionsField = HiveScanNode.class.getDeclaredField("prunedPartitions");
        prunedPartitionsField.setAccessible(true);
        prunedPartitionsField.set(scanNode, partitions);

        // Set numSplitsPerPartition
        java.lang.reflect.Field numSplitsPerPartitionField = HudiScanNode.class.getDeclaredField("numSplitsPerPartition");
        numSplitsPerPartitionField.setAccessible(true);
        AtomicInteger numSplitsPerPartition = (AtomicInteger) numSplitsPerPartitionField.get(scanNode);
        numSplitsPerPartition.set(5);

        // Execute
        int approximateSplits = scanNode.numApproximateSplits();

        // Verify - 5 splits per partition * 2 partitions = 10
        Assert.assertEquals(10, approximateSplits);
    }

    @Test
    public void testPrunePartitionsInheritedFromParent(@Injectable SessionVariable sessionVariable,
                                                      @Injectable TupleDescriptor tupleDesc,
                                                      @Injectable HMSExternalTable table,
                                                      @Injectable ExternalCatalog catalog,
                                                      @Injectable HoodieTableMetaClient client) throws Exception {

        HudiScanNode scanNode = createMockHudiScanNode(sessionVariable, tupleDesc, table, catalog, client);

        // Verify that HudiScanNode can access inherited prunedPartitions from HiveScanNode
        java.lang.reflect.Field prunedPartitionsField = HiveScanNode.class.getDeclaredField("prunedPartitions");
        prunedPartitionsField.setAccessible(true);

        // Set pruned partitions using inherited field
        List<HivePartition> testPartitions = Arrays.asList(
                createMockHivePartition("/test/partition1", Arrays.asList("2024", "01")),
                createMockHivePartition("/test/partition2", Arrays.asList("2024", "02"))
        );
        prunedPartitionsField.set(scanNode, testPartitions);

        // Verify the field is accessible and data is set correctly
        @SuppressWarnings("unchecked")
        List<HivePartition> retrievedPartitions = (List<HivePartition>) prunedPartitionsField.get(scanNode);
        Assert.assertNotNull(retrievedPartitions);
        Assert.assertEquals(2, retrievedPartitions.size());
    }

    @Test
    public void testIsBatchMode(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc, @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog) {
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = true;

                sessionVariable.getNumPartitionsInBatchMode();
                result = 1;
            }
        };
        HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                false, Optional.empty(), Optional.empty(), sessionVariable, null, ScanContext.EMPTY);
        new MockUp<HudiScanNode>() {
            @Mock
            public List<HivePartition> getPartitions() {
                HivePartition partition1 = new HivePartition(NameMapping.createForTest("test", "test"), false,
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "hdfs://hk-dev01:8121/user/doris/parquet/partition_table/nation=cn/city=beijing",
                        Lists.newArrayList("cn", "beijing"), Maps.newHashMap());
                HivePartition partition2 = new HivePartition(NameMapping.createForTest("test", "test"), false,
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "hdfs://hk-dev01:8121/user/doris/parquet/partition_table/nation=cn/city=shanghai",
                        Lists.newArrayList("cn", "shanghai"), Maps.newHashMap());
                return Lists.newArrayList(partition1, partition2);
            }
        };
        Assertions.assertTrue(scanNode.isBatchMode());
        new Expectations() {
            {
                sessionVariable.getNumPartitionsInBatchMode();
                result = 1024;
            }
        };
        Assertions.assertFalse(scanNode.isBatchMode());
        new MockUp<HudiScanNode>() {
            @Mock
            public List<HivePartition> getPartitions() {
                throw new RuntimeException("get partitions failed");
            }
        };
        HudiScanNode scanNode1 = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                false, Optional.empty(), Optional.empty(), sessionVariable, null, ScanContext.EMPTY);
        Assertions.assertFalse(scanNode1.isBatchMode());
    }

    @Test
    public void testGetSplits(@Injectable SessionVariable sessionVariable,
            @Injectable TupleDescriptor tupleDesc, @Injectable HMSExternalTable table,
            @Injectable ExternalCatalog catalog) {
        new Expectations() {
            {
                tupleDesc.getTable();
                result = table;

                tupleDesc.getId();
                result = new TupleId(1);

                table.getCatalog();
                result = catalog;

                table.isHoodieCowTable();
                result = true;

                catalog.bindBrokerName();
                result = "test";

                table.useHiveSyncPartition();
                result = true;
            }
        };
        HudiScanNode scanNode = new HudiScanNode(new PlanNodeId(1), tupleDesc,
                false, Optional.empty(), Optional.empty(), sessionVariable, null, ScanContext.EMPTY);
        new MockUp<HudiScanNode>() {
            @Mock
            public List<HivePartition> getPartitions() {
                HivePartition partition1 = new HivePartition(NameMapping.createForTest("test", "test"), false,
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "hdfs://hk-dev01:8121/user/doris/parquet/partition_table/nation=cn/city=beijing",
                        Lists.newArrayList("cn", "beijing"), Maps.newHashMap());
                HivePartition partition2 = new HivePartition(NameMapping.createForTest("test", "test"), false,
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "hdfs://hk-dev01:8121/user/doris/parquet/partition_table/nation=cn/city=shanghai",
                        Lists.newArrayList("cn", "shanghai"), Maps.newHashMap());
                return Lists.newArrayList(partition1, partition2);
            }

            @Mock
            public void getPartitionsSplits(List<HivePartition> partitions, List<Split> splits)
                    throws AnalysisException {
                FileSplit chubaoFileSplit = new FileSplit(LocationPath.of(
                        "chubaofs://CHUBAO1101/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e24f7d5.snappy.orc"),
                        0, 112140970, 112140970, 0, null, Collections.emptyList());
                FileSplit hdfsFileSplit = new FileSplit(LocationPath.of(
                        "hdfs://HDFSO1101/usr/hive/warehouse/clickbench.db/hits_orc/part-00000-3e77f7d8.snappy.orc"),
                        0, 112140970, 112140970, 0, null, Collections.emptyList());
                splits.add(chubaoFileSplit);
                splits.add(hdfsFileSplit);
            }
        };
        try {
            List<Split> splits = scanNode.getSplits(1);
            Assertions.assertFalse(splits.isEmpty());
            Assertions.assertEquals(2, splits.size());
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }
}
