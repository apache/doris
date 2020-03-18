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

package org.apache.doris.load.loadv2;

import com.google.common.collect.Sets;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.EtlClusterDesc;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SingleRangePartitionDesc;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo;
import org.apache.spark.launcher.SparkAppHandle;

import com.google.gson.Gson;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SparkLoadPendingTaskTest {

    @Test
    public void testExecuteTask(@Injectable SparkLoadJob sparkLoadJob,
                                @Injectable EtlClusterDesc etlClusterDesc,
                                @Mocked Catalog catalog,
                                @Injectable Database database,
                                @Injectable OlapTable table,
                                @Injectable SparkAppHandle handle) throws LoadException {
        long dbId = 0L;
        long tableId = 1L;

        // columns
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("c1", Type.BIGINT, true, null, false, null, ""));

        // indexes
        Map<Long, List<Column>> indexIdToSchema = Maps.newHashMap();
        long indexId = 3L;
        indexIdToSchema.put(indexId, columns);

        // partition and distribution infos
        long partitionId = 2L;
        DistributionInfo distributionInfo = new RandomDistributionInfo(2);
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        Partition partition = new Partition(partitionId, "p1", null, distributionInfo);
        List<Partition> partitions = Lists.newArrayList(partition);

        // file group
        Map<BrokerFileGroupAggInfo.FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
        DataDescription desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                                                   null, null, null, false, null);
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        brokerFileGroups.add(brokerFileGroup);
        BrokerFileGroupAggInfo.FileGroupAggKey aggKey = new BrokerFileGroupAggInfo.FileGroupAggKey(tableId, null);
        aggKeyToFileGroups.put(aggKey, brokerFileGroups);

        new Expectations() {
            {
                catalog.getDb(dbId);
                result = database;
                database.getTable(tableId);
                result = table;
                table.getPartitions();
                result = partitions;
                table.getIndexIdToSchema();
                result = indexIdToSchema;
                table.getDefaultDistributionInfo();
                result = distributionInfo;
                table.getSchemaHashByIndexId(indexId);
                result = 123;
                table.getPartitionInfo();
                result = partitionInfo;
                table.getPartition(partitionId);
                result = partition;
                table.getKeysTypeByIndexId(indexId);
                result = KeysType.DUP_KEYS;
                table.getBaseIndexId();
                result = indexId;
            }
        };

        new MockUp<SparkEtlJobHandler>() {
            @Mock
            public SparkAppHandle submitEtlJob(long loadJobId, String loadLabel, String sparkMaster,
                                               Map<String, String> sparkConfigs, String jobJsonConfig) {
                return handle;
            }
        };

        SparkLoadPendingTask task = new SparkLoadPendingTask(sparkLoadJob, aggKeyToFileGroups, etlClusterDesc);
        task.init();
        SparkPendingTaskAttachment attachment = Deencapsulation.getField(task, "attachment");
        Assert.assertTrue(attachment.getHandle() == null);
        task.executeTask();
        Assert.assertTrue(attachment.getHandle() != null);
    }

    @Test(expected = LoadException.class)
    public void testNoDb(@Injectable SparkLoadJob sparkLoadJob,
                         @Injectable EtlClusterDesc etlClusterDesc,
                         @Mocked Catalog catalog) throws LoadException {
        long dbId = 0L;

        new Expectations() {
            {
                catalog.getDb(dbId);
                result = null;
            }
        };

        SparkLoadPendingTask task = new SparkLoadPendingTask(sparkLoadJob, null, etlClusterDesc);
        task.init();
    }

    @Test(expected = LoadException.class)
    public void testNoTable(@Injectable SparkLoadJob sparkLoadJob,
                            @Injectable EtlClusterDesc etlClusterDesc,
                            @Mocked Catalog catalog,
                            @Injectable Database database) throws LoadException {
        long dbId = 0L;
        long tableId = 1L;

        Map<BrokerFileGroupAggInfo.FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
        DataDescription desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                                                   null, null, null, false, null);
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        brokerFileGroups.add(brokerFileGroup);
        BrokerFileGroupAggInfo.FileGroupAggKey aggKey = new BrokerFileGroupAggInfo.FileGroupAggKey(tableId, null);
        aggKeyToFileGroups.put(aggKey, brokerFileGroups);

        new Expectations() {
            {
                catalog.getDb(dbId);
                result = database;
                database.getTable(tableId);
                result = null;
            }
        };

        SparkLoadPendingTask task = new SparkLoadPendingTask(sparkLoadJob, aggKeyToFileGroups, etlClusterDesc);
        task.init();
    }

    @Test
    public void testRangePartitionHashDistribution(@Injectable SparkLoadJob sparkLoadJob,
                                                   @Injectable EtlClusterDesc etlClusterDesc,
                                                   @Mocked Catalog catalog,
                                                   @Injectable Database database,
                                                   @Injectable OlapTable table) throws LoadException, DdlException, AnalysisException {
        long dbId = 0L;
        long tableId = 1L;

        // c1 is partition column, c2 is distribution column
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("c1", Type.INT, true, null, false, null, ""));
        columns.add(new Column("c2", ScalarType.createVarchar(10), true, null, false, null, ""));
        columns.add(new Column("c3", Type.INT, false, AggregateType.SUM, false, null, ""));

        // indexes
        Map<Long, List<Column>> indexIdToSchema = Maps.newHashMap();
        long index1Id = 3L;
        indexIdToSchema.put(index1Id, columns);
        long index2Id = 4L;
        indexIdToSchema.put(index2Id, Lists.newArrayList(columns.get(0), columns.get(2)));

        // partition and distribution info
        long partition1Id = 2L;
        long partition2Id = 5L;
        int distributionColumnIndex = 1;
        DistributionInfo distributionInfo = new HashDistributionInfo(3, Lists.newArrayList(columns.get(distributionColumnIndex)));
        Partition partition1 = new Partition(partition1Id, "p1", null,
                                             distributionInfo);
        Partition partition2 = new Partition(partition2Id, "p2", null,
                                             new HashDistributionInfo(4, Lists.newArrayList(columns.get(distributionColumnIndex))));
        int partitionColumnIndex = 0;
        List<Partition> partitions = Lists.newArrayList(partition1, partition2);
        RangePartitionInfo partitionInfo = new RangePartitionInfo(Lists.newArrayList(columns.get(partitionColumnIndex)));
        PartitionKeyDesc partitionKeyDesc1 = new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("10")));
        SingleRangePartitionDesc partitionDesc1 = new SingleRangePartitionDesc(false, "p1", partitionKeyDesc1, null);
        partitionDesc1.analyze(1, null);
        partitionInfo.handleNewSinglePartitionDesc(partitionDesc1, partition1Id);
        PartitionKeyDesc partitionKeyDesc2 = new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("20")));
        SingleRangePartitionDesc partitionDesc2 = new SingleRangePartitionDesc(false, "p2", partitionKeyDesc2, null);
        partitionDesc2.analyze(1, null);
        partitionInfo.handleNewSinglePartitionDesc(partitionDesc2, partition2Id);

        // file group
        Map<BrokerFileGroupAggInfo.FileGroupAggKey, List<BrokerFileGroup>> aggKeyToFileGroups = Maps.newHashMap();
        List<BrokerFileGroup> brokerFileGroups = Lists.newArrayList();
        DataDescription desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                                                   null, null, null, false, null);
        BrokerFileGroup brokerFileGroup = new BrokerFileGroup(desc);
        brokerFileGroups.add(brokerFileGroup);
        BrokerFileGroupAggInfo.FileGroupAggKey aggKey = new BrokerFileGroupAggInfo.FileGroupAggKey(tableId, null);
        aggKeyToFileGroups.put(aggKey, brokerFileGroups);

        new Expectations() {
            {
                catalog.getDb(dbId);
                result = database;
                database.getTable(tableId);
                result = table;
                table.getPartitions();
                result = partitions;
                table.getIndexIdToSchema();
                result = indexIdToSchema;
                table.getDefaultDistributionInfo();
                result = distributionInfo;
                table.getSchemaHashByIndexId(index1Id);
                result = 123;
                table.getSchemaHashByIndexId(index2Id);
                result = 234;
                table.getPartitionInfo();
                result = partitionInfo;
                table.getPartition(partition1Id);
                result = partition1;
                table.getPartition(partition2Id);
                result = partition2;
                table.getKeysTypeByIndexId(index1Id);
                result = KeysType.AGG_KEYS;
                table.getKeysTypeByIndexId(index2Id);
                result = KeysType.AGG_KEYS;
                table.getBaseIndexId();
                result = index1Id;
            }
        };

        SparkLoadPendingTask task = new SparkLoadPendingTask(sparkLoadJob, aggKeyToFileGroups, etlClusterDesc);
        task.init();
        String jsonConfig = Deencapsulation.invoke(task, "configToJson");
        Gson gson = new Gson();
        Map<String, Object> configMap = gson.fromJson(jsonConfig, Map.class);
        Map<String, Object> tables = (Map<String, Object>) configMap.get("tables");

        // check table id
        Assert.assertEquals(1, tables.size());
        Assert.assertTrue(tables.containsKey(String.valueOf(tableId)));

        Map<String, Object> tableMap = (Map<String, Object>) tables.get(String.valueOf(tableId));
        // check indexes
        List<Map<String, Object>> indexes = (List<Map<String, Object>>) tableMap.get("indexes");
        Assert.assertEquals(2, indexes.size());
        Set<Long> indexIds = Sets.newHashSet(index1Id, index2Id);
        Assert.assertTrue(indexIds.contains(new Double((double)indexes.get(0).get("index_id")).longValue()));
        Assert.assertTrue(indexIds.contains(new Double((double)indexes.get(1).get("index_id")).longValue()));

        // check base index columns
        for (Map<String, Object> indexMap : indexes) {
            if (new Double((double) indexMap.get("index_id")).longValue() == index1Id) {
                Assert.assertTrue((Boolean) indexMap.get("is_base_index"));

                List<Map<String, Object>> columnMaps = (List<Map<String, Object>>) indexMap.get("columns");
                Assert.assertEquals(3, columnMaps.size());
                for (int i = 0; i < columns.size(); i++) {
                    Assert.assertTrue(columns.get(i).getName().endsWith((String) columnMaps.get(i).get("column_name")));
                }
            }
        }

        // check partitions
        Map<String, Object> partitionInfoMap = (Map<String, Object>) tableMap.get("partition_info");
        Assert.assertEquals("RANGE", partitionInfoMap.get("partition_type"));
        List<String> partitionColumns = (List<String>) partitionInfoMap.get("partition_column_refs");
        Assert.assertEquals(1, partitionColumns.size());
        Assert.assertEquals(columns.get(partitionColumnIndex).getName(), partitionColumns.get(0));
        List<Map<String, Object>> partitionMaps = (List<Map<String, Object>>) partitionInfoMap.get("partitions");
        Assert.assertEquals(2, partitionMaps.size());

        // check file group
        List<Object> fileGroups = (List<Object>) tableMap.get("file_groups");
        Assert.assertEquals(1, fileGroups.size());
    }
}
