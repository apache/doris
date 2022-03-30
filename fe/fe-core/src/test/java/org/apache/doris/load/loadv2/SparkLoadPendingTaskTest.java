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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SinglePartitionDesc;
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
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.SparkResource;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.BrokerFileGroupAggInfo;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlFileGroup;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlIndex;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlPartition;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlPartitionInfo;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlTable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;

public class SparkLoadPendingTaskTest {

    @Test
    public void testExecuteTask(@Injectable SparkLoadJob sparkLoadJob,
                                @Injectable SparkResource resource,
                                @Injectable BrokerDesc brokerDesc,
                                @Mocked Catalog catalog, @Injectable SparkLoadAppHandle handle,
                                @Injectable Database database,
                                @Injectable OlapTable table) throws UserException {
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
        DistributionInfo distributionInfo = new HashDistributionInfo(2, Lists.newArrayList(columns.get(0)));
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
                sparkLoadJob.getHandle();
                result = handle;
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

        String appId = "application_15888888888_0088";
        new MockUp<SparkEtlJobHandler>() {
            @Mock
            public void submitEtlJob(long loadJobId, String loadLabel, EtlJobConfig etlJobConfig,
                                     SparkResource resource, BrokerDesc brokerDesc, SparkLoadAppHandle handle,
                                     SparkPendingTaskAttachment attachment) throws LoadException {
                attachment.setAppId(appId);
            }
        };

        SparkLoadPendingTask task = new SparkLoadPendingTask(sparkLoadJob, aggKeyToFileGroups, resource, brokerDesc);
        task.init();
        SparkPendingTaskAttachment attachment = Deencapsulation.getField(task, "attachment");
        Assert.assertEquals(null, attachment.getAppId());
        task.executeTask();
        Assert.assertEquals(appId, attachment.getAppId());
    }

    @Test
    public void testRangePartitionHashDistribution(@Injectable SparkLoadJob sparkLoadJob,
                                                   @Injectable SparkResource resource,
                                                   @Injectable BrokerDesc brokerDesc,
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
        PartitionKeyDesc partitionKeyDesc1 = PartitionKeyDesc.createLessThan(Lists.newArrayList(new PartitionValue("10")));
        SinglePartitionDesc partitionDesc1 = new SinglePartitionDesc(false, "p1", partitionKeyDesc1, null);
        partitionDesc1.analyze(1, null);
        partitionInfo.handleNewSinglePartitionDesc(partitionDesc1, partition1Id, false);
        PartitionKeyDesc partitionKeyDesc2 = PartitionKeyDesc.createLessThan(Lists.newArrayList(new PartitionValue("20")));
        SinglePartitionDesc partitionDesc2 = new SinglePartitionDesc(false, "p2", partitionKeyDesc2, null);
        partitionDesc2.analyze(1, null);
        partitionInfo.handleNewSinglePartitionDesc(partitionDesc2, partition2Id, false);

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

        SparkLoadPendingTask task = new SparkLoadPendingTask(sparkLoadJob, aggKeyToFileGroups, resource, brokerDesc);
        EtlJobConfig etlJobConfig = Deencapsulation.getField(task, "etlJobConfig");
        Assert.assertEquals(null, etlJobConfig);
        task.init();
        etlJobConfig = Deencapsulation.getField(task, "etlJobConfig");
        Assert.assertTrue(etlJobConfig != null);

        // check table id
        Map<Long, EtlTable> idToEtlTable = etlJobConfig.tables;
        Assert.assertEquals(1, idToEtlTable.size());
        Assert.assertTrue(idToEtlTable.containsKey(tableId));

        // check indexes
        EtlTable etlTable = idToEtlTable.get(tableId);
        List<EtlIndex> etlIndexes = etlTable.indexes;
        Assert.assertEquals(2, etlIndexes.size());
        Assert.assertEquals(index1Id, etlIndexes.get(0).indexId);
        Assert.assertEquals(index2Id, etlIndexes.get(1).indexId);

        // check base index columns
        EtlIndex baseIndex = etlIndexes.get(0);
        Assert.assertTrue(baseIndex.isBaseIndex);
        Assert.assertEquals(3, baseIndex.columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Assert.assertEquals(columns.get(i).getName(), baseIndex.columns.get(i).columnName);
        }
        Assert.assertEquals("AGGREGATE", baseIndex.indexType);

        // check partitions
        EtlPartitionInfo etlPartitionInfo = etlTable.partitionInfo;
        Assert.assertEquals("RANGE", etlPartitionInfo.partitionType);
        List<String> partitionColumns = etlPartitionInfo.partitionColumnRefs;
        Assert.assertEquals(1, partitionColumns.size());
        Assert.assertEquals(columns.get(partitionColumnIndex).getName(), partitionColumns.get(0));
        List<String> distributionColumns = etlPartitionInfo.distributionColumnRefs;
        Assert.assertEquals(1, distributionColumns.size());
        Assert.assertEquals(columns.get(distributionColumnIndex).getName(), distributionColumns.get(0));
        List<EtlPartition> etlPartitions = etlPartitionInfo.partitions;
        Assert.assertEquals(2, etlPartitions.size());

        // check file group
        List<EtlFileGroup> etlFileGroups = etlTable.fileGroups;
        Assert.assertEquals(1, etlFileGroups.size());
    }
}
