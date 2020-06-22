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
//

package org.apache.doris.load.loadv2.dpp;

import org.apache.doris.load.loadv2.dpp.DorisRangePartitioner;

import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.junit.Assert;
import org.junit.Test;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;

import java.util.ArrayList;
import java.util.List;

public class DorisRangePartitionerTest {

    @Test
    public void testRangePartitioner() {
        List<Object> startKeys = new ArrayList<>();
        startKeys.add(new Integer(0));
        List<Object> endKeys = new ArrayList<>();
        endKeys.add(new Integer(100));
        EtlJobConfig.EtlPartition partition1 = new EtlJobConfig.EtlPartition(
                10000, startKeys, endKeys, false, 3);

        List<Object> startKeys2 = new ArrayList<>();
        startKeys2.add(new Integer(100));
        List<Object> endKeys2 = new ArrayList<>();
        endKeys2.add(new Integer(200));
        EtlJobConfig.EtlPartition partition2 = new EtlJobConfig.EtlPartition(
                10001, startKeys2, endKeys2, false, 4);

        List<Object> startKeys3 = new ArrayList<>();
        startKeys3.add(new Integer(200));
        List<Object> endKeys3 = new ArrayList<>();
        endKeys3.add(new Integer(300));
        EtlJobConfig.EtlPartition partition3 = new EtlJobConfig.EtlPartition(
                10002, startKeys3, endKeys3, false, 5);

        List<EtlJobConfig.EtlPartition> partitions = new ArrayList<>();
        partitions.add(partition1);
        partitions.add(partition2);
        partitions.add(partition3);

        List<String> partitionColumns = new ArrayList<>();
        partitionColumns.add("id");
        List<String> bucketColumns = new ArrayList<>();
        bucketColumns.add("key");
        EtlJobConfig.EtlPartitionInfo partitionInfo = new EtlJobConfig.EtlPartitionInfo(
                "RANGE", partitionColumns, bucketColumns, partitions);
        List<DorisRangePartitioner.PartitionRangeKey> partitionRangeKeys = new ArrayList<>();
        for (EtlJobConfig.EtlPartition partition : partitions) {
            DorisRangePartitioner.PartitionRangeKey partitionRangeKey = new DorisRangePartitioner.PartitionRangeKey();
            partitionRangeKey.isMaxPartition = false;
            partitionRangeKey.startKeys = new DppColumns(partition.startKeys);
            partitionRangeKey.endKeys = new DppColumns(partition.endKeys);
            partitionRangeKeys.add(partitionRangeKey);
        }
        List<Integer> partitionKeyIndexes = new ArrayList<>();
        partitionKeyIndexes.add(0);
        DorisRangePartitioner rangePartitioner = new DorisRangePartitioner(partitionInfo, partitionKeyIndexes, partitionRangeKeys);
        int num = rangePartitioner.numPartitions();
        Assert.assertEquals(3, num);

        List<Object> fields1 = new ArrayList<>();
        fields1.add(-100);
        fields1.add("name");
        DppColumns record1 = new DppColumns(fields1);
        int id1 = rangePartitioner.getPartition(record1);
        Assert.assertEquals(-1, id1);

        List<Object> fields2 = new ArrayList<>();
        fields2.add(10);
        fields2.add("name");
        DppColumns record2 = new DppColumns(fields2);
        int id2 = rangePartitioner.getPartition(record2);
        Assert.assertEquals(0, id2);

        List<Object> fields3 = new ArrayList<>();
        fields3.add(110);
        fields3.add("name");
        DppColumns record3 = new DppColumns(fields3);
        int id3 = rangePartitioner.getPartition(record3);
        Assert.assertEquals(1, id3);

        List<Object> fields4 = new ArrayList<>();
        fields4.add(210);
        fields4.add("name");
        DppColumns record4 = new DppColumns(fields4);
        int id4 = rangePartitioner.getPartition(record4);
        Assert.assertEquals(2, id4);

        List<Object> fields5 = new ArrayList<>();
        fields5.add(310);
        fields5.add("name");
        DppColumns record5 = new DppColumns(fields5);
        int id5 = rangePartitioner.getPartition(record5);
        Assert.assertEquals(-1, id5);
    }

    @Test
    public void testUnpartitionedPartitioner() {
        List<String> partitionColumns = new ArrayList<>();
        List<String> bucketColumns = new ArrayList<>();
        bucketColumns.add("key");
        EtlJobConfig.EtlPartitionInfo partitionInfo = new EtlJobConfig.EtlPartitionInfo(
                "UNPARTITIONED", null, bucketColumns, null);
        List<DorisRangePartitioner.PartitionRangeKey> partitionRangeKeys = new ArrayList<>();
        List<Class> partitionSchema = new ArrayList<>();
        partitionSchema.add(Integer.class);
        List<Integer> partitionKeyIndexes = new ArrayList<>();
        partitionKeyIndexes.add(0);
        DorisRangePartitioner rangePartitioner = new DorisRangePartitioner(partitionInfo, partitionKeyIndexes, null);
        int num = rangePartitioner.numPartitions();
        Assert.assertEquals(1, num);

        List<Object> fields = new ArrayList<>();
        fields.add(100);
        fields.add("name");
        DppColumns record = new DppColumns(fields);
        int id = rangePartitioner.getPartition(record);
        Assert.assertEquals(0, id);
    }
}