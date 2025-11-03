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

package org.apache.doris.datasource.iceberg;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

public class IcebergPartitionInfoTest {

    @Test
    public void testGetLatestSnapshotId() {
        IcebergPartition p1 = new IcebergPartition("p1", 0, 0, 0, 0, 1, 101, null, null);
        IcebergPartition p2 = new IcebergPartition("p2", 0, 0, 0, 0, 2, 102, null, null);
        IcebergPartition p3 = new IcebergPartition("p3", 0, 0, 0, 0, 3, 103, null, null);
        Map<String, IcebergPartition> nameToIcebergPartition = Maps.newHashMap();
        nameToIcebergPartition.put(p1.getPartitionName(), p1);
        nameToIcebergPartition.put(p2.getPartitionName(), p2);
        nameToIcebergPartition.put(p3.getPartitionName(), p3);
        Map<String, Set<String>> nameToIcebergPartitionNames = Maps.newHashMap();
        Set<String> names = Sets.newHashSet();
        names.add("p1");
        names.add("p2");
        nameToIcebergPartitionNames.put("p1", names);

        IcebergPartitionInfo info = new IcebergPartitionInfo(null, nameToIcebergPartition, nameToIcebergPartitionNames);
        long snapshot1 = info.getLatestSnapshotId("p1");
        long snapshot2 = info.getLatestSnapshotId("p2");
        long snapshot3 = info.getLatestSnapshotId("p3");
        Assertions.assertEquals(102, snapshot1);
        Assertions.assertEquals(102, snapshot2);
        Assertions.assertEquals(103, snapshot3);
    }
}
