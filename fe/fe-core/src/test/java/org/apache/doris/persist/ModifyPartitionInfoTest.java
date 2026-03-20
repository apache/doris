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

package org.apache.doris.persist;

import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Map;

public class ModifyPartitionInfoTest {

    @Test
    public void testConstructorAndGetters() {
        DataProperty dataProperty = new DataProperty(TStorageMedium.SSD);
        ReplicaAllocation replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
        Map<String, String> tblProperties = new HashMap<>();
        tblProperties.put("key1", "value1");

        ModifyPartitionInfo info = new ModifyPartitionInfo(
                1L, 2L, 3L, dataProperty, replicaAlloc,
                true, "policy1", tblProperties, "p1", false);

        Assert.assertEquals(1L, info.getDbId());
        Assert.assertEquals(2L, info.getTableId());
        Assert.assertEquals(3L, info.getPartitionId());
        Assert.assertEquals(dataProperty, info.getDataProperty());
        Assert.assertEquals(replicaAlloc, info.getReplicaAlloc());
        Assert.assertTrue(info.isInMemory());
        Assert.assertEquals("policy1", info.getStoragePolicy());
        Assert.assertEquals("p1", info.getPartitionName());
        Assert.assertFalse(info.isTempPartition());
        Assert.assertEquals(1, info.getTblProperties().size());
        Assert.assertEquals("value1", info.getTblProperties().get("key1"));
    }

    @Test
    public void testConstructorWithNullTblProperties() {
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        ReplicaAllocation replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;

        ModifyPartitionInfo info = new ModifyPartitionInfo(
                1L, 2L, 3L, dataProperty, replicaAlloc,
                false, "", null, "p1", true);

        Assert.assertNotNull(info.getTblProperties());
        Assert.assertTrue(info.getTblProperties().isEmpty());
        Assert.assertTrue(info.isTempPartition());
    }

    @Test
    public void testSetTblProperties() {
        ModifyPartitionInfo info = new ModifyPartitionInfo();
        Map<String, String> props = Maps.newHashMap();
        props.put("a", "b");
        info.setTblProperties(props);
        Assert.assertEquals(props, info.getTblProperties());
    }

    @Test
    public void testEquals() {
        DataProperty dp1 = new DataProperty(TStorageMedium.SSD);
        ReplicaAllocation ra = ReplicaAllocation.DEFAULT_ALLOCATION;

        ModifyPartitionInfo info1 = new ModifyPartitionInfo(
                1L, 2L, 3L, dp1, ra, true, "policy", null, "p1", false);
        ModifyPartitionInfo info2 = new ModifyPartitionInfo(
                1L, 2L, 3L, dp1, ra, true, "policy", null, "p1", false);

        Assert.assertEquals(info1, info2);
        Assert.assertEquals(info1, info1);
    }

    @Test
    public void testNotEquals() {
        DataProperty dp1 = new DataProperty(TStorageMedium.SSD);
        DataProperty dp2 = new DataProperty(TStorageMedium.HDD);
        ReplicaAllocation ra = ReplicaAllocation.DEFAULT_ALLOCATION;

        ModifyPartitionInfo info1 = new ModifyPartitionInfo(
                1L, 2L, 3L, dp1, ra, true, "policy", null, "p1", false);
        ModifyPartitionInfo info2 = new ModifyPartitionInfo(
                1L, 2L, 3L, dp2, ra, true, "policy", null, "p1", false);

        Assert.assertNotEquals(info1, info2);
        Assert.assertNotEquals(info1, "not a ModifyPartitionInfo");
    }

    @Test
    public void testWriteAndRead() throws Exception {
        DataProperty dataProperty = new DataProperty(TStorageMedium.SSD);
        ReplicaAllocation replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
        Map<String, String> tblProperties = new HashMap<>();
        tblProperties.put("key1", "value1");

        ModifyPartitionInfo original = new ModifyPartitionInfo(
                10L, 20L, 30L, dataProperty, replicaAlloc,
                true, "s3_policy", tblProperties, "partition0", false);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        original.write(dos);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        ModifyPartitionInfo deserialized = ModifyPartitionInfo.read(dis);

        Assert.assertEquals(original.getDbId(), deserialized.getDbId());
        Assert.assertEquals(original.getTableId(), deserialized.getTableId());
        Assert.assertEquals(original.getPartitionId(), deserialized.getPartitionId());
        Assert.assertEquals(original.isInMemory(), deserialized.isInMemory());
        Assert.assertEquals(original.getStoragePolicy(), deserialized.getStoragePolicy());
        Assert.assertEquals(original.getPartitionName(), deserialized.getPartitionName());
        Assert.assertEquals(original.isTempPartition(), deserialized.isTempPartition());
    }
}
