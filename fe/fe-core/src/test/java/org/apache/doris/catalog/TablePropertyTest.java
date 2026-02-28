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

package org.apache.doris.catalog;


import org.apache.doris.catalog.DataProperty.MediumAllocationMode;
import org.apache.doris.common.util.PropertyAnalyzer;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TablePropertyTest {

    @Test
    public void testBuildMediumAllocationModeWithStorageMedium() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "HDD");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildStorageMedium();
        tableProperty.buildMediumAllocationMode();
        Assert.assertEquals(MediumAllocationMode.STRICT, tableProperty.getMediumAllocationMode());
    }

    @Test
    public void testBuildMediumAllocationModeWithoutStorageMedium() {
        HashMap<String, String> properties = new HashMap<>();
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildStorageMedium();
        tableProperty.buildMediumAllocationMode();
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, tableProperty.getMediumAllocationMode());
    }

    @Test
    public void testBuildMediumAllocationModeExplicit() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_MEDIUM_ALLOCATION_MODE, "adaptive");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildStorageMedium();
        tableProperty.buildMediumAllocationMode();
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, tableProperty.getMediumAllocationMode());
    }

    @Test
    public void testSetMediumAllocationMode() {
        HashMap<String, String> properties = new HashMap<>();
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.setMediumAllocationMode(MediumAllocationMode.STRICT);
        Assert.assertEquals(MediumAllocationMode.STRICT, tableProperty.getMediumAllocationMode());

        tableProperty.setMediumAllocationMode(MediumAllocationMode.ADAPTIVE);
        Assert.assertEquals(MediumAllocationMode.ADAPTIVE, tableProperty.getMediumAllocationMode());
    }

    @Test
    public void testGetMediumAllocationModeLazyBuild() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildStorageMedium();
        Assert.assertEquals(MediumAllocationMode.STRICT, tableProperty.getMediumAllocationMode());
    }

    @Test
    public void testBuildPartitionRetentionCountValid() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_COUNT, "10");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildPartitionRetentionCount();
        Assert.assertEquals(10, tableProperty.getPartitionRetentionCount());
    }

    @Test
    public void testBuildPartitionRetentionCountZero() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_COUNT, "0");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildPartitionRetentionCount();
        Assert.assertEquals(-1, tableProperty.getPartitionRetentionCount());
    }

    @Test
    public void testBuildPartitionRetentionCountNegative() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_COUNT, "-5");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildPartitionRetentionCount();
        Assert.assertEquals(-1, tableProperty.getPartitionRetentionCount());
    }

    @Test
    public void testBuildPartitionRetentionCountInvalid() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_COUNT, "abc");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildPartitionRetentionCount();
        Assert.assertEquals(-1, tableProperty.getPartitionRetentionCount());
    }

    @Test
    public void testBuildPartitionRetentionCountNull() {
        HashMap<String, String> properties = new HashMap<>();
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildPartitionRetentionCount();
        Assert.assertEquals(-1, tableProperty.getPartitionRetentionCount());
    }

    @Test
    public void testBuildColumnSeqMappingEmpty() {
        HashMap<String, String> properties = new HashMap<>();
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildColumnSeqMapping();
        Assert.assertTrue(tableProperty.getColumnSeqMapping().isEmpty());
    }

    @Test
    public void testBuildColumnSeqMappingWithColumns() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_SEQUENCE_MAPPING + ".seq1", "col1,col2,col3");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildColumnSeqMapping();

        Map<String, List<String>> mapping = tableProperty.getColumnSeqMapping();
        Assert.assertEquals(1, mapping.size());
        List<String> cols = mapping.get("seq1");
        Assert.assertNotNull(cols);
        Assert.assertEquals(3, cols.size());
        Assert.assertEquals("col1", cols.get(0));
        Assert.assertEquals("col2", cols.get(1));
        Assert.assertEquals("col3", cols.get(2));
    }

    @Test
    public void testBuildColumnSeqMappingEmptyValue() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_SEQUENCE_MAPPING + ".seq1", "");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildColumnSeqMapping();

        Map<String, List<String>> mapping = tableProperty.getColumnSeqMapping();
        Assert.assertEquals(1, mapping.size());
        List<String> cols = mapping.get("seq1");
        Assert.assertNotNull(cols);
        Assert.assertTrue(cols.isEmpty());
    }

    @Test
    public void testRemoveDuplicateReplicaNumProperty() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put(DynamicPartitionProperty.REPLICATION_NUM, "3");
        properties.put(DynamicPartitionProperty.REPLICATION_ALLOCATION, "tag.location.default:3");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.modifyTableProperties(new HashMap<>());

        Assert.assertFalse(tableProperty.getProperties()
                .containsKey(DynamicPartitionProperty.REPLICATION_NUM));
        Assert.assertTrue(tableProperty.getProperties()
                .containsKey(DynamicPartitionProperty.REPLICATION_ALLOCATION));
    }

    @Test
    public void testRemoveDuplicateReplicaNumPropertyOnlyNum() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put(DynamicPartitionProperty.REPLICATION_NUM, "3");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.modifyTableProperties(new HashMap<>());

        Assert.assertTrue(tableProperty.getProperties()
                .containsKey(DynamicPartitionProperty.REPLICATION_NUM));
    }

    @Test
    public void testBuildStorageMediumHdd() {
        HashMap<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "HDD");
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildStorageMedium();
        Assert.assertEquals(org.apache.doris.thrift.TStorageMedium.HDD, tableProperty.getStorageMedium());
    }

    @Test
    public void testBuildStorageMediumNull() {
        HashMap<String, String> properties = new HashMap<>();
        TableProperty tableProperty = new TableProperty(properties);
        tableProperty.buildStorageMedium();
        Assert.assertNull(tableProperty.getStorageMedium());
    }
}
