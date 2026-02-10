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

package org.apache.doris.job.offset.kafka;

import org.apache.doris.persist.gson.GsonUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KafkaOffsetTest {

    @Test
    public void testKafkaOffsetSerialization() {
        // Create KafkaOffset
        Map<Integer, Long> partitionOffsets = new HashMap<>();
        partitionOffsets.put(0, 100L);
        partitionOffsets.put(1, 200L);
        partitionOffsets.put(2, 300L);
        
        KafkaOffset offset = new KafkaOffset("test_topic", "kafka_catalog", "default");
        offset.setPartitionOffsets(partitionOffsets);
        
        // Serialize
        String json = offset.toSerializedJson();
        Assert.assertNotNull(json);
        Assert.assertTrue(json.contains("test_topic"));
        
        // Deserialize
        KafkaOffset deserializedOffset = GsonUtils.GSON.fromJson(json, KafkaOffset.class);
        Assert.assertEquals("test_topic", deserializedOffset.getTopic());
        Assert.assertEquals("kafka_catalog", deserializedOffset.getCatalogName());
        Assert.assertEquals("default", deserializedOffset.getDatabaseName());
        Assert.assertEquals(3, deserializedOffset.getPartitionOffsets().size());
        Assert.assertEquals(Long.valueOf(100L), deserializedOffset.getPartitionOffsets().get(0));
        Assert.assertEquals(Long.valueOf(200L), deserializedOffset.getPartitionOffsets().get(1));
        Assert.assertEquals(Long.valueOf(300L), deserializedOffset.getPartitionOffsets().get(2));
    }
    
    @Test
    public void testKafkaOffsetUpdatePartition() {
        KafkaOffset offset = new KafkaOffset("test_topic", "catalog", "db");
        Map<Integer, Long> partitionOffsets = new HashMap<>();
        partitionOffsets.put(0, 0L);
        offset.setPartitionOffsets(partitionOffsets);
        
        // Update single partition
        offset.updatePartitionOffset(0, 1000L);
        Assert.assertEquals(Long.valueOf(1000L), offset.getPartitionOffsets().get(0));
        
        // Add new partition
        offset.updatePartitionOffset(1, 500L);
        Assert.assertEquals(2, offset.getPartitionOffsets().size());
        Assert.assertEquals(Long.valueOf(500L), offset.getPartitionOffsets().get(1));
    }
    
    @Test
    public void testKafkaOffsetGetPartitionOffset() {
        KafkaOffset offset = new KafkaOffset("topic", "catalog", "db");
        Map<Integer, Long> partitionOffsets = new HashMap<>();
        partitionOffsets.put(0, 100L);
        offset.setPartitionOffsets(partitionOffsets);
        
        // Existing partition
        Assert.assertEquals(100L, offset.getPartitionOffset(0));
        
        // Non-existing partition (should return 0)
        Assert.assertEquals(0L, offset.getPartitionOffset(99));
    }
    
    @Test
    public void testKafkaOffsetIsEmpty() {
        KafkaOffset offset = new KafkaOffset();
        Assert.assertTrue(offset.isEmpty());
        
        offset = new KafkaOffset("topic", "catalog", "db");
        Assert.assertTrue(offset.isEmpty());
        
        offset.updatePartitionOffset(0, 100L);
        Assert.assertFalse(offset.isEmpty());
    }
    
    @Test
    public void testKafkaOffsetIsValidOffset() {
        KafkaOffset offset = new KafkaOffset();
        Assert.assertFalse(offset.isValidOffset());
        
        offset = new KafkaOffset("topic", "catalog", "db");
        Assert.assertTrue(offset.isValidOffset());
    }
    
    @Test
    public void testKafkaOffsetCopy() {
        KafkaOffset offset = new KafkaOffset("topic", "catalog", "db");
        offset.updatePartitionOffset(0, 100L);
        offset.updatePartitionOffset(1, 200L);
        
        KafkaOffset copy = offset.copy();
        
        Assert.assertEquals(offset.getTopic(), copy.getTopic());
        Assert.assertEquals(offset.getPartitionOffsets().size(), copy.getPartitionOffsets().size());
        
        // Modify original should not affect copy
        offset.updatePartitionOffset(0, 999L);
        Assert.assertEquals(Long.valueOf(100L), copy.getPartitionOffsets().get(0));
    }
    
    @Test
    public void testKafkaPartitionOffsetShowRange() {
        KafkaPartitionOffset partitionOffset = new KafkaPartitionOffset(0, 100L, 200L);
        String range = partitionOffset.showRange();
        Assert.assertTrue(range.contains("partition=0"));
        Assert.assertTrue(range.contains("100"));
        Assert.assertTrue(range.contains("200"));
    }
    
    @Test
    public void testKafkaPartitionOffsetIsEmpty() {
        // Empty when startOffset >= endOffset
        KafkaPartitionOffset empty = new KafkaPartitionOffset(0, 100L, 100L);
        Assert.assertTrue(empty.isEmpty());
        
        KafkaPartitionOffset notEmpty = new KafkaPartitionOffset(0, 100L, 200L);
        Assert.assertFalse(notEmpty.isEmpty());
    }
    
    @Test
    public void testKafkaPartitionOffsetIsValidOffset() {
        // Valid offset
        KafkaPartitionOffset valid = new KafkaPartitionOffset(0, 100L, 200L);
        Assert.assertTrue(valid.isValidOffset());
        
        // Invalid - startOffset < 0
        KafkaPartitionOffset invalid1 = new KafkaPartitionOffset(0, -1L, 100L);
        Assert.assertFalse(invalid1.isValidOffset());
        
        // Invalid - endOffset <= startOffset
        KafkaPartitionOffset invalid2 = new KafkaPartitionOffset(0, 100L, 100L);
        Assert.assertFalse(invalid2.isValidOffset());
    }
    
    @Test
    public void testKafkaPartitionOffsetGetExpectedRows() {
        KafkaPartitionOffset offset = new KafkaPartitionOffset(0, 100L, 200L);
        Assert.assertEquals(100L, offset.getExpectedRows());
    }
    
    @Test
    public void testKafkaPartitionOffsetSerialization() {
        KafkaPartitionOffset offset = new KafkaPartitionOffset(5, 1000L, 2000L);
        offset.setConsumedRows(800L);
        
        String json = offset.toSerializedJson();
        Assert.assertNotNull(json);
        
        KafkaPartitionOffset deserialized = GsonUtils.GSON.fromJson(json, KafkaPartitionOffset.class);
        Assert.assertEquals(5, deserialized.getPartitionId());
        Assert.assertEquals(1000L, deserialized.getStartOffset());
        Assert.assertEquals(2000L, deserialized.getEndOffset());
        Assert.assertEquals(800L, deserialized.getConsumedRows());
    }
}
