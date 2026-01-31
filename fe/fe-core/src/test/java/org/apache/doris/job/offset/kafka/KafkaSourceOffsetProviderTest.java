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

import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.job.offset.Offset;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaSourceOffsetProviderTest {

    private KafkaSourceOffsetProvider provider;
    
    @Before
    public void setUp() {
        provider = new KafkaSourceOffsetProvider();
    }
    
    @Test
    public void testGetSourceType() {
        Assert.assertEquals("kafka", provider.getSourceType());
    }
    
    @Test
    public void testGetNextOffsetsNoNewData() {
        // Set current offset equal to latest - no new data
        KafkaOffset currentOffset = new KafkaOffset("topic", "catalog", "db");
        currentOffset.updatePartitionOffset(0, 100L);
        provider.setCurrentOffset(currentOffset);
        
        Map<Integer, Long> latestOffsets = new HashMap<>();
        latestOffsets.put(0, 100L);  // Same as current
        provider.setLatestOffsets(latestOffsets);
        provider.setMaxBatchRows(100L);
        
        StreamingJobProperties jobProps = new StreamingJobProperties(new HashMap<>());
        List<KafkaPartitionOffset> nextOffsets = provider.getNextPartitionOffsets(jobProps);
        
        Assert.assertTrue(nextOffsets.isEmpty());
    }
    
    @Test
    public void testGetNextOffsetsWithNewData() {
        // Set up current offset
        KafkaOffset currentOffset = new KafkaOffset("topic", "catalog", "db");
        currentOffset.updatePartitionOffset(0, 100L);
        currentOffset.updatePartitionOffset(1, 200L);
        currentOffset.updatePartitionOffset(2, 0L);
        provider.setCurrentOffset(currentOffset);
        
        // Set up latest offsets
        Map<Integer, Long> latestOffsets = new HashMap<>();
        latestOffsets.put(0, 500L);   // Has new data
        latestOffsets.put(1, 200L);   // No new data
        latestOffsets.put(2, 1000L);  // Has new data
        provider.setLatestOffsets(latestOffsets);
        provider.setMaxBatchRows(100L);
        
        StreamingJobProperties jobProps = new StreamingJobProperties(new HashMap<>());
        List<KafkaPartitionOffset> nextOffsets = provider.getNextPartitionOffsets(jobProps);
        
        // Should return 2 partitions (0 and 2 have new data)
        Assert.assertEquals(2, nextOffsets.size());
        
        // Verify offset range calculation
        for (KafkaPartitionOffset offset : nextOffsets) {
            if (offset.getPartitionId() == 0) {
                Assert.assertEquals(100L, offset.getStartOffset());
                Assert.assertEquals(200L, offset.getEndOffset()); // min(100+100, 500)
            } else if (offset.getPartitionId() == 2) {
                Assert.assertEquals(0L, offset.getStartOffset());
                Assert.assertEquals(100L, offset.getEndOffset()); // min(0+100, 1000)
            }
        }
    }
    
    @Test
    public void testGetNextOffsetsLimitedByLatest() {
        // Test case where latest offset is less than current + maxBatchRows
        KafkaOffset currentOffset = new KafkaOffset("topic", "catalog", "db");
        currentOffset.updatePartitionOffset(0, 900L);
        provider.setCurrentOffset(currentOffset);
        
        Map<Integer, Long> latestOffsets = new HashMap<>();
        latestOffsets.put(0, 950L);  // Only 50 messages available
        provider.setLatestOffsets(latestOffsets);
        provider.setMaxBatchRows(100L);
        
        StreamingJobProperties jobProps = new StreamingJobProperties(new HashMap<>());
        List<KafkaPartitionOffset> nextOffsets = provider.getNextPartitionOffsets(jobProps);
        
        Assert.assertEquals(1, nextOffsets.size());
        KafkaPartitionOffset offset = nextOffsets.get(0);
        Assert.assertEquals(900L, offset.getStartOffset());
        Assert.assertEquals(950L, offset.getEndOffset());  // Limited by latest
    }
    
    @Test
    public void testHasMoreDataToConsumeTrue() {
        KafkaOffset currentOffset = new KafkaOffset("topic", "catalog", "db");
        currentOffset.updatePartitionOffset(0, 100L);
        provider.setCurrentOffset(currentOffset);
        
        Map<Integer, Long> latestOffsets = new HashMap<>();
        latestOffsets.put(0, 200L);
        provider.setLatestOffsets(latestOffsets);
        
        Assert.assertTrue(provider.hasMoreDataToConsume());
    }
    
    @Test
    public void testHasMoreDataToConsumeFalse() {
        KafkaOffset currentOffset = new KafkaOffset("topic", "catalog", "db");
        currentOffset.updatePartitionOffset(0, 100L);
        provider.setCurrentOffset(currentOffset);
        
        Map<Integer, Long> latestOffsets = new HashMap<>();
        latestOffsets.put(0, 100L);  // Same as current
        provider.setLatestOffsets(latestOffsets);
        
        Assert.assertFalse(provider.hasMoreDataToConsume());
    }
    
    @Test
    public void testHasMoreDataToConsumeEmptyOffset() {
        // Empty current offset should return true (needs initialization)
        Assert.assertTrue(provider.hasMoreDataToConsume());
    }
    
    @Test
    public void testUpdatePartitionOffset() {
        KafkaOffset currentOffset = new KafkaOffset("topic", "catalog", "db");
        currentOffset.updatePartitionOffset(0, 0L);
        provider.setCurrentOffset(currentOffset);
        
        // Simulate task completion with 50 rows consumed
        provider.updatePartitionOffset(0, 50L);
        
        Assert.assertEquals(Long.valueOf(50L), 
                provider.getCurrentOffset().getPartitionOffsets().get(0));
    }
    
    @Test
    public void testDeserializeOffset() {
        String json = "{\"partitionOffsets\":{\"0\":100,\"1\":200},\"topic\":\"test\",\"catalogName\":\"catalog\",\"databaseName\":\"db\"}";
        Offset offset = provider.deserializeOffset(json);
        
        Assert.assertTrue(offset instanceof KafkaOffset);
        KafkaOffset kafkaOffset = (KafkaOffset) offset;
        Assert.assertEquals("test", kafkaOffset.getTopic());
        Assert.assertEquals(Long.valueOf(100L), kafkaOffset.getPartitionOffsets().get(0));
        Assert.assertEquals(Long.valueOf(200L), kafkaOffset.getPartitionOffsets().get(1));
    }
    
    @Test
    public void testGetShowCurrentOffset() {
        KafkaOffset currentOffset = new KafkaOffset("topic", "catalog", "db");
        currentOffset.updatePartitionOffset(0, 100L);
        currentOffset.updatePartitionOffset(1, 200L);
        provider.setCurrentOffset(currentOffset);
        
        String show = provider.getShowCurrentOffset();
        Assert.assertNotNull(show);
        Assert.assertTrue(show.contains("topic"));
    }
    
    @Test
    public void testGetShowMaxOffset() {
        Map<Integer, Long> latestOffsets = new HashMap<>();
        latestOffsets.put(0, 1000L);
        latestOffsets.put(1, 2000L);
        provider.setLatestOffsets(latestOffsets);
        
        String show = provider.getShowMaxOffset();
        Assert.assertNotNull(show);
        Assert.assertTrue(show.contains("p0=1000") || show.contains("p1=2000"));
    }
    
    @Test
    public void testGetPersistInfo() {
        KafkaOffset currentOffset = new KafkaOffset("topic", "catalog", "db");
        currentOffset.updatePartitionOffset(0, 100L);
        provider.setCurrentOffset(currentOffset);
        
        String persistInfo = provider.getPersistInfo();
        Assert.assertNotNull(persistInfo);
        Assert.assertTrue(persistInfo.contains("topic"));
        Assert.assertTrue(persistInfo.contains("100"));
    }
    
    // ============ Tests for Independent Pipeline Model ============
    
    @Test
    public void testGetNextPartitionOffsetSinglePartitionWithData() {
        // Set up current offset
        KafkaOffset currentOffset = new KafkaOffset("topic", "catalog", "db");
        currentOffset.updatePartitionOffset(0, 100L);
        currentOffset.updatePartitionOffset(1, 200L);
        provider.setCurrentOffset(currentOffset);
        
        // Set up latest offsets
        Map<Integer, Long> latestOffsets = new HashMap<>();
        latestOffsets.put(0, 500L);
        latestOffsets.put(1, 200L);  // No new data
        provider.setLatestOffsets(latestOffsets);
        provider.setMaxBatchRows(100L);
        
        StreamingJobProperties jobProps = new StreamingJobProperties(new HashMap<>());
        
        // Partition 0 has data
        KafkaPartitionOffset offset0 = provider.getNextPartitionOffset(0, jobProps);
        Assert.assertNotNull(offset0);
        Assert.assertEquals(0, offset0.getPartitionId());
        Assert.assertEquals(100L, offset0.getStartOffset());
        Assert.assertEquals(200L, offset0.getEndOffset());
        
        // Partition 1 has no data
        KafkaPartitionOffset offset1 = provider.getNextPartitionOffset(1, jobProps);
        Assert.assertNull(offset1);
    }
    
    @Test
    public void testGetNextPartitionOffsetLimitedByLatest() {
        KafkaOffset currentOffset = new KafkaOffset("topic", "catalog", "db");
        currentOffset.updatePartitionOffset(0, 900L);
        provider.setCurrentOffset(currentOffset);
        
        Map<Integer, Long> latestOffsets = new HashMap<>();
        latestOffsets.put(0, 930L);  // Only 30 messages available
        provider.setLatestOffsets(latestOffsets);
        provider.setMaxBatchRows(100L);
        
        StreamingJobProperties jobProps = new StreamingJobProperties(new HashMap<>());
        KafkaPartitionOffset offset = provider.getNextPartitionOffset(0, jobProps);
        
        Assert.assertNotNull(offset);
        Assert.assertEquals(900L, offset.getStartOffset());
        Assert.assertEquals(930L, offset.getEndOffset());  // Limited by latest, not maxBatchRows
    }
    
    @Test
    public void testHasMoreDataForPartitionTrue() {
        KafkaOffset currentOffset = new KafkaOffset("topic", "catalog", "db");
        currentOffset.updatePartitionOffset(0, 100L);
        currentOffset.updatePartitionOffset(1, 200L);
        provider.setCurrentOffset(currentOffset);
        
        Map<Integer, Long> latestOffsets = new HashMap<>();
        latestOffsets.put(0, 500L);  // Has data
        latestOffsets.put(1, 200L);  // No data
        provider.setLatestOffsets(latestOffsets);
        
        Assert.assertTrue(provider.hasMoreDataForPartition(0));
        Assert.assertFalse(provider.hasMoreDataForPartition(1));
    }
    
    @Test
    public void testHasMoreDataForPartitionUnknownPartition() {
        KafkaOffset currentOffset = new KafkaOffset("topic", "catalog", "db");
        currentOffset.updatePartitionOffset(0, 100L);
        provider.setCurrentOffset(currentOffset);
        
        Map<Integer, Long> latestOffsets = new HashMap<>();
        latestOffsets.put(0, 500L);
        provider.setLatestOffsets(latestOffsets);
        
        // Partition 99 doesn't exist
        Assert.assertFalse(provider.hasMoreDataForPartition(99));
    }
    
    @Test
    public void testGetAllPartitionIds() {
        KafkaOffset currentOffset = new KafkaOffset("topic", "catalog", "db");
        currentOffset.updatePartitionOffset(0, 100L);
        currentOffset.updatePartitionOffset(1, 200L);
        currentOffset.updatePartitionOffset(2, 300L);
        provider.setCurrentOffset(currentOffset);
        
        java.util.Set<Integer> partitionIds = provider.getAllPartitionIds();
        
        Assert.assertEquals(3, partitionIds.size());
        Assert.assertTrue(partitionIds.contains(0));
        Assert.assertTrue(partitionIds.contains(1));
        Assert.assertTrue(partitionIds.contains(2));
    }
    
    @Test
    public void testGetAllPartitionIdsEmpty() {
        // No current offset set
        java.util.Set<Integer> partitionIds = provider.getAllPartitionIds();
        Assert.assertTrue(partitionIds.isEmpty());
    }
    
    @Test
    public void testIndependentPipelineScenario() {
        // Simulates the independent pipeline model:
        // 1. Create initial tasks for all partitions
        // 2. Partition 0 completes, gets new task immediately
        // 3. Partition 1 has no more data, becomes idle
        // 4. Later, partition 1 gets new data and restarts
        
        KafkaOffset currentOffset = new KafkaOffset("topic", "catalog", "db");
        currentOffset.updatePartitionOffset(0, 0L);
        currentOffset.updatePartitionOffset(1, 0L);
        provider.setCurrentOffset(currentOffset);
        
        Map<Integer, Long> latestOffsets = new HashMap<>();
        latestOffsets.put(0, 200L);
        latestOffsets.put(1, 50L);
        provider.setLatestOffsets(latestOffsets);
        provider.setMaxBatchRows(100L);
        
        StreamingJobProperties jobProps = new StreamingJobProperties(new HashMap<>());
        
        // Step 1: Get initial offsets
        List<KafkaPartitionOffset> initialOffsets = provider.getNextPartitionOffsets(jobProps);
        Assert.assertEquals(2, initialOffsets.size());
        
        // Step 2: Partition 0 completes (consumed 100 rows), gets next task
        provider.updatePartitionOffset(0, 100L);
        KafkaPartitionOffset nextOffset0 = provider.getNextPartitionOffset(0, jobProps);
        Assert.assertNotNull(nextOffset0);
        Assert.assertEquals(100L, nextOffset0.getStartOffset());
        Assert.assertEquals(200L, nextOffset0.getEndOffset());
        
        // Step 3: Partition 1 completes (consumed 50 rows), no more data
        provider.updatePartitionOffset(1, 50L);
        KafkaPartitionOffset nextOffset1 = provider.getNextPartitionOffset(1, jobProps);
        Assert.assertNull(nextOffset1);  // No more data
        Assert.assertFalse(provider.hasMoreDataForPartition(1));
        
        // Step 4: New data arrives for partition 1
        latestOffsets.put(1, 150L);
        provider.setLatestOffsets(latestOffsets);
        Assert.assertTrue(provider.hasMoreDataForPartition(1));
        
        // Step 5: Partition 1 restarts
        KafkaPartitionOffset restartOffset1 = provider.getNextPartitionOffset(1, jobProps);
        Assert.assertNotNull(restartOffset1);
        Assert.assertEquals(50L, restartOffset1.getStartOffset());
        Assert.assertEquals(150L, restartOffset1.getEndOffset());
    }
}
