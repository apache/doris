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

package org.apache.doris.catalog.stream;

import org.apache.doris.catalog.stream.OlapTableStream.PartitionStreamState;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.transaction.TransactionCommitFailedException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class OlapTableStreamUpdateTest {

    @Test
    public void testCheckPartitionOffsetForHistoricalConsumeOk() throws Exception {
        Map<Long, PartitionStreamState> partitionStates =
                Collections.singletonMap(1L, new PartitionStreamState(null, -1, 100L));

        // history partition prev offset is encoded as negative (see StreamConsumptionInfoExtractor)
        Map<Long, Long> prev = Collections.singletonMap(1L, -100L);
        Map<Long, Long> next = Collections.emptyMap();
        OlapTableStreamUpdate update = new OlapTableStreamUpdate(prev, next);

        update.checkPartitionOffset("test_db", "s1", partitionStates);
    }

    @Test
    public void testCheckPartitionOffsetForHistoricalConsumeConflict() {
        Map<Long, PartitionStreamState> partitionStates =
                Collections.singletonMap(1L, new PartitionStreamState(null, -1, 99L));

        // history partition prev offset is encoded as negative; -(-100)=100 != 99 -> conflict
        Map<Long, Long> prev = Collections.singletonMap(1L, -100L);
        Map<Long, Long> next = new HashMap<>();
        OlapTableStreamUpdate update = new OlapTableStreamUpdate(prev, next);

        TransactionCommitFailedException exception = Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> update.checkPartitionOffset("test_db", "s1", partitionStates));
        Assertions.assertTrue(exception.getMessage().contains("history offset not consumed"));
    }

    @Test
    public void testIncrementalOffsetValidation() throws Exception {
        Map<Long, PartitionStreamState> states = Map.of(1L, new PartitionStreamState(100L, 10, null));
        new OlapTableStreamUpdate(Map.of(1L, 100L), Map.of(1L, 200L))
                .checkPartitionOffset("test_db", "s1", states);
        Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> new OlapTableStreamUpdate(Map.of(1L, 99L), Map.of(1L, 200L))
                        .checkPartitionOffset("test_db", "s1", states));
        Assertions.assertThrows(TransactionCommitFailedException.class,
                () -> new OlapTableStreamUpdate(Collections.emptyMap(), Map.of(1L, 200L))
                        .checkPartitionOffset("test_db", "s1", states));
        // A newly added partition has no recorded offset and can be consumed without prev.
        new OlapTableStreamUpdate(Collections.emptyMap(), Map.of(2L, 200L))
                .checkPartitionOffset("test_db", "s1", states);
        // A history-only state must not be mistaken for an incremental offset.
        new OlapTableStreamUpdate(Collections.emptyMap(), Map.of(2L, 200L))
                .checkPartitionOffset("test_db", "s1", Map.of(2L, new PartitionStreamState(null, -1, 100L)));
    }

    @Test
    public void testPartitionStatePersistenceAndConsumption() throws Exception {
        OlapTableStream stream = new OlapTableStream();
        Map<Long, PartitionStreamState> states = new HashMap<>();
        states.put(1L, new PartitionStreamState(null, -1, 100L));
        states.put(2L, new PartitionStreamState(200L, 20, null));
        states.put(3L, new PartitionStreamState(-1L, -1, null));
        Deencapsulation.setField(stream, "partitionStates", states);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        stream.write(new DataOutputStream(bytes));
        OlapTableStream restored = OlapTableStream.read(new DataInputStream(
                new ByteArrayInputStream(bytes.toByteArray())));
        Assertions.assertTrue(restored.hasHistoricalData(1L));
        Assertions.assertFalse(restored.hasConsumedData(1L));
        Assertions.assertTrue(restored.hasConsumedData(2L));
        Assertions.assertFalse(restored.hasConsumedData(3L));
        Assertions.assertFalse(restored.hasConsumedData(4L));

        Map<Long, PartitionStreamState> restoredStates = Deencapsulation.getField(restored, "partitionStates");
        Assertions.assertNull(restoredStates.get(1L).getOffset());
        Assertions.assertEquals(100L, restoredStates.get(1L).getHistoricalTso());
        Assertions.assertEquals(20, restoredStates.get(2L).getConsumptionTime());
        restored.writeLock();
        try {
            OlapTableStreamUpdate update = new OlapTableStreamUpdate(Map.of(1L, -100L), Map.of(1L, 150L));
            update.checkPartitionOffset("test_db", "s1", restoredStates);
            restored.unprotectedUpdateStreamUpdate(update, 30L);
            Assertions.assertFalse(restored.hasHistoricalData(1L));
            Assertions.assertEquals(150L, restoredStates.get(1L).getOffset());
            Assertions.assertEquals(30, restoredStates.get(1L).getConsumptionTime());
            Assertions.assertEquals(Set.of(1L, 3L),
                    restored.unprotectedCollectStalePartitionOffsetIds(Set.of(2L)));
            restored.unprotectedPrunePartitionOffsets(Set.of(1L, 3L));
            Assertions.assertEquals(Set.of(2L), restoredStates.keySet());
        } finally {
            restored.writeUnlock();
        }
    }
}
