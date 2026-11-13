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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.stream.OlapTableStream.PartitionStreamState;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.transaction.TransactionCommitFailedException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
        // This fixture exercises offsets without registering a source table in a catalog.
        restored = Mockito.spy(restored);
        Mockito.doReturn(null).when(restored).getBaseTableNullable();
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

    @Test
    public void testPendingTupleIsPublishedTogetherAndDoesNotFollowReports() throws Exception {
        try (CountsFixture fixture = new CountsFixture()) {
            fixture.base.setRowCount(5L);
            fixture.changelog.setRowCount(10L);
            fixture.stream.unprotectedPublishPartitionState(10L,
                    new PartitionStreamState(100L, 1L, null).withRowCounts(fixture.source, 10L));
            PartitionStreamState previous = fixture.stream.getPartitionState(10L);
            fixture.base.setRowCount(50L);
            fixture.changelog.setRowCount(100L);
            OlapTableStreamUpdate update = new OlapTableStreamUpdate(Map.of(10L, 100L), Map.of(10L, 200L));
            fixture.stream.unprotectedPrepareStreamUpdate(update);
            Assertions.assertSame(previous, fixture.stream.getPartitionState(10L));
            Assertions.assertEquals(200L, update.getNext().get(10L).getOffset());
            Assertions.assertEquals(50L, update.getNext().get(10L).getBaseRowCount());
            Assertions.assertEquals(100L, update.getNext().get(10L).getChangelogRowCount());
            // A later BE report cannot change the tuple already captured by this update.
            fixture.base.setRowCount(70L);
            fixture.changelog.setRowCount(130L);
            fixture.stream.unprotectedUpdateStreamUpdate(update, 2L);
            PartitionStreamState committed = fixture.stream.getPartitionState(10L);
            Assertions.assertEquals(200L, committed.getOffset());
            Assertions.assertEquals(50L, committed.getBaseRowCount());
            Assertions.assertEquals(100L, committed.getChangelogRowCount());
            Assertions.assertTrue(committed.matches(fixture.source, 10L, 200L));
            OlapTableStreamUpdate unchanged = new OlapTableStreamUpdate(Map.of(10L, 200L), Map.of(10L, 200L));
            fixture.stream.unprotectedPrepareStreamUpdate(unchanged);
            fixture.stream.unprotectedUpdateStreamUpdate(unchanged, 3L);
            Assertions.assertEquals(50L, fixture.stream.getPartitionState(10L).getBaseRowCount());
            Assertions.assertEquals(100L, fixture.stream.getPartitionState(10L).getChangelogRowCount());
        }
    }

    @Test
    public void testUpdateAndImagePersistTheSameTupleAndReplayClearsOnlyBinding() throws Exception {
        try (CountsFixture fixture = new CountsFixture()) {
            fixture.base.setRowCount(50L);
            fixture.changelog.setRowCount(100L);
            OlapTableStreamUpdate update = new OlapTableStreamUpdate(Map.of(), Map.of(10L, 200L));
            fixture.stream.unprotectedPrepareStreamUpdate(update);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            new TableStreamUpdateInfo(1L, 2L, update).write(new DataOutputStream(bytes));
            TableStreamUpdateInfo restored = TableStreamUpdateInfo.read(new DataInputStream(
                    new ByteArrayInputStream(bytes.toByteArray())));
            OlapTableStreamUpdate restoredUpdate = (OlapTableStreamUpdate) restored.getUpdate();
            Assertions.assertEquals(update.getNext(), restoredUpdate.getNext());
            fixture.base.setRowCount(70L);
            fixture.changelog.setRowCount(130L);
            fixture.stream.unprotectedUpdateStreamUpdate(restoredUpdate, 2L, true);
            PartitionStreamState replayed = fixture.stream.getPartitionState(10L);
            Assertions.assertEquals(200L, replayed.getOffset());
            Assertions.assertEquals(50L, replayed.getBaseRowCount());
            Assertions.assertEquals(100L, replayed.getChangelogRowCount());
            Assertions.assertFalse(replayed.matches(fixture.source, 10L, 200L));
            OlapTableStream image = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(fixture.stream), OlapTableStream.class);
            Assertions.assertEquals(replayed, image.getPartitionState(10L));
            Assertions.assertFalse(image.getPartitionState(10L).matches(fixture.source, 10L, 200L));
        }
    }

    private static class CountsFixture implements AutoCloseable {
        private final MaterializedIndex base = new MaterializedIndex(1L, MaterializedIndex.IndexState.NORMAL);
        private final MaterializedIndex changelog = new MaterializedIndex(2L, MaterializedIndex.IndexState.NORMAL);
        private final OlapTable source = Mockito.mock(OlapTable.class);
        private final OlapTableStream stream = Mockito.spy(new OlapTableStream());
        private final MockedStatic<Env> env = Mockito.mockStatic(Env.class);

        private CountsFixture() {
            Env current = Mockito.mock(Env.class);
            env.when(Env::getCurrentEnv).thenReturn(current);
            Partition partition = new Partition(10L, "p", base, new RandomDistributionInfo(1));
            partition.createRollupIndex(changelog);
            MaterializedIndexMeta meta = Mockito.mock(MaterializedIndexMeta.class);
            Mockito.when(meta.getIndexId()).thenReturn(2L);
            Mockito.when(source.getBaseIndexId()).thenReturn(1L);
            Mockito.when(source.hasChangelog()).thenReturn(true);
            Mockito.when(source.getChangelogIndexMeta()).thenReturn(meta);
            Mockito.when(source.getPartition(10L)).thenReturn(partition);
            Mockito.when(source.tryReadLock(0, TimeUnit.MILLISECONDS)).thenReturn(true);
            Mockito.doReturn(source).when(stream).getBaseTableNullable();
            stream.writeLock();
        }

        @Override
        public void close() {
            stream.writeUnlock();
            env.close();
        }
    }

}
