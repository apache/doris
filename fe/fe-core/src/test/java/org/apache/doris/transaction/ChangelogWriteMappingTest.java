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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.LocalTabletInvertedIndex;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.master.ReportHandler;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TChangelogIndexWriteSnapshot;
import org.apache.doris.thrift.TChangelogWriteColumnMapping;
import org.apache.doris.thrift.TChangelogWriteColumnMappings;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TPublishVersionRequest;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.SetMultimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ChangelogWriteMappingTest {
    private TChangelogWriteColumnMapping entry(int source, int current) {
        return new TChangelogWriteColumnMapping().setSourceColumnUniqueId(source).setCurrentColumnUniqueId(current);
    }

    private ChangelogWriteMapping mapping(boolean historical, TChangelogWriteColumnMapping... entries) {
        return ChangelogWriteMapping.fromThrift(new TChangelogWriteColumnMappings()
                .setNeedHistoricalValue(historical).setEntries(Arrays.asList(entries)));
    }

    @Test
    void columnRelationshipsUseUidsDespiteReorderedTargets() {
        Column key = new Column("key", PrimitiveType.INT);
        key.setIsKey(true);
        key.setUniqueId(3);
        Column value = new Column("value", PrimitiveType.INT);
        value.setUniqueId(7);
        Column currentKey = Column.generateChangelogKeyColumn(key);
        currentKey.setUniqueId(31);
        Column currentValue = Column.generateAfterValueColumn(value);
        currentValue.setUniqueId(71);
        Column before = Column.generateBeforeValueColumn(value);
        before.setUniqueId(72);
        ChangelogWriteMapping mapping = ChangelogWriteMapping.fromColumns(Arrays.asList(key, value),
                Arrays.asList(before, currentKey, currentValue), true);
        List<TChangelogWriteColumnMapping> entries = mapping.toThrift().getEntries();
        Assertions.assertEquals(3, entries.get(0).getSourceColumnUniqueId());
        Assertions.assertEquals(31, entries.get(0).getCurrentColumnUniqueId());
        Assertions.assertFalse(entries.get(0).isSetBeforeColumnUniqueId());
        Assertions.assertEquals(7, entries.get(1).getSourceColumnUniqueId());
        Assertions.assertEquals(71, entries.get(1).getCurrentColumnUniqueId());
        Assertions.assertEquals(72, entries.get(1).getBeforeColumnUniqueId());
        Assertions.assertThrows(IllegalStateException.class, () -> ChangelogWriteMapping.fromColumns(
                Arrays.asList(key, value), Arrays.asList(currentKey, currentValue), true));
    }

    @Test
    void indexSnapshotRoundTripRejectsMissingAndDuplicateIndexes() {
        ChangelogWriteMapping mapping = mapping(false, entry(1, 1));
        Map<Long, ChangelogWriteMapping> snapshots = ImmutableMap.of(7L, mapping, 2L, mapping);
        List<TChangelogIndexWriteSnapshot> wire = ChangelogWriteMapping.toThrift(snapshots);
        Assertions.assertEquals(2L, wire.get(0).getSourceIndexId());
        Assertions.assertEquals(snapshots, ChangelogWriteMapping.fromThrift(wire));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ChangelogWriteMapping.fromThrift(Arrays.asList(wire.get(0), wire.get(0))));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ChangelogWriteMapping.fromThrift(Collections.singletonList(new TChangelogIndexWriteSnapshot())));
        Assertions.assertEquals(Collections.emptyMap(), ChangelogWriteMapping.fromThrift(Collections.emptyList()));
    }

    @Test
    void publishKeepsActualChildSnapshotAfterReplayAndReportCapture() {
        TransactionState state = new TransactionState();
        state.registerChangelogWriteMappings(101, ImmutableMap.of(5L, mapping(true, entry(1, 11))));
        state.registerChangelogWriteMappings(102, ImmutableMap.of(5L, mapping(false, entry(1, 21))));
        state.setTransactionStatus(TransactionStatus.VISIBLE);
        TransactionState replay = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(state), TransactionState.class);
        PublishVersionTask.RepublishInfo report = new PublishVersionTask.RepublishInfo(
                new TPartitionVersionInfo(40, 2, 0), replay.getChangelogWriteMappings(102));
        PublishVersionTask task = new PublishVersionTask(10, 102, 1,
                Collections.singletonList(report.getPartitionVersionInfo()), 0);
        task.setChangelogWriteMappings(report.getMappings());
        TPublishVersionRequest request = task.toThrift();
        Assertions.assertEquals(102, request.getTransactionId());
        Assertions.assertEquals(5, request.getChangelogColumnMappings().get(0).getSourceIndexId());
        Assertions.assertEquals(21, request.getChangelogColumnMappings().get(0).getMappings()
                .getEntries().get(0).getCurrentColumnUniqueId());
        Assertions.assertFalse(request.getChangelogColumnMappings().get(0).getMappings().isNeedHistoricalValue());
        request.getChangelogColumnMappings().clear();
        Assertions.assertEquals(1, task.toThrift().getChangelogColumnMappingsSize());
    }

    @Test
    void tabletReportRepublishCapturesChildMappingAndPartitionTogether() {
        TransactionState state = new TransactionState();
        state.setSubTxnIds(Arrays.asList(101L, 102L));
        state.registerChangelogWriteMappings(101, ImmutableMap.of(5L, mapping(true, entry(1, 11))));
        state.registerChangelogWriteMappings(102, ImmutableMap.of(5L, mapping(false, entry(1, 21))));
        TableCommitInfo commit = new TableCommitInfo(20);
        commit.addPartitionCommitInfo(new PartitionCommitInfo(30, "", 40, 0, false));
        state.getSubTxnIdToTableCommitInfo().put(102L, commit);
        state.setTransactionStatus(TransactionStatus.VISIBLE);
        Map<Long, SetMultimap<Long, PublishVersionTask.RepublishInfo>> reports = new HashMap<>();
        Deencapsulation.invoke(new LocalTabletInvertedIndex(), "publishPartition", state, 102L,
                new TabletMeta(-1, 20, 30, 5, 0, TStorageMedium.HDD, false), 30L, reports);
        try (MockedStatic<AgentTaskExecutor> executor = Mockito.mockStatic(AgentTaskExecutor.class)) {
            Deencapsulation.invoke(ReportHandler.class, "handleRepublishVersionInfo", reports, 9001L);
            PublishVersionTask task = (PublishVersionTask) AgentTaskQueue.getTask(9001, TTaskType.PUBLISH_VERSION, 102);
            TPublishVersionRequest request = task.toThrift();
            Assertions.assertEquals(40, request.getPartitionVersionInfos().get(0).getVersion());
            Assertions.assertEquals(21, request.getChangelogColumnMappings().get(0).getMappings()
                    .getEntries().get(0).getCurrentColumnUniqueId());
            Assertions.assertFalse(request.getChangelogColumnMappings().get(0).getMappings().isNeedHistoricalValue());
        } finally {
            AgentTaskQueue.removeTask(9001, TTaskType.PUBLISH_VERSION, 102);
        }
    }

    @Test
    void registrationBoundaryRejectsMissingAndAbortedTransactions() throws Exception {
        GlobalTransactionMgrIface manager = Mockito.mock(GlobalTransactionMgrIface.class, Mockito.CALLS_REAL_METHODS);
        Map<Long, ChangelogWriteMapping> snapshots = ImmutableMap.of(5L, mapping(false, entry(1, 10)));
        Assertions.assertThrows(UserException.class, () -> manager.registerChangelogWriteMappings(1, 100, snapshots));
        TransactionState state = new TransactionState();
        Mockito.when(manager.getTransactionState(1, 100)).thenReturn(state);
        manager.registerChangelogWriteMappings(1, 100, snapshots);
        Assertions.assertEquals(snapshots, state.getChangelogWriteMappings(100));
        state.setTransactionStatus(TransactionStatus.ABORTED);
        Assertions.assertThrows(UserException.class, () -> manager.registerChangelogWriteMappings(1, 100, snapshots));
        Assertions.assertDoesNotThrow(() -> manager.registerChangelogWriteMappings(1, 999, Collections.emptyMap()));
    }

    @Test
    void installationDetachesMutableCallerMaps() {
        Map<Long, ChangelogWriteMapping> input = new HashMap<>();
        input.put(5L, mapping(false, entry(1, 10)));
        TransactionState state = new TransactionState();
        state.registerChangelogWriteMappings(100, input);
        input.clear();
        Assertions.assertEquals(Collections.singleton(5L), state.getChangelogWriteMappings(100).keySet());
    }

    @Test
    void canonicalOrderAndKeyOnlyHistoricalRoundTrip() {
        ChangelogWriteMapping first = mapping(true, entry(2, 20), entry(1, 10));
        ChangelogWriteMapping second = mapping(true, entry(1, 10), entry(2, 20));
        Assertions.assertEquals(first, second);
        Assertions.assertEquals(first.hashCode(), second.hashCode());
        Assertions.assertEquals(first, ChangelogWriteMapping.fromThrift(first.toThrift()));
        Assertions.assertTrue(first.toThrift().isNeedHistoricalValue());
        Assertions.assertEquals(1, first.toThrift().getEntries().get(0).getSourceColumnUniqueId());
        first.toThrift().getEntries().clear();
        Assertions.assertEquals(2, first.toThrift().getEntriesSize());
    }

    @Test
    void malformedMappingsAreRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ChangelogWriteMapping.fromThrift(new TChangelogWriteColumnMappings()));
        Assertions.assertThrows(IllegalArgumentException.class, () -> mapping(false, entry(1, 10), entry(1, 11)));
        Assertions.assertThrows(IllegalArgumentException.class, () -> mapping(false, entry(1, 10), entry(2, 10)));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> mapping(false, entry(1, 10).setBeforeColumnUniqueId(11)));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> mapping(true, entry(1, 10).setBeforeColumnUniqueId(10)));
        Assertions.assertDoesNotThrow(() -> mapping(false, entry(1, 1)));
    }

    @Test
    void precommitFreezesAtomicSnapshotsAndReplayPreservesThem() throws Exception {
        TransactionState state = new TransactionState();
        ChangelogWriteMapping mapping = mapping(true, entry(1, 10));
        state.registerChangelogWriteMappings(100, ImmutableMap.of(5L, mapping));
        Assertions.assertThrows(IllegalStateException.class, () -> state.registerChangelogWriteMappings(100,
                ImmutableMap.of(6L, mapping, 5L, mapping(false, entry(1, 10)))));
        Assertions.assertEquals(Collections.singleton(5L), state.getChangelogWriteMappings(100).keySet());
        state.setTransactionStatus(TransactionStatus.PRECOMMITTED);
        TransactionState replay = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(state), TransactionState.class);
        replay.registerChangelogWriteMappings(100, ImmutableMap.of(5L, mapping));
        Assertions.assertThrows(IllegalStateException.class,
                () -> replay.registerChangelogWriteMappings(100, ImmutableMap.of(6L, mapping)));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> replay.getChangelogWriteMappings(100).clear());
        Assertions.assertEquals(mapping, replay.getChangelogWriteMappings(100).get(5L));
    }

    @Test
    void remoteEmptyIsCompleteAndCannotBeInstalledAfterFreeze() {
        TransactionState state = new TransactionState();
        state.installRemoteChangelogWriteMappings(Collections.emptyMap());
        state.setTransactionStatus(TransactionStatus.COMMITTED);
        state.installRemoteChangelogWriteMappings(Collections.emptyMap());
        TransactionState replay = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(state), TransactionState.class);
        replay.installRemoteChangelogWriteMappings(Collections.emptyMap());
        Assertions.assertThrows(IllegalStateException.class, () -> state.installRemoteChangelogWriteMappings(
                ImmutableMap.of(5L, mapping(false, entry(1, 10)))));
        TransactionState frozen = new TransactionState();
        frozen.setTransactionStatus(TransactionStatus.PRECOMMITTED);
        Assertions.assertThrows(IllegalStateException.class,
                () -> frozen.installRemoteChangelogWriteMappings(Collections.emptyMap()));
        frozen.setTransactionStatus(TransactionStatus.UNKNOWN);
        Assertions.assertThrows(IllegalStateException.class,
                () -> frozen.registerChangelogWriteMappings(100, Collections.emptyMap()));
        frozen.setTransactionStatus(TransactionStatus.ABORTED);
        Assertions.assertThrows(IllegalStateException.class,
                () -> frozen.registerChangelogWriteMappings(100, Collections.emptyMap()));
    }
}
