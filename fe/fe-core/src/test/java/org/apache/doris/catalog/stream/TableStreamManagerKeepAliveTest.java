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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.CloudTableStreamReadStateHelper;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.TableStreamCleanupInfo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.tso.TSOService;
import org.apache.doris.tso.TSOTimestamp;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TableStreamManagerKeepAliveTest extends TestWithFeService {
    private static final long TTL_SECONDS = 10;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;
        Config.enable_feature_changelog = true;
        createDatabase("test_stream_keepalive");
        connectContext.setDatabase("test_stream_keepalive");
    }

    @Test
    public void testKeepAliveRecordsTsoForIdlePartitionAndReplaysIt() throws Exception {
        StreamContext context = createStreamContext("idle");
        long idleOffset = tsoSecondsAgo(60);
        setIdle(context, "p1", idleOffset);
        // p2 has a change after its offset: it is not kept alive.
        setIdle(context, "p2", idleOffset);
        setPartitionTso(context.partition("p2"), idleOffset + 1);
        long keepAliveTso = tsoSecondsAgo(0);

        List<TableStreamCleanupInfo> logged = new ArrayList<>();
        keepAlive(context, keepAliveTso, logged);

        Assertions.assertEquals(keepAliveTso, keepAliveTso(context, context.partition("p1").getId()));
        Assertions.assertNull(keepAliveTso(context, context.partition("p2").getId()));
        // The offset itself does not move.
        Assertions.assertEquals(idleOffset, context.stream.getPartitionOffset(context.partition("p1").getId()));

        // What was logged, replayed on a follower that has the same offsets, gives the same keep-alive TSOs.
        Assertions.assertEquals(1, logged.size());
        TableStreamCleanupInfo replayed = TableStreamCleanupInfo.read(roundTrip(logged.get(0)));
        Deencapsulation.setField(context.stream, "partitionKeepAlive",
                new ConcurrentHashMap<Long, OlapTableStream.PartitionKeepAlive>());
        Env.getCurrentEnv().getTableStreamManager().replayTableStreamCleanup(replayed);
        Assertions.assertEquals(keepAliveTso, keepAliveTso(context, context.partition("p1").getId()));
        Assertions.assertNull(keepAliveTso(context, context.partition("p2").getId()));
    }

    @Test
    public void testKeepAliveSkipsPartitionWithCommittedInvisibleTransaction() throws Exception {
        StreamContext context = createStreamContext("invisible");
        setIdle(context, "p1", tsoSecondsAgo(60));
        // A committed transaction that is not visible yet has taken a version, and its TSO is not in the
        // partition TSO until it is.
        Partition partition = context.partition("p1");
        partition.setNextVersion(partition.getVisibleVersion() + 2);

        keepAlive(context, tsoSecondsAgo(0), new ArrayList<>());

        Assertions.assertNull(keepAliveTso(context, partition.getId()));
    }

    @Test
    public void testKeepAliveSkipsPartitionInInitialSnapshot() throws Exception {
        StreamContext context = createStreamContext("initial_snapshot");
        long partitionId = context.partition("p1").getId();
        setIdle(context, "p1", tsoSecondsAgo(60));
        Deencapsulation.setField(context.stream, "historicalPartitionTSO",
                new HashMap<>(Collections.singletonMap(partitionId, tsoSecondsAgo(60))));

        keepAlive(context, tsoSecondsAgo(0), new ArrayList<>());

        Assertions.assertNull(keepAliveTso(context, partitionId));
    }

    @Test
    public void testKeepAliveRenewsOnlyOnceTheMarginIsBelowHalfTheTtl() throws Exception {
        StreamContext context = createStreamContext("margin");
        long partitionId = context.partition("p1").getId();
        // Two seconds old under a ten-second TTL: eight seconds of margin left, nothing to do.
        setIdle(context, "p1", tsoSecondsAgo(2));
        keepAlive(context, tsoSecondsAgo(0), new ArrayList<>());
        Assertions.assertNull(keepAliveTso(context, partitionId));

        // An old offset with a keep-alive TSO two seconds old: the keep-alive TSO is what leaves the margin.
        long offset = tsoSecondsAgo(60);
        setIdle(context, "p1", offset);
        long freshKeepAliveTso = tsoSecondsAgo(2);
        keepAliveDirectly(context, partitionId, offset, freshKeepAliveTso);
        keepAlive(context, tsoSecondsAgo(0), new ArrayList<>());
        Assertions.assertEquals(freshKeepAliveTso, keepAliveTso(context, partitionId));

        // Six seconds old: less than half the TTL is left, and it is renewed.
        Deencapsulation.setField(context.stream, "partitionKeepAlive", new ConcurrentHashMap<>(
                Collections.singletonMap(partitionId,
                        new OlapTableStream.PartitionKeepAlive(offset, tsoSecondsAgo(6)))));
        long renewed = tsoSecondsAgo(0);
        keepAlive(context, renewed, new ArrayList<>());
        Assertions.assertEquals(renewed, keepAliveTso(context, partitionId));
    }

    @Test
    public void testKeepAliveSkipsTableWithoutTtl() throws Exception {
        // A table whose changelog is on always has a TTL; it has none once the changelog stops applying one.
        StreamContext context = createStreamContext("no_ttl");
        context.baseTable.getChangelogConfig().setTtlSeconds(0);
        Assertions.assertFalse(context.baseTable.hasChangelogTtl());
        setIdle(context, "p1", tsoSecondsAgo(60));

        keepAlive(context, tsoSecondsAgo(0), new ArrayList<>());

        Assertions.assertNull(keepAliveTso(context, context.partition("p1").getId()));
    }

    @Test
    public void testKeepAliveSkipsTtlThatNeverExpiresAnOffset() throws Exception {
        // A TTL longer than the time since the epoch never expires an offset: there is nothing to keep alive.
        StreamContext context = createStreamContext("long_ttl");
        context.baseTable.getChangelogConfig().setTtlSeconds(Long.MAX_VALUE);
        Assertions.assertTrue(context.baseTable.hasChangelogTtl());
        setIdle(context, "p1", tsoSecondsAgo(60));

        keepAlive(context, tsoSecondsAgo(0), new ArrayList<>());

        Assertions.assertNull(keepAliveTso(context, context.partition("p1").getId()));
    }

    @Test
    public void testCleanupPrunesKeepAliveTsoOfDroppedPartition() throws Exception {
        StreamContext context = createStreamContext("prune");
        long keptPartitionId = context.partition("p1").getId();
        long droppedPartitionId = context.partition("p2").getId();
        keepAliveDirectly(context, keptPartitionId, tsoSecondsAgo(60), tsoSecondsAgo(1));
        keepAliveDirectly(context, droppedPartitionId, tsoSecondsAgo(60), tsoSecondsAgo(1));

        alterTableSync("alter table test_stream_keepalive." + context.baseTable.getName() + " drop partition p2");
        Env.getCurrentEnv().getTableStreamManager().cleanupStalePartitionOffsets();

        Assertions.assertNotNull(context.stream.getKeepAlive(keptPartitionId));
        Assertions.assertNull(context.stream.getKeepAlive(droppedPartitionId));
    }

    @Test
    public void testStateWrittenBeforeKeepAliveTsoReads() throws Exception {
        StreamContext context = createStreamContext("old_json");
        long partitionId = context.partition("p1").getId();
        setIdle(context, "p1", tsoSecondsAgo(60));
        String json = GsonUtils.GSON.toJson(context.stream);
        Assertions.assertTrue(json.contains("\"pka\""), json);
        String oldJson = json.replaceAll(",?\"pka\":\\{[^}]*}", "");
        Assertions.assertFalse(oldJson.contains("\"pka\""), oldJson);

        OlapTableStream oldStream = GsonUtils.GSON.fromJson(oldJson, OlapTableStream.class);
        Assertions.assertNull(oldStream.getKeepAlive(partitionId));
        Assertions.assertEquals(context.stream.getPartitionOffset(partitionId),
                oldStream.getPartitionOffset(partitionId));

        TableStreamCleanupInfo oldInfo = GsonUtils.GSON.fromJson(
                "{\"entries\":[],\"staleDbIds\":[],\"staleStreamIds\":[]}", TableStreamCleanupInfo.class);
        Assertions.assertTrue(oldInfo.getKeepAliveEntries().isEmpty());
    }

    @Test
    public void testKeepAliveTsoReadsBackFromTheImage() throws Exception {
        StreamContext context = createStreamContext("image");
        long partitionId = context.partition("p1").getId();
        long offset = tsoSecondsAgo(60);
        setIdle(context, "p1", offset);
        long keepAliveTso = tsoSecondsAgo(1);
        keepAliveDirectly(context, partitionId, offset, keepAliveTso);

        OlapTableStream read = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(context.stream), OlapTableStream.class);
        OlapTableStream.PartitionKeepAlive keepAlive = read.getKeepAlive(partitionId);
        Assertions.assertNotNull(keepAlive);
        Assertions.assertEquals(offset, keepAlive.getOffset());
        Assertions.assertEquals(keepAliveTso, keepAlive.getKeepAliveTso());
        // An IVM refresh reads it without the stream lock, so it stays a concurrent map once read from an image.
        Assertions.assertInstanceOf(ConcurrentMap.class, Deencapsulation.getField(read, "partitionKeepAlive"));
    }

    @Test
    public void testKeepAliveTakenAgainstAnotherOffsetDoesNotCount() throws Exception {
        StreamContext context = createStreamContext("moved_offset");
        long partitionId = context.partition("p1").getId();
        // A fresh keep-alive TSO taken against an older offset than the one the partition is read from now, which
        // is a minute old: it does not count, and the round renews against the current offset.
        long offset = tsoSecondsAgo(60);
        setIdle(context, "p1", offset);
        keepAliveDirectly(context, partitionId, offset - 1, tsoSecondsAgo(1));
        Assertions.assertEquals(offset,
                OlapTableStream.effectiveOffset(offset, context.stream.getKeepAlive(partitionId)));

        long renewed = tsoSecondsAgo(0);
        keepAlive(context, renewed, new ArrayList<>());
        Assertions.assertEquals(renewed, keepAliveTso(context, partitionId));
    }

    @Test
    public void testWrapperCountsKeepAliveOnlyForTheOffsetItWasTakenAgainst() throws Exception {
        StreamContext context = createStreamContext("wrapper");
        long partitionId = context.partition("p1").getId();
        List<Long> selected = Collections.singletonList(partitionId);
        long offset = tsoSecondsAgo(60);
        setIdle(context, "p1", offset);
        long keepAliveTso = tsoSecondsAgo(1);
        keepAliveDirectly(context, partitionId, offset, keepAliveTso);

        // In shared-nothing mode the read starts from the stream's offset.
        Assertions.assertEquals(keepAliveTso,
                new OlapTableStreamWrapper(context.stream, context.baseTable, selected).getKeepAliveTso(partitionId));
        // In cloud mode it starts from the offset MetaService reports.
        OlapTableStreamWrapper cloudRead = new OlapTableStreamWrapper(context.stream, context.baseTable, selected);
        cloudRead.installCloudReadStates(Collections.singletonMap(partitionId,
                consumed(partitionId, offset, offset + 5)));
        Assertions.assertEquals(keepAliveTso, cloudRead.getKeepAliveTso(partitionId));
        OlapTableStreamWrapper otherCloudRead =
                new OlapTableStreamWrapper(context.stream, context.baseTable, selected);
        otherCloudRead.installCloudReadStates(Collections.singletonMap(partitionId,
                consumed(partitionId, offset + 1, offset + 5)));
        Assertions.assertNull(otherCloudRead.getKeepAliveTso(partitionId));

        // A consumption moved the offset: the keep-alive TSO no longer counts.
        setIdle(context, "p1", offset + 1);
        Assertions.assertNull(
                new OlapTableStreamWrapper(context.stream, context.baseTable, selected).getKeepAliveTso(partitionId));
    }

    @Test
    public void testCloudKeepAliveRecordsCommittedTsoForPartitionAtItsEnd() throws Exception {
        StreamContext context = createStreamContext("cloud_idle");
        long p1 = context.partition("p1").getId();
        long p2 = context.partition("p2").getId();
        long offset = tsoSecondsAgo(60);
        long committedTso = tsoSecondsAgo(0);
        Map<Long, Cloud.TableStreamPartitionReadStatePB> states = new HashMap<>();
        states.put(p1, consumed(p1, offset, offset));
        // A change after the offset: not at its end.
        states.put(p2, consumed(p2, offset, offset + 1));

        List<Map<Cloud.TableStreamIdentityPB, Set<Long>>> requests = new ArrayList<>();
        List<TableStreamCleanupInfo> logged = new ArrayList<>();
        cloudKeepAlive(Collections.singletonList(context.stream), tsoStatus(committedTso), 10, states, requests,
                logged);

        Assertions.assertEquals(1, requests.size());
        Cloud.TableStreamIdentityPB identity = Cloud.TableStreamIdentityPB.newBuilder()
                .setBaseDbId(context.stream.getBaseTableInfo().getDbId())
                .setBaseTableId(context.baseTable.getId())
                .setStreamDbId(context.stream.getDatabase().getId())
                .setStreamId(context.stream.getId())
                .build();
        Assertions.assertEquals(Collections.singletonMap(identity, ImmutableSet.of(p1, p2)), requests.get(0));
        assertKeepAlive(context, p1, offset, committedTso);
        Assertions.assertNull(context.stream.getKeepAlive(p2));

        // What was logged replays to the same keep-alive TSOs.
        Assertions.assertEquals(1, logged.size());
        TableStreamCleanupInfo replayed = TableStreamCleanupInfo.read(roundTrip(logged.get(0)));
        Deencapsulation.setField(context.stream, "partitionKeepAlive",
                new ConcurrentHashMap<Long, OlapTableStream.PartitionKeepAlive>());
        Env.getCurrentEnv().getTableStreamManager().replayTableStreamCleanup(replayed);
        assertKeepAlive(context, p1, offset, committedTso);
        Assertions.assertNull(context.stream.getKeepAlive(p2));
    }

    @Test
    public void testCloudKeepAliveSkipsPartitionsWithoutARealOffset() throws Exception {
        StreamContext context = createStreamContext("cloud_no_offset");
        StreamContext unknown = createStreamContext("cloud_unknown");
        long p1 = context.partition("p1").getId();
        long p2 = context.partition("p2").getId();
        long offset = tsoSecondsAgo(60);
        Map<Long, Cloud.TableStreamPartitionReadStatePB> states = new HashMap<>();
        states.put(p1, Cloud.TableStreamPartitionReadStatePB.newBuilder().setPartitionId(p1)
                .setOffsetState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING)
                .setOffsetTso(offset).setEndTso(offset).setVisibleVersion(2).build());
        // The sentinel offset of a partition that was empty when the stream was created.
        states.put(p2, consumed(p2, -1, -1));

        List<TableStreamCleanupInfo> logged = new ArrayList<>();
        cloudKeepAlive(ImmutableList.of(context.stream, unknown.stream), tsoStatus(tsoSecondsAgo(0)), 10, states,
                new ArrayList<>(), logged);

        Assertions.assertTrue(logged.isEmpty());
        Assertions.assertNull(context.stream.getKeepAlive(p1));
        Assertions.assertNull(context.stream.getKeepAlive(p2));
        Assertions.assertNull(unknown.stream.getKeepAlive(unknown.partition("p1").getId()));
    }

    @Test
    public void testCloudKeepAliveRenewsOnlyWhenTheMarginIsLowAndTheWatermarkIsPastIt() throws Exception {
        StreamContext context = createStreamContext("cloud_margin");
        long p1 = context.partition("p1").getId();
        long p2 = context.partition("p2").getId();
        long oldOffset = tsoSecondsAgo(60);
        Map<Long, Cloud.TableStreamPartitionReadStatePB> states = new HashMap<>();
        // Two seconds old under a ten-second TTL: eight seconds of margin left.
        states.put(p1, consumed(p1, tsoSecondsAgo(2), tsoSecondsAgo(2)));
        // An old offset whose keep-alive TSO, taken against it, is two seconds old.
        states.put(p2, consumed(p2, oldOffset, oldOffset));
        long freshKeepAliveTso = tsoSecondsAgo(2);
        keepAliveDirectly(context, p2, oldOffset, freshKeepAliveTso);

        cloudKeepAlive(Collections.singletonList(context.stream), tsoStatus(tsoSecondsAgo(0)), 10, states,
                new ArrayList<>(), new ArrayList<>());
        Assertions.assertNull(context.stream.getKeepAlive(p1));
        assertKeepAlive(context, p2, oldOffset, freshKeepAliveTso);

        // Low margin, but the watermark is not past the offset: nothing it could add.
        states.put(p1, consumed(p1, oldOffset, oldOffset));
        cloudKeepAlive(Collections.singletonList(context.stream), tsoStatus(oldOffset), 10, states,
                new ArrayList<>(), new ArrayList<>());
        Assertions.assertNull(context.stream.getKeepAlive(p1));
    }

    @Test
    public void testCloudKeepAliveWaitsForCommittedTso() throws Exception {
        StreamContext context = createStreamContext("cloud_tso");
        long p1 = context.partition("p1").getId();
        Map<Long, Cloud.TableStreamPartitionReadStatePB> states = new HashMap<>();
        states.put(p1, consumed(p1, tsoSecondsAgo(60), tsoSecondsAgo(60)));
        List<Map<Cloud.TableStreamIdentityPB, Set<Long>>> requests = new ArrayList<>();

        // Not calibrated yet, or calibrated before any committed TSO was persisted: no read at all.
        cloudKeepAlive(Collections.singletonList(context.stream),
                new TSOService.TSOStatusSnapshot(false, 0, 0, 0), 10, states, requests, new ArrayList<>());
        cloudKeepAlive(Collections.singletonList(context.stream),
                new TSOService.TSOStatusSnapshot(true, tsoSecondsAgo(0), 0, 0), 10, states, requests,
                new ArrayList<>());

        Assertions.assertTrue(requests.isEmpty());
        Assertions.assertNull(context.stream.getKeepAlive(p1));
    }

    @Test
    public void testCloudKeepAliveSkipsRoundWhenReadStatesFail() throws Exception {
        StreamContext context = createStreamContext("cloud_failure");
        List<TableStreamCleanupInfo> logged = new ArrayList<>();

        cloudKeepAlive(Collections.singletonList(context.stream), tsoStatus(tsoSecondsAgo(0)), 10, null,
                new ArrayList<>(), logged);

        Assertions.assertTrue(logged.isEmpty());
        Assertions.assertNull(context.stream.getKeepAlive(context.partition("p1").getId()));
    }

    @Test
    public void testCloudKeepAliveReadsInBatches() throws Exception {
        StreamContext first = createStreamContext("cloud_batch_1");
        StreamContext second = createStreamContext("cloud_batch_2");
        long offset = tsoSecondsAgo(60);
        Map<Long, Cloud.TableStreamPartitionReadStatePB> states = new HashMap<>();
        for (StreamContext context : ImmutableList.of(first, second)) {
            for (String name : ImmutableList.of("p1", "p2")) {
                long partitionId = context.partition(name).getId();
                states.put(partitionId, consumed(partitionId, offset, offset));
            }
        }
        long committedTso = tsoSecondsAgo(0);

        // Two partitions a read: one read for each stream.
        List<Map<Cloud.TableStreamIdentityPB, Set<Long>>> requests = new ArrayList<>();
        List<TableStreamCleanupInfo> logged = new ArrayList<>();
        cloudKeepAlive(ImmutableList.of(first.stream, second.stream), tsoStatus(committedTso), 2, states, requests,
                logged);
        Assertions.assertEquals(2, requests.size());
        Assertions.assertEquals(2, logged.size());
        for (StreamContext context : ImmutableList.of(first, second)) {
            assertKeepAlive(context, context.partition("p1").getId(), offset, committedTso);
            assertKeepAlive(context, context.partition("p2").getId(), offset, committedTso);
        }

        // Four partitions a read: both streams in one.
        requests.clear();
        cloudKeepAlive(ImmutableList.of(first.stream, second.stream), tsoStatus(committedTso), 4, states, requests,
                new ArrayList<>());
        Assertions.assertEquals(1, requests.size());
    }

    @Test
    public void testCloudKeepAliveFencesTheWatermarkBeforeAnyRead() throws Exception {
        StreamContext first = createStreamContext("cloud_fence_1");
        StreamContext second = createStreamContext("cloud_fence_2");
        long offset = tsoSecondsAgo(60);
        Map<Long, Cloud.TableStreamPartitionReadStatePB> states = new HashMap<>();
        for (StreamContext context : ImmutableList.of(first, second)) {
            for (String name : ImmutableList.of("p1", "p2")) {
                long partitionId = context.partition(name).getId();
                states.put(partitionId, consumed(partitionId, offset, offset));
            }
        }
        long committedTso = tsoSecondsAgo(0);

        // A commit at or below the watermark that the FE gave up on after a response it may outlive, such as a
        // MetaService transaction that timed out once its commit was sent, can still land. MetaService is fenced at
        // the watermark once a round, before any of its reads, so such a commit either shows in them or never lands.
        List<String> calls = new ArrayList<>();
        cloudKeepAlive(ImmutableList.of(first.stream, second.stream), tsoStatus(committedTso), 2, states,
                new ArrayList<>(), new ArrayList<>(), true, calls);

        Assertions.assertEquals(ImmutableList.of("fence " + committedTso, "read", "read"), calls);
        for (StreamContext context : ImmutableList.of(first, second)) {
            assertKeepAlive(context, context.partition("p1").getId(), offset, committedTso);
            assertKeepAlive(context, context.partition("p2").getId(), offset, committedTso);
        }
    }

    @Test
    public void testCloudKeepAliveSkipsRoundWhenTheFenceFails() throws Exception {
        StreamContext context = createStreamContext("cloud_fence_failure");
        long p1 = context.partition("p1").getId();
        long offset = tsoSecondsAgo(60);
        Map<Long, Cloud.TableStreamPartitionReadStatePB> states = new HashMap<>();
        states.put(p1, consumed(p1, offset, offset));
        long committedTso = tsoSecondsAgo(0);

        // Without the fence, a read cannot tell a partition with no change up to the watermark from one whose change
        // at or below it has not landed yet: nothing is read, and nothing is recorded.
        List<String> calls = new ArrayList<>();
        List<TableStreamCleanupInfo> logged = new ArrayList<>();
        cloudKeepAlive(Collections.singletonList(context.stream), tsoStatus(committedTso), 10, states,
                new ArrayList<>(), logged, false, calls);

        Assertions.assertEquals(ImmutableList.of("fence " + committedTso), calls);
        Assertions.assertTrue(logged.isEmpty());
        Assertions.assertNull(context.stream.getKeepAlive(p1));
    }

    private void keepAlive(StreamContext context, long keepAliveTso, List<TableStreamCleanupInfo> logged)
            throws Exception {
        TSOService tsoService = Env.getCurrentEnv().getTSOService();
        TSOService fixedTso = Mockito.mock(TSOService.class);
        Mockito.when(fixedTso.getTSO()).thenReturn(keepAliveTso);
        EditLog editLog = Env.getCurrentEnv().getEditLog();
        EditLog spyEditLog = Mockito.spy(editLog);
        Mockito.doAnswer(invocation -> {
            logged.add(invocation.getArgument(0));
            return Mockito.mock(EditLog.EditLogItem.class);
        }).when(spyEditLog).logTableStreamCleanup(Mockito.any(TableStreamCleanupInfo.class));
        Deencapsulation.setField(Env.getCurrentEnv(), "tsoService", fixedTso);
        Env.getCurrentEnv().setEditLog(spyEditLog);
        try {
            Optional<EditLog.EditLogItem> item =
                    Env.getCurrentEnv().getTableStreamManager().keepAliveIdlePartitions(context.stream);
            Assertions.assertEquals(!logged.isEmpty(), item.isPresent());
        } finally {
            Env.getCurrentEnv().setEditLog(editLog);
            Deencapsulation.setField(Env.getCurrentEnv(), "tsoService", tsoService);
        }
    }

    // Runs a cloud keep-alive round with the TSO status and the read states given, and records the reads asked
    // for and the entries logged; null read states make the read fail. MetaService takes every TSO fence.
    private void cloudKeepAlive(List<OlapTableStream> streams, TSOService.TSOStatusSnapshot tsoStatus,
            int batchPartitions, Map<Long, Cloud.TableStreamPartitionReadStatePB> states,
            List<Map<Cloud.TableStreamIdentityPB, Set<Long>>> requests, List<TableStreamCleanupInfo> logged)
            throws Exception {
        cloudKeepAlive(streams, tsoStatus, batchPartitions, states, requests, logged, true, new ArrayList<>());
    }

    // The same, and also records the calls to MetaService in the order they are made: "fence <tso>" for a TSO
    // fence and "read" for a read of the offsets. A fence MetaService does not take fails.
    private void cloudKeepAlive(List<OlapTableStream> streams, TSOService.TSOStatusSnapshot tsoStatus,
            int batchPartitions, Map<Long, Cloud.TableStreamPartitionReadStatePB> states,
            List<Map<Cloud.TableStreamIdentityPB, Set<Long>>> requests, List<TableStreamCleanupInfo> logged,
            boolean fenceTaken, List<String> calls) throws Exception {
        TSOService tsoService = Env.getCurrentEnv().getTSOService();
        TSOService fixedTso = Mockito.mock(TSOService.class);
        Mockito.when(fixedTso.getStatusSnapshot()).thenReturn(tsoStatus);
        GlobalTransactionMgrIface globalTransactionMgr = Env.getCurrentEnv().getGlobalTransactionMgr();
        GlobalTransactionMgrIface spyGlobalTransactionMgr = Mockito.spy(globalTransactionMgr);
        Mockito.doAnswer(invocation -> {
            long proposedFenceTso = invocation.getArgument(0);
            calls.add("fence " + proposedFenceTso);
            if (!fenceTaken) {
                throw new UserException("Failed to advance TSO fence");
            }
            return proposedFenceTso;
        }).when(spyGlobalTransactionMgr).advanceTsoFence(Mockito.anyLong());
        EditLog editLog = Env.getCurrentEnv().getEditLog();
        EditLog spyEditLog = Mockito.spy(editLog);
        Mockito.doAnswer(invocation -> {
            logged.add(invocation.getArgument(0));
            return Mockito.mock(EditLog.EditLogItem.class);
        }).when(spyEditLog).logTableStreamCleanup(Mockito.any(TableStreamCleanupInfo.class));
        Deencapsulation.setField(Env.getCurrentEnv(), "tsoService", fixedTso);
        Deencapsulation.setField(Env.getCurrentEnv(), "globalTransactionMgr", spyGlobalTransactionMgr);
        Env.getCurrentEnv().setEditLog(spyEditLog);
        int loggedBefore = logged.size();
        try (MockedStatic<CloudTableStreamReadStateHelper> helper =
                Mockito.mockStatic(CloudTableStreamReadStateHelper.class)) {
            helper.when(() -> CloudTableStreamReadStateHelper.getReadStates(Mockito.any())).thenAnswer(invocation -> {
                calls.add("read");
                Map<Cloud.TableStreamIdentityPB, Set<Long>> request = invocation.getArgument(0);
                requests.add(new LinkedHashMap<>(request));
                if (states == null) {
                    throw new UserException("MetaService is unavailable");
                }
                Map<Cloud.TableStreamIdentityPB, Map<Long, Cloud.TableStreamPartitionReadStatePB>> response =
                        new LinkedHashMap<>();
                request.forEach((identity, partitionIds) -> {
                    Map<Long, Cloud.TableStreamPartitionReadStatePB> partitionStates = new LinkedHashMap<>();
                    for (Long partitionId : partitionIds) {
                        partitionStates.put(partitionId, states.getOrDefault(partitionId,
                                Cloud.TableStreamPartitionReadStatePB.newBuilder().setPartitionId(partitionId)
                                        .setOffsetState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_UNKNOWN)
                                        .setEndTso(tsoSecondsAgo(60)).setVisibleVersion(2).build()));
                    }
                    response.put(identity, partitionStates);
                });
                return response;
            });
            List<EditLog.EditLogItem> items =
                    Env.getCurrentEnv().getTableStreamManager().keepAliveIdleCloudPartitions(streams, batchPartitions);
            Assertions.assertEquals(logged.size() - loggedBefore, items.size());
        } finally {
            Env.getCurrentEnv().setEditLog(editLog);
            Deencapsulation.setField(Env.getCurrentEnv(), "globalTransactionMgr", globalTransactionMgr);
            Deencapsulation.setField(Env.getCurrentEnv(), "tsoService", tsoService);
        }
    }

    private static TSOService.TSOStatusSnapshot tsoStatus(long committedTso) {
        return new TSOService.TSOStatusSnapshot(true, tsoSecondsAgo(0), 0, committedTso);
    }

    private static Cloud.TableStreamPartitionReadStatePB consumed(long partitionId, long offset, long endTso) {
        return Cloud.TableStreamPartitionReadStatePB.newBuilder()
                .setPartitionId(partitionId)
                .setOffsetState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED)
                .setOffsetTso(offset)
                .setEndTso(endTso)
                .setVisibleVersion(2)
                .build();
    }

    private static void assertKeepAlive(StreamContext context, long partitionId, long offset, long keepAliveTso) {
        OlapTableStream.PartitionKeepAlive keepAlive = context.stream.getKeepAlive(partitionId);
        Assertions.assertNotNull(keepAlive);
        Assertions.assertEquals(offset, keepAlive.getOffset());
        Assertions.assertEquals(keepAliveTso, keepAlive.getKeepAliveTso());
    }

    private static void keepAliveDirectly(StreamContext context, long partitionId, long offset, long keepAliveTso) {
        context.stream.writeLock();
        try {
            context.stream.unprotectedKeepAlive(Collections.singletonMap(partitionId, offset), keepAliveTso);
        } finally {
            context.stream.writeUnlock();
        }
    }

    // The keep-alive TSO the stream holds for the partition, which has to be taken against its current offset.
    private static Long keepAliveTso(StreamContext context, long partitionId) {
        OlapTableStream.PartitionKeepAlive keepAlive = context.stream.getKeepAlive(partitionId);
        if (keepAlive == null) {
            return null;
        }
        Assertions.assertEquals(context.stream.getPartitionOffset(partitionId).longValue(), keepAlive.getOffset());
        return keepAlive.getKeepAliveTso();
    }

    // The partition is consumed up to its last change at offset, and nothing is committed after it.
    private static void setIdle(StreamContext context, String partitionName, long offset) {
        Partition partition = context.partition(partitionName);
        setPartitionTso(partition, offset);
        Map<Long, Long> partitionOffset = new HashMap<>(Deencapsulation.getField(context.stream, "partitionOffset"));
        partitionOffset.put(partition.getId(), offset);
        Deencapsulation.setField(context.stream, "partitionOffset", partitionOffset);
    }

    private static void setPartitionTso(Partition partition, long tso) {
        long version = partition.getVisibleVersion() + 1;
        partition.setVisibleVersionAndTime(version, System.currentTimeMillis(), tso);
        partition.setNextVersion(version + 1);
    }

    private static long tsoSecondsAgo(long seconds) {
        return TSOTimestamp.composePhysicalTimestamp(System.currentTimeMillis() - seconds * 1000L);
    }

    private static DataInputStream roundTrip(TableStreamCleanupInfo info) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        info.write(new DataOutputStream(bytes));
        return new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()));
    }

    private StreamContext createStreamContext(String suffix) throws Exception {
        String tableName = "tbl_" + suffix;
        String streamName = "s_" + suffix;
        createTable("create table test_stream_keepalive." + tableName + " (\n"
                + "  k1 int,\n"
                + "  k2 int\n"
                + ")\n"
                + "unique key(k1)\n"
                + "partition by range(k1)\n"
                + "(partition p1 values less than (\"100\"),\n"
                + " partition p2 values less than (\"200\"))\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\"=\"1\",\"changelog.enable\"=\"true\","
                + "\"changelog.need_historical_value\"=\"true\",\"changelog.ttl_seconds\"=\"" + TTL_SECONDS + "\")");
        createTable("create stream test_stream_keepalive." + streamName + " on table test_stream_keepalive."
                + tableName + " properties('show_initial_rows' = 'false')");
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream_keepalive");
        return new StreamContext((OlapTable) db.getTableOrMetaException(tableName),
                (OlapTableStream) db.getTableOrMetaException(streamName));
    }

    private static class StreamContext {
        private final OlapTable baseTable;
        private final OlapTableStream stream;

        private StreamContext(OlapTable baseTable, OlapTableStream stream) {
            this.baseTable = baseTable;
            this.stream = stream;
        }

        private Partition partition(String name) {
            return baseTable.getPartition(name);
        }
    }
}
