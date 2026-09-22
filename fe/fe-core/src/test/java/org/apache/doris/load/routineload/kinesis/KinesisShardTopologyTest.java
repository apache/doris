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

package org.apache.doris.load.routineload.kinesis;

import org.apache.doris.persist.KinesisShardTopologyOperation;
import org.apache.doris.proto.InternalService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KinesisShardTopologyTest {
    private static InternalService.PShardInfo shard(String id, String parent, boolean closed) {
        InternalService.PShardInfo.Builder builder = InternalService.PShardInfo.newBuilder()
                .setShardId(id).setClosed(closed);
        if (parent != null) {
            builder.setParentShardId(parent);
        }
        return builder.build();
    }

    @Test
    public void testSplitStateTransitionsAndSourceViews() {
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(shard("P", null, false)), "900", "TRIM_HORIZON");
        Assertions.assertEquals(KinesisShardTopology.ShardState.ACTIVE,
                topology.getNodes().get("P").getState());

        topology.mergeChildShardInfos(Map.of("C1", Set.of("P"), "C2", Set.of("P")),
                KinesisProgress.TRIM_HORIZON_VAL);
        topology.mergeShardInfos(List.of(shard("P", null, false), shard("C1", "P", false),
                shard("C2", "P", false)), "LATEST", KinesisProgress.TRIM_HORIZON_VAL);
        Assertions.assertEquals(KinesisShardTopology.ShardState.PENDING_PARENT,
                topology.getNodes().get("C1").getState());
        Assertions.assertEquals(List.of("C1", "C2", "P"), topology.getOpenShardIds());
        Assertions.assertEquals(List.of("P"), topology.getReadyShardIds());

        topology.markDraining("P");
        Assertions.assertEquals(KinesisShardTopology.ShardState.DRAINING,
                topology.getNodes().get("P").getState());
        Assertions.assertEquals(List.of("P"), topology.getClosedShardIds());

        topology.markCompleted("P");
        Assertions.assertEquals(KinesisShardTopology.ShardState.ACTIVE,
                topology.getNodes().get("C1").getState());
        Assertions.assertEquals(KinesisShardTopology.ShardState.ACTIVE,
                topology.getNodes().get("C2").getState());
        Assertions.assertEquals(List.of("C1", "C2"), topology.getReadyShardIds());
    }

    @Test
    public void testLatestResolutionIsConcreteAndIdempotent() {
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(shard("P", null, false)), "LATEST", "TRIM_HORIZON");
        Assertions.assertEquals(List.of("P"), topology.getUnresolvedLatestShardIds());

        topology.resolveInitialPositions(Map.of("P", "900"));
        topology.resolveInitialPositions(Map.of("P", "901"));
        Assertions.assertEquals("900", topology.getStartPosition("P"));
        Assertions.assertTrue(topology.getUnresolvedLatestShardIds().isEmpty());
    }

    @Test
    public void testCommittedParentDoesNotReleaseChildUntilVisible() {
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(shard("P", null, false), shard("C", "P", false)),
                "900", "TRIM_HORIZON");

        topology.markEndCommitted("P", 11L);
        Assertions.assertEquals(KinesisShardTopology.ShardState.PENDING_PARENT,
                topology.getNodes().get("C").getState());
        topology.completeVisibleShards(11L);
        Assertions.assertEquals(KinesisShardTopology.ShardState.ACTIVE,
                topology.getNodes().get("C").getState());
    }

    @Test
    public void testInitialSnapshotOutsideAncestorDoesNotBlockChild() {
        KinesisShardTopology topology = new KinesisShardTopology();
        // P is only referenced as C's parent and is already outside retention when the job starts.
        topology.mergeShardInfos(List.of(shard("C", "P", false)), "TRIM_HORIZON", "TRIM_HORIZON");

        Assertions.assertTrue(topology.getNodes().get("P").isOutsideInitialSnapshot());
        Assertions.assertEquals(KinesisShardTopology.ShardState.DISCOVERED,
                topology.getNodes().get("P").getState());
        Assertions.assertEquals(KinesisShardTopology.ShardState.ACTIVE,
                topology.getNodes().get("C").getState());
        Assertions.assertEquals(List.of("C"), topology.getReadyShardIds());
        Assertions.assertTrue(topology.getOpenShardIds().stream().noneMatch("P"::equals));
        Assertions.assertTrue(topology.getClosedShardIds().stream().noneMatch("P"::equals));
    }

    /**
     * A shard discovered after the initial snapshot starts at TRIM_HORIZON, never at the job's
     * configured default. This is the regression where the consumed parent is already gone from
     * progress when its children are discovered: the decision must not depend on an empty progress
     * map, otherwise the children would be initialized as first-setup shards and skip their
     * backlog.
     */
    @Test
    public void testShardDiscoveredAfterInitialSnapshotUsesTrimHorizon() {
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(shard("P", null, false)), KinesisProgress.POSITION_LATEST,
                KinesisProgress.POSITION_TRIM_HORIZON);
        Assertions.assertEquals(KinesisProgress.POSITION_LATEST, topology.getStartPosition("P"));
        Assertions.assertTrue(topology.isInitialSnapshotFinalized());

        // P is fully consumed before the children are discovered.
        topology.markCompleted("P");
        topology.mergeShardInfos(
                List.of(shard("P", null, true), shard("C1", "P", false), shard("C2", "P", false)),
                KinesisProgress.POSITION_LATEST, KinesisProgress.POSITION_TRIM_HORIZON);

        Assertions.assertEquals(KinesisProgress.POSITION_TRIM_HORIZON,
                topology.getStartPosition("C1"));
        Assertions.assertEquals(KinesisProgress.POSITION_TRIM_HORIZON,
                topology.getStartPosition("C2"));
        Assertions.assertEquals(List.of("C1", "C2"), topology.getReadyShardIds());
    }

    /**
     * The child lineage carried by a transaction attachment must resolve to the same start position
     * as a shard discovered by scanning, and must still wait for the parent's visibility barrier.
     */
    @Test
    public void testChildShardLineageUsesTrimHorizonAfterParentCompletion() {
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(shard("P", null, false)), KinesisProgress.POSITION_LATEST,
                KinesisProgress.POSITION_TRIM_HORIZON);
        topology.mergeChildShardInfos(Map.of("C1", Set.of("P"), "C2", Set.of("P")), "LATEST");

        Assertions.assertEquals(KinesisProgress.POSITION_TRIM_HORIZON,
                topology.getStartPosition("C1"));
        Assertions.assertTrue(topology.getReadyShardIds().isEmpty());

        topology.markEndCommitted("P", 11L);
        topology.completeVisibleShards(11L);
        Assertions.assertTrue(topology.getReadyShardIds().isEmpty());

        // A ChildShards hint is not enough to schedule the child. ListShards must confirm metadata.
        topology.mergeShardInfos(List.of(shard("P", null, true), shard("C1", "P", false),
                shard("C2", "P", false)), KinesisProgress.POSITION_LATEST,
                KinesisProgress.POSITION_TRIM_HORIZON);
        Assertions.assertEquals(List.of("C1", "C2"), topology.getReadyShardIds());
    }

    @Test
    public void testChildHintDoesNotCauseFalseLineageErrorDuringStaleScan() {
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(shard("P", null, false)), "TRIM_HORIZON", "TRIM_HORIZON");
        topology.mergeChildShardInfos(Map.of("C", Set.of("P")), KinesisProgress.TRIM_HORIZON_VAL);

        // This scan started before C was returned by GetRecords and therefore does not contain C.
        topology.mergeShardInfos(List.of(shard("P", null, true)), "TRIM_HORIZON", "TRIM_HORIZON");

        Assertions.assertNull(topology.getLineageError());
        Assertions.assertEquals(KinesisShardTopology.ShardState.DISCOVERED,
                topology.getNodes().get("C").getState());
    }

    @Test
    public void testInitiallyClosedShardIsDrainingAndReady() {
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(shard("P", null, true)), "TRIM_HORIZON", "TRIM_HORIZON");

        Assertions.assertEquals(KinesisShardTopology.ShardState.DRAINING,
                topology.getNodes().get("P").getState());
        Assertions.assertEquals(List.of("P"), topology.getReadyShardIds());
        Assertions.assertEquals(List.of("P"), topology.getClosedShardIds());

        topology.markEndCommitted("P", 12L);
        Assertions.assertTrue(topology.getReadyShardIds().isEmpty());
        topology.completeVisibleShards(12L);
        Assertions.assertEquals(KinesisShardTopology.ShardState.COMPLETED,
                topology.getNodes().get("P").getState());
    }

    @Test
    public void testMissingRuntimeParentStopsLineageInsteadOfWaitingForever() {
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(shard("P", null, false)), "900", "TRIM_HORIZON");
        topology.mergeChildShardInfos(Map.of("C", Set.of("P")), KinesisProgress.TRIM_HORIZON_VAL);
        topology.mergeShardInfos(List.of(shard("C", "P", false)), "LATEST", "TRIM_HORIZON");
        Assertions.assertTrue(topology.getLineageError().contains("P"));
    }

    @Test
    public void testPositionReplayRequiresTopologyJournal() {
        KinesisShardTopology topology = new KinesisShardTopology();
        Assertions.assertThrows(NullPointerException.class,
                () -> topology.resolveInitialPositions(Map.of("P", "900")));
    }

    @Test
    public void testRepeatedDiscoveryAndReplayAreIdempotent() {
        KinesisShardTopology topology = new KinesisShardTopology();
        List<InternalService.PShardInfo> infos = List.of(
                shard("P", null, true), shard("C", "P", false));
        topology.mergeShardInfos(infos, KinesisProgress.POSITION_LATEST,
                KinesisProgress.POSITION_TRIM_HORIZON);
        topology.resolveInitialPositions(Map.of("P", "900", "C", "TRIM_HORIZON"));
        String snapshot = topology.toJson();

        topology.mergeShardInfos(infos, KinesisProgress.POSITION_LATEST,
                KinesisProgress.POSITION_TRIM_HORIZON);
        topology.resolveInitialPositions(Map.of("P", "901", "C", "TRIM_HORIZON"));
        Assertions.assertEquals(snapshot, topology.toJson());
    }

    @Test
    public void testMultiGenerationMergeWaitsForEveryParent() {
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(shard("P", null, false), shard("C1", "P", false),
                shard("C2", "P", false)), KinesisProgress.POSITION_TRIM_HORIZON,
                KinesisProgress.POSITION_TRIM_HORIZON);
        topology.markCompleted("P");

        topology.mergeShardInfos(List.of(shard("P", null, true), shard("C1", "P", true),
                shard("C2", "P", true), shardWithParents("G", "C1", "C2", false)),
                KinesisProgress.POSITION_TRIM_HORIZON, KinesisProgress.POSITION_TRIM_HORIZON);
        Assertions.assertEquals(KinesisShardTopology.ShardState.PENDING_PARENT,
                topology.getNodes().get("G").getState());

        topology.markEndCommitted("C1", 21L);
        topology.completeVisibleShards(21L);
        Assertions.assertEquals(KinesisShardTopology.ShardState.PENDING_PARENT,
                topology.getNodes().get("G").getState());

        topology.markEndCommitted("C2", 22L);
        topology.completeVisibleShards(22L);
        Assertions.assertEquals(KinesisShardTopology.ShardState.ACTIVE,
                topology.getNodes().get("G").getState());
    }

    private static InternalService.PShardInfo shardWithParents(String id, String parent,
            String adjacentParent, boolean closed) {
        return InternalService.PShardInfo.newBuilder().setShardId(id).setParentShardId(parent)
                .setAdjacentParentShardId(adjacentParent).setClosed(closed).build();
    }

    @Test
    public void testTopologyJournalRoundTrip() throws Exception {
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(shard("P", null, true)), "900", "TRIM_HORIZON");
        KinesisShardTopologyOperation operation = new KinesisShardTopologyOperation(7L,
                List.of(shard("P", null, true)), "900", Map.of("P", "900"));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        operation.write(new DataOutputStream(bytes));
        KinesisShardTopologyOperation restored = KinesisShardTopologyOperation.read(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        Assertions.assertEquals(7L, restored.getJobId());
        Assertions.assertEquals("900", restored.getInitialPositions().get("P"));
        Assertions.assertTrue(restored.getShardInfos().get(0).getClosed());
    }

    @Test
    public void testPendingChildWithClosedSourceWaitsForAllParents() {
        KinesisShardTopology topology = new KinesisShardTopology();
        topology.mergeShardInfos(List.of(shard("P1", null, false), shard("P2", null, false)),
                "TRIM_HORIZON", "TRIM_HORIZON");
        topology.mergeChildShardInfos(Map.of("C", Set.of("P1", "P2")),
                KinesisProgress.TRIM_HORIZON_VAL);
        topology.markDraining("C");
        topology.markCompleted("P1");
        Assertions.assertEquals(KinesisShardTopology.ShardState.PENDING_PARENT,
                topology.getNodes().get("C").getState());
        topology.markCompleted("P2");
        Assertions.assertEquals(KinesisShardTopology.ShardState.DRAINING,
                topology.getNodes().get("C").getState());
    }
}
