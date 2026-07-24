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

package org.apache.doris.clone;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletHealth;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.common.UserException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;

public class RowBinlogTabletLocalityTest {
    private static final long VISIBLE_VERSION = 10L;
    private static final long BASE_TABLET_ID = 100L;
    private static final long ROW_BINLOG_TABLET_ID = 200L;

    private SystemInfoService infoService;
    private MockedStatic<Env> mockedEnvStatic;

    @BeforeEach
    public void setUp() {
        infoService = new SystemInfoService();
        for (long beId = 1; beId <= 5; beId++) {
            Backend backend = new Backend(beId, "127.0.0." + beId, 9050);
            backend.setAlive(true);
            infoService.addBackend(backend);
        }

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(infoService);
    }

    @AfterEach
    public void tearDown() {
        mockedEnvStatic.close();
    }

    @Test
    public void rowBinlogHealthyRequiresSameBackendAndPath() {
        TabletPair pair = createTabletPair(
                replicas(1, 2, 3),
                replicas(1, 2, 3));

        RowBinlogTabletLocality.RowBinlogHealthResult result = getHealth(pair);

        Assertions.assertEquals(TabletStatus.HEALTHY, result.getTabletHealth().status);
        Assertions.assertEquals(pair.baseTablet, result.getBaseTablet());
        Assertions.assertEquals(
                ImmutableMap.of(1L, 10L, 2L, 20L, 3L, 30L),
                result.getRequiredDestPathHashByBackend());
        Assertions.assertEquals(Sets.newHashSet(1L, 2L, 3L), result.getRequiredBackends());
    }

    @Test
    public void rowBinlogMismatchUsesEffectiveBaseBackends() {
        TabletPair pair = createTabletPair(
                replicas(1, 2, 3, 4),
                replicas(1, 2, 3));

        RowBinlogTabletLocality.RowBinlogHealthResult result = getHealth(pair);

        Assertions.assertEquals(TabletStatus.COLOCATE_MISMATCH, result.getTabletHealth().status);
        Assertions.assertEquals(40L, result.getRequiredDestPathHashByBackend().get(4L));
        Assertions.assertEquals(Sets.newHashSet(1L, 2L, 3L, 4L), result.getRequiredBackends());
    }

    @Test
    public void rowBinlogHealthResultAppliesBaseTargetsToSchedulerContext() {
        TabletPair pair = createTabletPair(
                replicas(1, 2, 3, 4),
                replicas(1, 2, 3));
        RowBinlogTabletLocality.RowBinlogHealthResult result = getHealth(pair);
        TabletSchedCtx tabletCtx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR,
                1L, 2L, 3L, 4L, ROW_BINLOG_TABLET_ID, new ReplicaAllocation((short) 3),
                System.currentTimeMillis());

        result.applyTo(tabletCtx);

        Assertions.assertEquals(Sets.newHashSet(1L, 2L, 3L, 4L), tabletCtx.getColocateBackendsSet());
        Assertions.assertEquals(ImmutableMap.of(1L, 10L, 2L, 20L, 3L, 30L, 4L, 40L),
                tabletCtx.getRowBinlogRequiredDestPathHashByBackend());
    }

    @Test
    public void rowBinlogWrongPathBecomesRedundant() {
        TabletPair pair = createTabletPair(
                replicas(1, 2, 3),
                replicas(replicaSpec(1, 10), replicaSpec(2, 200), replicaSpec(3, 30)));

        TabletHealth health = getHealth(pair).getTabletHealth();

        Assertions.assertEquals(TabletStatus.COLOCATE_REDUNDANT, health.status);
    }

    @Test
    public void rowBinlogVersionIncompleteOnlyWhenPathMatches() {
        TabletPair incompletePair = createTabletPair(
                replicas(1, 2, 3),
                replicas(replicaSpec(1, 10), replicaSpec(2, 20, VISIBLE_VERSION - 1, -1), replicaSpec(3, 30)));

        Assertions.assertEquals(TabletStatus.VERSION_INCOMPLETE, getHealth(incompletePair).getTabletHealth().status);

        TabletPair wrongPathPair = createTabletPair(
                replicas(1, 2, 3),
                replicas(replicaSpec(1, 10), replicaSpec(2, 200, VISIBLE_VERSION - 1, -1), replicaSpec(3, 30)));

        Assertions.assertEquals(TabletStatus.COLOCATE_REDUNDANT, getHealth(wrongPathPair).getTabletHealth().status);
    }

    @Test
    public void binlogMissingBaseReplicaIsExcludedFromTargets() throws UserException {
        TabletPair pair = createTabletPair(
                replicas(1, 2, 3),
                replicas(2, 3));
        Replica baseReplica = pair.baseTablet.getReplicaByBackendId(1L);
        baseReplica.setBinlogMissing(true);

        RowBinlogTabletLocality.RowBinlogHealthResult result = getHealth(pair);

        Assertions.assertEquals(TabletStatus.HEALTHY, result.getTabletHealth().status);
        Assertions.assertFalse(result.getRequiredBackends().contains(1L));

        Multimap<Long, Long> backendPathMap = pair.baseTablet.getNormalReplicaBackendPathMap();
        Assertions.assertFalse(backendPathMap.containsKey(1L));
        Assertions.assertEquals(Sets.newHashSet(2L, 3L), backendPathMap.keySet());
    }

    @Test
    public void completePairCountRequiresVersionAndSamePath() {
        TabletPair pair = createTabletPair(
                replicas(
                        replicaSpec(1, 10),
                        replicaSpec(2, 20),
                        replicaSpec(3, 30),
                        replicaSpec(4, 40),
                        replicaSpec(5, 50)),
                replicas(
                        replicaSpec(1, 10),
                        replicaSpec(2, 200),
                        replicaSpec(3, 30, VISIBLE_VERSION - 1, -1),
                        replicaSpec(4, 40),
                        replicaSpec(5, 50)));
        pair.baseTablet.getReplicaByBackendId(4L).setBad(true);
        pair.baseTablet.getReplicaByBackendId(5L).setBinlogMissing(true);

        Assertions.assertEquals(1,
                RowBinlogTabletLocality.getCompletePairCount(pair.baseTablet, pair.rowBinlogTablet,
                        VISIBLE_VERSION, true));
        Assertions.assertTrue(RowBinlogTabletLocality.isCompletePair(
                pair.baseTablet.getReplicaByBackendId(1L), pair.rowBinlogTablet.getReplicaByBackendId(1L),
                VISIBLE_VERSION, true));
        Assertions.assertFalse(RowBinlogTabletLocality.isCompletePair(
                pair.baseTablet.getReplicaByBackendId(2L), pair.rowBinlogTablet.getReplicaByBackendId(2L),
                VISIBLE_VERSION, true));
    }

    @Test
    public void baseRedundantWaitsUntilRowBinlogPairIsComplete() {
        TabletPair beforeRowBinlogCatchup = createTabletPair(
                replicas(1, 2, 3, 4),
                replicas(1, 2, 3));

        Assertions.assertEquals(3,
                RowBinlogTabletLocality.getCompletePairCount(
                        beforeRowBinlogCatchup.baseTablet, beforeRowBinlogCatchup.rowBinlogTablet,
                        VISIBLE_VERSION, true));
        Assertions.assertEquals(TabletStatus.COLOCATE_MISMATCH,
                getHealth(beforeRowBinlogCatchup).getTabletHealth().status);

        TabletPair afterRowBinlogCatchup = createTabletPair(
                replicas(1, 2, 3, 4),
                replicas(1, 2, 3, 4));

        Assertions.assertEquals(4,
                RowBinlogTabletLocality.getCompletePairCount(
                        afterRowBinlogCatchup.baseTablet, afterRowBinlogCatchup.rowBinlogTablet,
                        VISIBLE_VERSION, true));
        Assertions.assertEquals(TabletStatus.COLOCATE_REDUNDANT,
                getHealth(afterRowBinlogCatchup).getTabletHealth().status);
    }

    @Test
    public void preferredBaseRepairPathUsesExistingBinlogReplica() {
        TabletPair pair = createTabletPair(
                replicas(1, 2),
                replicas(1, 2, 3, 4));
        pair.rowBinlogTablet.getReplicaByBackendId(4L).setBad(true);

        Map<Long, Long> preferredPathByBackend = RowBinlogTabletLocality.getPreferredBaseRepairPathByBackend(
                pair.partition, pair.baseTablet, VISIBLE_VERSION);

        Assertions.assertEquals(ImmutableMap.of(1L, 10L, 2L, 20L, 3L, 30L), preferredPathByBackend);
    }

    @Test
    public void missingBaseOrNoEffectiveBaseIsUnrecoverable() {
        TabletPair missingBasePair = createTabletPair(
                replicas(1, 2, 3),
                replicas(1, 2, 3));
        missingBasePair.rowBinlogTablet.setAlignedTabletId(9999L);

        Assertions.assertEquals(TabletStatus.UNRECOVERABLE, getHealth(missingBasePair).getTabletHealth().status);

        TabletPair noEffectiveBasePair = createTabletPair(
                replicas(1, 2, 3),
                replicas(1, 2, 3));
        noEffectiveBasePair.baseTablet.getReplicas().forEach(replica -> replica.setBinlogMissing(true));

        Assertions.assertEquals(TabletStatus.UNRECOVERABLE, getHealth(noEffectiveBasePair).getTabletHealth().status);
    }

    private RowBinlogTabletLocality.RowBinlogHealthResult getHealth(TabletPair pair) {
        return RowBinlogTabletLocality.getRowBinlogHealth(
                pair.partition, pair.rowBinlogTablet, new ReplicaAllocation((short) 3), VISIBLE_VERSION);
    }

    private TabletPair createTabletPair(ReplicaSpec[] baseReplicaSpecs, ReplicaSpec[] rowBinlogReplicaSpecs) {
        MaterializedIndex baseIndex = new MaterializedIndex(10L, IndexState.NORMAL);
        MaterializedIndex rowBinlogIndex = new MaterializedIndex(20L, IndexState.ROW_BINLOG);
        Tablet baseTablet = new LocalTablet(BASE_TABLET_ID);
        Tablet rowBinlogTablet = new LocalTablet(ROW_BINLOG_TABLET_ID);
        baseTablet.setAlignedTabletId(ROW_BINLOG_TABLET_ID);
        rowBinlogTablet.setAlignedTabletId(BASE_TABLET_ID);

        for (ReplicaSpec spec : baseReplicaSpecs) {
            baseTablet.addReplica(createReplica(spec), true);
        }
        for (ReplicaSpec spec : rowBinlogReplicaSpecs) {
            rowBinlogTablet.addReplica(createReplica(spec), true);
        }

        baseIndex.addTablet(baseTablet, null, true);
        rowBinlogIndex.addTablet(rowBinlogTablet, null, true);

        Partition partition = new Partition(1L, "p1", baseIndex, null);
        partition.updateVisibleVersion(VISIBLE_VERSION);
        partition.createRollupIndex(rowBinlogIndex);
        return new TabletPair(partition, baseTablet, rowBinlogTablet);
    }

    private Replica createReplica(ReplicaSpec spec) {
        Replica replica = new LocalReplica(spec.backendId * 1000 + spec.pathHash, spec.backendId, spec.version, 0,
                100L, 0L, 100L, ReplicaState.NORMAL, spec.lastFailedVersion, spec.version);
        replica.setPathHash(spec.pathHash);
        return replica;
    }

    private ReplicaSpec[] replicas(long... backendIds) {
        ReplicaSpec[] specs = new ReplicaSpec[backendIds.length];
        for (int i = 0; i < backendIds.length; i++) {
            specs[i] = replicaSpec(backendIds[i], backendIds[i] * 10);
        }
        return specs;
    }

    private ReplicaSpec[] replicas(ReplicaSpec... specs) {
        return specs;
    }

    private ReplicaSpec replicaSpec(long backendId, long pathHash) {
        return replicaSpec(backendId, pathHash, VISIBLE_VERSION, -1);
    }

    private ReplicaSpec replicaSpec(long backendId, long pathHash, long version, long lastFailedVersion) {
        return new ReplicaSpec(backendId, pathHash, version, lastFailedVersion);
    }

    private static class TabletPair {
        private final Partition partition;
        private final Tablet baseTablet;
        private final Tablet rowBinlogTablet;

        private TabletPair(Partition partition, Tablet baseTablet, Tablet rowBinlogTablet) {
            this.partition = partition;
            this.baseTablet = baseTablet;
            this.rowBinlogTablet = rowBinlogTablet;
        }
    }

    private static class ReplicaSpec {
        private final long backendId;
        private final long pathHash;
        private final long version;
        private final long lastFailedVersion;

        private ReplicaSpec(long backendId, long pathHash, long version, long lastFailedVersion) {
            this.backendId = backendId;
            this.pathHash = pathHash;
            this.version = version;
            this.lastFailedVersion = lastFailedVersion;
        }
    }
}
