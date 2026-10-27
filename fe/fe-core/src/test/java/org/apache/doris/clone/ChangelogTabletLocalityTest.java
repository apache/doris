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
import org.apache.doris.changelog.ChangelogRepairReason;
import org.apache.doris.changelog.ChangelogTabletLocality;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.TabletSchedulerDetailProcDir;
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

import java.util.List;
import java.util.Map;

public class ChangelogTabletLocalityTest {
    private static final long VISIBLE_VERSION = 10L;
    private static final long BASE_TABLET_ID = 100L;
    private static final long CHANGELOG_TABLET_ID = 200L;

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
    public void changelogHealthyRequiresSameBackendAndPath() {
        TabletPair pair = createTabletPair(
                replicas(1, 2, 3),
                replicas(1, 2, 3));

        ChangelogTabletLocality.ChangelogHealthResult result = getHealth(pair);

        Assertions.assertEquals(TabletStatus.HEALTHY, result.getTabletHealth().status);
        Assertions.assertEquals(ChangelogRepairReason.NONE, result.getRepairReason());
        Assertions.assertEquals(pair.baseTablet, result.getBaseTablet());
        Assertions.assertEquals(
                ImmutableMap.of(1L, 10L, 2L, 20L, 3L, 30L),
                result.getRequiredDestPathHashByBackend());
        Assertions.assertEquals(
                ImmutableMap.of(1L, 10L, 2L, 20L, 3L, 30L),
                result.getObservedPathHashByBackend());
        Assertions.assertEquals(Sets.newHashSet(1L, 2L, 3L), result.getRequiredBackends());
    }

    @Test
    public void changelogMismatchUsesEffectiveBaseBackends() {
        TabletPair pair = createTabletPair(
                replicas(1, 2, 3, 4),
                replicas(1, 2, 3));

        ChangelogTabletLocality.ChangelogHealthResult result = getHealth(pair);

        Assertions.assertEquals(TabletStatus.COLOCATE_MISMATCH, result.getTabletHealth().status);
        Assertions.assertEquals(ChangelogRepairReason.BACKEND_MISMATCH, result.getRepairReason());
        Assertions.assertEquals(ImmutableMap.of(1L, 10L, 2L, 20L, 3L, 30L),
                result.getObservedPathHashByBackend());
        Assertions.assertEquals(Priority.HIGH, result.getTabletHealth().priority);
        Assertions.assertEquals(40L, result.getRequiredDestPathHashByBackend().get(4L));
        Assertions.assertEquals(Sets.newHashSet(1L, 2L, 3L, 4L), result.getRequiredBackends());
    }

    @Test
    public void changelogHealthResultAppliesBaseTargetsToSchedulerContext() {
        TabletPair pair = createTabletPair(
                replicas(1, 2, 3, 4),
                replicas(1, 2, 3));
        ChangelogTabletLocality.ChangelogHealthResult result = getHealth(pair);
        TabletSchedCtx tabletCtx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR,
                1L, 2L, 3L, 4L, CHANGELOG_TABLET_ID, new ReplicaAllocation((short) 3),
                System.currentTimeMillis());

        result.applyTo(tabletCtx);

        Assertions.assertEquals(Sets.newHashSet(1L, 2L, 3L, 4L), tabletCtx.getColocateBackendsSet());
        Assertions.assertEquals(BASE_TABLET_ID, tabletCtx.getChangelogBaseTabletId());
        Assertions.assertEquals(ChangelogRepairReason.BACKEND_MISMATCH,
                tabletCtx.getChangelogRepairReason());
        Assertions.assertEquals(ImmutableMap.of(1L, 10L, 2L, 20L, 3L, 30L, 4L, 40L),
                tabletCtx.getChangelogRequiredDestPathHashByBackend());
        Assertions.assertEquals(ImmutableMap.of(1L, 10L, 2L, 20L, 3L, 30L),
                tabletCtx.getChangelogObservedPathHashByBackend());

        List<String> brief = tabletCtx.getBrief();
        Assertions.assertEquals(TabletSchedulerDetailProcDir.TITLE_NAMES.size(), brief.size());
        Assertions.assertEquals("BACKEND_MISMATCH", brief.get(
                TabletSchedulerDetailProcDir.TITLE_NAMES.indexOf("ChangelogRepairReason")));
        Assertions.assertEquals(String.valueOf(BASE_TABLET_ID), brief.get(
                TabletSchedulerDetailProcDir.TITLE_NAMES.indexOf("BaseTabletId")));
        Assertions.assertEquals("1:10,2:20,3:30,4:40", brief.get(
                TabletSchedulerDetailProcDir.TITLE_NAMES.indexOf("ExpectedLocations")));
        Assertions.assertEquals("1:10,2:20,3:30", brief.get(
                TabletSchedulerDetailProcDir.TITLE_NAMES.indexOf("ObservedLocations")));
        Assertions.assertEquals("reason: BACKEND_MISMATCH, base tablet id: 100, "
                + "expected locations: 1:10,2:20,3:30,4:40, observed locations: 1:10,2:20,3:30",
                tabletCtx.getChangelogRepairInfo());
    }

    @Test
    public void changelogWrongPathBecomesMismatch() {
        TabletPair pair = createTabletPair(
                replicas(1, 2, 3),
                replicas(replicaSpec(1, 10), replicaSpec(2, 200), replicaSpec(3, 30)));

        ChangelogTabletLocality.ChangelogHealthResult result = getHealth(pair);

        Assertions.assertEquals(TabletStatus.COLOCATE_MISMATCH, result.getTabletHealth().status);
        Assertions.assertEquals(ChangelogRepairReason.PATH_MISMATCH, result.getRepairReason());
        Assertions.assertEquals(ImmutableMap.of(1L, 10L, 2L, 200L, 3L, 30L),
                result.getObservedPathHashByBackend());
    }

    @Test
    public void backendMismatchTakesPriorityWhenPathAlsoMismatches() {
        TabletPair pair = createTabletPair(
                replicas(1, 2, 3),
                replicas(replicaSpec(1, 100), replicaSpec(2, 20)));

        ChangelogTabletLocality.ChangelogHealthResult result = getHealth(pair);

        Assertions.assertEquals(TabletStatus.COLOCATE_MISMATCH, result.getTabletHealth().status);
        Assertions.assertEquals(ChangelogRepairReason.BACKEND_MISMATCH, result.getRepairReason());
        Assertions.assertEquals(ImmutableMap.of(1L, 100L, 2L, 20L),
                result.getObservedPathHashByBackend());
    }

    @Test
    public void changelogVersionIncompleteTakesPriorityOverWrongPath() {
        TabletPair incompletePair = createTabletPair(
                replicas(1, 2, 3),
                replicas(replicaSpec(1, 10), replicaSpec(2, 20, VISIBLE_VERSION - 1, -1), replicaSpec(3, 30)));

        TabletHealth incompleteHealth = getHealth(incompletePair).getTabletHealth();
        Assertions.assertEquals(TabletStatus.VERSION_INCOMPLETE, incompleteHealth.status);
        Assertions.assertEquals(Priority.VERY_HIGH, incompleteHealth.priority);

        TabletPair wrongPathPair = createTabletPair(
                replicas(1, 2, 3),
                replicas(replicaSpec(1, 10), replicaSpec(2, 200, VISIBLE_VERSION - 1, -1), replicaSpec(3, 30)));

        TabletHealth wrongPathHealth = getHealth(wrongPathPair).getTabletHealth();
        Assertions.assertEquals(TabletStatus.VERSION_INCOMPLETE, wrongPathHealth.status);
        Assertions.assertEquals(Priority.VERY_HIGH, wrongPathHealth.priority);
    }

    @Test
    public void singleReplicaWrongPathUsesRepairableMismatchStatus() {
        TabletPair pair = createTabletPair(
                replicas(1),
                replicas(replicaSpec(1, 100)));

        ChangelogTabletLocality.ChangelogHealthResult result = ChangelogTabletLocality.getChangelogHealth(
                pair.partition, pair.changelogTablet, new ReplicaAllocation((short) 1), VISIBLE_VERSION);

        Assertions.assertEquals(TabletStatus.COLOCATE_MISMATCH, result.getTabletHealth().status);
        Assertions.assertEquals(ChangelogRepairReason.PATH_MISMATCH, result.getRepairReason());
    }

    @Test
    public void changelogMissingBaseReplicaIsExcludedFromTargets() throws UserException {
        TabletPair pair = createTabletPair(
                replicas(1, 2, 3),
                replicas(2, 3));
        Replica baseReplica = pair.baseTablet.getReplicaByBackendId(1L);
        baseReplica.setChangelogMissing(true);

        ChangelogTabletLocality.ChangelogHealthResult result = getHealth(pair);

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
        pair.baseTablet.getReplicaByBackendId(5L).setChangelogMissing(true);

        Assertions.assertEquals(1,
                ChangelogTabletLocality.getCompletePairCount(pair.baseTablet, pair.changelogTablet,
                        VISIBLE_VERSION, true));
        Assertions.assertTrue(ChangelogTabletLocality.isCompletePair(
                pair.baseTablet.getReplicaByBackendId(1L), pair.changelogTablet.getReplicaByBackendId(1L),
                VISIBLE_VERSION, true));
        Assertions.assertFalse(ChangelogTabletLocality.isCompletePair(
                pair.baseTablet.getReplicaByBackendId(2L), pair.changelogTablet.getReplicaByBackendId(2L),
                VISIBLE_VERSION, true));
    }

    @Test
    public void baseRedundantWaitsUntilChangelogPairIsComplete() {
        TabletPair beforeChangelogCatchup = createTabletPair(
                replicas(1, 2, 3, 4),
                replicas(1, 2, 3));

        Assertions.assertEquals(3,
                ChangelogTabletLocality.getCompletePairCount(
                        beforeChangelogCatchup.baseTablet, beforeChangelogCatchup.changelogTablet,
                        VISIBLE_VERSION, true));
        Assertions.assertEquals(TabletStatus.COLOCATE_MISMATCH,
                getHealth(beforeChangelogCatchup).getTabletHealth().status);

        TabletPair afterChangelogCatchup = createTabletPair(
                replicas(1, 2, 3, 4),
                replicas(1, 2, 3, 4));

        Assertions.assertEquals(4,
                ChangelogTabletLocality.getCompletePairCount(
                        afterChangelogCatchup.baseTablet, afterChangelogCatchup.changelogTablet,
                        VISIBLE_VERSION, true));
        ChangelogTabletLocality.ChangelogHealthResult redundantResult = getHealth(afterChangelogCatchup);
        Assertions.assertEquals(TabletStatus.COLOCATE_REDUNDANT, redundantResult.getTabletHealth().status);
        Assertions.assertEquals(ChangelogRepairReason.REDUNDANT, redundantResult.getRepairReason());
        Assertions.assertEquals(ImmutableMap.of(1L, 10L, 2L, 20L, 3L, 30L, 4L, 40L),
                redundantResult.getObservedPathHashByBackend());
    }

    @Test
    public void preferredBaseRepairPathUsesExistingChangelogReplica() {
        TabletPair pair = createTabletPair(
                replicas(1, 2),
                replicas(1, 2, 3, 4));
        pair.changelogTablet.getReplicaByBackendId(4L).setBad(true);

        Map<Long, Long> preferredPathByBackend = ChangelogTabletLocality.getPreferredBaseRepairPathByBackend(
                pair.partition, pair.baseTablet, VISIBLE_VERSION);

        Assertions.assertEquals(ImmutableMap.of(1L, 10L, 2L, 20L, 3L, 30L), preferredPathByBackend);
    }

    @Test
    public void baseTabletWithoutChangelogIndexHasNoCompanion() {
        MaterializedIndex baseIndex = new MaterializedIndex(10L, IndexState.NORMAL);
        Tablet baseTablet = new LocalTablet(BASE_TABLET_ID);
        baseIndex.addTablet(baseTablet, null, true);
        Partition partition = new Partition(1L, "p1", baseIndex, null);

        Assertions.assertNull(ChangelogTabletLocality.getChangelogTablet(partition, baseTablet));
        Assertions.assertTrue(ChangelogTabletLocality.getPreferredBaseRepairPathByBackend(
                partition, baseTablet, VISIBLE_VERSION).isEmpty());
    }

    @Test
    public void inconsistentDirectionalLinksAreRejected() {
        TabletPair pair = createTabletPair(
                replicas(1, 2, 3),
                replicas(1, 2, 3));
        pair.baseTablet.setChangelogTabletId(9999L);

        ChangelogTabletLocality.ChangelogHealthResult healthResult = getHealth(pair);
        Assertions.assertEquals(TabletStatus.UNRECOVERABLE, healthResult.getTabletHealth().status);
        TabletSchedCtx tabletCtx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR,
                1L, 2L, 3L, 4L, pair.changelogTablet.getId(), new ReplicaAllocation((short) 3),
                System.currentTimeMillis());
        tabletCtx.setChangelogRequiredDestPathHashByBackend(ImmutableMap.of(1L, 10L));
        healthResult.applyTo(tabletCtx);
        Assertions.assertTrue(tabletCtx.getChangelogRequiredDestPathHashByBackend().isEmpty());
        Assertions.assertTrue(tabletCtx.getColocateBackendsSet().isEmpty());
        Assertions.assertEquals(-1L, tabletCtx.getChangelogBaseTabletId());
        Assertions.assertEquals(ChangelogRepairReason.NONE, tabletCtx.getChangelogRepairReason());
        Assertions.assertEquals(ImmutableMap.of(1L, 10L, 2L, 20L, 3L, 30L),
                tabletCtx.getChangelogObservedPathHashByBackend());

        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> ChangelogTabletLocality.getChangelogTablet(pair.partition, pair.baseTablet));
        Assertions.assertTrue(exception.getMessage().contains("invalid changelog tablet pair"));
    }

    @Test
    public void crossedExistingDirectionalLinksAreRejected() {
        TabletPair pair = createTabletPair(
                replicas(1, 2, 3),
                replicas(1, 2, 3));
        Tablet otherBaseTablet = new LocalTablet(BASE_TABLET_ID + 1);
        Tablet otherChangelogTablet = new LocalTablet(CHANGELOG_TABLET_ID + 1);
        otherBaseTablet.setChangelogTabletId(otherChangelogTablet.getId());
        otherChangelogTablet.setChangelogBaseTabletId(otherBaseTablet.getId());
        pair.partition.getBaseIndex().addTablet(otherBaseTablet, null, true);
        pair.partition.getIndex(20L).addTablet(otherChangelogTablet, null, true);
        pair.baseTablet.setChangelogTabletId(otherChangelogTablet.getId());

        Assertions.assertEquals(TabletStatus.UNRECOVERABLE, getHealth(pair).getTabletHealth().status);
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> ChangelogTabletLocality.getChangelogTablet(pair.partition, pair.baseTablet));
        Assertions.assertTrue(exception.getMessage().contains("points to base tablet"));
    }

    @Test
    public void missingDirectionalLinkIsRejectedWithoutUnboxingNull() {
        MaterializedIndex baseIndex = new MaterializedIndex(10L, IndexState.NORMAL);
        MaterializedIndex changelogIndex = new MaterializedIndex(20L, IndexState.NORMAL);
        changelogIndex.setIsChangelog(true);
        Tablet baseTablet = new LocalTablet(BASE_TABLET_ID);
        Tablet changelogTablet = new LocalTablet(CHANGELOG_TABLET_ID);
        changelogTablet.setChangelogBaseTabletId(BASE_TABLET_ID);
        baseIndex.addTablet(baseTablet, null, true);
        changelogIndex.addTablet(changelogTablet, null, true);
        Partition partition = new Partition(1L, "p1", baseIndex, null);
        partition.createRollupIndex(changelogIndex);

        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> ChangelogTabletLocality.getChangelogTablet(partition, baseTablet));
        Assertions.assertTrue(exception.getMessage().contains("has no changelog tablet link"));
    }

    @Test
    public void missingBaseOrNoEffectiveBaseIsUnrecoverable() {
        TabletPair missingBasePair = createTabletPair(
                replicas(1, 2, 3),
                replicas(1, 2, 3));
        missingBasePair.changelogTablet.setChangelogBaseTabletId(9999L);

        Assertions.assertEquals(TabletStatus.UNRECOVERABLE, getHealth(missingBasePair).getTabletHealth().status);

        TabletPair noEffectiveBasePair = createTabletPair(
                replicas(1, 2, 3),
                replicas(1, 2, 3));
        noEffectiveBasePair.baseTablet.getReplicas().forEach(replica -> replica.setChangelogMissing(true));

        Assertions.assertEquals(TabletStatus.UNRECOVERABLE, getHealth(noEffectiveBasePair).getTabletHealth().status);
    }

    private ChangelogTabletLocality.ChangelogHealthResult getHealth(TabletPair pair) {
        return ChangelogTabletLocality.getChangelogHealth(
                pair.partition, pair.changelogTablet, new ReplicaAllocation((short) 3), VISIBLE_VERSION);
    }

    private TabletPair createTabletPair(ReplicaSpec[] baseReplicaSpecs, ReplicaSpec[] changelogReplicaSpecs) {
        MaterializedIndex baseIndex = new MaterializedIndex(10L, IndexState.NORMAL);
        MaterializedIndex changelogIndex = new MaterializedIndex(20L, IndexState.NORMAL);
        changelogIndex.setIsChangelog(true);
        Tablet baseTablet = new LocalTablet(BASE_TABLET_ID);
        Tablet changelogTablet = new LocalTablet(CHANGELOG_TABLET_ID);
        baseTablet.setChangelogTabletId(CHANGELOG_TABLET_ID);
        changelogTablet.setChangelogBaseTabletId(BASE_TABLET_ID);

        for (ReplicaSpec spec : baseReplicaSpecs) {
            baseTablet.addReplica(createReplica(spec), true);
        }
        for (ReplicaSpec spec : changelogReplicaSpecs) {
            changelogTablet.addReplica(createReplica(spec), true);
        }

        baseIndex.addTablet(baseTablet, null, true);
        changelogIndex.addTablet(changelogTablet, null, true);

        Partition partition = new Partition(1L, "p1", baseIndex, null);
        partition.updateVisibleVersion(VISIBLE_VERSION);
        partition.createRollupIndex(changelogIndex);
        return new TabletPair(partition, baseTablet, changelogTablet);
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
        private final Tablet changelogTablet;

        private TabletPair(Partition partition, Tablet baseTablet, Tablet changelogTablet) {
            this.partition = partition;
            this.baseTablet = baseTablet;
            this.changelogTablet = changelogTablet;
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
