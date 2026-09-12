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

import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Sets;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TabletTest {

    private Tablet tablet;
    private Replica replica1;
    private Replica replica2;
    private Replica replica3;

    private TabletInvertedIndex invertedIndex;
    private SystemInfoService  infoService;

    private Env env = Mockito.mock(Env.class);

    private MockedStatic<Env> mockedEnvStatic;

    @BeforeEach
    public void makeTablet() {
        invertedIndex = new LocalTabletInvertedIndex();
        infoService = new SystemInfoService();
        for (long beId = 1L; beId <= 4L; beId++) {
            Backend be = new Backend(beId, "127.0.0." + beId, 8030);
            be.setAlive(true);
            infoService.addBackend(be);
        }

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(infoService);
        mockedEnvStatic.when(Env::isCheckpointThread).thenReturn(false);

        tablet = new LocalTablet(1);
        TabletMeta tabletMeta = new TabletMeta(10, 20, 30, 40, 1, TStorageMedium.HDD,
                false /* isRowBinlog */);
        invertedIndex.addTablet(1, tabletMeta);
        replica1 = new LocalReplica(1L, 1L, 100L, 0, 200000L, 0, 3000L, ReplicaState.NORMAL, 0, 0);
        replica2 = new LocalReplica(2L, 2L, 100L, 0, 200000L, 0, 3000L, ReplicaState.NORMAL, 0, 0);
        replica3 = new LocalReplica(3L, 3L, 100L, 0, 200000L, 0, 3000L, ReplicaState.NORMAL, 0, 0);
        tablet.addReplica(replica1);
        tablet.addReplica(replica2);
        tablet.addReplica(replica3);
    }

    @AfterEach
    public void tearDown() {
        mockedEnvStatic.close();
    }

    @Test
    public void getMethodTest() {
        Assertions.assertEquals(replica1, tablet.getReplicaById(replica1.getId()));
        Assertions.assertEquals(replica2, tablet.getReplicaById(replica2.getId()));
        Assertions.assertEquals(replica3, tablet.getReplicaById(replica3.getId()));

        Assertions.assertEquals(3, tablet.getReplicas().size());
        Assertions.assertEquals(replica1, tablet.getReplicaByBackendId(replica1.getBackendIdWithoutException()));
        Assertions.assertEquals(replica2, tablet.getReplicaByBackendId(replica2.getBackendIdWithoutException()));
        Assertions.assertEquals(replica3, tablet.getReplicaByBackendId(replica3.getBackendIdWithoutException()));


        long newTabletId = 20000;
        tablet.setTabletId(newTabletId);
        Assertions.assertEquals("tabletId=" + newTabletId, tablet.toString());
    }

    @Test
    public void testGetReplicaStatsOnlyUsesNormalReplicas() {
        Tablet statsTablet = new LocalTablet(2);
        invertedIndex.addTablet(2, new TabletMeta(10, 20, 30, 40, 1, TStorageMedium.HDD,
                false /* isRowBinlog */));
        statsTablet.addReplica(new LocalReplica(11L, 1L, 100L, 0, 10L, 0L, 100L, ReplicaState.NORMAL, 0L, 100L));
        statsTablet.addReplica(new LocalReplica(12L, 2L, 100L, 0, 20L, 0L, 200L, ReplicaState.NORMAL, 0L, 100L));
        statsTablet.addReplica(new LocalReplica(13L, 3L, 100L, 0, 0L, 0L, 300L, ReplicaState.NORMAL, 0L, 100L));
        statsTablet.addReplica(new LocalReplica(14L, 4L, 100L, 0, 40L, 0L, 400L, ReplicaState.DECOMMISSION,
                0L, 100L));

        Assertions.assertEquals(30L, statsTablet.getDataSize(false, false));
        Assertions.assertEquals(10L, statsTablet.getDataSize(true, false));
        Assertions.assertEquals(30L, statsTablet.getDataSize(false, true));
        Assertions.assertEquals(600L, statsTablet.getRowCount(false));
        Assertions.assertEquals(200L, statsTablet.getRowCount(true));
    }

    @Test
    public void deleteReplicaTest() {
        // delete replica1
        Assertions.assertTrue(tablet.deleteReplicaByBackendId(replica1.getBackendIdWithoutException()));
        Assertions.assertNull(tablet.getReplicaById(replica1.getId()));

        // err: re-delete replica1
        Assertions.assertFalse(tablet.deleteReplicaByBackendId(replica1.getBackendIdWithoutException()));
        Assertions.assertFalse(tablet.deleteReplica(replica1));
        Assertions.assertNull(tablet.getReplicaById(replica1.getId()));

        // delete replica2
        Assertions.assertTrue(tablet.deleteReplica(replica2));
        Assertions.assertEquals(1, tablet.getReplicas().size());
    }

    @Test
    public void testGetReplicasReturnsImmutableSnapshot() {
        List<Replica> snapshot = tablet.getReplicas();
        Assertions.assertEquals(3, snapshot.size());

        // A write after the snapshot was taken must not be visible in it (copy-on-write).
        Replica replica4 = new LocalReplica(4L, 4L, 100L, 0, 200000L, 0, 3000L, ReplicaState.NORMAL, 0, 0);
        tablet.addReplica(replica4);
        Assertions.assertEquals(3, snapshot.size());
        Assertions.assertEquals(4, tablet.getReplicas().size());

        // The returned snapshot is read-only.
        Assertions.assertThrows(UnsupportedOperationException.class, () -> snapshot.add(replica4));
    }

    @Test
    public void testLocalReplicaBinlogMissingTimeoutAndRetryBudget() {
        long originTimeoutSecond = Config.tablet_binlog_missing_timeout_second;
        int originMaxTimes = Config.tablet_binlog_missing_max_times;
        try {
            Config.tablet_binlog_missing_timeout_second = 60;
            Config.tablet_binlog_missing_max_times = 2;

            replica1.setBinlogMissing(true);
            Assertions.assertTrue(replica1.isBinlogMissing());

            replica1.consumeBinlogMissingRetry();
            Assertions.assertTrue(replica1.isBinlogMissing());
            replica1.consumeBinlogMissingRetry();
            Assertions.assertFalse(replica1.isBinlogMissing());

            replica1.setBinlogMissing(true);
            Assertions.assertTrue(replica1.isBinlogMissing());
            replica1.setBinlogMissing(false);
            Assertions.assertFalse(replica1.isBinlogMissing());

            Config.tablet_binlog_missing_timeout_second = 0;
            replica1.setBinlogMissing(true);
            Assertions.assertFalse(replica1.isBinlogMissing());
        } finally {
            Config.tablet_binlog_missing_timeout_second = originTimeoutSecond;
            Config.tablet_binlog_missing_max_times = originMaxTimes;
            replica1.setBinlogMissing(false);
        }
    }

    @Test
    public void testIterateReplicasWhileMutatingDoesNotThrow() {
        // Iterating the snapshot returned by getReplicas() must not throw
        // ConcurrentModificationException even when the tablet is structurally modified
        // during iteration.
        int seen = 0;
        for (Replica r : tablet.getReplicas()) {
            Assertions.assertNotNull(r);
            tablet.addReplica(new LocalReplica(100L + seen, 100L + seen, 100L, 0, 200000L, 0, 3000L,
                    ReplicaState.NORMAL, 0, 0));
            tablet.deleteReplicaByBackendId(2L);
            seen++;
        }
        Assertions.assertEquals(3, seen);
    }

    @Test
    public void testConcurrentGetReplicasNeverThrows() throws InterruptedException {
        // A reader repeatedly snapshots and iterates getReplicas() while a writer keeps
        // mutating the replica list. Copy-on-write guarantees the reader never observes a
        // partially built list or throws ConcurrentModificationException.
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean(false);

        Thread writer = new Thread(() -> {
            long id = 1000L;
            while (!stop.get()) {
                // Reuse a small set of backend ids so the list stays bounded while still
                // exercising the add/replace path.
                long beId = id % 8;
                tablet.addReplica(new LocalReplica(id, beId, 100L, 0, 200000L, 0, 3000L,
                        ReplicaState.NORMAL, 0, 0), true);
                id++;
            }
        });

        Thread reader = new Thread(() -> {
            try {
                for (int i = 0; i < 50000 && error.get() == null; i++) {
                    for (Replica r : tablet.getReplicas()) {
                        r.getId();
                    }
                }
            } catch (Throwable t) {
                error.set(t);
            } finally {
                stop.set(true);
            }
        });

        writer.start();
        reader.start();
        reader.join();
        stop.set(true);
        writer.join();

        if (error.get() != null) {
            Assertions.fail("getReplicas() iteration threw under concurrent mutation: " + error.get());
        }
    }

    @Test
    public void testSerialization() throws Exception {
        final Path path = Files.createTempFile("olapTabletTest", "tmp");
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));
        Text.writeString(dos, GsonUtils.GSON.toJson(tablet));
        dos.flush();
        dos.close();

        // 2. Read a object from file
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));
        Tablet rTablet1 = GsonUtils.GSON.fromJson(Text.readString(dis), Tablet.class);
        Assertions.assertEquals(1, rTablet1.getId());
        Assertions.assertEquals(3, rTablet1.getReplicas().size());
        Assertions.assertEquals(rTablet1.getReplicas().get(0).getVersion(), rTablet1.getReplicas().get(1).getVersion());

        Assertions.assertEquals(rTablet1, tablet);
        Assertions.assertEquals(rTablet1, rTablet1);

        Tablet tablet2 = new LocalTablet(1);
        Replica replica1 = new LocalReplica(1L, 1L, 100L, 0, 200000L, 0, 3000L, ReplicaState.NORMAL, 0, 0);
        Replica replica2 = new LocalReplica(2L, 2L, 100L, 0, 200000L, 0, 3000L, ReplicaState.NORMAL, 0, 0);
        Replica replica3 = new LocalReplica(3L, 3L, 100L, 0, 200000L, 0, 3000L, ReplicaState.NORMAL, 0, 0);
        tablet2.addReplica(replica1);
        tablet2.addReplica(replica2);
        Assertions.assertNotEquals(tablet2, tablet);
        tablet2.addReplica(replica3);
        Assertions.assertEquals(tablet2, tablet);

        Tablet tablet3 = new LocalTablet(1);
        tablet3.addReplica(replica1);
        tablet3.addReplica(replica2);
        tablet3.addReplica(new LocalReplica(4L, 4L, 100L, 0, 200000L, 0, 3000L, ReplicaState.NORMAL, 0, 0));
        Assertions.assertNotEquals(tablet3, tablet);

        dis.close();
        Files.delete(path);
    }

    @Test
    public void testRowBinlogTabletIdsGsonUpgradeCompatibility() {
        Tablet baseTablet = new LocalTablet(10L);
        baseTablet.setRowBinlogTabletId(20L);
        JsonObject baseTabletJson = JsonParser.parseString(GsonUtils.GSON.toJson(baseTablet)).getAsJsonObject();
        Tablet deserializedBaseTablet = GsonUtils.GSON.fromJson(baseTabletJson, Tablet.class);
        Assertions.assertEquals(20L, deserializedBaseTablet.getRowBinlogTabletId());
        Assertions.assertNull(deserializedBaseTablet.rowBinlogBaseTabletId);

        Tablet rowBinlogTablet = new LocalTablet(20L);
        rowBinlogTablet.setRowBinlogBaseTabletId(10L);
        JsonObject rowBinlogTabletJson = JsonParser.parseString(GsonUtils.GSON.toJson(rowBinlogTablet))
                .getAsJsonObject();
        Tablet deserializedRowBinlogTablet = GsonUtils.GSON.fromJson(rowBinlogTabletJson, Tablet.class);
        Assertions.assertEquals(10L, deserializedRowBinlogTablet.getRowBinlogBaseTabletId());
        Assertions.assertNull(deserializedRowBinlogTablet.rowBinlogTabletId);

        baseTabletJson.remove("rbti");
        baseTabletJson.remove("rbbti");
        Tablet deserializedLegacyTablet = GsonUtils.GSON.fromJson(baseTabletJson, Tablet.class);
        Assertions.assertNull(deserializedLegacyTablet.rowBinlogTabletId);
        Assertions.assertNull(deserializedLegacyTablet.rowBinlogBaseTabletId);
    }

    /**
     * check the tablet's Tablet.TabletStatus, the right location is [1 2 3]
     * @param backendId2ReplicaIsBad beId -> if replica is a bad replica
     */
    @SafeVarargs
    private final void testTabletColocateHealthStatus0(Tablet.TabletStatus exceptedTabletStatus,
            Pair<Long, Boolean>... backendId2ReplicaIsBad) {
        Tablet tablet = new LocalTablet(1);
        int replicaId = 1;
        for (Pair<Long, Boolean> pair : backendId2ReplicaIsBad) {
            long versionAndSuccessVersion = 100L;
            long lastFailVersion = -1L;
            if (pair.second) {
                versionAndSuccessVersion = 99L;
                lastFailVersion = 100L;
            }
            tablet.addReplica(new LocalReplica(replicaId++, pair.first, versionAndSuccessVersion, 0,
                    200000L, 0, 3000L, ReplicaState.NORMAL, lastFailVersion, versionAndSuccessVersion));
        }
        Assertions.assertEquals(tablet.getColocateHealth(100L, new ReplicaAllocation((short) 3),
                Sets.newHashSet(1L, 2L, 3L)).status, exceptedTabletStatus);
    }

    @Test
    public void testTabletColocateHealthStatus() {
        // [1 2 4]
        testTabletColocateHealthStatus0(
                Tablet.TabletStatus.COLOCATE_MISMATCH,
                Pair.of(1L, false), Pair.of(2L, false), Pair.of(4L, false)
        );

        // [1 2 3(bad)]
        testTabletColocateHealthStatus0(
                Tablet.TabletStatus.VERSION_INCOMPLETE,
                Pair.of(1L, false), Pair.of(2L, false), Pair.of(3L, true)
        );

        // 1 2 3 4(good)
        testTabletColocateHealthStatus0(
                Tablet.TabletStatus.COLOCATE_REDUNDANT,
                Pair.of(1L, false), Pair.of(2L, false), Pair.of(3L, false), Pair.of(4L, false)
        );

        // [1 2 3 4(bad)]
        testTabletColocateHealthStatus0(
                Tablet.TabletStatus.COLOCATE_REDUNDANT,
                Pair.of(1L, false), Pair.of(2L, false), Pair.of(3L, false), Pair.of(4L, true)
        );
    }

    @Test
    public void testGetMinReplicaRowCount() {
        Tablet t = new LocalTablet(1);
        long row = t.getMinReplicaRowCount(1);
        Assertions.assertEquals(0, row);

        Replica r1 = new LocalReplica(1, 1, 10, 0, 0, 0, 100, ReplicaState.NORMAL, 0, 10);
        t.addReplica(r1);
        row = t.getMinReplicaRowCount(10);
        Assertions.assertEquals(100, row);

        row = t.getMinReplicaRowCount(11);
        Assertions.assertEquals(0, row);

        Replica r2 = new LocalReplica(2, 2, 10, 0, 0, 0, 110, ReplicaState.NORMAL, 0, 10);
        Replica r3 = new LocalReplica(3, 3, 10, 0, 0, 0, 90, ReplicaState.NORMAL, 0, 10);
        t.addReplica(r2);
        t.addReplica(r3);
        row = t.getMinReplicaRowCount(11);
        Assertions.assertEquals(0, row);
        row = t.getMinReplicaRowCount(9);
        Assertions.assertEquals(90, row);

        r3.setBad(true);
        row = t.getMinReplicaRowCount(9);
        Assertions.assertEquals(100, row);

        r3.setBad(false);
        row = t.getMinReplicaRowCount(9);
        Assertions.assertEquals(90, row);

        r2.updateVersion(11);
        row = t.getMinReplicaRowCount(9);
        Assertions.assertEquals(110, row);
    }
}
