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

import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.LocalTabletInvertedIndex;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet.TabletHealth;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.clone.SchedException.Status;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.CloneTask;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TCloneReq;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTabletInfo;
import org.apache.doris.thrift.TTaskType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

class TabletSchedCtxBinlogConfigTest {
    private MockedStatic<Env> mockedEnv;
    private OlapTable table;
    private LocalTablet tablet;
    private TabletSchedCtx tabletCtx;
    private EditLog editLog;
    private BinlogConfig config;

    @BeforeEach
    void setUp() throws Exception {
        SystemInfoService infoService = new SystemInfoService();
        Backend srcBackend = new Backend(10001L, "127.0.0.1", 9050);
        Backend destBackend = new Backend(10002L, "127.0.0.2", 9050);
        srcBackend.setAlive(true);
        destBackend.setAlive(true);
        infoService.addBackend(srcBackend);
        infoService.addBackend(destBackend);

        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(infoService);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getNextId()).thenReturn(7L);
        editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        LocalTabletInvertedIndex invertedIndex = new LocalTabletInvertedIndex();
        mockedEnv.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);

        tablet = new LocalTablet(5L);
        Replica srcReplica = new LocalReplica(6L, srcBackend.getId(), 10L, 100,
                100L, 0L, 1L, ReplicaState.NORMAL, -1L, 10L);
        srcReplica.setPathHash(101L);
        tablet.addReplica(srcReplica, true);
        MaterializedIndex index = new MaterializedIndex(4L, MaterializedIndex.IndexState.NORMAL);
        index.addTablet(tablet, null, true);
        Partition partition = new Partition(3L, "p1", index, null);
        partition.updateVisibleVersion(10L);
        invertedIndex.addTablet(tablet.getId(), new TabletMeta(1L, 2L, 3L, 4L, 100, TStorageMedium.HDD, true));

        config = new BinlogConfig(true, 600L, BinlogConfig.MAX_BYTES, BinlogConfig.MAX_HISTORY_NUMS,
                BinlogConfig.BinlogFormat.ROW, false);
        config.setConfigVersion(11L);
        table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getPartition(3L)).thenReturn(partition);
        Mockito.when(table.getSchemaHashByIndexId(4L)).thenReturn(100);
        Mockito.when(table.getBinlogConfig()).thenReturn(config);
        PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
        Mockito.when(partitionInfo.getReplicaAllocation(3L)).thenReturn(new ReplicaAllocation((short) 2));
        Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);
        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getTableOrException(Mockito.eq(2L), Mockito.any())).thenReturn(table);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getDbOrException(Mockito.eq(1L), Mockito.any())).thenReturn(db);
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

        tabletCtx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR, 1L, 2L, 3L, 4L, 5L,
                new ReplicaAllocation((short) 2), System.currentTimeMillis());
        tabletCtx.setTablet(tablet);
        tabletCtx.setVersionInfo(10L, 10L);
        tabletCtx.setSchemaHash(100);
        tabletCtx.setStorageMedium(TStorageMedium.HDD);
        tabletCtx.setBinlogConfig(config);
        tabletCtx.setSrc(srcReplica);
        tabletCtx.setDest(destBackend.getId(), 102L);
        TabletHealth health = new TabletHealth();
        health.status = TabletStatus.REPLICA_MISSING;
        tabletCtx.setTabletHealth(health);
    }

    @AfterEach
    void tearDown() {
        mockedEnv.close();
    }

    @Test
    void cloneTaskKeepsConfigurationSnapshot() throws Exception {
        CloneTask task = tabletCtx.createCloneReplicaAndTask();
        config.setConfigVersion(12L);
        config.setTtlSeconds(1200L);

        TCloneReq request = task.toThrift();
        Assertions.assertEquals(11L, request.getBinlogConfig().getConfigVersion());
        Assertions.assertEquals(600L, request.getBinlogConfig().getTtlSeconds());
        request.getBinlogConfig().setTtlSeconds(1L);
        Assertions.assertEquals(600L, task.toThrift().getBinlogConfig().getTtlSeconds());
    }

    @Test
    void cloneUsesPendingRetentionTargetBeforeVisibleTtlIsPublished() throws Exception {
        config.setPendingTtlSeconds(1200L);
        tabletCtx.setBinlogConfig(config);
        CloneTask task = tabletCtx.createCloneReplicaAndTask();

        Assertions.assertEquals(600L, config.getTtlSeconds());
        Assertions.assertEquals(1200L, task.toThrift().getBinlogConfig().getTtlSeconds());
        assertCloneRejected(task, tabletInfo(11L, 600L));

        tabletCtx.finishCloneTask(task, finishRequest(task, tabletInfo(11L, 1200L)));
        Assertions.assertEquals(ReplicaState.NORMAL, tablet.getReplicaByBackendId(10002L).getState());
    }

    @Test
    void staleGenerationCannotAdmitCloneAfterAlterEvenWithMatchingTtl() throws Exception {
        CloneTask task = tabletCtx.createCloneReplicaAndTask();
        config.setConfigVersion(12L);

        assertCloneRejected(task, tabletInfo(11L, 600L));
    }

    @Test
    void matchingGenerationWithStaleTtlCannotAdmitClone() throws Exception {
        CloneTask task = tabletCtx.createCloneReplicaAndTask();
        config.setTtlSeconds(1200L);

        assertCloneRejected(task, tabletInfo(11L, 600L));
    }

    @Test
    void missingInstalledConfigurationCannotAdmitRowBinlogClone() throws Exception {
        CloneTask task = tabletCtx.createCloneReplicaAndTask();
        TTabletInfo report = tabletInfo(11L, 600L);
        report.unsetBinlogConfigVersion();
        assertCloneRejected(task, report);

        report.setBinlogConfigVersion(11L);
        report.unsetBinlogTtlSeconds();
        assertCloneRejected(task, report);
    }

    @Test
    void actualConfigurationUpdatedByAlterCanAdmitOlderCloneTask() throws Exception {
        CloneTask task = tabletCtx.createCloneReplicaAndTask();
        config.setConfigVersion(12L);
        config.setTtlSeconds(1200L);

        tabletCtx.finishCloneTask(task, finishRequest(task, tabletInfo(12L, 1200L)));

        Assertions.assertEquals(ReplicaState.NORMAL, tablet.getReplicaByBackendId(10002L).getState());
        Assertions.assertEquals(TabletSchedCtx.State.FINISHED, tabletCtx.getState());
        Mockito.verify(editLog).logAddReplica(Mockito.any());
    }

    @Test
    void staleRepairReportDoesNotModifyExistingReplica() throws Exception {
        Replica replica = new LocalReplica(8L, 10002L, 5L, 100, 50L, 0L, 1L,
                ReplicaState.NORMAL, 10L, 5L);
        replica.setPathHash(102L);
        tablet.addReplica(replica);
        tabletCtx.setTabletStatus(TabletStatus.VERSION_INCOMPLETE);
        CloneTask task = tabletCtx.createCloneReplicaAndTask();
        config.setConfigVersion(12L);

        SchedException error = Assertions.assertThrows(SchedException.class,
                () -> tabletCtx.finishCloneTask(task, finishRequest(task, tabletInfo(11L, 600L))));

        Assertions.assertEquals(Status.RUNNING_FAILED, error.getStatus());
        Assertions.assertEquals(ReplicaState.NORMAL, replica.getState());
        Assertions.assertEquals(5L, replica.getVersion());
        Mockito.verifyNoInteractions(editLog);
    }

    @Test
    void nonRowCloneAcceptsReportWithoutBinlogConfiguration() throws Exception {
        Mockito.when(table.getBinlogConfig()).thenReturn(new BinlogConfig());
        tabletCtx.setBinlogConfig(new BinlogConfig());
        CloneTask task = tabletCtx.createCloneReplicaAndTask();
        TTabletInfo report = tabletInfo(0L, 0L);
        report.unsetBinlogConfigVersion();
        report.unsetBinlogTtlSeconds();

        tabletCtx.finishCloneTask(task, finishRequest(task, report));

        Assertions.assertEquals(ReplicaState.NORMAL, tablet.getReplicaByBackendId(10002L).getState());
    }

    private void assertCloneRejected(CloneTask task, TTabletInfo report) {
        SchedException error = Assertions.assertThrows(SchedException.class,
                () -> tabletCtx.finishCloneTask(task, finishRequest(task, report)));
        Assertions.assertEquals(Status.RUNNING_FAILED, error.getStatus());
        Replica replica = tablet.getReplicaByBackendId(10002L);
        Assertions.assertEquals(ReplicaState.CLONE, replica.getState());
        Assertions.assertEquals(-1L, replica.getVersion());
        Mockito.verifyNoInteractions(editLog);
    }

    private TTabletInfo tabletInfo(long generation, long ttlSeconds) {
        TTabletInfo report = new TTabletInfo(5L, 100, 10L, 0L, 1L, 100L);
        report.setPathHash(102L);
        report.setBinlogConfigVersion(generation);
        report.setBinlogTtlSeconds(ttlSeconds);
        return report;
    }

    private TFinishTaskRequest finishRequest(CloneTask task, TTabletInfo report) {
        TFinishTaskRequest request = new TFinishTaskRequest(new TBackend("127.0.0.2", 9050, 8040),
                TTaskType.CLONE, task.getSignature(), new TStatus(TStatusCode.OK));
        request.setFinishTabletInfos(Collections.singletonList(report));
        return request;
    }
}
