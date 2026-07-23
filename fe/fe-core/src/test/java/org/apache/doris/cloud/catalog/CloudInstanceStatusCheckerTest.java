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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.StorageVaultMgr;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cloud.CacheHotspotManager;
import org.apache.doris.cloud.CloudWarmUpJob;
import org.apache.doris.cloud.OnTablesFilter.TableFilterRule;
import org.apache.doris.cloud.OnTablesFilter.TableFilterRule.RuleType;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.plans.commands.WarmUpClusterCommand;
import org.apache.doris.persist.EditLog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CloudInstanceStatusCheckerTest {
    private String originalCloudUniqueId;
    private CloudSystemInfoService cloudSystemInfoService;
    private CacheHotspotManager cacheHotspotManager;
    private InternalCatalog internalCatalog;
    private List<DatabaseIf<? extends TableIf>> databases;
    private MockedStatic<Env> mockedEnv;
    private CloudEnv cloudEnv;

    @BeforeEach
    public void setUp() {
        originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "test_cloud";

        cloudSystemInfoService = Mockito.spy(new CloudSystemInfoService());
        cacheHotspotManager = new CacheHotspotManager(cloudSystemInfoService);
        internalCatalog = Mockito.mock(InternalCatalog.class);
        databases = new ArrayList<>();
        Mockito.when(internalCatalog.getAllDbs()).thenAnswer(invocation -> databases);

        cloudEnv = Mockito.mock(CloudEnv.class);
        AtomicLong nextId = new AtomicLong(10000L);
        Mockito.when(cloudEnv.getNextId()).thenAnswer(invocation -> nextId.incrementAndGet());
        Mockito.when(cloudEnv.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        Mockito.when(cloudEnv.getStorageVaultMgr()).thenReturn(Mockito.mock(StorageVaultMgr.class));
        Mockito.when(cloudEnv.getCacheHotspotMgr()).thenReturn(cacheHotspotManager);
        Mockito.when(cloudEnv.isMaster()).thenReturn(false);

        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(cloudEnv);
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(internalCatalog);
        mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(cloudSystemInfoService);
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnv != null) {
            mockedEnv.close();
        }
        Config.cloud_unique_id = originalCloudUniqueId;
    }

    @Test
    public void testSyncInstanceCreatesVirtualComputeGroup() {
        addComputeGroup("active_cg_id", "active_cg");
        addComputeGroup("standby_cg_id", "standby_cg");
        Mockito.doReturn(instanceResponseWithVirtualComputeGroup()).when(cloudSystemInfoService).getCloudInstance();

        new CloudInstanceStatusChecker(cloudSystemInfoService).runAfterCatalogReady();

        CloudComputeGroupMeta virtualComputeGroup = cloudSystemInfoService.getComputeGroupById("vcg_id");
        Assertions.assertNotNull(virtualComputeGroup);
        Assertions.assertTrue(virtualComputeGroup.isVirtual());
        Assertions.assertEquals("vcg", virtualComputeGroup.getName());
        Assertions.assertEquals(Arrays.asList("active_cg", "standby_cg"),
                virtualComputeGroup.getSubComputeGroups());
        Assertions.assertEquals("active_cg", virtualComputeGroup.getActiveComputeGroup());
        Assertions.assertEquals("standby_cg", virtualComputeGroup.getStandbyComputeGroup());
    }

    @Test
    public void testSyncInstanceCreatesVirtualComputeGroupAndCancelsTableLevelLoadEvent() throws Exception {
        databases.add(mockDb("ods", mockTable(1001, "orders")));
        addComputeGroup("active_cg_id", "active_cg");
        addComputeGroup("standby_cg_id", "standby_cg");
        long tableLevelJobId = cacheHotspotManager.createJob(buildEventDrivenStmt("active_cg", "standby_cg",
                new TableFilterRule(RuleType.INCLUDE, "ods.*")));
        Mockito.doReturn(instanceResponseWithVirtualComputeGroup()).when(cloudSystemInfoService).getCloudInstance();
        Mockito.when(cloudEnv.isMaster()).thenReturn(true);

        RecordingAppender appender = new RecordingAppender("vcg-create-cancel-table-warmup-test");
        Logger logger = (Logger) LogManager.getLogger(CloudInstanceStatusChecker.class);
        appender.start();
        logger.addAppender(appender);
        try (MockedStatic<CloudSystemInfoService> mockedCloudSystemInfoService =
                     Mockito.mockStatic(CloudSystemInfoService.class, Mockito.CALLS_REAL_METHODS)) {
            mockedCloudSystemInfoService.when(() -> CloudSystemInfoService.updateFileCacheJobIds(
                    Mockito.any(CloudComputeGroupMeta.class), Mockito.anyList())).thenAnswer(invocation -> null);

            new CloudInstanceStatusChecker(cloudSystemInfoService).runAfterCatalogReady();
            mockedCloudSystemInfoService.verify(() -> CloudSystemInfoService.updateFileCacheJobIds(
                    Mockito.any(CloudComputeGroupMeta.class), Mockito.anyList()));
        } finally {
            logger.removeAppender(appender);
            appender.stop();
        }

        CloudComputeGroupMeta virtualComputeGroup = cloudSystemInfoService.getComputeGroupById("vcg_id");
        Assertions.assertNotNull(virtualComputeGroup);
        Assertions.assertTrue(virtualComputeGroup.isVirtual());
        Assertions.assertFalse(virtualComputeGroup.isNeedRebuildFileCache());

        CloudWarmUpJob tableLevelJob = cacheHotspotManager.getCloudWarmUpJob(tableLevelJobId);
        Assertions.assertEquals(CloudWarmUpJob.JobState.CANCELLED, tableLevelJob.getJobState());
        Assertions.assertTrue(tableLevelJob.getErrMsg().contains(
                "vcg cancel table-level load-event warm up job before rebuilding file cache jobs"));
        Assertions.assertTrue(tableLevelJob.getErrMsg().contains("virtual compute group 'vcg'"));

        Assertions.assertEquals(3, cacheHotspotManager.getAllJobInfos(10).size());
        Assertions.assertTrue(cacheHotspotManager.getCloudWarmUpJobs().values().stream().anyMatch(job ->
                job.getJobType() == CloudWarmUpJob.JobType.CLUSTER
                        && job.isPeriodic()
                        && "active_cg".equals(job.getSrcClusterName())
                        && "standby_cg".equals(job.getDstClusterName())));
        Assertions.assertTrue(cacheHotspotManager.getCloudWarmUpJobs().values().stream().anyMatch(job ->
                job.getJobType() == CloudWarmUpJob.JobType.CLUSTER
                        && job.isEventDriven()
                        && job.getSyncEvent() == CloudWarmUpJob.SyncEvent.LOAD
                        && !job.hasTableFilter()
                        && "active_cg".equals(job.getSrcClusterName())
                        && "standby_cg".equals(job.getDstClusterName())));

        String logs = appender.messagesAsString();
        Assertions.assertFalse(logs.contains("failed to create virtual compute group vcg"), logs);
        Assertions.assertTrue(logs.contains("warmup-vcg rebuild-start"), logs);
        Assertions.assertTrue(logs.contains("warmup-vcg rebuild-finish"), logs);
        Assertions.assertTrue(logs.contains("srcCluster=active_cg"), logs);
        Assertions.assertTrue(logs.contains("dstCluster=standby_cg"), logs);
    }

    private void addComputeGroup(String computeGroupId, String computeGroupName) {
        cloudSystemInfoService.addComputeGroup(computeGroupId,
                new CloudComputeGroupMeta(computeGroupId, computeGroupName, CloudComputeGroupMeta.ComputeTypeEnum.COMPUTE));
    }

    private Cloud.GetInstanceResponse instanceResponseWithVirtualComputeGroup() {
        Cloud.ClusterPB activeComputeGroup = computeGroup("active_cg_id", "active_cg");
        Cloud.ClusterPB standbyComputeGroup = computeGroup("standby_cg_id", "standby_cg");
        Cloud.ClusterPB virtualComputeGroup = Cloud.ClusterPB.newBuilder()
                .setClusterId("vcg_id")
                .setClusterName("vcg")
                .setType(Cloud.ClusterPB.Type.VIRTUAL)
                .addClusterNames("active_cg")
                .addClusterNames("standby_cg")
                .setClusterPolicy(Cloud.ClusterPolicy.newBuilder()
                        .setType(Cloud.ClusterPolicy.PolicyType.ActiveStandby)
                        .setActiveClusterName("active_cg")
                        .addStandbyClusterNames("standby_cg")
                        .build())
                .build();
        return Cloud.GetInstanceResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                        .setCode(Cloud.MetaServiceCode.OK)
                        .setMsg("OK")
                        .build())
                .setInstance(Cloud.InstanceInfoPB.newBuilder()
                        .setStatus(Cloud.InstanceInfoPB.Status.NORMAL)
                        .addClusters(activeComputeGroup)
                        .addClusters(standbyComputeGroup)
                        .addClusters(virtualComputeGroup)
                        .build())
                .build();
    }

    private Cloud.ClusterPB computeGroup(String computeGroupId, String computeGroupName) {
        return Cloud.ClusterPB.newBuilder()
                .setClusterId(computeGroupId)
                .setClusterName(computeGroupName)
                .setType(Cloud.ClusterPB.Type.COMPUTE)
                .build();
    }

    @SuppressWarnings("unchecked")
    private DatabaseIf<TableIf> mockDb(String name, TableIf... tables) {
        DatabaseIf<TableIf> db = Mockito.mock(DatabaseIf.class);
        Mockito.when(db.getFullName()).thenReturn(name);
        HashSet<String> tableNames = new HashSet<>();
        for (TableIf table : tables) {
            tableNames.add(table.getName());
            Mockito.when(db.getTableNullable(table.getName())).thenReturn(table);
        }
        Mockito.when(db.getTableNamesOrEmptyWithLock()).thenReturn(tableNames);
        return db;
    }

    private TableIf mockTable(long id, String name) {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getId()).thenReturn(id);
        Mockito.when(table.getName()).thenReturn(name);
        Mockito.when(table.getType()).thenReturn(TableIf.TableType.OLAP);
        Mockito.when(table.isManagedTable()).thenReturn(true);
        return table;
    }

    private WarmUpClusterCommand buildEventDrivenStmt(String src, String dst, TableFilterRule... rules) {
        Map<String, String> properties = new HashMap<>();
        properties.put("sync_mode", "event_driven");
        properties.put("sync_event", "load");
        return new WarmUpClusterCommand(new ArrayList<>(), src, dst, false, false,
                properties, Arrays.asList(rules));
    }

    private static class RecordingAppender extends AbstractAppender {
        private final List<String> messages = new ArrayList<>();

        RecordingAppender(String name) {
            super(name, null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }

        String messagesAsString() {
            return String.join("\n", messages);
        }
    }
}
