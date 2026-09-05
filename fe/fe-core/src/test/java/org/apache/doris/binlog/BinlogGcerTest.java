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

package org.apache.doris.binlog;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.UpdateTabletMetaInfoTask;
import org.apache.doris.tso.TSOService;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;

public class BinlogGcerTest {
    private boolean originalEnableFeatureBinlog;
    private boolean originalRunningUnitTest;
    private String originalCloudUniqueId;
    private String originalDeployMode;
    private String originalMetaServiceEndpoint;

    @BeforeEach
    public void setUp() {
        originalEnableFeatureBinlog = Config.enable_feature_binlog;
        originalRunningUnitTest = FeConstants.runningUnitTest;
        originalCloudUniqueId = Config.cloud_unique_id;
        originalDeployMode = Config.deploy_mode;
        originalMetaServiceEndpoint = Config.meta_service_endpoint;
        Config.enable_feature_binlog = true;
        FeConstants.runningUnitTest = false;
        Config.cloud_unique_id = "";
        Config.deploy_mode = "";
        Config.meta_service_endpoint = "127.0.0.1:20121";
    }

    @AfterEach
    public void tearDown() {
        Config.enable_feature_binlog = originalEnableFeatureBinlog;
        FeConstants.runningUnitTest = originalRunningUnitTest;
        Config.cloud_unique_id = originalCloudUniqueId;
        Config.deploy_mode = originalDeployMode;
        Config.meta_service_endpoint = originalMetaServiceEndpoint;
    }

    @Test
    public void testSyncLocalRowBinlogTtlReferenceTso() {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        TSOService tsoService = Mockito.mock(TSOService.class);
        OlapTable table = rowBinlogTtlTable(101L, 7L);
        Database db = database(table);
        Mockito.when(tsoService.getTSO()).thenReturn(123L);
        Mockito.when(catalog.getDbs()).thenReturn(Arrays.asList(db));

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<AgentTaskExecutor> executorMock = Mockito.mockStatic(AgentTaskExecutor.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            envMock.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            envMock.when(Env::getCurrentTSOService).thenReturn(tsoService);

            new BinlogGcer().syncRowBinlogTtlReferenceTso();

            ArgumentCaptor<AgentBatchTask> captor = ArgumentCaptor.forClass(AgentBatchTask.class);
            executorMock.verify(() -> AgentTaskExecutor.submit(captor.capture()));
            Assertions.assertEquals(1, captor.getValue().getTaskNum());
            AgentTask task = captor.getValue().getAllTasks().get(0);
            Assertions.assertTrue(task instanceof UpdateTabletMetaInfoTask);
            Assertions.assertEquals(7L, task.getBackendId());
            Assertions.assertEquals(123L, ((UpdateTabletMetaInfoTask) task).toThrift()
                    .getTabletMetaInfos().get(0).getRowBinlogTtlReferenceTso());
        }
    }

    @Test
    public void testSyncCloudRowBinlogTtlReferenceTso() throws Exception {
        Config.cloud_unique_id = "cloud-test";
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        TSOService tsoService = Mockito.mock(TSOService.class);
        OlapTable table = rowBinlogTtlTable(101L, 7L);
        Database db = database(table);
        Mockito.when(tsoService.getTSO()).thenReturn(456L);
        Mockito.when(catalog.getDbs()).thenReturn(Arrays.asList(db));
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfoService.getAllBackendsByAllCluster()).thenReturn(ImmutableMap.of());

        MetaServiceProxy metaServiceProxy = Mockito.mock(MetaServiceProxy.class);
        Mockito.when(metaServiceProxy.updateTablet(Mockito.any())).thenReturn(
                Cloud.UpdateTabletResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK))
                        .build());

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<MetaServiceProxy> metaServiceMock = Mockito.mockStatic(MetaServiceProxy.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            envMock.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            envMock.when(Env::getCurrentTSOService).thenReturn(tsoService);
            envMock.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            metaServiceMock.when(MetaServiceProxy::getInstance).thenReturn(metaServiceProxy);

            new BinlogGcer().syncRowBinlogTtlReferenceTso();
            Mockito.verify(tsoService).getTSO();
        }

        ArgumentCaptor<Cloud.UpdateTabletRequest> captor =
                ArgumentCaptor.forClass(Cloud.UpdateTabletRequest.class);
        Mockito.verify(metaServiceProxy).updateTablet(captor.capture());
        Assertions.assertEquals(1, captor.getValue().getTabletMetaInfosCount());
        Assertions.assertEquals(101L, captor.getValue().getTabletMetaInfos(0).getTabletId());
        Assertions.assertEquals(456L,
                captor.getValue().getTabletMetaInfos(0).getRowBinlogTtlReferenceTso());
    }

    @Test
    public void testDisabledFeatureDoesNotRequestTso() {
        Config.enable_feature_binlog = false;
        TSOService tsoService = Mockito.mock(TSOService.class);
        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            envMock.when(Env::getCurrentTSOService).thenReturn(tsoService);
            new BinlogGcer().syncRowBinlogTtlReferenceTso();
        }
        Mockito.verifyNoInteractions(tsoService);
    }

    private Database database(OlapTable table) {
        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getTables()).thenReturn(Arrays.asList(table));
        return db;
    }

    private OlapTable rowBinlogTtlTable(long tabletId, long backendId) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        MaterializedIndex baseIndex = Mockito.mock(MaterializedIndex.class);
        MaterializedIndex rowBinlogIndex = Mockito.mock(MaterializedIndex.class);
        Tablet tablet = Mockito.mock(Tablet.class);
        Replica replica = Mockito.mock(Replica.class);
        Mockito.when(table.hasRowBinlogTtl()).thenReturn(true);
        Mockito.when(table.getName()).thenReturn("tbl");
        Mockito.when(table.getPartitions()).thenReturn(Arrays.asList(partition));
        Mockito.when(partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE, true))
                .thenReturn(Arrays.asList(baseIndex, rowBinlogIndex));
        Mockito.when(rowBinlogIndex.isRowBinlog()).thenReturn(true);
        Mockito.when(rowBinlogIndex.getTablets()).thenReturn(Arrays.asList(tablet));
        Mockito.when(tablet.getId()).thenReturn(tabletId);
        Mockito.when(tablet.getReplicas()).thenReturn(Arrays.asList(replica));
        Mockito.when(replica.getBackendIdWithoutException()).thenReturn(backendId);
        return table;
    }
}
