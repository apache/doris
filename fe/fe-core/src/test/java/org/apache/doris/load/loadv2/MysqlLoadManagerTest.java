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

package org.apache.doris.load.loadv2;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.load.StreamLoadHandler;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.BackendSelectionPolicyFactory;
import org.apache.doris.resource.BackendSelectionService;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Collections;

public class MysqlLoadManagerTest {
    private final String originalCloudUniqueId = Config.cloud_unique_id;

    @AfterEach
    public void tearDown() {
        Config.cloud_unique_id = originalCloudUniqueId;
        ConnectContext.remove();
    }

    @Test
    public void testSelectBackendForCloudMySqlLoadIgnoresLoadSelection() throws Exception {
        Config.cloud_unique_id = "cloud";
        ConnectContext context = new ConnectContext();
        context.setCloudCluster("cg1");
        context.setThreadLocalInfo();
        MysqlLoadManager manager = new MysqlLoadManager();
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getHost()).thenReturn("be-host");
        Mockito.when(backend.getHttpPort()).thenReturn(8040);

        try (MockedStatic<StreamLoadHandler> mockedStreamLoad = Mockito.mockStatic(StreamLoadHandler.class);
                MockedStatic<BackendSelectionPolicyFactory> mockedSelection =
                        Mockito.mockStatic(BackendSelectionPolicyFactory.class)) {
            mockedStreamLoad.when(() -> StreamLoadHandler.selectBackend("cg1")).thenReturn(backend);
            mockedSelection.when(BackendSelectionPolicyFactory::get)
                    .thenThrow(new AssertionError("cloud mysql load should not use load selection"));

            String url = invokeSelectBackendForMySqlLoad(manager, context, "db1", "tbl1");

            Assertions.assertEquals("http://be-host:8040/api/db1/tbl1/_stream_load", url);
            mockedStreamLoad.verify(() -> StreamLoadHandler.selectBackend("cg1"));
        }
    }

    @Test
    public void testSelectBackendForMySqlLoadUsesExplicitContext() throws Exception {
        Config.cloud_unique_id = "";
        ConnectContext context = new ConnectContext();
        MysqlLoadManager manager = new MysqlLoadManager();
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getHost()).thenReturn("be-host");
        Mockito.when(backend.getHttpPort()).thenReturn(8040);
        Mockito.when(systemInfoService.selectBackendIdsByPolicy(Mockito.any(), Mockito.eq(-1)))
                .thenReturn(Collections.singletonList(1L));
        Mockito.when(systemInfoService.getBackend(1L)).thenReturn(backend);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedStatic<BackendSelectionService> mockedSelection =
                        Mockito.mockStatic(BackendSelectionService.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            mockedSelection.when(() -> BackendSelectionService.chooseLoadBackend(
                    Mockito.same(context), Mockito.anyList())).thenReturn(backend);

            String url = invokeSelectBackendForMySqlLoad(manager, context, "db1", "tbl1");

            Assertions.assertEquals("http://be-host:8040/api/db1/tbl1/_stream_load", url);
            mockedSelection.verify(() -> BackendSelectionService.chooseLoadBackend(
                    Mockito.same(context), Mockito.anyList()));
        }
    }

    private String invokeSelectBackendForMySqlLoad(MysqlLoadManager manager, ConnectContext context,
            String database, String table)
            throws Exception {
        Method method = MysqlLoadManager.class.getDeclaredMethod("selectBackendForMySqlLoad",
                ConnectContext.class, String.class, String.class);
        method.setAccessible(true);
        return (String) method.invoke(manager, context, database, table);
    }
}
