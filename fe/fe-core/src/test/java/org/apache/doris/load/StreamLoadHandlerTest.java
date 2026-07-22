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

package org.apache.doris.load;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TStreamLoadPutResult;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

public class StreamLoadHandlerTest {
    @Test
    public void testSetCloudClusterUsesBackendComputeGroup() throws Exception {
        SystemInfoService originalSystemInfoService = Env.getCurrentSystemInfo();
        String originalCloudUniqueId = Config.cloud_unique_id;
        Backend backend = createBackend(10001L, "127.0.0.1");
        backend.setCloudClusterName("backend_compute_group");
        CloudSystemInfoService systemInfoService =
                new TestCloudSystemInfoService(Arrays.asList(backend));
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setUser("");
        request.setBackendId(backend.getId());
        request.setCloudCluster("header_compute_group");

        try {
            Config.cloud_unique_id = "test_cloud_unique_id";
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", systemInfoService);
            ConnectContext.remove();

            StreamLoadHandler handler = new StreamLoadHandler(
                    request, null, new TStreamLoadPutResult(), "127.0.0.1");
            handler.setCloudCluster();

            Assert.assertEquals("backend_compute_group",
                    ConnectContext.get().getSessionVariable().getCloudCluster());
            Assert.assertEquals("backend_compute_group", request.getCloudCluster());
        } finally {
            ConnectContext.remove();
            Config.cloud_unique_id = originalCloudUniqueId;
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfoService);
        }
    }

    @Test
    public void testGroupCommitValidatesBackendComputeGroupPrivilege() throws Exception {
        String originalCloudUniqueId = Config.cloud_unique_id;
        Backend backend = createBackend(10001L, "127.0.0.1");
        backend.setCloudClusterName("backend_compute_group");
        CloudSystemInfoService systemInfoService =
                new TestCloudSystemInfoService(Arrays.asList(backend));
        CloudEnv cloudEnv = Mockito.mock(CloudEnv.class);
        InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
        Auth auth = Mockito.mock(Auth.class);
        Mockito.when(cloudEnv.getInternalCatalog()).thenReturn(internalCatalog);
        Mockito.when(internalCatalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(cloudEnv.getAuth()).thenReturn(auth);
        Mockito.doAnswer(invocation -> {
            List<UserIdentity> currentUser = invocation.getArgument(3);
            currentUser.add(UserIdentity.createAnalyzedUserIdentWithIp("test_user", "%"));
            return null;
        }).when(auth).checkPlainPassword(Mockito.eq("test_user"), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyList());
        Mockito.doThrow(new DdlException("USAGE denied"))
                .when(cloudEnv).changeCloudCluster(
                        Mockito.eq("backend_compute_group"), Mockito.any(ConnectContext.class));

        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setUser("test_user");
        request.setUserIp("127.0.0.1");
        request.setPasswd("test_password");
        request.setBackendId(backend.getId());
        request.setCloudCluster("header_compute_group");
        request.setGroupCommitMode("sync_mode");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Config.cloud_unique_id = "test_cloud_unique_id";
            mockedEnv.when(Env::getCurrentEnv).thenReturn(cloudEnv);
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            ConnectContext.remove();

            StreamLoadHandler handler = new StreamLoadHandler(
                    request, null, new TStreamLoadPutResult(), "127.0.0.1");
            try {
                handler.setCloudCluster();
                Assert.fail("group commit should validate compute group privilege");
            } catch (DdlException e) {
                Assert.assertTrue(e.getMessage().contains("USAGE denied"));
            }

            Mockito.verify(cloudEnv).changeCloudCluster(
                    Mockito.eq("backend_compute_group"), Mockito.any(ConnectContext.class));
        } finally {
            ConnectContext.remove();
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    private Backend createBackend(long id, String host) {
        Backend backend = new Backend(id, host, 9050);
        backend.setAlive(true);
        return backend;
    }

    private static class TestCloudSystemInfoService extends CloudSystemInfoService {
        private final List<Backend> backends;

        private TestCloudSystemInfoService(List<Backend> backends) {
            this.backends = backends;
        }

        @Override
        public Backend getBackend(long backendId) {
            return backends.stream().filter(backend -> backend.getId() == backendId).findFirst().orElse(null);
        }
    }
}
