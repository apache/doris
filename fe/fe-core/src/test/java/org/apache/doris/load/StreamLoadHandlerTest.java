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

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TStreamLoadPutResult;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StreamLoadHandlerTest {
    @Test
    public void testSelectBackendSkipsDecommissioningBackend() throws Exception {
        SystemInfoService originalSystemInfoService = Env.getCurrentSystemInfo();
        Backend decommissioningBackend = createBackend(10001L, "127.0.0.1");
        decommissioningBackend.setDecommissioning(true);
        Backend selectedBackend = createBackend(10002L, "127.0.0.2");
        CloudSystemInfoService systemInfoService =
                new TestCloudSystemInfoService(Arrays.asList(decommissioningBackend, selectedBackend));

        try {
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", systemInfoService);

            Assert.assertEquals(selectedBackend.getId(), StreamLoadHandler.selectBackend("cluster0").getId());
        } finally {
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfoService);
        }
    }

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
        public List<Backend> getBackendsByClusterName(final String clusterName) {
            return backends;
        }

        @Override
        public Backend getBackend(long backendId) {
            return backends.stream().filter(backend -> backend.getId() == backendId).findFirst().orElse(null);
        }
    }
}
