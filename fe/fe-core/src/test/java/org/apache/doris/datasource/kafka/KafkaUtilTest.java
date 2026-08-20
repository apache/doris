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

package org.apache.doris.datasource.kafka;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class KafkaUtilTest {
    @Test
    public void testGetInfoFailureMessageIncludesComputeGroup() {
        Assert.assertEquals("failed to get info: no alive backends, compute group: routine-load-compute-group,",
                KafkaUtil.getInfoFailureMessage("no alive backends", "routine-load-compute-group"));
        Assert.assertEquals("failed to get info: no alive backends,",
                KafkaUtil.getInfoFailureMessage("no alive backends", null));
    }

    @Test
    public void testGetBackendIdsForMetaRequestUsesRoutineLoadComputeGroup() throws Exception {
        String originalCloudUniqueId = Config.cloud_unique_id;
        Backend routineLoadBackend = new Backend(10001L, "127.0.0.1", 9050);
        Backend otherComputeGroupBackend = new Backend(10002L, "127.0.0.2", 9050);
        CloudSystemInfoService systemInfoService = Mockito.mock(CloudSystemInfoService.class);
        Mockito.when(systemInfoService.getBackendsByClusterName("routine-load-compute-group"))
                .thenReturn(Collections.singletonList(routineLoadBackend));
        Mockito.when(systemInfoService.getAllBackendIds(true))
                .thenReturn(Arrays.asList(routineLoadBackend.getId(), otherComputeGroupBackend.getId()));

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            Config.cloud_unique_id = "test-cloud";
            envStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            List<Long> backendIds = KafkaUtil.getBackendIdsForMetaRequest("routine-load-compute-group");

            Assert.assertEquals(Collections.singletonList(routineLoadBackend.getId()), backendIds);
            Mockito.verify(systemInfoService).getBackendsByClusterName("routine-load-compute-group");
            Mockito.verify(systemInfoService, Mockito.never()).getAllBackendIds(true);
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    @Test
    public void testGetAvailableBackendIdsForMetaRequestKeepsBlacklistFallbackInComputeGroup() {
        long routineLoadBackendId = 10001L;
        long otherComputeGroupBackendId = 10002L;
        Backend routineLoadBackend = mockAvailableBackend();
        Backend otherComputeGroupBackend = mockAvailableBackend();
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfoService.getBackend(routineLoadBackendId)).thenReturn(routineLoadBackend);
        Mockito.when(systemInfoService.getBackend(otherComputeGroupBackendId)).thenReturn(otherComputeGroupBackend);

        RoutineLoadManager routineLoadManager = Mockito.mock(RoutineLoadManager.class);
        Mockito.when(routineLoadManager.isInBlacklist(routineLoadBackendId)).thenReturn(true);
        Map<Long, Long> blacklist = new HashMap<>();
        blacklist.put(routineLoadBackendId, 1L);
        blacklist.put(otherComputeGroupBackendId, 1L);
        Mockito.when(routineLoadManager.getBlacklist()).thenReturn(blacklist);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getRoutineLoadManager()).thenReturn(routineLoadManager);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);

            List<Long> backendIds = KafkaUtil.getAvailableBackendIdsForMetaRequest(
                    Collections.singletonList(routineLoadBackendId), new HashSet<>());

            Assert.assertEquals(Collections.singletonList(routineLoadBackendId), backendIds);
            Mockito.verify(systemInfoService, Mockito.never()).getBackend(otherComputeGroupBackendId);
        }
    }

    @Test
    public void testGetBackendIdsForMetaRequestRejectsMissingCloudComputeGroup() {
        String originalCloudUniqueId = Config.cloud_unique_id;
        CloudSystemInfoService systemInfoService = Mockito.mock(CloudSystemInfoService.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            Config.cloud_unique_id = "test-cloud";
            envStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            LoadException nullException = Assert.assertThrows(
                    LoadException.class, () -> KafkaUtil.getBackendIdsForMetaRequest(null));
            LoadException emptyException = Assert.assertThrows(
                    LoadException.class, () -> KafkaUtil.getBackendIdsForMetaRequest(""));

            Assert.assertEquals("compute group is empty when getting kafka meta", nullException.getDetailMessage());
            Assert.assertEquals("compute group is empty when getting kafka meta", emptyException.getDetailMessage());
            Mockito.verifyNoInteractions(systemInfoService);
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    @Test
    public void testGetBackendIdsForMetaRequestPreservesNonCloudSelection() throws Exception {
        String originalCloudUniqueId = Config.cloud_unique_id;
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        List<Long> allBackendIds = Arrays.asList(10001L, 10002L);
        Mockito.when(systemInfoService.getAllBackendIds(true)).thenReturn(allBackendIds);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            Config.cloud_unique_id = "";
            envStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            Assert.assertEquals(allBackendIds, KafkaUtil.getBackendIdsForMetaRequest(null));
            Mockito.verify(systemInfoService).getAllBackendIds(true);
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    private Backend mockAvailableBackend() {
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.isLoadAvailable()).thenReturn(true);
        return backend;
    }
}
