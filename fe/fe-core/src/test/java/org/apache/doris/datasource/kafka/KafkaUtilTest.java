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
import org.apache.doris.system.Backend;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class KafkaUtilTest {
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

            List<Long> backendIds =
                    KafkaUtil.getBackendIdsForMetaRequest("routine-load-compute-group");

            Assert.assertEquals(Collections.singletonList(routineLoadBackend.getId()), backendIds);
            Mockito.verify(systemInfoService).getBackendsByClusterName("routine-load-compute-group");
            Mockito.verify(systemInfoService, Mockito.never()).getAllBackendIds(true);
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }
}
