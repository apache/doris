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

package org.apache.doris.cloud.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.system.Backend;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

class CloudRoutineLoadManagerTest {

    private static final long JOB_ID = 10001L;
    private static final String CLUSTER_NAME = "cluster0";

    @Test
    void testGetAvailableBackendIdsExcludesDecommissionBackends() throws Exception {
        CloudSystemInfoService infoService = Mockito.mock(CloudSystemInfoService.class);
        RoutineLoadJob job = Mockito.mock(RoutineLoadJob.class);
        Mockito.when(job.getCloudCluster()).thenReturn(CLUSTER_NAME);

        Backend normalBackend = createBackend(1001L, true, false, false);
        Backend decommissioningBackend = createBackend(1002L, true, true, false);
        Backend decommissionedBackend = createBackend(1003L, true, false, true);
        Backend deadBackend = createBackend(1004L, false, false, false);
        Mockito.when(infoService.getBackendsByClusterName(CLUSTER_NAME))
                .thenReturn(Arrays.asList(normalBackend, decommissioningBackend, decommissionedBackend, deadBackend));

        CloudRoutineLoadManager manager = new CloudRoutineLoadManager() {
            @Override
            public RoutineLoadJob getJob(long jobId) {
                Assertions.assertEquals(JOB_ID, jobId);
                return job;
            }
        };

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(infoService);

            List<Long> backendIds = manager.getAvailableBackendIds(JOB_ID);

            Assertions.assertEquals(Arrays.asList(1001L), backendIds);
        }
    }

    private Backend createBackend(long id, boolean alive, boolean decommissioning, boolean decommissioned) {
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getId()).thenReturn(id);
        Mockito.when(backend.isAlive()).thenReturn(alive);
        Mockito.when(backend.isLoadAvailable()).thenReturn(alive);
        Mockito.when(backend.isDecommissioning()).thenReturn(decommissioning);
        Mockito.when(backend.isDecommissioned()).thenReturn(decommissioned);
        return backend;
    }
}
