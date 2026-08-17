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

package org.apache.doris.load.routineload;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.load.CloudRoutineLoadManager;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RoutineLoadBackendSelectionTest {
    @Test
    public void testAvailableBeForTaskDoesNotReuseDecommissioningPreviousBeWhenEligibleBesAreSaturated()
            throws UserException {
        int originalMaxRoutineLoadTaskNumPerBe = Config.max_routine_load_task_num_per_be;
        SystemInfoService originalSystemInfoService = Env.getCurrentSystemInfo();
        Backend previousBackend = createBackend(10001L, "127.0.0.1");
        previousBackend.setDecommissioning(true);
        Backend eligibleBackend = createBackend(10002L, "127.0.0.2");
        SystemInfoService systemInfoService = new SystemInfoService();
        systemInfoService.addBackend(previousBackend);
        systemInfoService.addBackend(eligibleBackend);

        try {
            Config.max_routine_load_task_num_per_be = 0;
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", systemInfoService);
            RoutineLoadManager routineLoadManager = new TestRoutineLoadManager(
                    Collections.singletonList(eligibleBackend.getId()));

            Assert.assertEquals(-1L, routineLoadManager.getAvailableBeForTask(1L, previousBackend.getId()));
        } finally {
            Config.max_routine_load_task_num_per_be = originalMaxRoutineLoadTaskNumPerBe;
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfoService);
        }
    }

    @Test
    public void testCloudAvailableBackendIdsSkipsLoadDisabledBackend() throws Exception {
        SystemInfoService originalSystemInfoService = Env.getCurrentSystemInfo();
        Backend loadDisabledBackend = createBackend(10001L, "127.0.0.1");
        loadDisabledBackend.setLoadDisabled(true);
        Backend selectedBackend = createBackend(10002L, "127.0.0.2");
        CloudSystemInfoService systemInfoService =
                new TestCloudSystemInfoService(Arrays.asList(loadDisabledBackend, selectedBackend));
        RoutineLoadJob routineLoadJob = Mockito.mock(RoutineLoadJob.class);
        Mockito.when(routineLoadJob.getCloudCluster()).thenReturn("cluster0");

        try {
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", systemInfoService);
            TestCloudRoutineLoadManager routineLoadManager = new TestCloudRoutineLoadManager(routineLoadJob);

            Assert.assertEquals(Collections.singletonList(selectedBackend.getId()),
                    routineLoadManager.getAvailableBackendIdsForTest(1L));
        } finally {
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfoService);
        }
    }

    @Test
    public void testAvailableBeForTaskDoesNotReuseSaturatedPreviousBe() throws Exception {
        int originalMaxRoutineLoadTaskNumPerBe = Config.max_routine_load_task_num_per_be;
        SystemInfoService originalSystemInfoService = Env.getCurrentSystemInfo();
        Backend previousBackend = createBackend(10001L, "127.0.0.1");
        Backend decommissioningBackend = createBackend(10002L, "127.0.0.2");
        decommissioningBackend.setDecommissioning(true);
        SystemInfoService systemInfoService = new SystemInfoService();
        systemInfoService.addBackend(previousBackend);
        systemInfoService.addBackend(decommissioningBackend);
        RoutineLoadJob routineLoadJob = Mockito.mock(RoutineLoadJob.class);
        Mockito.when(routineLoadJob.getState()).thenReturn(RoutineLoadJob.JobState.RUNNING);
        Mockito.when(routineLoadJob.getBeCurrentTasksNumMap())
                .thenReturn(Collections.singletonMap(previousBackend.getId(), 1));
        Map<Long, RoutineLoadJob> idToRoutineLoadJob = new HashMap<>();
        idToRoutineLoadJob.put(1L, routineLoadJob);

        try {
            Config.max_routine_load_task_num_per_be = 1;
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", systemInfoService);
            RoutineLoadManager routineLoadManager = new TestRoutineLoadManager(
                    Collections.singletonList(previousBackend.getId()));
            Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);

            routineLoadManager.updateBeIdToMaxConcurrentTasks();
            Assert.assertEquals(0, routineLoadManager.getClusterIdleSlotNum());
            Assert.assertEquals(-1L, routineLoadManager.getAvailableBeForTask(1L, previousBackend.getId()));
        } finally {
            Config.max_routine_load_task_num_per_be = originalMaxRoutineLoadTaskNumPerBe;
            Deencapsulation.setField(Env.getCurrentEnv(), "systemInfo", originalSystemInfoService);
        }
    }

    private Backend createBackend(long id, String host) {
        Backend backend = new Backend(id, host, 9050);
        backend.setAlive(true);
        return backend;
    }

    private static class TestRoutineLoadManager extends RoutineLoadManager {
        private final List<Long> availableBackendIds;

        private TestRoutineLoadManager(List<Long> availableBackendIds) {
            this.availableBackendIds = availableBackendIds;
        }

        @Override
        protected List<Long> getAvailableBackendIds(long jobId) {
            return availableBackendIds;
        }
    }

    private static class TestCloudRoutineLoadManager extends CloudRoutineLoadManager {
        private final RoutineLoadJob routineLoadJob;

        private TestCloudRoutineLoadManager(RoutineLoadJob routineLoadJob) {
            this.routineLoadJob = routineLoadJob;
        }

        @Override
        public RoutineLoadJob getJob(long jobId) {
            return routineLoadJob;
        }

        private List<Long> getAvailableBackendIdsForTest(long jobId) throws LoadException {
            return super.getAvailableBackendIds(jobId);
        }
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
    }
}
