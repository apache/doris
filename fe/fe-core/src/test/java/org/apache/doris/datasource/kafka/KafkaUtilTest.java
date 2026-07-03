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
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class KafkaUtilTest {

    @Test
    void testKafkaProxyBackendAvailableExcludesDecommissionBackends() {
        Assertions.assertFalse(KafkaUtil.isKafkaProxyBackendAvailable(
                createBackend(true, true, false), false));
        Assertions.assertFalse(KafkaUtil.isKafkaProxyBackendAvailable(
                createBackend(true, false, true), false));
        Assertions.assertFalse(KafkaUtil.isKafkaProxyBackendAvailable(
                createBackend(false, false, false), false));
        Assertions.assertFalse(KafkaUtil.isKafkaProxyBackendAvailable(
                createBackend(true, false, false), true));
        Assertions.assertTrue(KafkaUtil.isKafkaProxyBackendAvailable(
                createBackend(true, false, false), false));
    }

    @Test
    void testAddKafkaProxyBackendIfAvailableExcludesDecommissionFallbackBackends() {
        SystemInfoService infoService = Mockito.mock(SystemInfoService.class);
        Backend normalBackend = createBackend(true, false, false);
        Backend decommissioningBackend = createBackend(true, true, false);
        Backend decommissionedBackend = createBackend(true, false, true);
        Backend unavailableBackend = createBackend(false, false, false);
        Backend failedBackend = createBackend(true, false, false);
        Mockito.when(infoService.getBackend(1001L)).thenReturn(normalBackend);
        Mockito.when(infoService.getBackend(1002L)).thenReturn(decommissioningBackend);
        Mockito.when(infoService.getBackend(1003L)).thenReturn(decommissionedBackend);
        Mockito.when(infoService.getBackend(1004L)).thenReturn(unavailableBackend);
        Mockito.when(infoService.getBackend(1005L)).thenReturn(failedBackend);

        List<Long> backendIds = new ArrayList<>();
        Set<Long> failedBeIds = new HashSet<>(Arrays.asList(1005L));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(infoService);

            KafkaUtil.addKafkaProxyBackendIfAvailable(backendIds, 1001L, failedBeIds);
            KafkaUtil.addKafkaProxyBackendIfAvailable(backendIds, 1002L, failedBeIds);
            KafkaUtil.addKafkaProxyBackendIfAvailable(backendIds, 1003L, failedBeIds);
            KafkaUtil.addKafkaProxyBackendIfAvailable(backendIds, 1004L, failedBeIds);
            KafkaUtil.addKafkaProxyBackendIfAvailable(backendIds, 1005L, failedBeIds);
        }

        Assertions.assertEquals(Arrays.asList(1001L), backendIds);
    }

    private Backend createBackend(boolean loadAvailable, boolean decommissioning, boolean decommissioned) {
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.isLoadAvailable()).thenReturn(loadAvailable);
        Mockito.when(backend.isDecommissioning()).thenReturn(decommissioning);
        Mockito.when(backend.isDecommissioned()).thenReturn(decommissioned);
        return backend;
    }
}
