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

import org.apache.doris.common.Config;
import org.apache.doris.system.Backend;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class KafkaUtilTest {

    @Test
    void testKafkaProxyBackendAvailableChecksDecommissioningOnlyInCloudMode() {
        String originDeployMode = Config.deploy_mode;
        String originCloudUniqueId = Config.cloud_unique_id;
        try {
            Config.deploy_mode = "";
            Config.cloud_unique_id = "";
            Assertions.assertTrue(KafkaUtil.isKafkaProxyBackendAvailable(
                    createBackend(true, true, false), false));
            Assertions.assertFalse(KafkaUtil.isKafkaProxyBackendAvailable(
                    createBackend(true, false, true), false));

            Config.deploy_mode = "cloud";
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
        } finally {
            Config.deploy_mode = originDeployMode;
            Config.cloud_unique_id = originCloudUniqueId;
        }
    }

    private Backend createBackend(boolean loadAvailable, boolean decommissioning, boolean decommissioned) {
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.isLoadAvailable()).thenReturn(loadAvailable);
        Mockito.when(backend.isDecommissioning()).thenReturn(decommissioning);
        Mockito.when(backend.isDecommissioned()).thenReturn(decommissioned);
        return backend;
    }
}
