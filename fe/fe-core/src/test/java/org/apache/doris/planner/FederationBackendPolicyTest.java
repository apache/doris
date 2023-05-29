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

package org.apache.doris.planner;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.external.FederationBackendPolicy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Arrays;
import java.util.List;

public class FederationBackendPolicyTest {
    @Mocked
    private Env env;

    @Before
    public void setUp() {
        Backend backend1 = new Backend(1L, "192.168.1.1", 9050);
        backend1.setAlive(true);

        Backend backend2 = new Backend(2L, "192.168.1.2", 9050);
        backend2.setAlive(true);

        Backend backend3 = new Backend(3L, "192.168.1.3", 9050);
        backend3.setAlive(true);

        Backend backend4 = new Backend(4L, "192.168.1.4", 9050);
        backend4.setAlive(false);

        SystemInfoService service = new SystemInfoService();
        service.addBackend(backend1);
        service.addBackend(backend2);
        service.addBackend(backend3);
        service.addBackend(backend4);

        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return service;
            }
        };

    }

    @Test
    public void testGetNextBe() throws UserException {
        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();
        Assertions.assertTrue(policy.numBackends() == 3);
        for (int i = 0; i < policy.numBackends(); i++) {
            Assertions.assertNotEquals(policy.getNextBe().getHost(), "192.168.1.4");
        }
    }

    @Test
    public void testGetNextBeWithPreLocations() throws UserException {
        FederationBackendPolicy policy = new FederationBackendPolicy();
        policy.init();
        Assertions.assertTrue(policy.numBackends() == 3);
        List<String> preferredLocations = Arrays.asList("192.168.1.3");
        Assertions.assertEquals(policy.getNextBe(preferredLocations).getHost(), "192.168.1.3");
    }
}
