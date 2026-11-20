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

package org.apache.doris.httpv2.rest;

import org.apache.doris.DorisFE;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.system.SystemInfoService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.http.ResponseEntity;

import java.util.Collections;

public class HealthActionTest {
    @Test
    public void testReadinessTracksCatalogDuringMasterElection() {
        boolean runningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        Env env = Mockito.mock(Env.class);
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfo.getAllBackendIds(Mockito.anyBoolean())).thenReturn(Collections.emptyList());
        try (MockedStatic<DorisFE> server = Mockito.mockStatic(DorisFE.class);
                MockedStatic<Env> catalog = Mockito.mockStatic(Env.class)) {
            server.when(DorisFE::isServerReady).thenReturn(true);
            catalog.when(Env::getCurrentEnv).thenReturn(env);
            catalog.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
            HealthAction action = new HealthAction();

            Mockito.when(env.isReady()).thenReturn(true);
            Assertions.assertEquals(200, ((ResponseEntity<?>) action.execute(null, null)).getStatusCode().value());
            // The process stays up while its catalog is unavailable during election.
            Mockito.when(env.isReady()).thenReturn(false);
            Assertions.assertEquals(503, ((ResponseEntity<?>) action.execute(null, null)).getStatusCode().value());
            Mockito.when(env.isReady()).thenReturn(true);
            Assertions.assertEquals(200, ((ResponseEntity<?>) action.execute(null, null)).getStatusCode().value());
            // A ready catalog must not make a draining process healthy again.
            server.when(DorisFE::isServerReady).thenReturn(false);
            Assertions.assertEquals(503, ((ResponseEntity<?>) action.execute(null, null)).getStatusCode().value());
        } finally {
            FeConstants.runningUnitTest = runningUnitTest;
        }
    }
}
