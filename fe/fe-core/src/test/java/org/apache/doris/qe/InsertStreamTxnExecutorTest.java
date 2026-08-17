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

package org.apache.doris.qe;

import org.apache.doris.catalog.Env;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class InsertStreamTxnExecutorTest {

    @Test
    public void testSelectBackendForTxnLoadUsesCurrentClusterBackends() throws Exception {
        SystemInfoService systemInfoService = Mockito.spy(new SystemInfoService());
        Backend otherClusterBackend = createBackend(10001L, "127.0.0.1", 9060);
        Backend currentClusterBackend = createBackend(10002L, "127.0.0.2", 9061);
        systemInfoService.addBackend(otherClusterBackend);
        systemInfoService.addBackend(currentClusterBackend);
        Mockito.doReturn(ImmutableMap.of(currentClusterBackend.getId(), currentClusterBackend))
                .when(systemInfoService).getBackendsByCurrentCluster();

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            Backend selectedBackend = InsertStreamTxnExecutor.selectBackendForTxnLoad();

            Assert.assertEquals(currentClusterBackend.getId(), selectedBackend.getId());
        }
    }

    private Backend createBackend(long id, String host, int brpcPort) {
        Backend backend = new Backend(id, host, 9050);
        backend.setAlive(true);
        backend.setBrpcPort(brpcPort);
        return backend;
    }
}
