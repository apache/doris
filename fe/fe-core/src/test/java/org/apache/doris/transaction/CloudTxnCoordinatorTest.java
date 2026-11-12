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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class CloudTxnCoordinatorTest {
    private String previousDeployMode;
    private String previousCloudUniqueId;
    private MockedStatic<Env> mockedEnv;
    private Backend backend;
    private TransactionState transaction;

    @BeforeEach
    public void setUp() {
        previousDeployMode = Config.deploy_mode;
        previousCloudUniqueId = Config.cloud_unique_id;
        Config.deploy_mode = "cloud";
        Config.cloud_unique_id = "";
        backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getHost()).thenReturn("be-0.example");
        Mockito.when(backend.isAlive()).thenReturn(true);
        Mockito.when(backend.getLastStartTime()).thenReturn(100L);
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfo.getBackend(123L)).thenReturn(backend);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
        transaction = new TransactionState(1L, Lists.newArrayList(2L), 3L,
                "coordinator", null, LoadJobSourceType.BACKEND_STREAMING,
                new TxnCoordinator(TxnSourceType.BE, 123L, "old-pod-ip", 100L), -1L, 1000L);
    }

    @AfterEach
    public void tearDown() {
        mockedEnv.close();
        Config.deploy_mode = previousDeployMode;
        Config.cloud_unique_id = previousCloudUniqueId;
    }

    @Test
    public void testRestartWithDifferentAddress() {
        Mockito.when(backend.getLastStartTime()).thenReturn(101L);
        Assertions.assertTrue(GlobalTransactionMgr.checkFailedTxnsByCoordinator(transaction));
    }

    @Test
    public void testCurrentProcessWithDifferentAddress() {
        Assertions.assertFalse(GlobalTransactionMgr.checkFailedTxnsByCoordinator(transaction));
    }

    @Test
    public void testLostHeartbeatWithDifferentAddress() {
        Mockito.when(backend.isAlive()).thenReturn(false);
        Mockito.when(backend.getLastUpdateMs()).thenReturn(0L);
        Assertions.assertTrue(GlobalTransactionMgr.checkFailedTxnsByCoordinator(transaction));
    }

    @Test
    public void testRecentHeartbeatWithDifferentAddress() {
        Mockito.when(backend.isAlive()).thenReturn(false);
        Mockito.when(backend.getLastUpdateMs()).thenReturn(System.currentTimeMillis());
        Assertions.assertFalse(GlobalTransactionMgr.checkFailedTxnsByCoordinator(transaction));
    }

    @Test
    public void testNonCloudRetainsAddressCheck() {
        Config.deploy_mode = "local";
        Mockito.when(backend.getLastStartTime()).thenReturn(101L);
        Assertions.assertFalse(GlobalTransactionMgr.checkFailedTxnsByCoordinator(transaction));
        Mockito.when(backend.getHost()).thenReturn("old-pod-ip");
        Assertions.assertTrue(GlobalTransactionMgr.checkFailedTxnsByCoordinator(transaction));
    }
}
