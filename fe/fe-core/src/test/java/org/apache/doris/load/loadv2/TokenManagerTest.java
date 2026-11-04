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

package org.apache.doris.load.loadv2;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TokenManager;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.GenericPool;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TMySqlLoadAcquireTokenResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

public class TokenManagerTest {
    private boolean previousRunningUnitTest;
    private int previousQueueSize;
    private final List<TokenManager> startedManagers = new ArrayList<>();

    @BeforeEach
    public void runBefore() {
        previousRunningUnitTest = FeConstants.runningUnitTest;
        previousQueueSize = Config.token_queue_size;
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void runAfter() {
        for (TokenManager manager : startedManagers) {
            ScheduledExecutorService generator = Deencapsulation.getField(manager, "tokenGenerator");
            generator.shutdownNow();
        }
        FeConstants.runningUnitTest = previousRunningUnitTest;
        Config.token_queue_size = previousQueueSize;
    }

    private TokenManager startManager() {
        TokenManager manager = new TokenManager();
        manager.start();
        startedManagers.add(manager);
        return manager;
    }

    @Test
    public void testTokenCheck() throws UserException {
        TokenManager tokenManager = startManager();
        String token = tokenManager.acquireToken();
        Assertions.assertTrue(tokenManager.checkAuthToken(token));
    }

    @Test
    public void testSameToken() throws UserException {
        TokenManager tokenManager = startManager();
        String token1 = tokenManager.acquireToken();
        String token2 = tokenManager.acquireToken();
        Assertions.assertNotNull(token1);
        Assertions.assertEquals(token1, token2);
    }

    @Test
    public void testSigningQueueInitializationRotationAndSnapshot() throws UserException {
        Config.token_queue_size = 3;
        TokenManager manager = startManager();
        List<String> initial = manager.getAuthTokens();
        Assertions.assertEquals(2, initial.size());
        Assertions.assertNotEquals(initial.get(0), initial.get(1));
        Assertions.assertEquals(initial.get(0), manager.acquireSigningToken());
        Assertions.assertEquals(initial.get(1), manager.acquireToken());

        Deencapsulation.invoke(manager, "addNewToken", "third-token");
        Assertions.assertEquals(initial.get(1), manager.acquireSigningToken());
        Assertions.assertEquals("third-token", manager.acquireToken());
        Assertions.assertTrue(manager.checkAuthToken(initial.get(0)));
        Assertions.assertEquals(2, initial.size());

        Deencapsulation.invoke(manager, "addNewToken", "fourth-token");
        Assertions.assertEquals(List.of(initial.get(1), "third-token", "fourth-token"), manager.getAuthTokens());
        Assertions.assertEquals("third-token", manager.acquireSigningToken());
        Assertions.assertFalse(manager.checkAuthToken(initial.get(0)));
        manager.getAuthTokens().clear();
        Assertions.assertEquals(3, manager.getAuthTokens().size());
    }

    @Test
    public void testRejectsQueueWithoutSigningGeneration() {
        Config.token_queue_size = 1;
        Assertions.assertThrows(IllegalArgumentException.class, () -> new TokenManager().start());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFollowerSigningTokenDuringMasterUpgrade() throws Exception {
        FeConstants.runningUnitTest = false;
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getMasterHost()).thenReturn("127.0.0.1");
        Mockito.when(env.getMasterRpcPort()).thenReturn(9020);
        TNetworkAddress address = new TNetworkAddress("127.0.0.1", 9020);
        FrontendService.Client client = Mockito.mock(FrontendService.Client.class);
        GenericPool<FrontendService.Client> pool = Mockito.mock(GenericPool.class);
        Mockito.when(pool.borrowObject(address, 300000)).thenReturn(client);
        GenericPool<FrontendService.Client> previousPool = ClientPool.frontendPool;
        ClientPool.frontendPool = pool;
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            TokenManager manager = new TokenManager();
            TMySqlLoadAcquireTokenResult result = new TMySqlLoadAcquireTokenResult();
            result.setStatus(new TStatus(TStatusCode.OK));
            result.setToken("latest-token");
            Mockito.when(client.acquireToken()).thenReturn(result);
            // An old master does not serialize signing_token.
            Assertions.assertEquals("latest-token", manager.acquireSigningToken());
            result.setSigningToken("signing-token");
            Assertions.assertEquals("signing-token", manager.acquireSigningToken());
            Assertions.assertEquals("latest-token", manager.acquireToken());
            Mockito.verify(pool, Mockito.times(3)).returnObject(address, client);

            // A present but empty key must not silently fall back to the latest token.
            result.setSigningToken("");
            Assertions.assertThrows(UserException.class, manager::acquireSigningToken);
            result.unsetSigningToken();
            result.unsetToken();
            Assertions.assertThrows(UserException.class, manager::acquireSigningToken);
            result.setToken("");
            Assertions.assertThrows(UserException.class, manager::acquireSigningToken);

            result.setToken("latest-token");
            result.setStatus(new TStatus(TStatusCode.NOT_MASTER));
            Assertions.assertThrows(UserException.class, manager::acquireSigningToken);
            Mockito.clearInvocations(pool);
            Mockito.when(client.acquireToken()).thenThrow(new TException("RPC failed"));
            Assertions.assertThrows(UserException.class, manager::acquireSigningToken);
            Mockito.verify(pool).invalidateObject(address, client);
            Mockito.verify(pool, Mockito.never()).returnObject(address, client);
        } finally {
            ClientPool.frontendPool = previousPool;
        }
    }
}
