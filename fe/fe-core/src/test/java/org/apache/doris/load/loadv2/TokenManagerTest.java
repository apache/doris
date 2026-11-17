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
    public void testAuthQueueInitializationRotationAndSnapshot() throws UserException {
        Config.token_queue_size = 4;
        TokenManager manager = startManager();
        List<String> initial = manager.getAuthTokens();
        Assertions.assertEquals(2, initial.size());
        Assertions.assertNotEquals(initial.get(0), initial.get(1));
        Assertions.assertEquals(initial, manager.acquireAuthTokens());
        Assertions.assertEquals(initial.get(0), manager.acquireSigningToken());
        Assertions.assertEquals(initial.get(1), manager.acquireToken());

        Deencapsulation.invoke(manager, "addNewToken", "third-token");
        Assertions.assertEquals(List.of(initial.get(0), initial.get(1), "third-token"),
                manager.acquireAuthTokens());
        Assertions.assertEquals("third-token", manager.acquireToken());
        Assertions.assertEquals(initial.get(1), manager.acquireSigningToken());
        Assertions.assertTrue(manager.checkAuthToken(initial.get(0)));
        Assertions.assertEquals(2, initial.size());

        Deencapsulation.invoke(manager, "addNewToken", "fourth-token");
        Assertions.assertEquals(List.of(initial.get(0), initial.get(1), "third-token", "fourth-token"),
                manager.getAuthTokens());
        Assertions.assertEquals(manager.getAuthTokens(), manager.acquireAuthTokens());
        Assertions.assertEquals("third-token", manager.acquireSigningToken());

        Deencapsulation.invoke(manager, "addNewToken", "fifth-token");
        Assertions.assertFalse(manager.checkAuthToken(initial.get(0)));
        Assertions.assertTrue(manager.checkAuthToken("third-token"));
        Deencapsulation.invoke(manager, "addNewToken", "sixth-token");
        Assertions.assertTrue(manager.checkAuthToken("third-token"));
        Deencapsulation.invoke(manager, "addNewToken", "seventh-token");
        Assertions.assertFalse(manager.checkAuthToken("third-token"));
        manager.getAuthTokens().clear();
        Assertions.assertEquals(4, manager.getAuthTokens().size());
    }

    @Test
    public void testMinimumQueueCapacity() {
        for (int configuredSize : new int[] {-1, 0, 1, 2, 3, 4, 6}) {
            Config.token_queue_size = configuredSize;
            TokenManager manager = startManager();
            for (int i = 0; i < 8; i++) {
                Deencapsulation.invoke(manager, "addNewToken", "token-" + i);
            }
            List<String> expected = configuredSize <= 4
                    ? List.of("token-4", "token-5", "token-6", "token-7")
                    : List.of("token-2", "token-3", "token-4", "token-5", "token-6", "token-7");
            Assertions.assertEquals(expected, manager.getAuthTokens(), "configured capacity: " + configuredSize);
        }
    }

    @Test
    public void testRestartWithoutCachedAuthTokens() throws UserException {
        TokenManager manager = startManager();
        List<String> previousTokens = manager.getAuthTokens();
        ScheduledExecutorService previousGenerator = Deencapsulation.getField(manager, "tokenGenerator");
        manager.start();
        Assertions.assertTrue(previousGenerator.isShutdown());
        List<String> tokens = manager.getAuthTokens();
        Assertions.assertEquals(2, tokens.size());
        Assertions.assertFalse(previousTokens.contains(tokens.get(0)));
        Assertions.assertFalse(previousTokens.contains(tokens.get(1)));
        Assertions.assertEquals(tokens, manager.acquireAuthTokens());
        Assertions.assertEquals(tokens.get(1), manager.acquireToken());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFollowerAuthTokensDuringMasterUpgrade() throws Exception {
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
            TokenManager manager = startManager();
            TMySqlLoadAcquireTokenResult result = new TMySqlLoadAcquireTokenResult();
            result.setStatus(new TStatus(TStatusCode.OK));
            result.setToken("latest-token");
            Mockito.when(client.acquireToken()).thenReturn(result);
            // An old master only serializes token.
            Assertions.assertEquals("latest-token", manager.acquireSigningToken());
            TokenManager legacyManager = startManager();
            Assertions.assertEquals(List.of("latest-token"), legacyManager.acquireAuthTokens());
            List<String> received = List.of("older-a", "older-b", "older-c", "older-d",
                    "signing-token", "latest-token");
            List<String> mutableReceived = new ArrayList<>(received);
            result.setAuthTokens(mutableReceived);
            Assertions.assertEquals("signing-token", manager.acquireSigningToken());
            mutableReceived.clear();
            Assertions.assertEquals("latest-token", manager.acquireToken());
            Mockito.verify(pool, Mockito.times(4)).returnObject(address, client);

            // A present but empty queue must not silently fall back to the legacy token.
            result.setAuthTokens(List.of());
            Assertions.assertThrows(UserException.class, manager::acquireAuthTokens);
            result.setAuthTokens(List.of("", "latest-token"));
            Assertions.assertThrows(UserException.class, manager::acquireAuthTokens);
            result.unsetAuthTokens();
            result.unsetToken();
            Assertions.assertThrows(UserException.class, manager::acquireAuthTokens);
            result.setToken("");
            Assertions.assertThrows(UserException.class, manager::acquireAuthTokens);

            result.setToken("latest-token");
            result.setStatus(new TStatus(TStatusCode.NOT_MASTER));
            Assertions.assertThrows(UserException.class, manager::acquireAuthTokens);
            Mockito.clearInvocations(pool);
            Mockito.when(client.acquireToken()).thenThrow(new TException("RPC failed"));
            Assertions.assertThrows(UserException.class, manager::acquireAuthTokens);
            Mockito.verify(pool).invalidateObject(address, client);
            Mockito.verify(pool, Mockito.never()).returnObject(address, client);

            // Promotion must retain the complete last successful queue despite subsequent failures.
            Mockito.when(env.isMaster()).thenReturn(true);
            Config.token_queue_size = 4;
            Object previousQueue = Deencapsulation.getField(manager, "tokenQueue");
            ScheduledExecutorService previousGenerator = Deencapsulation.getField(manager, "tokenGenerator");
            manager.start();
            Assertions.assertTrue(previousGenerator.isShutdown());
            List<String> tokens = manager.getAuthTokens();
            Assertions.assertEquals(received, tokens);
            Assertions.assertEquals(received, manager.acquireAuthTokens());
            Assertions.assertEquals("latest-token", manager.acquireToken());

            // Simulate an old scheduled task that was waiting for start() to release the lock.
            Deencapsulation.invoke(manager, "rotateToken", previousQueue);
            Assertions.assertEquals(tokens, manager.getAuthTokens());
            Object currentQueue = Deencapsulation.getField(manager, "tokenQueue");
            Deencapsulation.invoke(manager, "rotateToken", currentQueue);
            Assertions.assertEquals("latest-token", manager.acquireAuthTokens().get(4));

            legacyManager.start();
            Assertions.assertEquals(2, legacyManager.acquireAuthTokens().size());
            Assertions.assertEquals("latest-token", legacyManager.getAuthTokens().get(0));

            // A token RPC started as a follower can finish after start() rebuilt the queue.
            Mockito.when(env.isMaster()).thenReturn(false);
            result.setStatus(new TStatus(TStatusCode.OK));
            result.setAuthTokens(List.of("remote-old", "remote-signing", "remote-current"));
            Mockito.doAnswer(invocation -> {
                Mockito.when(env.isMaster()).thenReturn(true);
                manager.start();
                return result;
            }).when(client).acquireToken();
            Assertions.assertEquals(received, manager.acquireAuthTokens());
            Assertions.assertEquals(received, manager.getAuthTokens());
        } finally {
            ClientPool.frontendPool = previousPool;
        }
    }
}
