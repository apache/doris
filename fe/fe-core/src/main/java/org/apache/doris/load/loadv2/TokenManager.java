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
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.CustomThreadFactory;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TMySqlLoadAcquireTokenResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.EvictingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TokenManager {
    private static final Logger LOG = LogManager.getLogger(TokenManager.class);

    private  int thriftTimeoutMs = 300 * 1000;
    private  EvictingQueue<String> tokenQueue;
    private  String latestToken;
    private  ScheduledExecutorService tokenGenerator;

    public TokenManager() {
    }

    public void start() {
        this.tokenQueue = EvictingQueue.create(Config.token_queue_size);
        // init one token to avoid async issue.
        this.addNewToken(generateNewToken());
        this.tokenGenerator = Executors.newScheduledThreadPool(1,
                new CustomThreadFactory("token-generator"));
        this.tokenGenerator.scheduleAtFixedRate(() -> this.addNewToken(generateNewToken()), 0,
                Config.token_generate_period_hour, TimeUnit.HOURS);
    }

    private void addNewToken(String token) {
        tokenQueue.offer(token);
        latestToken = token;
    }

    private String generateNewToken() {
        return UUID.randomUUID().toString();
    }

    public String acquireToken() throws UserException {
        if (Env.getCurrentEnv().isMaster() || FeConstants.runningUnitTest) {
            return latestToken;
        } else {
            try {
                return acquireTokenFromMaster();
            } catch (TException e) {
                LOG.warn("acquire token error", e);
                throw new UserException("Acquire token from master failed", e);
            }
        }
    }

    private String acquireTokenFromMaster() throws TException {
        TNetworkAddress thriftAddress = getMasterAddress();
        FrontendService.Client client = getClient(thriftAddress);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Send acquire token to Master {}", thriftAddress);
        }

        boolean isReturnToPool = false;
        try {
            TMySqlLoadAcquireTokenResult result = client.acquireToken();
            isReturnToPool = true;
            if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                throw new TException("get acquire token failed.");
            }
            return result.getToken();
        } catch (TTransportException e) {
            boolean ok = ClientPool.frontendPool.reopen(client, thriftTimeoutMs);
            if (!ok) {
                throw e;
            }
            if (e.getType() == TTransportException.TIMED_OUT) {
                throw e;
            } else {
                TMySqlLoadAcquireTokenResult result = client.acquireToken();
                if (result.getStatus().getStatusCode() != TStatusCode.OK) {
                    throw new TException("acquire token from master failed. " + result.getStatus());
                }
                isReturnToPool = true;
                return result.getToken();
            }
        } finally {
            if (isReturnToPool) {
                ClientPool.frontendPool.returnObject(thriftAddress, client);
            } else {
                ClientPool.frontendPool.invalidateObject(thriftAddress, client);
            }
        }
    }

    /**
     * Check if the token is valid.
     * If this is not Master FE, will send the request to Master FE.
     */
    public boolean checkAuthToken(String token) throws UserException {
        if (Env.getCurrentEnv().isMaster() || FeConstants.runningUnitTest) {
            return tokenQueue.contains(token);
        } else {
            try {
                return checkTokenFromMaster(token);
            } catch (TException e) {
                LOG.warn("check token error", e);
                throw new UserException("Check token from master failed", e);
            }
        }
    }

    private boolean checkTokenFromMaster(String token) throws TException {
        TNetworkAddress thriftAddress = getMasterAddress();
        FrontendService.Client client = getClient(thriftAddress);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Send check token to Master {}", thriftAddress);
        }

        boolean isReturnToPool = false;
        try {
            boolean result = client.checkToken(token);
            isReturnToPool = true;
            return result;
        } catch (TTransportException e) {
            boolean ok = ClientPool.frontendPool.reopen(client, thriftTimeoutMs);
            if (!ok) {
                throw e;
            }
            if (e.getType() == TTransportException.TIMED_OUT) {
                throw e;
            } else {
                boolean result = client.checkToken(token);
                isReturnToPool = true;
                return result;
            }
        } finally {
            if (isReturnToPool) {
                ClientPool.frontendPool.returnObject(thriftAddress, client);
            } else {
                ClientPool.frontendPool.invalidateObject(thriftAddress, client);
            }
        }
    }


    private TNetworkAddress getMasterAddress() throws TException {
        Env.getCurrentEnv().checkReadyOrThrowTException();
        String masterHost = Env.getCurrentEnv().getMasterHost();
        int masterRpcPort = Env.getCurrentEnv().getMasterRpcPort();
        return new TNetworkAddress(masterHost, masterRpcPort);
    }

    private FrontendService.Client getClient(TNetworkAddress thriftAddress) throws TException {
        try {
            return ClientPool.frontendPool.borrowObject(thriftAddress, thriftTimeoutMs);
        } catch (Exception e) {
            // may throw NullPointerException. add err msg
            throw new TException("Failed to get master client.", e);
        }
    }
}
