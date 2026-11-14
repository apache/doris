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

package org.apache.doris.catalog;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class is used to manage the token for internal authentication.
 * It will generate a new token every 12 hours(Config.token_generate_period_hour)
 * and keep at most 6 tokens(Config.token_queue_size).
 * So each token will be valid for 3 days.
 * Only Master FE can generate a new token.
 */
public class TokenManager {
    private static final Logger LOG = LogManager.getLogger(TokenManager.class);

    private  int thriftTimeoutMs = 300 * 1000;
    private  EvictingQueue<String> tokenQueue;
    private volatile String latestToken;
    // Protected by this; retain an immutable copy across follower-to-master promotion.
    private List<String> lastAcquiredAuthTokens = List.of();
    private  ScheduledExecutorService tokenGenerator;

    public TokenManager() {
    }

    public synchronized void start() {
        int tokenQueueSize = Config.token_queue_size;
        // Retain signing keys for at least two rotation periods after their last use.
        if (tokenQueueSize < 4) {
            LOG.warn("token_queue_size is {}, using the minimum capacity of 4", tokenQueueSize);
            tokenQueueSize = 4;
        }
        // Promotion calls start() again on the same TokenManager.
        if (this.tokenGenerator != null) {
            this.tokenGenerator.shutdownNow();
        }
        // Preserve the entire cached queue even if this FE has a smaller configured capacity.
        this.tokenQueue = EvictingQueue.create(Math.max(tokenQueueSize, lastAcquiredAuthTokens.size()));
        for (String token : lastAcquiredAuthTokens) {
            this.addNewToken(token);
        }
        // An uncached follower needs two initial tokens; an old master may have supplied only one.
        // A complete cached queue is restored unchanged, preserving both signing and current keys.
        while (this.tokenQueue.size() < 2) {
            this.addNewToken(generateNewToken());
        }
        this.tokenGenerator = Executors.newScheduledThreadPool(1,
                new CustomThreadFactory("token-generator"));
        EvictingQueue<String> queue = this.tokenQueue;
        // Publish the initial queue before the first scheduled rotation.
        this.tokenGenerator.scheduleAtFixedRate(() -> rotateToken(queue),
                Config.token_generate_period_hour,
                Config.token_generate_period_hour, TimeUnit.HOURS);
    }

    private synchronized void rotateToken(EvictingQueue<String> queue) {
        // An old task may already be waiting for this lock when promotion stops its executor.
        // It must not rotate the newly initialized queue before its first heartbeat.
        if (queue != tokenQueue) {
            return;
        }
        addNewToken(generateNewToken());
    }

    private synchronized void addNewToken(String token) {
        tokenQueue.offer(token);
        latestToken = token;
    }

    // Called by the master's heartbeat sender. Oldest first, newest last; copy under the
    // rotation lock so both heartbeat fields can be populated from the same generation.
    public synchronized List<String> getAuthTokens() {
        return new ArrayList<>(tokenQueue);
    }

    private String generateNewToken() {
        return UUID.randomUUID().toString();
    }

    public String acquireToken() throws UserException {
        if (Env.getCurrentEnv().isMaster() || FeConstants.runningUnitTest) {
            return latestToken;
        } else {
            try {
                return acquireTokenFromMaster().getToken();
            } catch (TException e) {
                LOG.warn("acquire token error", e);
                throw new UserException("Acquire token from master failed", e);
            }
        }
    }

    public String acquireSigningToken() throws UserException {
        List<String> tokens = acquireAuthTokens();
        // Sign with the second newest token so BEs have time to receive it through heartbeats.
        // An old master returns only its current token during rolling upgrades.
        return tokens.get(Math.max(0, tokens.size() - 2));
    }

    public List<String> acquireAuthTokens() throws UserException {
        EvictingQueue<String> queue;
        synchronized (this) {
            if (Env.getCurrentEnv().isMaster() || FeConstants.runningUnitTest) {
                return getAuthTokens();
            }
            queue = tokenQueue;
        }
        try {
            TMySqlLoadAcquireTokenResult result = acquireTokenFromMaster();
            List<String> tokens;
            if (result.isSetAuthTokens()) {
                tokens = result.getAuthTokens();
            } else {
                // An old master supplies only its current token during rolling upgrades.
                tokens = Collections.singletonList(result.getToken());
            }
            // A present but empty queue or key is an error, not a legacy response.
            if (tokens.isEmpty()) {
                throw new UserException("Auth tokens are not available from master");
            }
            for (String token : tokens) {
                if (token == null || token.isEmpty()) {
                    throw new UserException("Auth tokens are not available from master");
                }
            }
            tokens = List.copyOf(tokens);
            synchronized (this) {
                // A successful RPC may return after promotion rebuilt the queue. Use that
                // queue instead of issuing tickets with keys absent from the new heartbeats.
                if (queue != tokenQueue) {
                    return getAuthTokens();
                }
                lastAcquiredAuthTokens = tokens;
                return tokens;
            }
        } catch (TException e) {
            LOG.warn("acquire auth tokens error", e);
            throw new UserException("Acquire auth tokens from master failed", e);
        }
    }

    private TMySqlLoadAcquireTokenResult acquireTokenFromMaster() throws TException {
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
            return result;
        } catch (TTransportException e) {
            boolean ok = ClientPool.frontendPool.reopenOrClear(thriftAddress, client, thriftTimeoutMs);
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

    /**
     * Check if the token is valid.
     * If this is not Master FE, will send the request to Master FE.
     */
    public boolean checkAuthToken(String token) throws UserException {
        if (Env.getCurrentEnv().isMaster() || FeConstants.runningUnitTest) {
            synchronized (this) {
                return tokenQueue.contains(token);
            }
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
            boolean ok = ClientPool.frontendPool.reopenOrClear(thriftAddress, client, thriftTimeoutMs);
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
