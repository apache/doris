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
import org.apache.doris.common.Config;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterTxnExecutor;

import com.google.common.collect.EvictingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TokenManager {
    private static final Logger LOG = LogManager.getLogger(TokenManager.class);

    private final EvictingQueue<String> tokenQueue;
    private final ScheduledExecutorService tokenGenerator;

    public TokenManager() {
        this.tokenQueue = EvictingQueue.create(Config.token_queue_size);
        this.tokenGenerator = Executors.newScheduledThreadPool(1);
        this.tokenGenerator.scheduleAtFixedRate(() -> {
            tokenQueue.offer(UUID.randomUUID().toString());
        }, 1, Config.token_generate_period_hour, TimeUnit.HOURS);
    }

    // this method only will be called in master node, since stream load only send message to master.
    public boolean checkAuthToken(String token) {
        return tokenQueue.contains(token);
    }

    // context only use in no master branch.
    public String acquireToken(ConnectContext context) {
        if (Env.getCurrentEnv().isMaster()) {
            return tokenQueue.peek();
        } else {
            MasterTxnExecutor masterTxnExecutor = new MasterTxnExecutor(context);
            try {
                return masterTxnExecutor.acquireToken();
            } catch (TException e) {
                LOG.warn("acquire token error", e);
                return null;
            }
        }
    }
}
