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
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TFrontendReportAliveSessionRequest;
import org.apache.doris.thrift.TFrontendReportAliveSessionResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutorService;

/*
 * FESessionMgr is for collecting alive sessions from frontends.
 * Only run on master FE.
 */
public class FESessionMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(FESessionMgr.class);

    private final ExecutorService executor;

    private int clusterId;

    private String token;

    public FESessionMgr() {
        super("fe-session-mgr", Config.alive_session_update_interval_second * 1000);
        this.executor = ThreadPoolManager.newDaemonFixedThreadPool(Config.fe_session_mgr_threads_num,
            Config.fe_session_mgr_blocking_queue_size, "all-fe-session-mgr-pool", false);
    }

    @Override
    protected void runAfterCatalogReady() {
        List<Frontend> frontends = Env.getCurrentEnv().getFrontends(null);
        for (Frontend frontend : frontends) {
            if (!frontend.isAlive()) {
                continue;
            }
            FEAliveSessionHandler handler = new FEAliveSessionHandler(frontend, clusterId, token);
            executor.submit(handler);
        }
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public void setToken(String token) {
        this.token = token;
    }

    private class FEAliveSessionHandler implements Runnable {
        private final Frontend fe;
        private final int clusterId;
        private final String token;

        public FEAliveSessionHandler(Frontend fe, int clusterId, String token) {
            this.fe = fe;
            this.clusterId = clusterId;
            this.token = token;
        }

        @Override
        public void run() {
            Env env = Env.getCurrentEnv();
            HostInfo selfNode = env.getSelfNode();
            if (fe.getHost().equals(selfNode.getHost())) {
                if (env.isReady()) {
                    List<String> sessionIds = env.getAllAliveSessionIds();
                    for (String sessionId : sessionIds) {
                        Env.getCurrentEnv().checkAndRefreshSession(sessionId);
                    }
                } else {
                    LOG.info("Master FE is not ready");
                }
            } else {
                getAliveSessionAndRefresh();
            }
        }

        private void getAliveSessionAndRefresh() {
            FrontendService.Client client = null;
            TNetworkAddress addr = new TNetworkAddress(fe.getHost(), fe.getRpcPort());
            boolean ok = false;
            try {
                client = ClientPool.frontendPool.borrowObject(addr);
                TFrontendReportAliveSessionRequest request = new TFrontendReportAliveSessionRequest(clusterId, token);
                TFrontendReportAliveSessionResult result = client.getAliveSessions(request);
                ok = true;
                if (result.getStatus() == TStatusCode.OK) {
                    List<String> sessionIds = result.getSessionIdList();
                    for (String sessionId : sessionIds) {
                        Env.getCurrentEnv().checkAndRefreshSession(sessionId);
                    }
                } else {
                    LOG.warn("Error occurred when get alive session from " + fe.getHost()
                            + ", msg = " + result.getMsg());
                }
            } catch (Exception e) {
                LOG.warn("Error occurred when get alive session from " + fe.getHost()
                        + ", msg = " + e.getMessage());
            } finally {
                if (ok) {
                    ClientPool.frontendPool.returnObject(addr, client);
                } else {
                    ClientPool.frontendPool.invalidateObject(addr, client);
                }
            }
        }
    }
}
