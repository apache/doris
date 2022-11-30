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

package org.apache.doris.system;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.Version;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.persist.HbPackage;
import org.apache.doris.resource.Tag;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.HeartbeatResponse.HbStatus;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.HeartbeatService;
import org.apache.doris.thrift.TBackendInfo;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerPingBrokerRequest;
import org.apache.doris.thrift.TBrokerVersion;
import org.apache.doris.thrift.TFrontendPingFrontendRequest;
import org.apache.doris.thrift.TFrontendPingFrontendResult;
import org.apache.doris.thrift.TFrontendPingFrontendStatusCode;
import org.apache.doris.thrift.THeartbeatResult;
import org.apache.doris.thrift.TMasterInfo;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Heartbeat manager run as a daemon at a fix interval.
 * For now, it will send heartbeat to all Frontends, Backends and Brokers
 */
public class HeartbeatMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(HeartbeatMgr.class);

    private final ExecutorService executor;
    private SystemInfoService nodeMgr;
    private HeartbeatFlags heartbeatFlags;

    private static volatile AtomicReference<TMasterInfo> masterInfo = new AtomicReference<>();

    public HeartbeatMgr(SystemInfoService nodeMgr, boolean needRegisterMetric) {
        super("heartbeat mgr", FeConstants.heartbeat_interval_second * 1000);
        this.nodeMgr = nodeMgr;
        this.executor = ThreadPoolManager.newDaemonFixedThreadPool(Config.heartbeat_mgr_threads_num,
                Config.heartbeat_mgr_blocking_queue_size, "heartbeat-mgr-pool", needRegisterMetric);
        this.heartbeatFlags = new HeartbeatFlags();
    }

    public void setMaster(int clusterId, String token, long epoch) {
        TMasterInfo tMasterInfo = new TMasterInfo(
                new TNetworkAddress(FrontendOptions.getLocalHostAddress(), Config.rpc_port), clusterId, epoch);
        tMasterInfo.setToken(token);
        tMasterInfo.setHttpPort(Config.http_port);
        long flags = heartbeatFlags.getHeartbeatFlags();
        tMasterInfo.setHeartbeatFlags(flags);
        masterInfo.set(tMasterInfo);
    }

    /**
     * At each round:
     * 1. send heartbeat to all nodes
     * 2. collect the heartbeat response from all nodes, and handle them
     */
    @Override
    protected void runAfterCatalogReady() {
        List<Future<HeartbeatResponse>> hbResponses = Lists.newArrayList();

        // send backend heartbeat
        for (Backend backend : nodeMgr.getIdToBackend().values()) {
            BackendHeartbeatHandler handler = new BackendHeartbeatHandler(backend);
            hbResponses.add(executor.submit(handler));
        }

        // send frontend heartbeat
        List<Frontend> frontends = Env.getCurrentEnv().getFrontends(null);
        for (Frontend frontend : frontends) {
            FrontendHeartbeatHandler handler = new FrontendHeartbeatHandler(frontend,
                    Env.getCurrentEnv().getClusterId(),
                    Env.getCurrentEnv().getToken());
            hbResponses.add(executor.submit(handler));
        }

        // send broker heartbeat;
        Map<String, List<FsBroker>> brokerMap = Maps.newHashMap(
                Env.getCurrentEnv().getBrokerMgr().getBrokerListMap());
        for (Map.Entry<String, List<FsBroker>> entry : brokerMap.entrySet()) {
            for (FsBroker brokerAddress : entry.getValue()) {
                BrokerHeartbeatHandler handler = new BrokerHeartbeatHandler(entry.getKey(), brokerAddress,
                        masterInfo.get().getNetworkAddress().getHostname());
                hbResponses.add(executor.submit(handler));
            }
        }

        // collect all heartbeat responses and handle them.
        // and also we find which node's info is changed, if is changed, we need collect them and write
        // an edit log to synchronize the info to other Frontends
        HbPackage hbPackage = new HbPackage();
        for (Future<HeartbeatResponse> future : hbResponses) {
            boolean isChanged = false;
            try {
                // the heartbeat rpc's timeout is 5 seconds, so we will not be blocked here very long.
                HeartbeatResponse response = future.get();
                if (response.getStatus() != HbStatus.OK) {
                    LOG.warn("get bad heartbeat response: {}", response);
                }
                isChanged = handleHbResponse(response, false);

                if (isChanged) {
                    hbPackage.addHbResponse(response);
                }
            } catch (InterruptedException | ExecutionException e) {
                LOG.warn("got exception when doing heartbeat", e);
            }
        } // end for all results

        Env.getCurrentEnv().getEditLog().logHeartbeat(hbPackage);
    }

    private boolean handleHbResponse(HeartbeatResponse response, boolean isReplay) {
        switch (response.getType()) {
            case FRONTEND: {
                FrontendHbResponse hbResponse = (FrontendHbResponse) response;
                Frontend fe = Env.getCurrentEnv().getFeByName(hbResponse.getName());
                if (fe != null) {
                    return fe.handleHbResponse(hbResponse, isReplay);
                }
                break;
            }
            case BACKEND: {
                BackendHbResponse hbResponse = (BackendHbResponse) response;
                Backend be = nodeMgr.getBackend(hbResponse.getBeId());
                if (be != null) {
                    boolean isChanged = be.handleHbResponse(hbResponse);
                    if (hbResponse.getStatus() != HbStatus.OK) {
                        // invalid all connections cached in ClientPool
                        ClientPool.backendPool.clearPool(new TNetworkAddress(be.getHost(), be.getBePort()));
                        if (!isReplay) {
                            Env.getCurrentEnv().getGlobalTransactionMgr()
                                    .abortTxnWhenCoordinateBeDown(be.getHost(), 100);
                        }
                    }
                    return isChanged;
                }
                break;
            }
            case BROKER: {
                BrokerHbResponse hbResponse = (BrokerHbResponse) response;
                FsBroker broker = Env.getCurrentEnv().getBrokerMgr().getBroker(
                        hbResponse.getName(), hbResponse.getHost(), hbResponse.getPort());
                if (broker != null) {
                    boolean isChanged = broker.handleHbResponse(hbResponse);
                    if (hbResponse.getStatus() != HbStatus.OK) {
                        // invalid all connections cached in ClientPool
                        ClientPool.brokerPool.clearPool(new TNetworkAddress(broker.ip, broker.port));
                    }
                    return isChanged;
                }
                break;
            }
            default:
                break;
        }
        return false;
    }

    // backend heartbeat
    private class BackendHeartbeatHandler implements Callable<HeartbeatResponse> {
        private Backend backend;

        public BackendHeartbeatHandler(Backend backend) {
            this.backend = backend;
        }

        @Override
        public HeartbeatResponse call() {
            long backendId = backend.getId();
            HeartbeatService.Client client = null;

            TNetworkAddress beAddr = new TNetworkAddress(backend.getHost(), backend.getHeartbeatPort());
            boolean ok = false;
            try {
                TMasterInfo copiedMasterInfo = new TMasterInfo(masterInfo.get());
                copiedMasterInfo.setBackendIp(backend.getHost());
                long flags = heartbeatFlags.getHeartbeatFlags();
                copiedMasterInfo.setHeartbeatFlags(flags);
                copiedMasterInfo.setBackendId(backendId);
                THeartbeatResult result;
                if (!FeConstants.runningUnitTest) {
                    client = ClientPool.backendHeartbeatPool.borrowObject(beAddr);
                    result = client.heartbeat(copiedMasterInfo);
                } else {
                    // Mocked result
                    TBackendInfo backendInfo = new TBackendInfo();
                    backendInfo.setBePort(1);
                    backendInfo.setHttpPort(2);
                    backendInfo.setBeRpcPort(3);
                    backendInfo.setBrpcPort(4);
                    backendInfo.setVersion("test-1234");
                    result = new THeartbeatResult();
                    result.setStatus(new TStatus(TStatusCode.OK));
                    result.setBackendInfo(backendInfo);
                }

                ok = true;
                if (result.getStatus().getStatusCode() == TStatusCode.OK) {
                    TBackendInfo tBackendInfo = result.getBackendInfo();
                    int bePort = tBackendInfo.getBePort();
                    int httpPort = tBackendInfo.getHttpPort();
                    int brpcPort = -1;
                    if (tBackendInfo.isSetBrpcPort()) {
                        brpcPort = tBackendInfo.getBrpcPort();
                    }
                    String version = "";
                    if (tBackendInfo.isSetVersion()) {
                        version = tBackendInfo.getVersion();
                    }
                    long beStartTime = tBackendInfo.isSetBeStartTime()
                            ? tBackendInfo.getBeStartTime() : System.currentTimeMillis();
                    // backend.updateOnce(bePort, httpPort, beRpcPort, brpcPort);
                    String nodeRole = Tag.VALUE_MIX;
                    if (tBackendInfo.isSetBeNodeRole()) {
                        nodeRole = tBackendInfo.getBeNodeRole();
                    }
                    return new BackendHbResponse(backendId, bePort, httpPort, brpcPort,
                            System.currentTimeMillis(), beStartTime, version, nodeRole);
                } else {
                    return new BackendHbResponse(backendId, backend.getHost(),
                            result.getStatus().getErrorMsgs().isEmpty()
                                    ? "Unknown error" : result.getStatus().getErrorMsgs().get(0));
                }
            } catch (Exception e) {
                LOG.warn("backend heartbeat got exception", e);
                return new BackendHbResponse(backendId, backend.getHost(),
                        Strings.isNullOrEmpty(e.getMessage()) ? "got exception" : e.getMessage());
            } finally {
                if (client != null) {
                    if (ok) {
                        ClientPool.backendHeartbeatPool.returnObject(beAddr, client);
                    } else {
                        ClientPool.backendHeartbeatPool.invalidateObject(beAddr, client);
                    }
                }
            }
        }
    }

    // frontend heartbeat
    public static class FrontendHeartbeatHandler implements Callable<HeartbeatResponse> {
        private Frontend fe;
        private int clusterId;
        private String token;

        public FrontendHeartbeatHandler(Frontend fe, int clusterId, String token) {
            this.fe = fe;
            this.clusterId = clusterId;
            this.token = token;
        }

        @Override
        public HeartbeatResponse call() {
            if (fe.getHost().equals(Env.getCurrentEnv().getSelfNode().first)) {
                // heartbeat to self
                if (Env.getCurrentEnv().isReady()) {
                    return new FrontendHbResponse(fe.getNodeName(), Config.query_port, Config.rpc_port,
                            Env.getCurrentEnv().getMaxJournalId(), System.currentTimeMillis(),
                            Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH);
                } else {
                    return new FrontendHbResponse(fe.getNodeName(), "not ready");
                }
            }

            return getHeartbeatResponse();
        }

        private HeartbeatResponse getHeartbeatResponse() {
            FrontendService.Client client = null;
            TNetworkAddress addr = new TNetworkAddress(fe.getHost(), Config.rpc_port);
            boolean ok = false;
            try {
                client = ClientPool.frontendHeartbeatPool.borrowObject(addr);
                TFrontendPingFrontendRequest request = new TFrontendPingFrontendRequest(clusterId, token);
                TFrontendPingFrontendResult result = client.ping(request);
                ok = true;
                if (result.getStatus() == TFrontendPingFrontendStatusCode.OK) {
                    return new FrontendHbResponse(fe.getNodeName(), result.getQueryPort(),
                            result.getRpcPort(), result.getReplayedJournalId(),
                            System.currentTimeMillis(), result.getVersion());

                } else {
                    return new FrontendHbResponse(fe.getNodeName(), result.getMsg());
                }
            } catch (Exception e) {
                return new FrontendHbResponse(fe.getNodeName(),
                        Strings.isNullOrEmpty(e.getMessage()) ? "got exception" : e.getMessage());
            } finally {
                if (ok) {
                    ClientPool.frontendHeartbeatPool.returnObject(addr, client);
                } else {
                    ClientPool.frontendHeartbeatPool.invalidateObject(addr, client);
                }
            }
        }
    }

    // broker heartbeat handler
    public static class BrokerHeartbeatHandler implements Callable<HeartbeatResponse> {
        private String brokerName;
        private FsBroker broker;
        private String clientId;

        public BrokerHeartbeatHandler(String brokerName, FsBroker broker, String clientId) {
            this.brokerName = brokerName;
            this.broker = broker;
            this.clientId = clientId;
        }

        @Override
        public HeartbeatResponse call() {
            TPaloBrokerService.Client client = null;
            TNetworkAddress addr = new TNetworkAddress(broker.ip, broker.port);
            boolean ok = false;
            try {
                client = ClientPool.brokerPool.borrowObject(addr);
                TBrokerPingBrokerRequest request = new TBrokerPingBrokerRequest(TBrokerVersion.VERSION_ONE,
                        clientId);
                TBrokerOperationStatus status = client.ping(request);
                ok = true;

                if (status.getStatusCode() != TBrokerOperationStatusCode.OK) {
                    return new BrokerHbResponse(brokerName, broker.ip, broker.port, status.getMessage());
                } else {
                    return new BrokerHbResponse(brokerName, broker.ip, broker.port, System.currentTimeMillis());
                }

            } catch (Exception e) {
                return new BrokerHbResponse(brokerName, broker.ip, broker.port,
                        Strings.isNullOrEmpty(e.getMessage()) ? "got exception" : e.getMessage());
            } finally {
                if (ok) {
                    ClientPool.brokerPool.returnObject(addr, client);
                } else {
                    ClientPool.brokerPool.invalidateObject(addr, client);
                }
            }
        }
    }

    public void replayHearbeat(HbPackage hbPackage) {
        for (HeartbeatResponse hbResult : hbPackage.getHbResults()) {
            handleHbResponse(hbResult, true);
        }
    }

}
