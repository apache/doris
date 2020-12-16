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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.Version;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.Util;
import org.apache.doris.http.rest.BootstrapFinishAction;
import org.apache.doris.persist.HbPackage;
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
import org.apache.doris.thrift.TStatusCode;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

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
        List<Frontend> frontends = Catalog.getCurrentCatalog().getFrontends(null);
        for (Frontend frontend : frontends) {
            FrontendHeartbeatHandler handler = new FrontendHeartbeatHandler(frontend,
                    Catalog.getCurrentCatalog().getClusterId(),
                    Catalog.getCurrentCatalog().getToken());
            hbResponses.add(executor.submit(handler));
        }

        // send broker heartbeat;
        Map<String, List<FsBroker>> brokerMap = Maps.newHashMap(
                Catalog.getCurrentCatalog().getBrokerMgr().getBrokerListMap());
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
                continue;
            }
        } // end for all results

        Catalog.getCurrentCatalog().getEditLog().logHeartbeat(hbPackage);
    }

    private boolean handleHbResponse(HeartbeatResponse response, boolean isReplay) {
        switch (response.getType()) {
            case FRONTEND: {
                FrontendHbResponse hbResponse = (FrontendHbResponse) response;
                Frontend fe = Catalog.getCurrentCatalog().getFeByName(hbResponse.getName());
                if (fe != null) {
                    return fe.handleHbResponse(hbResponse);
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
                            Catalog.getCurrentCatalog().getGlobalTransactionMgr().abortTxnWhenCoordinateBeDown(be.getHost(), 100);
                        }
                    }
                    return isChanged;
                }
                break;
            }
            case BROKER: {
                BrokerHbResponse hbResponse = (BrokerHbResponse) response;
                FsBroker broker = Catalog.getCurrentCatalog().getBrokerMgr().getBroker(
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
                client = ClientPool.backendHeartbeatPool.borrowObject(beAddr);

                TMasterInfo copiedMasterInfo = new TMasterInfo(masterInfo.get());
                copiedMasterInfo.setBackendIp(backend.getHost());
                long flags = heartbeatFlags.getHeartbeatFlags();
                copiedMasterInfo.setHeartbeatFlags(flags);
                copiedMasterInfo.setBackendId(backendId);
                THeartbeatResult result = client.heartbeat(copiedMasterInfo);

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
                    long beStartTime = tBackendInfo.isSetBeStartTime() ? tBackendInfo.getBeStartTime() : System.currentTimeMillis();
                    // backend.updateOnce(bePort, httpPort, beRpcPort, brpcPort);
                    return new BackendHbResponse(backendId, bePort, httpPort, brpcPort, System.currentTimeMillis(), beStartTime, version);
                } else {
                    return new BackendHbResponse(backendId, result.getStatus().getErrorMsgs().isEmpty() ? "Unknown error"
                            : result.getStatus().getErrorMsgs().get(0));
                }
            } catch (Exception e) {
                LOG.warn("backend heartbeat got exception", e);
                return new BackendHbResponse(backendId,
                        Strings.isNullOrEmpty(e.getMessage()) ? "got exception" : e.getMessage());
            } finally {
                if (ok) {
                    ClientPool.backendHeartbeatPool.returnObject(beAddr, client);
                } else {
                    ClientPool.backendHeartbeatPool.invalidateObject(beAddr, client);
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
            if (fe.getHost().equals(Catalog.getCurrentCatalog().getSelfNode().first)) {
                // heartbeat to self
                if (Catalog.getCurrentCatalog().isReady()) {
                    return new FrontendHbResponse(fe.getNodeName(), Config.query_port, Config.rpc_port,
                            Catalog.getCurrentCatalog().getMaxJournalId(), System.currentTimeMillis(),
                            Version.DORIS_BUILD_VERSION + "-" + Version.DORIS_BUILD_SHORT_HASH);
                } else {
                    return new FrontendHbResponse(fe.getNodeName(), "not ready");
                }
            }
            if (Config.enable_fe_heartbeat_by_thrift) {
                return getHeartbeatResponseByThrift();
            } else {
                return getHeartbeatResponseByHttp();
            }
        }

        private HeartbeatResponse getHeartbeatResponseByHttp() {
            String url = "http://" + fe.getHost() + ":" + Config.http_port
                    + "/api/bootstrap?cluster_id=" + clusterId + "&token=" + token;
            try {
                String result = Util.getResultForUrl(url, null, 2000, 2000);
                /*
                 * Old return:
                 * {"replayedJournalId":191224,"queryPort":9131,"rpcPort":9121,"status":"OK","msg":"Success"}
                 * {"replayedJournalId":0,"queryPort":0,"rpcPort":0,"status":"FAILED","msg":"not ready"}
                 *
                 * New return:
                 * {"msg":"success","code":0,"data":{"queryPort":9333,"rpcPort":9222,"replayedJournalId":197},"count":0}
                 * {"msg":"not ready","code":1,"data":null,"count":0}
                 */
                JSONObject root = new JSONObject(result);
                if (root.has("status")) {
                    // old return
                    String status = root.getString("status");
                    if (!"OK".equals(status)) {
                        return new FrontendHbResponse(fe.getNodeName(), root.getString("msg"));
                    } else {
                        long replayedJournalId = root.getLong(BootstrapFinishAction.REPLAYED_JOURNAL_ID);
                        int queryPort = root.getInt(BootstrapFinishAction.QUERY_PORT);
                        int rpcPort = root.getInt(BootstrapFinishAction.RPC_PORT);
                        String version = root.getString(BootstrapFinishAction.VERSION);
                        return new FrontendHbResponse(fe.getNodeName(), queryPort, rpcPort, replayedJournalId,
                                System.currentTimeMillis(), version == null ? "unknown" : version);
                    }
                } else if (root.has("code")) {
                    // new return
                    int code = root.getInt("code");
                    if (code != 0) {
                        return new FrontendHbResponse(fe.getNodeName(), root.getString("msg"));
                    } else {
                        JSONObject dataObj = root.getJSONObject("data");
                        long replayedJournalId = dataObj.getLong(BootstrapFinishAction.REPLAYED_JOURNAL_ID);
                        int queryPort = dataObj.getInt(BootstrapFinishAction.QUERY_PORT);
                        int rpcPort = dataObj.getInt(BootstrapFinishAction.RPC_PORT);
                        // TODO(wb) support new return for version here
                        return new FrontendHbResponse(fe.getNodeName(), queryPort, rpcPort, replayedJournalId,
                                System.currentTimeMillis(), "unknown");
                    }
                } else {
                    throw new Exception("invalid return value: " + result);
                }
            } catch (Exception e) {
                return new FrontendHbResponse(fe.getNodeName(),
                        Strings.isNullOrEmpty(e.getMessage()) ? "got exception" : e.getMessage());
            }
        }

        private HeartbeatResponse getHeartbeatResponseByThrift() {
            FrontendService.Client client = null;
            TNetworkAddress addr = new TNetworkAddress(fe.getHost(), fe.getRpcPort());
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

