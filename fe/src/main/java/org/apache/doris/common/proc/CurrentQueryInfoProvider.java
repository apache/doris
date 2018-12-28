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

package org.apache.doris.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.rpc.*;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Provide running query's PlanNode informations, includeing execution State
 * , IO consumpation and CPU consumpation.
 */
public class CurrentQueryInfoProvider {
    private static final Logger LOG = LogManager.getLogger(CurrentQueryInfoProvider.class);

    public CurrentQueryInfoProvider() {
    }

    public QueryExecInfo getQueryExecInfoFromRemote(QueryStatisticsItem item) throws AnalysisException {
        return getQueryExecInfoFromRemote(Lists.newArrayList(item)).iterator().next();
    }

    public Collection<QueryExecInfo> getQueryExecInfoFromRemote(Collection<QueryStatisticsItem> items) throws AnalysisException {
        final Map<TNetworkAddress, Request> requestMap = Maps.newHashMap();
        final Map<PUniqueId, QueryExecInfo> queryExecInfoMap = Maps.newHashMap();
        final Map<TNetworkAddress, TNetworkAddress> brpcAddressMap = Maps.newHashMap();
        for (QueryStatisticsItem item : items) {
            final QueryExecInfo queryExecInfo = new QueryExecInfo(item.getQueryId());
            for (QueryStatisticsItem.FragmentInstanceInfo instanceInfo : item.getFragmentInstanceInfos()) {
                // use brpc address
                TNetworkAddress brpcNetAddress = brpcAddressMap.get(instanceInfo.getAddress());
                if (brpcNetAddress == null) {
                    try {
                        brpcNetAddress = toBrpcHost(instanceInfo.getAddress());
                        brpcAddressMap.put(instanceInfo.getAddress(), brpcNetAddress);
                    } catch (Exception e) {
                        LOG.warn(e.getMessage());
                        throw new AnalysisException(e.getMessage());
                    }
                }
                // merge different queries's requests
                Request request = requestMap.get(brpcNetAddress);
                if (request == null) {
                    request = new Request(brpcNetAddress);
                    requestMap.put(brpcNetAddress, request);
                }
                final PUniqueId pUId = new PUniqueId(instanceInfo.getInstanceId());
                request.addInstanceId(pUId);

                // map instance to QueryExecInfo and fragment to instance
                Preconditions.checkArgument(queryExecInfoMap.get(instanceInfo.getInstanceId()) == null);
                queryExecInfoMap.put(new PUniqueId(instanceInfo.getInstanceId()), queryExecInfo);
                queryExecInfo.addMappingInstanceToFragment(instanceInfo.getFragmentId(), pUId);
            }
        }
        return recvResponse(queryExecInfoMap, sendRequest(requestMap));
    }

    private List<Pair<Request, Future<PFetchFragmentExecInfosResult>>> sendRequest(
            Map<TNetworkAddress, Request> requestMap) throws AnalysisException {
        final List<Pair<Request, Future<PFetchFragmentExecInfosResult>>> futures = Lists.newArrayList();
        for (TNetworkAddress address : requestMap.keySet()) {
            final Request request = requestMap.get(address);
            final PFetchFragmentExecInfoRequest pbRequest =
                    new PFetchFragmentExecInfoRequest(request.getAllInstanceIds());
            try {
                futures.add(Pair.create(request, BackendServiceProxy.getInstance().
                        fetchFragmentExecInfosAsync(address, pbRequest)));
            } catch (RpcException e) {
                throw new AnalysisException("Sending request fails for query's execution informations.");
            }
        }
        return futures;
    }

    private void fillQueryExecInfo(TNetworkAddress address,
                                    PFetchFragmentExecInfosResult fragmentExecInfoResult,
                                   Map<PUniqueId, QueryExecInfo> queryExecInfoMap) {
        for (PInstanceExecInfo info : fragmentExecInfoResult.execInfos) {
            final QueryExecInfo queryExecInfo
                    = queryExecInfoMap.get(info.instanceId);
            final FragmentExecInfo fragmentExecInfo
                    = queryExecInfo.getFragmentWithInstance(info.instanceId);
            final InstanceExecInfo instanceExecInfo
                    = fragmentExecInfo.createInstance(info.instanceId,
                    FragmentExecState.values()[info.execStatus], address.toString());
            if (info.execInfos != null) {
                for (PPlanNodeExecInfo pPlanNodeExecInfo : info.execInfos) {
                    instanceExecInfo.createPlanNode(
                                    pPlanNodeExecInfo.id,
                                    pPlanNodeExecInfo.type,
                                    pPlanNodeExecInfo.ioByByte,
                                    pPlanNodeExecInfo.cpuConsumpation);
                }
            }
        }
    }

    private Collection<QueryExecInfo> recvResponse(Map<PUniqueId, QueryExecInfo> queryExecInfoMap,
                                                   List<Pair<Request, Future<PFetchFragmentExecInfosResult>>> futures)
            throws AnalysisException {
        final String reasonPrefix = "Fail to receive result.";
        for (Pair<Request, Future<PFetchFragmentExecInfosResult>> pair : futures) {
            try {
                final PFetchFragmentExecInfosResult result
                        = pair.second.get(10, TimeUnit.SECONDS);
                TStatusCode code = TStatusCode.findByValue(result.status.code);
                String errMsg = null;
                if (result.status.msgs != null
                        && !result.status.msgs.isEmpty()) {
                    errMsg = result.status.msgs.get(0);
                }

                if (code == TStatusCode.OK && errMsg == null) {
                    fillQueryExecInfo(pair.first.address, result, queryExecInfoMap);
                } else {
                    LOG.warn(reasonPrefix + " reason:" + " code:" + errMsg + code);
                    throw new AnalysisException(reasonPrefix);
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOG.warn(reasonPrefix + " reason:" + e);
                throw new AnalysisException(reasonPrefix);
            }

        }
        return queryExecInfoMap.values();
    }

    private TNetworkAddress toBrpcHost(TNetworkAddress host) throws AnalysisException {
        final Backend backend = Catalog.getCurrentSystemInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (backend == null) {
            throw new AnalysisException(new StringBuilder("Backend ")
                    .append(host.getHostname())
                    .append(":")
                    .append(host.getPort())
                    .append(" does not exist")
                    .toString());
        }
        if (backend.getBrpcPort() < 0) {
            throw new AnalysisException("BRPC port is't exist.");
        }
        return new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
    }

    private enum FragmentExecState {
        RUNNING,
        WAIT,
        FINISHED,
        NONE
    }

    public static class QueryExecInfo {
        private final String queryId;
        private final Map<PUniqueId, String> instanceIdToFragmentId;
        private final Map<String, FragmentExecInfo> fragmentExecInfoMap;

        public QueryExecInfo(String queryId) {
            this.queryId = queryId;
            this.fragmentExecInfoMap = Maps.newHashMap();
            this.instanceIdToFragmentId = Maps.newHashMap();
        }

        public void addMappingInstanceToFragment(String fragmentId, PUniqueId instanceId) {
            this.instanceIdToFragmentId.put(instanceId, fragmentId.toString());
            FragmentExecInfo info = fragmentExecInfoMap.get(fragmentId);
            if (info == null) {
                info = new FragmentExecInfo(fragmentId);
                fragmentExecInfoMap.put(fragmentId, info);
            }
        }

        public FragmentExecInfo getFragmentWithInstance(PUniqueId pUId) {
            final String instanceId = this.instanceIdToFragmentId.get(pUId);
            return fragmentExecInfoMap.get(instanceId);
        }

        public Collection<FragmentExecInfo> getFragmentExecInfo() {
            return fragmentExecInfoMap.values();
        }

        public String getQueryId() {
            return queryId;
        }
    }

    public static class FragmentExecInfo {
        private final String fragmentId;
        private final Map<PUniqueId, InstanceExecInfo> instanceExecInfoMap;

        public FragmentExecInfo(String fragmentId) {
            this.fragmentId = fragmentId;
            this.instanceExecInfoMap = Maps.newHashMap();
        }

        public String getFragmentId() {
            return fragmentId;
        }

        public InstanceExecInfo createInstance(PUniqueId instanceId, FragmentExecState execState, String host) {
            InstanceExecInfo instanceExecInfo = instanceExecInfoMap.get(instanceId);
            if (instanceExecInfo == null) {
                instanceExecInfo = new InstanceExecInfo(instanceId.toString(), execState, host);
                instanceExecInfoMap.put(instanceId, instanceExecInfo);
            }
            return instanceExecInfo;
        }

        public Collection<InstanceExecInfo> getInstanceExecInfo() {
            return instanceExecInfoMap.values();
        }
    }

    public static class InstanceExecInfo {
        private final String instanceId;
        private final FragmentExecState execState;
        private final List<PlanNodeExecInfo> planNodeExecInfos;
        private final String host;

        public InstanceExecInfo(String instanceId, FragmentExecState execState, String host) {
            this.instanceId = instanceId;
            this.execState = execState;
            this.planNodeExecInfos = Lists.newArrayList();
            this.host = host;
        }

        public String getInstanceId() {
            return instanceId;
        }

        public String getHost() {
            return host;
        }

        public String getExecState() {
            return execState.name();
        }

        public PlanNodeExecInfo createPlanNode(int id, int type, long ioByByte, long cpuConsumpation) {
            final PlanNodeExecInfo planNodeExecInfo = new PlanNodeExecInfo(id, type, ioByByte, cpuConsumpation);
            planNodeExecInfos.add(planNodeExecInfo);
            return planNodeExecInfo; 
        }

        public Collection<PlanNodeExecInfo> getPlanNodeExecInfo() {
            return planNodeExecInfos;
        }
    }

    public static class PlanNodeExecInfo {
        private final int id;
        private final int type;
        private final long ioByByte;
        private final long cpuConsumpation;

        public PlanNodeExecInfo(int id, int type, long ioByByte, long cpuConsumpation) {
            this.id = id;
            this.type = type;
            this.ioByByte = ioByByte;
            this.cpuConsumpation = cpuConsumpation;
        }

        public int getId() {
            return id;
        }

        public int getType() {
            return type;
        }

        public long getIoByByte() {
            return ioByByte;
        }

        public long getCpuConsumpation() {
            return cpuConsumpation;
        }
    }

    private static class Request {
        private final TNetworkAddress address;
        private final List<PUniqueId> instanceIds;

        public Request(TNetworkAddress address) {
            this.address = address;
            this.instanceIds = Lists.newArrayList();
        }

        public TNetworkAddress getAddress() {
            return address;
        }

        public List<PUniqueId> getAllInstanceIds() {
            return instanceIds;
        }

        public void addInstanceId(PUniqueId pUId) {
            this.instanceIds.add(pUId);
        }
    }
}
