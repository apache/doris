// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.PFetchFragmentExecInfoRequest;
import org.apache.doris.rpc.PFetchFragmentExecInfosResult;
import org.apache.doris.rpc.PFragmentExecInfo;
import org.apache.doris.rpc.PUniqueId;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/*
 * show proc "/current_queries/{query_id}/fragments"
 */
public class CurrentQueryFragmentProcNode implements ProcNodeInterface {
    private static final Logger LOG = LogManager.getLogger(StmtExecutor.class);
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("FragmentId").add("InstanceId").add("Host").add("ExecState").build();
    private QueryStatisticsItem item;

    public CurrentQueryFragmentProcNode(QueryStatisticsItem item) {
        this.item = item;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        return requestFragmentExecInfos();
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

    private ProcResult requestFragmentExecInfos() throws AnalysisException {

        // create request and remove redundant rpc
        final Map<TNetworkAddress, Request> requestMap = Maps.newHashMap();
        for (QueryStatisticsItem.FragmentInstanceInfo info : item.getFragmentInstanceInfos()) {
            final TNetworkAddress brpcNetAddress;
            try {
                brpcNetAddress = toBrpcHost(info.getAddress());
            } catch (Exception e) {
                LOG.warn(e.getMessage());
                throw new AnalysisException(e.getMessage());
            }
            Request request = requestMap.get(brpcNetAddress);
            if (request == null) {
                request = new Request(brpcNetAddress);
                requestMap.put(brpcNetAddress, request);
            }
            request.addInstanceId(info.getFragmentId(), new PUniqueId(info.getInstanceId()));
        }

        // send request
        final List<Pair<Request, Future<PFetchFragmentExecInfosResult>>> futures = Lists.newArrayList();
        for (TNetworkAddress address : requestMap.keySet()) {
            final Request request = requestMap.get(address);
            final PFetchFragmentExecInfoRequest pbRequest =
                    new PFetchFragmentExecInfoRequest(request.getInstanceId());
            try {
                futures.add(Pair.create(request, BackendServiceProxy.getInstance().
                        fetchFragmentExecInfosAsync(address, pbRequest)));
            } catch (RpcException e) {
                throw new AnalysisException("exec rpc error");
            }
        }

        final List<List<String>> sortedRowDatas = Lists.newArrayList();
        // get result
        for (Pair<Request, Future<PFetchFragmentExecInfosResult>> pair : futures) {
            TStatusCode code;
            String errMsg = null;
            try {
                final PFetchFragmentExecInfosResult fragmentExecInfoResult
                        = pair.second.get(10, TimeUnit.SECONDS);
                code = TStatusCode.findByValue(fragmentExecInfoResult.status.code);
                if (fragmentExecInfoResult.status.msgs != null
                        && !fragmentExecInfoResult.status.msgs.isEmpty()) {
                    errMsg = fragmentExecInfoResult.status.msgs.get(0);
                }

                if (errMsg == null) {
                    for (PFragmentExecInfo info : fragmentExecInfoResult.execInfos) {
                        final List<String> rowData = Lists.newArrayList();
                        rowData.add(pair.first.getFragmentId(info.instanceId).toString());
                        rowData.add(DebugUtil.printId(info.instanceId));
                        rowData.add(pair.first.getAddress().getHostname());
                        rowData.add(getFragmentExecState(info.execStatus));
                        sortedRowDatas.add(rowData);
                    }
                }
            } catch (ExecutionException e) {
                LOG.warn("catch a execute exception", e);
                code = TStatusCode.THRIFT_RPC_ERROR;
            } catch (InterruptedException e) {
                LOG.warn("catch a interrupt exception", e);
                code = TStatusCode.INTERNAL_ERROR;
            } catch (TimeoutException e) {
                LOG.warn("catch a timeout exception", e);
                code = TStatusCode.TIMEOUT;
            }

            if (code != TStatusCode.OK) {
                switch (code) {
                    case TIMEOUT:
                        errMsg = "query timeout";
                        break;
                    case THRIFT_RPC_ERROR:
                        errMsg = "rpc failed";
                        break;
                    default:
                        errMsg = "exec rpc error";
                }
                throw new AnalysisException(errMsg);
            }
        }
            
        // sort according to explain's fragment index
        sortedRowDatas.sort(new Comparator<List<String>>() {
            @Override
            public int compare(List<String> l1, List<String> l2) {
                final Integer fragmentId1 = Integer.valueOf(l1.get(0));
                final Integer fragmentId2 = Integer.valueOf(l2.get(0));
                return fragmentId1.compareTo(fragmentId2);
            }
        });
        final BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES.asList());
        result.setRows(sortedRowDatas);
        return result;
    }

    private enum FragmentExecState {
        RUNNING,
        WAIT,
        FINISHED,
        NONE
    }

    private String getFragmentExecState(int i) {
        if (i >= FragmentExecState.values().length) {
            // can't run here
            LOG.warn("Fetch uncorrect instance state.");
            return FragmentExecState.NONE.toString();
        }
        return FragmentExecState.values()[i].toString();
    }

    private static class Request {
        private final TNetworkAddress address;
        private List<String> fragmentIds;
        private List<PUniqueId> instanceIds;
        private Map<String, String> instanceIdToFragmentId;

        public Request(TNetworkAddress address) {
            this.address = address;
            this.fragmentIds = Lists.newArrayList();
            this.instanceIds = Lists.newArrayList();
            this.instanceIdToFragmentId = Maps.newHashMap();
        }

        public TNetworkAddress getAddress() {
            return address;
        }

        public List<PUniqueId> getInstanceId() {
            return instanceIds;
        }

        public void addInstanceId(String fragmentId, PUniqueId instanceId) {
            this.fragmentIds.add(fragmentId);
            this.instanceIds.add(instanceId);
            this.instanceIdToFragmentId.put(DebugUtil.printId(instanceId), fragmentId.toString());
        }

        public String getFragmentId(PUniqueId id) {
            return instanceIdToFragmentId.get(DebugUtil.printId(id));
        }
    }
}
