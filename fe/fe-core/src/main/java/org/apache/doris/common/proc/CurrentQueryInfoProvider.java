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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Counter;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
 * Provide running query's statistics.
 */
public class CurrentQueryInfoProvider {
    private static final Logger LOG = LogManager.getLogger(CurrentQueryInfoProvider.class);

    public CurrentQueryInfoProvider() {
    }

    /**
     * Firstly send request to trigger profile to report for specified query and wait a while,
     * Secondly get Counters from Coordinator's RuntimeProfile and return query's statistics.
     *
     * @param item
     * @return
     * @throws AnalysisException
     */
    public QueryStatistics getQueryStatistics(QueryStatisticsItem item) throws AnalysisException {
        triggerReportAndWait(item, getWaitingTimeForSingleQuery(), false);
        return new QueryStatistics(item.getQueryProfile());
    }

    /**
     * Same as above, but this will cause BE to report all queries profile.
     *
     * @param items
     * @return
     * @throws AnalysisException
     */
    public Map<String, QueryStatistics> getQueryStatistics(Collection<QueryStatisticsItem> items)
            throws AnalysisException {
        triggerReportAndWait(items, getWaitingTime(items.size()), true);
        final Map<String, QueryStatistics> queryStatisticsMap = Maps.newHashMap();
        for (QueryStatisticsItem item : items) {
            queryStatisticsMap.put(item.getQueryId(), new QueryStatistics(item.getQueryProfile()));
        }
        return queryStatisticsMap;
    }

    /**
     * Return query's instances statistics.
     *
     * @param item
     * @return
     * @throws AnalysisException
     */
    public Collection<InstanceStatistics> getInstanceStatistics(QueryStatisticsItem item) throws AnalysisException {
        triggerReportAndWait(item, getWaitingTimeForSingleQuery(), false);
        final Map<String, RuntimeProfile> instanceProfiles = collectInstanceProfile(item.getQueryProfile());
        final List<InstanceStatistics> instanceStatisticsList = Lists.newArrayList();
        for (QueryStatisticsItem.FragmentInstanceInfo instanceInfo : item.getFragmentInstanceInfos()) {
            final RuntimeProfile instanceProfile = instanceProfiles.get(DebugUtil.printId(instanceInfo.getInstanceId()));
            Preconditions.checkNotNull(instanceProfile);
            final InstanceStatistics Statistics =
                    new InstanceStatistics(
                            instanceInfo.getFragmentId(),
                            instanceInfo.getInstanceId(),
                            instanceInfo.getAddress(),
                            instanceProfile);
            instanceStatisticsList.add(Statistics);
        }
        return instanceStatisticsList;
    }

    /**
     * Profile trees is query profile -> fragment profile -> instance profile ....
     * @param queryProfile
     * @return instanceProfiles
     */
    private Map<String, RuntimeProfile> collectInstanceProfile(RuntimeProfile queryProfile) {
        final Map<String, RuntimeProfile> instanceProfiles = Maps.newHashMap();
        for (RuntimeProfile fragmentProfile : queryProfile.getChildMap().values()) {
            for (Map.Entry<String, RuntimeProfile> entry: fragmentProfile.getChildMap().entrySet()) {
                Preconditions.checkState(instanceProfiles.put(parseInstanceId(entry.getKey()), entry.getValue()) == null);
            }
        }
        return instanceProfiles;
    }

    /**
     * Instance profile key is "Instance ${instance_id} (host=$host $port)"
     * @param str
     * @return
     */
    private String parseInstanceId(String str) {
        final String[] elements = str.split(" ");
        if (elements.length == 4) {
            return elements[1];
        } else {
            Preconditions.checkState(false);
            return "";
        }
    }

    private long getWaitingTimeForSingleQuery() {
        return getWaitingTime(1);
    }

    /**
     * @param numOfQuery
     * @return unit(ms)
     */
    private long getWaitingTime(int numOfQuery) {
        final int oneQueryWaitingTime = 100;
        final int allQueryMaxWaitingTime = 2000;
        final int waitingTime = numOfQuery * oneQueryWaitingTime;
        return waitingTime > allQueryMaxWaitingTime ? allQueryMaxWaitingTime : waitingTime;
    }

    private void triggerReportAndWait(QueryStatisticsItem item, long waitingTime, boolean allQuery)
            throws AnalysisException {
        final List<QueryStatisticsItem> items = Lists.newArrayList(item);
        triggerReportAndWait(items, waitingTime, allQuery);
    }

    private void triggerReportAndWait(Collection<QueryStatisticsItem> items, long waitingTime, boolean allQuery)
            throws AnalysisException {
        triggerProfileReport(items, allQuery);
        try {
            Thread.currentThread().sleep(waitingTime);
        } catch (InterruptedException e) {
        }
    }

    /**
     * send report profile request.
     * @param items
     * @param allQuery true:all queries profile will be reported, false:specified queries profile will be reported.
     * @throws AnalysisException
     */
    private void triggerProfileReport(Collection<QueryStatisticsItem> items, boolean allQuery) throws AnalysisException {
        final Map<TNetworkAddress, Request> requests = Maps.newHashMap();
        final Map<TNetworkAddress, TNetworkAddress> brpcAddresses = Maps.newHashMap();
        for (QueryStatisticsItem item : items) {
            for (QueryStatisticsItem.FragmentInstanceInfo instanceInfo : item.getFragmentInstanceInfos()) {
                // use brpc address
                TNetworkAddress brpcNetAddress = brpcAddresses.get(instanceInfo.getAddress());
                if (brpcNetAddress == null) {
                    try {
                        brpcNetAddress = toBrpcHost(instanceInfo.getAddress());
                        brpcAddresses.put(instanceInfo.getAddress(), brpcNetAddress);
                    } catch (Exception e) {
                        LOG.warn(e.getMessage());
                        throw new AnalysisException(e.getMessage());
                    }
                }
                // merge different requests
                Request request = requests.get(brpcNetAddress);
                if (request == null) {
                    request = new Request(brpcNetAddress);
                    requests.put(brpcNetAddress, request);
                }
                // specified query instance which will report.
                if (!allQuery) {
                    final Types.PUniqueId pUId = Types.PUniqueId.newBuilder()
                            .setHi(instanceInfo.getInstanceId().hi)
                            .setLo(instanceInfo.getInstanceId().lo)
                            .build();
                    request.addInstanceId(pUId);
                }
            }
        }
        recvResponse(sendRequest(requests));
    }

    private List<Pair<Request, Future<InternalService.PTriggerProfileReportResult>>> sendRequest(
            Map<TNetworkAddress, Request> requests) throws AnalysisException {
        final List<Pair<Request, Future<InternalService.PTriggerProfileReportResult>>> futures = Lists.newArrayList();
        for (TNetworkAddress address : requests.keySet()) {
            final Request request = requests.get(address);
            final InternalService.PTriggerProfileReportRequest pbRequest = InternalService.PTriggerProfileReportRequest
                    .newBuilder().addAllInstanceIds(request.getInstanceIds()).build();
            try {
                futures.add(Pair.create(request, BackendServiceProxy.getInstance().
                        triggerProfileReportAsync(address, pbRequest)));
            } catch (RpcException e) {
                throw new AnalysisException("Sending request fails for query's execution information.");
            }
        }
        return futures;
    }

    private void recvResponse(List<Pair<Request, Future<InternalService.PTriggerProfileReportResult>>> futures)
            throws AnalysisException {
        final String reasonPrefix = "Fail to receive result.";
        for (Pair<Request, Future<InternalService.PTriggerProfileReportResult>> pair : futures) {
            try {
                final InternalService.PTriggerProfileReportResult result
                        = pair.second.get(2, TimeUnit.SECONDS);
                final TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
                if (code != TStatusCode.OK) {
                    String errMsg = "";
                    if (!result.getStatus().getErrorMsgsList().isEmpty()) {
                        errMsg = result.getStatus().getErrorMsgs(0);
                    }
                    throw new AnalysisException(reasonPrefix + " backend:" + pair.first.getAddress()
                            + " reason:" + errMsg);
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOG.warn(reasonPrefix + " reason:" + e.getCause());
                throw new AnalysisException(reasonPrefix);
            }

        }
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
            throw new AnalysisException("BRPC port isn't exist.");
        }
        return new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
    }


    public static class QueryStatistics {
        final List<Map<String, Counter>> counterMaps;

        public QueryStatistics(RuntimeProfile profile) {
            counterMaps = Lists.newArrayList();
            collectCounters(profile, counterMaps);
        }

        private void collectCounters(RuntimeProfile profile,
                                                List<Map<String, Counter>> counterMaps) {
            for (Map.Entry<String, RuntimeProfile> entry : profile.getChildMap().entrySet()) {
                counterMaps.add(entry.getValue().getCounterMap());
                collectCounters(entry.getValue(), counterMaps);
            }
        }

        public long getScanBytes() {
            long scanBytes = 0;
            for (Map<String, Counter> counters : counterMaps) {
                final Counter counter = counters.get("CompressedBytesRead");
                scanBytes += counter == null ? 0 : counter.getValue();
            }
            return scanBytes;
        }

        public long getRowsReturned() {
            long rowsReturned = 0;
            for (Map<String, Counter> counters : counterMaps) {
                final Counter counter = counters.get("RowsReturned");
                rowsReturned += counter == null ? 0 : counter.getValue();
            }
            return rowsReturned;
        }
    }

    public static class InstanceStatistics {
        private final String fragmentId;
        private final TUniqueId instanceId;
        private final TNetworkAddress address;
        private final QueryStatistics statistics;

        public InstanceStatistics(
                String fragmentId,
                TUniqueId instanceId,
                TNetworkAddress address,
                RuntimeProfile profile) {
            this.fragmentId = fragmentId;
            this.instanceId = instanceId;
            this.address = address;
            this.statistics = new QueryStatistics(profile);
        }

        public String getFragmentId() {
            return fragmentId;
        }

        public TUniqueId getInstanceId() {
            return instanceId;
        }

        public TNetworkAddress getAddress() {
            return address;
        }

        public long getRowsReturned() {
            return statistics.getRowsReturned();
        }

        public long getScanBytes() {
            return statistics.getScanBytes();
        }
    }

    private static class Request {
        private final TNetworkAddress address;
        private final List<Types.PUniqueId> instanceIds;

        public Request(TNetworkAddress address) {
            this.address = address;
            this.instanceIds = Lists.newArrayList();
        }

        public TNetworkAddress getAddress() {
            return address;
        }

        public List<Types.PUniqueId> getInstanceIds() {
            return instanceIds;
        }

        public void addInstanceId(Types.PUniqueId instanceId) {
            this.instanceIds.add(instanceId);
        }
    }
}
