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
import org.apache.doris.common.util.Counter;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.rpc.*;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Provide running query's PlanNode informations, IO consumption and CPU consumption.
 */
public class CurrentQueryInfoProvider {
    private static final Logger LOG = LogManager.getLogger(CurrentQueryInfoProvider.class);

    public CurrentQueryInfoProvider() {
    }

    /**
     * Firstly send request to trigger profile to report for specified query and wait a while,
     * Secondly get Counters from Coordinator's RuntimeProfile and return query's consumption.
     *
     * @param item
     * @return
     * @throws AnalysisException
     */
    public Consumption getQueryConsumption(QueryStatisticsItem item) throws AnalysisException {
        triggerReportAndWait(item, getWaitingTimeForSingleQuery(), false);
        return new Consumption(item.getQueryProfile());
    }

    /**
     * Same as getQueryConsumption, but this will cause BE to report all queries profile.
     *
     * @param items
     * @return
     * @throws AnalysisException
     */
    public Map<String, Consumption> getQueryConsumption(Collection<QueryStatisticsItem> items)
            throws AnalysisException {
        triggerReportAndWait(items, getWaitingTime(items.size()), true);
        final Map<String, Consumption> queryConsumptions = Maps.newHashMap();
        for (QueryStatisticsItem item : items) {
            queryConsumptions.put(item.getQueryId(), new Consumption(item.getQueryProfile()));
        }
        return queryConsumptions;
    }

    /**
     * Return query's instances consumption.
     *
     * @param item
     * @return
     * @throws AnalysisException
     */
    public Collection<InstanceConsumption> getQueryInstanceConsumption(QueryStatisticsItem item) throws AnalysisException {
        triggerReportAndWait(item, getWaitingTimeForSingleQuery(), false);
        final Map<String, RuntimeProfile> instanceProfiles = collectInstanceProfile(item.getQueryProfile());
        final List<InstanceConsumption> instanceConsumptions = Lists.newArrayList();
        for (QueryStatisticsItem.FragmentInstanceInfo instanceInfo : item.getFragmentInstanceInfos()) {
            final RuntimeProfile instanceProfile = instanceProfiles.get(DebugUtil.printId(instanceInfo.getInstanceId()));
            Preconditions.checkNotNull(instanceProfile);
            final InstanceConsumption consumption =
                    new InstanceConsumption(
                            instanceInfo.getFragmentId(),
                            instanceInfo.getInstanceId(),
                            instanceInfo.getAddress(),
                            instanceProfile);
            instanceConsumptions.add(consumption);
        }
        return instanceConsumptions;
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
                    final PUniqueId pUId = new PUniqueId(instanceInfo.getInstanceId());
                    request.addInstanceId(pUId);
                }
            }
        }
        recvResponse(sendRequest(requests));
    }

    private List<Pair<Request, Future<PTriggerProfileReportResult>>> sendRequest(
            Map<TNetworkAddress, Request> requests) throws AnalysisException {
        final List<Pair<Request, Future<PTriggerProfileReportResult>>> futures = Lists.newArrayList();
        for (TNetworkAddress address : requests.keySet()) {
            final Request request = requests.get(address);
            final PTriggerProfileReportRequest pbRequest =
                    new PTriggerProfileReportRequest(request.getInstanceIds());
            try {
                futures.add(Pair.create(request, BackendServiceProxy.getInstance().
                        triggerProfileReportAsync(address, pbRequest)));
            } catch (RpcException e) {
                throw new AnalysisException("Sending request fails for query's execution informations.");
            }
        }
        return futures;
    }

    private void recvResponse(List<Pair<Request, Future<PTriggerProfileReportResult>>> futures)
            throws AnalysisException {
        final String reasonPrefix = "Fail to receive result.";
        for (Pair<Request, Future<PTriggerProfileReportResult>> pair : futures) {
            try {
                final PTriggerProfileReportResult result
                        = pair.second.get(2, TimeUnit.SECONDS);
                final TStatusCode code = TStatusCode.findByValue(result.status.code);
                if (code != TStatusCode.OK) {
                    String errMsg = "";
                    if (result.status.msgs != null && !result.status.msgs.isEmpty()) {
                        errMsg = result.status.msgs.get(0);
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
            throw new AnalysisException("BRPC port is't exist.");
        }
        return new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
    }


    public static class Consumption {
        private final static String OLAP_SCAN_NODE = "OLAP_SCAN_NODE";
        private final static String HASH_JOIN_NODE = "HASH_JOIN_NODE";
        private final static String HASH_AGGREGATION_NODE = "AGGREGATION_NODE";
        private final static String SORT_NODE = "SORT_NODE";
        private final static String ANALYTIC_EVAL_NODE = "ANALYTIC_EVAL_NODE";
        private final static String UNION_NODE = "UNION_NODE";
        private final static String EXCHANGE_NODE = "EXCHANGE_NODE";

        protected final List<ConsumptionCalculator> calculators;

        public Consumption(RuntimeProfile profile) {
            this.calculators = Lists.newArrayList();
            init(profile);
        }

        private void init(RuntimeProfile profile) {
            final List<Map<String, Counter>> olapScanCounters = Lists.newArrayList();
            collectNodeProfileCounters(profile, olapScanCounters, OLAP_SCAN_NODE);
            calculators.add(new OlapScanNodeConsumptionCalculator(olapScanCounters));

            final List<Map<String, Counter>> hashJoinCounters = Lists.newArrayList();
            collectNodeProfileCounters(profile, hashJoinCounters, HASH_JOIN_NODE);
            calculators.add(new HashJoinConsumptionCalculator(hashJoinCounters));

            final List<Map<String, Counter>> hashAggCounters = Lists.newArrayList();
            collectNodeProfileCounters(profile, hashAggCounters, HASH_AGGREGATION_NODE);
            calculators.add(new HashAggConsumptionCalculator(hashAggCounters));

            final List<Map<String, Counter>> sortCounters = Lists.newArrayList();
            collectNodeProfileCounters(profile, sortCounters, SORT_NODE);
            calculators.add(new SortConsumptionCalculator(sortCounters));

            final List<Map<String, Counter>> windowsCounters = Lists.newArrayList();
            collectNodeProfileCounters(profile, windowsCounters, ANALYTIC_EVAL_NODE);
            calculators.add(new WindowsConsumptionCalculator(windowsCounters));

            final List<Map<String, Counter>> unionCounters = Lists.newArrayList();
            collectNodeProfileCounters(profile, unionCounters, UNION_NODE);
            calculators.add(new UnionConsumptionCalculator(unionCounters));

            final List<Map<String, Counter>> exchangeCounters = Lists.newArrayList();
            collectNodeProfileCounters(profile, exchangeCounters, EXCHANGE_NODE);
            calculators.add(new ExchangeConsumptionCalculator(exchangeCounters));
        }

        private void collectNodeProfileCounters(RuntimeProfile profile,
                                                List<Map<String, Counter>> counterMaps, String name) {
            for (Map.Entry<String, RuntimeProfile> entry : profile.getChildMap().entrySet()) {
                if (name.equals(parsePossibleExecNodeName(entry.getKey()))) {
                    counterMaps.add(entry.getValue().getCounterMap());
                }
                collectNodeProfileCounters(entry.getValue(), counterMaps, name);
            }
        }

        /**
         * ExecNode's RuntimeProfile name is "$node_type_name (id=?)"
         * @param str
         * @return
         */
        private String parsePossibleExecNodeName(String str) {
            final String[] elements = str.split(" ");
            if (elements.length == 2) {
                return elements[0];
            } else {
                return "";
            }
        }

        private long getTotalCpuConsumption() {
            long cpu = 0;
            for (ConsumptionCalculator consumption : calculators) {
                cpu += consumption.getProcessRows();
            }
            return cpu;
        }

        private long getTotalIoConsumption() {
            long io = 0;
            for (ConsumptionCalculator consumption : calculators) {
                io += consumption.getScanBytes();
            }
            return io;
        }

        public String getFormattingProcessRows() {
            final StringBuilder builder = new StringBuilder();
            builder.append(getTotalCpuConsumption()).append(" Rows");
            return builder.toString();
        }

        public String getFormattingScanBytes() {
            final Pair<Double, String> pair = DebugUtil.getByteUint(getTotalIoConsumption());
            final Formatter fmt = new Formatter();
            final StringBuilder builder = new StringBuilder();
            builder.append(fmt.format("%.2f", pair.first)).append(" ").append(pair.second);
            return builder.toString();
        }
    }

    public static class InstanceConsumption extends Consumption {
        private final String fragmentId;
        private final TUniqueId instanceId;
        private final TNetworkAddress address;

        public InstanceConsumption(
                String fragmentId,
                TUniqueId instanceId,
                TNetworkAddress address,
                RuntimeProfile profile) {
            super(profile);
            this.fragmentId = fragmentId;
            this.instanceId = instanceId;
            this.address = address;

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
    }

    private static abstract class ConsumptionCalculator {
        protected final List<Map<String, Counter>> counterMaps;

        public ConsumptionCalculator(List<Map<String, Counter>> counterMaps) {
            this.counterMaps = counterMaps;
        }

        public long getProcessRows() {
            long cpu = 0;
            for (Map<String, Counter> counters : counterMaps) {
                cpu += getProcessRows(counters);
            }
            return cpu;
        }

        public long getScanBytes() {
            long io = 0;
            for (Map<String, Counter> counters : counterMaps) {
                io += getScanBytes(counters);
            }
            return io;
        }

        protected long getProcessRows(Map<String, Counter> counters) {
            return 0;
        }

        protected long getScanBytes(Map<String, Counter> counters) {
            return 0;
        }
    }

    private static class OlapScanNodeConsumptionCalculator extends ConsumptionCalculator {
        public OlapScanNodeConsumptionCalculator(List<Map<String, Counter>> counterMaps) {
            super(counterMaps);
        }

        @Override
        protected long getScanBytes(Map<String, Counter> counters) {
            final Counter counter = counters.get("CompressedBytesRead");
            return counter == null ? 0 : counter.getValue();
        }
    }

    private static class HashJoinConsumptionCalculator extends ConsumptionCalculator {
        public HashJoinConsumptionCalculator(List<Map<String, Counter>> counterMaps) {
            super(counterMaps);
        }

        @Override
        protected long getProcessRows(Map<String, Counter> counters) {
            final Counter probeCounter = counters.get("ProbeRows");
            final Counter buildCounter = counters.get("BuildRows");
            return probeCounter == null || buildCounter == null ?
                    0 : probeCounter.getValue() + buildCounter.getValue();
        }
    }

    private static class HashAggConsumptionCalculator extends ConsumptionCalculator {
        public HashAggConsumptionCalculator(List<Map<String, Counter>> counterMaps) {
            super(counterMaps);
        }

        @Override
        protected long getProcessRows(Map<String, Counter> counters) {
            final Counter buildCounter = counters.get("BuildRows");
            return buildCounter == null ? 0 : buildCounter.getValue();
        }
    }

    private static class SortConsumptionCalculator extends ConsumptionCalculator {
        public SortConsumptionCalculator(List<Map<String, Counter>> counterMaps) {
            super(counterMaps);
        }

        @Override
        protected long getProcessRows(Map<String, Counter> counters) {
            final Counter sortRowsCounter = counters.get("SortRows");
            return sortRowsCounter == null ? 0 : sortRowsCounter.getValue();
        }
    }

    private static class WindowsConsumptionCalculator extends ConsumptionCalculator {
        public WindowsConsumptionCalculator(List<Map<String, Counter>> counterMaps) {
            super(counterMaps);
        }

        @Override
        protected long getProcessRows(Map<String, Counter> counters) {
            final Counter processRowsCounter = counters.get("ProcessRows");
            return processRowsCounter == null ? 0 : processRowsCounter.getValue();

        }
    }

    private static class UnionConsumptionCalculator extends ConsumptionCalculator {
        public UnionConsumptionCalculator(List<Map<String, Counter>> counterMaps) {
            super(counterMaps);
        }

        @Override
        protected long getProcessRows(Map<String, Counter> counters) {
            final Counter materializeRowsCounter = counters.get("MaterializeRows");
            return materializeRowsCounter == null ? 0 : materializeRowsCounter.getValue();
        }
    }

    private static class ExchangeConsumptionCalculator extends ConsumptionCalculator {

        public ExchangeConsumptionCalculator(List<Map<String, Counter>> counterMaps) {
            super(counterMaps);
        }

        @Override
        protected long getProcessRows(Map<String, Counter> counters) {
            final Counter mergeRowsCounter = counters.get("MergeRows");
            return mergeRowsCounter == null ? 0 : mergeRowsCounter.getValue();
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

        public List<PUniqueId> getInstanceIds() {
            return instanceIds;
        }

        public void addInstanceId(PUniqueId instanceId) {
            this.instanceIds.add(instanceId);
        }
    }
}
