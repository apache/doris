// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.qe;

import com.baidu.palo.analysis.Analyzer;
import com.baidu.palo.analysis.DescriptorTable;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.ClientPool;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.Reference;
import com.baidu.palo.common.Status;
import com.baidu.palo.common.util.DebugUtil;
import com.baidu.palo.common.util.RuntimeProfile;
import com.baidu.palo.planner.DataPartition;
import com.baidu.palo.planner.DataSink;
import com.baidu.palo.planner.PlanFragment;
import com.baidu.palo.planner.PlanFragmentId;
import com.baidu.palo.planner.PlanNode;
import com.baidu.palo.planner.PlanNodeId;
import com.baidu.palo.planner.Planner;
import com.baidu.palo.planner.ResultSink;
import com.baidu.palo.planner.ScanNode;
import com.baidu.palo.service.FrontendOptions;
import com.baidu.palo.system.Backend;
import com.baidu.palo.task.LoadEtlTask;
import com.baidu.palo.thrift.BackendService;
import com.baidu.palo.thrift.PaloInternalServiceVersion;
import com.baidu.palo.thrift.TCancelPlanFragmentParams;
import com.baidu.palo.thrift.TCancelPlanFragmentResult;
import com.baidu.palo.thrift.TDescriptorTable;
import com.baidu.palo.thrift.TExecPlanFragmentParams;
import com.baidu.palo.thrift.TExecPlanFragmentResult;
import com.baidu.palo.thrift.TNetworkAddress;
import com.baidu.palo.thrift.TPaloScanRange;
import com.baidu.palo.thrift.TPlanFragmentDestination;
import com.baidu.palo.thrift.TPlanFragmentExecParams;
import com.baidu.palo.thrift.TQueryGlobals;
import com.baidu.palo.thrift.TQueryOptions;
import com.baidu.palo.thrift.TQueryType;
import com.baidu.palo.thrift.TReportExecStatusParams;
import com.baidu.palo.thrift.TResourceInfo;
import com.baidu.palo.thrift.TResultBatch;
import com.baidu.palo.thrift.TScanRange;
import com.baidu.palo.thrift.TScanRangeLocation;
import com.baidu.palo.thrift.TScanRangeLocations;
import com.baidu.palo.thrift.TScanRangeParams;
import com.baidu.palo.thrift.TStatusCode;
import com.baidu.palo.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Coordinator {
    private static final Logger LOG = LogManager.getLogger(Coordinator.class);
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static String localIP = FrontendOptions.getLocalHostAddress();

    // Overall status of the entire query; set to the first reported fragment error
    // status or to CANCELLED, if Cancel() is called.
    Status queryStatus = new Status();

    Map<TNetworkAddress, Long> addressToBackendID = Maps.newHashMap();

    private ImmutableMap<Long, Backend> idToBackend = ImmutableMap.of();

    // copied from TQueryExecRequest; constant across all fragments
    private TDescriptorTable descTable;

    // Why we use query global?
    // When `NOW()` function is in sql, we need only one now(),
    // but, we execute `NOW()` distributed.
    // So we make a query global value here to make one `now()` value in one query process.
    private TQueryGlobals queryGlobals = new TQueryGlobals();
    private TQueryOptions queryOptions;
    private TNetworkAddress coordAddress;

    // protects all fields below
    private Lock lock = new ReentrantLock();

    // If true, the query is done returning all results.  It is possible that the
    // coordinator still needs to wait for cleanup on remote fragments (e.g. queries
    // with limit)
    // Once this is set to true, errors from remote fragments are ignored.
    private boolean returnedAllResults;

    private RuntimeProfile queryProfile;

    private List<RuntimeProfile> fragmentProfile;

    // populated in computeFragmentExecParams()
    private Map<PlanFragmentId, FragmentExecParams> fragmentExecParams = Maps.newHashMap();

    private List<PlanFragment> fragments;
    // vector is indexed by fragment index from TQueryExecRequest.fragments;
    // populated in computeScanRangeAssignment()
    private Map<PlanFragmentId, FragmentScanRangeAssignment> scanRangeAssignment =
            Maps.newHashMap();
    // backend execute state
    private List<BackendExecState> backendExecStates = Lists.newArrayList();
    private ResultReceiver receiver;
    // fragment instance id to backend state
    private ConcurrentMap<TUniqueId, BackendExecState> backendExecStateMap =
            Maps.newConcurrentMap();
    private List<ScanNode> scanNodes;
    // number of backends executing plan fragments on behalf of this query;
    // set in computeFragmentExecParams();
    // same as backend_exec_states_.size() after Exec()
    private int numBackends;

    private CountDownLatch profileDoneSignal;

    private boolean isBlockQuery;

    private int numReceivedRows = 0;

    private List<String> deltaUrls;
    private Map<String, String> loadCounters;
    private String trackingUrl;

    // for export
    private List<String> exportFiles;

    // Input parameter
    private TUniqueId queryId;
    private TResourceInfo tResourceInfo;
    private boolean needReport;

    private String clusterName;

    // Used for query
    public Coordinator(ConnectContext context, Analyzer analyzer, Planner planner) {
        this.isBlockQuery = planner.isBlockQuery();
        this.queryId = context.queryId();
        this.fragments = planner.getFragments();
        this.scanNodes = planner.getScanNodes();
        this.descTable = analyzer.getDescTbl().toThrift();
        this.returnedAllResults = false;
        this.queryOptions = context.getSessionVariable().toThrift();
        this.queryGlobals.setNow_string(DATE_FORMAT.format(new Date()));
        this.tResourceInfo = new TResourceInfo(context.getUser(),
                context.getSessionVariable().getResourceGroup());
        this.needReport = context.getSessionVariable().isReportSucc();
        this.clusterName = context.getClusterName();
    }

    // Used for pull load task coordinator
    public Coordinator(TUniqueId queryId, DescriptorTable descTable,
                       List<PlanFragment> fragments, List<ScanNode> scanNodes, String cluster) {
        this.isBlockQuery = true;
        this.queryId = queryId;
        this.descTable = descTable.toThrift();
        this.fragments = fragments;
        this.scanNodes = scanNodes;
        this.queryOptions = new TQueryOptions();
        this.queryGlobals.setNow_string(DATE_FORMAT.format(new Date()));
        this.tResourceInfo = new TResourceInfo("", "");
        this.needReport = true;
        this.clusterName = cluster;
    }

    public TUniqueId getQueryId() {
        return queryId;
    }

    public void setQueryType(TQueryType type) {
        this.queryOptions.setQuery_type(type);
    }

    public Status getExecStatus() {
        return queryStatus;
    }

    public RuntimeProfile getQueryProfile() {
        return queryProfile;
    }

    public List<String> getDeltaUrls() {
        return deltaUrls;
    }

    public Map<String, String> getLoadCounters() {
        return loadCounters;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void setExecMemoryLimit(long execMemoryLimit) {
        this.queryOptions.setMem_limit(execMemoryLimit);
    }

    public void setTimeout(int timeout) {
        this.queryOptions.setQuery_timeout(timeout);
    }

    // Initiate
    private void prepare() {
        for (PlanFragment fragment : fragments) {
            // resize scan range assigment
            scanRangeAssignment.put(fragment.getFragmentId(), new FragmentScanRangeAssignment());
            // resize fragment execute parameters
            fragmentExecParams.put(fragment.getFragmentId(), new FragmentExecParams(fragment));
        }
        coordAddress = new TNetworkAddress(localIP, Config.rpc_port);

        int fragmentSize = fragments.size();
        queryProfile = new RuntimeProfile("Execution Profile " + DebugUtil.printId(queryId));

        fragmentProfile = new ArrayList<RuntimeProfile>();
        for (int i = 0; i < fragmentSize; i ++) {
            fragmentProfile.add(new RuntimeProfile("Fragment " + i));
            queryProfile.addChild(fragmentProfile.get(i));
        }

        this.idToBackend = Catalog.getCurrentSystemInfo().getBackendsInCluster(clusterName);
        if (LOG.isDebugEnabled()) {
            LOG.debug("idToBackend size={}", idToBackend.size());
            for (Map.Entry<Long, Backend> entry : idToBackend.entrySet()) {
                Long backendID = entry.getKey();
                Backend backend = entry.getValue();
                LOG.debug("backend: {}-{}-{}", backendID, backend.getHost(), backend.getBePort());
            }
        }
    }

    private void lock() {
        lock.lock();
    }

    private void unlock() {
        lock.unlock();
    }

    private void traceInstance() {
        // TODO(zc): add a switch to close this function
        StringBuilder sb = new StringBuilder();
        int idx = 0;
        sb.append("id=").append(DebugUtil.printId(queryId)).append(",");
        sb.append("fragment=[");
        for (Map.Entry<PlanFragmentId, FragmentExecParams> entry : fragmentExecParams.entrySet()) {
            if (idx++ != 0) {
                sb.append(",");
            }
            sb.append(entry.getKey());
            entry.getValue().appendTo(sb);
        }
        sb.append("]");

        LOG.info(sb.toString());
    }

    private class ExecStatus {
        private TStatusCode errCode;
        private TNetworkAddress errAddress;

        public synchronized TStatusCode getErrCode() {
            return errCode;
        }

        public synchronized void setErrCode(TStatusCode errCode) {
            this.errCode = errCode;
        }

        public synchronized TNetworkAddress getErrAddress() {
            return errAddress;
        }

        public synchronized void setErrAddress(TNetworkAddress errAddress) {
            this.errAddress = errAddress;
        }

        public ExecStatus() {
            this.errCode = TStatusCode.OK;
            this.errAddress = new TNetworkAddress();
        }
    }

    // Initiate asynchronous execution of query. Returns as soon as all plan fragments
    // have started executing at their respective backends.
    // 'Request' must contain at least a coordinator plan fragment (ie, can't
    // be for a query like 'SELECT 1').
    // A call to Exec() must precede all other member function calls.
    public void exec() throws Exception {
        if (!scanNodes.isEmpty()) {
            LOG.debug("debug: in Coordinator::exec. planNode: {}", scanNodes.get(0).treeToThrift());
        }

        if (!fragments.isEmpty()) {
            LOG.debug("debug: in Coordinator::exec. fragment: {}", fragments.get(0).toThrift());
        }

        // prepare information
        prepare();
        // compute Fragment Instance
        computeScanRangeAssignment();
        computeFragmentExecParams();

        traceInstance();

        // create result receiver
        PlanFragmentId topId = fragments.get(0).getFragmentId();
        FragmentExecParams topParams = fragmentExecParams.get(topId);
        if (topParams.fragment.getSink() instanceof ResultSink) {
            TPlanFragmentDestination rootSource = new TPlanFragmentDestination();
            rootSource.fragment_instance_id = topParams.instanceIds.get(0);
            rootSource.server = topParams.hosts.get(0);
            receiver = new ResultReceiver(rootSource, addressToBackendID.get(rootSource.server),
                    queryOptions.query_timeout * 1000);
        } else {
            // This is a insert statement.
            this.queryOptions.setIs_report_success(true);
            deltaUrls = Lists.newArrayList();
            loadCounters = Maps.newHashMap();
        }

        // to keep things simple, make async Cancel() calls wait until plan fragment
        // execution has been initiated, otherwise we might try to cancel fragment
        // execution at backends where it hasn't even started
        profileDoneSignal = new CountDownLatch(numBackends);
        lock();
        try {
            // execute all instances from up to bottom
            int backendId = 0;
            int profileFragmentId = 0;
            for (PlanFragment fragment : fragments) {
                FragmentExecParams params = fragmentExecParams.get(fragment.getFragmentId());

                // set up exec states
                int numHosts = params.hosts.size();
                Preconditions.checkState(numHosts > 0);
                List<TExecPlanFragmentParams> tParams = params.toThrift(backendId);
                int instanceId = 0;
                for (TExecPlanFragmentParams tParam : tParams) {
                    // TODO: pool of pre-formatted BackendExecStates?
                    BackendExecState execState =
                            new BackendExecState(fragment.getFragmentId(), instanceId++,
                                    profileFragmentId, tParam, this.addressToBackendID);
                    backendExecStates.add(execState);
                    backendExecStateMap.put(tParam.params.getFragment_instance_id(), execState);
                    backendId++;
                }
                // Issue all rpcs in parallel
                ExecStatus status = new ExecStatus();
                ParallelExecutor.exec(backendExecStates, backendId - numHosts, numHosts, status);
                if (status.getErrCode() != TStatusCode.OK) {
                    String errMsg = "exec rpc error";
                    queryStatus.setStatus(errMsg);
                    LOG.warn("ParallelExecutor exec rpc error, fragmentId={} errBackend={}",
                            fragment.getFragmentId(), status.getErrAddress());
                    cancelInternal(); // err msg: rpc exec error
                    if (status.getErrCode() == TStatusCode.TIMEOUT) {
                        throw new InternalException(errMsg + " TIMEOUT");
                    } else if (status.getErrCode() == TStatusCode.THRIFT_RPC_ERROR) {
                        throw new TTransportException(errMsg + " THRIFT_RPC_ERROR");
                    } else {
                        throw new InternalException(errMsg + "UNKONWN");
                    }
                }
                profileFragmentId += 1;
            }
        } finally {
            unlock();
        }
    }

    public List<String> getExportFiles() {
        return exportFiles;
    }

    void updateExportFiles(List<String> files) {
        lock.lock();
        try {
            if (exportFiles == null) {
                exportFiles = Lists.newArrayList();
            }
            exportFiles.addAll(files);
        } finally {
            lock.unlock();
        }
    }

    void updateDeltas(List<String> urls) {
        lock.lock();
        try {
            deltaUrls.addAll(urls);
        } finally {
            lock.unlock();
        }
    }

    private void updateLoadCounters(Map<String, String> newLoadCounters) {
        lock.lock();
        try {
            long numRowsNormal = 0L;
            String value = this.loadCounters.get(LoadEtlTask.DPP_NORMAL_ALL);
            if (value != null) {
                numRowsNormal = Long.valueOf(value);
            }
            long numRowsAbnormal = 0L;
            value = this.loadCounters.get(LoadEtlTask.DPP_ABNORMAL_ALL);
            if (value != null) {
                numRowsAbnormal = Long.valueOf(value);
            }

            // new load counters
            value = newLoadCounters.get(LoadEtlTask.DPP_NORMAL_ALL);
            if (value != null) {
                numRowsNormal += Long.valueOf(value);
            }
            value = newLoadCounters.get(LoadEtlTask.DPP_ABNORMAL_ALL);
            if (value != null) {
                numRowsAbnormal += Long.valueOf(value);
            }

            this.loadCounters.put(LoadEtlTask.DPP_NORMAL_ALL, "" + numRowsNormal);
            this.loadCounters.put(LoadEtlTask.DPP_ABNORMAL_ALL, "" + numRowsAbnormal);
        } finally {
            lock.unlock();
        }
    }

    void updateStatus(Status status) {
        try {
            lock.lock();
            // The query is done and we are just waiting for remote fragments to clean up.
            // Ignore their cancelled updates.
            if (returnedAllResults && status.isCancelled()) {
                return;
            }
            // nothing to update
            if (status.ok()) {
                return;
            }

            // don't override an error status; also, cancellation has already started
            if (!queryStatus.ok()) {
                return;
            }

            queryStatus.setStatus(status);
            LOG.warn("One instance report fail throw updateStatus(), need cancel");
            cancelInternal();
        } finally {
            lock.unlock();
        }

    }

    TResultBatch getNext() throws Exception {
        if (receiver == null) {
            throw new InternalException("There is no receiver.");
        }

        TResultBatch  resultBatch;
        Status status = new Status();

        resultBatch = receiver.getNext(status);
        if (!status.ok()) {
            LOG.warn("get next fail, need cancel");
        }
        updateStatus(status);

        lock();
        Status copyStatus = new Status(queryStatus);
        unlock();

        if (!copyStatus.ok()) {
            if (Strings.isNullOrEmpty(copyStatus.getErrorMsg())) {
                copyStatus.rewriteErrorMsg();
            }
            if (copyStatus.isRpcError()) {
                throw new TTransportException(copyStatus.getErrorMsg());
            } else {

                String errMsg = copyStatus.getErrorMsg();
                LOG.warn("query failed: {}", errMsg);

                // hide host info
                int hostIndex = errMsg.indexOf("host");
                if (hostIndex != -1) {
                    errMsg = errMsg.substring(0, hostIndex);
                }
                throw new InternalException(errMsg);
            }
        }

        if (resultBatch == null) {
            this.returnedAllResults = true;

            // if this query is a block query do not cancel.
            Long numLimitRows  = fragments.get(0).getPlanRoot().getLimit();
            boolean hasLimit = numLimitRows > 0;
            if (!isBlockQuery && numBackends > 1 && hasLimit && numReceivedRows >= numLimitRows) {
                LOG.debug("no block query, return num >= limit rows, need cancel");
                cancelInternal();
            }
        } else {
            numReceivedRows += resultBatch.getRowsSize();
        }

        return resultBatch;
    }

    // Cancel execution of query. This includes the execution of the local plan
    // fragment,
    // if any, as well as all plan fragments on remote nodes.
    public void cancel() {
        lock();
        try {
            if (!queryStatus.ok()) {
                // we can't cancel twice
                return;
            } else {
                queryStatus.setStatus(Status.CANCELLED);
            }
            LOG.warn("cancel execution of query, this is outside invoke");
            cancelInternal();
        } finally {
            unlock();
        }
    }

    private void cancelInternal() {
        if (null != receiver) {
            receiver.cancel();
        }
        cancelRemoteFragments();
    }

    private void cancelRemoteFragments() {
        for (BackendExecState backendExecState : backendExecStates) {
            LOG.warn("cancelRemoteFragments initiated={} done={} hasCanceled={}",
                backendExecState.initiated, backendExecState.done, backendExecState.hasCanceled);
            BackendService.Client client = null;
            TNetworkAddress address = null;
            boolean isReturnToPool = false;
            try {
                backendExecState.lock();

                if (!backendExecState.initiated) {
                    continue;
                }
                // don't cancel if it is already finished
                if (backendExecState.done) {
                    continue;
                }
                if (backendExecState.hasCanceled) {
                    continue;
                }
                TCancelPlanFragmentResult thriftResult = null;
                TCancelPlanFragmentParams rpcParams = new TCancelPlanFragmentParams();
                rpcParams.setProtocol_version(PaloInternalServiceVersion.V1);
                rpcParams.setFragment_instance_id(backendExecState.getFragmentInstanceId());
                int cancelTimeoutMs = 5 * 1000;
                try {
                    address = backendExecState.getBackendAddress();
                    client = ClientPool.backendPool.borrowObject(address,
                            cancelTimeoutMs);
                    LOG.info("cancelRemoteFragments ip={} port={} rpcParams={}", address.hostname, address.port,
                            DebugUtil.printId(rpcParams.fragment_instance_id));
                    thriftResult = client.cancel_plan_fragment(rpcParams);
                    if (thriftResult.status.status_code == TStatusCode.OK) {
                        backendExecState.hasCanceled = true;
                    }
                    isReturnToPool = true;
                } catch (TTransportException e) {
                    LOG.warn("cancelRemoteFragments ip={} port={}", address.hostname, address.port);
                    LOG.warn("", e);
                    // retry
                    boolean ok = false;
                    if (client == null) {
                        client = ClientPool.backendPool.borrowObject(address, cancelTimeoutMs);
                        ok = true;
                    } else {
                        ok = ClientPool.backendPool.reopen(client,
                                cancelTimeoutMs);
                    }
                    if (!ok) {
                        LOG.warn("reopen rpc error" + address.hostname + " exception", e);
                        if (address != null) {
                            SimpleScheduler.updateBlacklistBackends(
                                    this.addressToBackendID.get(address));
                        }
                        continue;
                    }
                    if (e.getType() == TTransportException.TIMED_OUT) {
                        LOG.warn("reopen rpc error" + address.hostname + " exception", e);
                        continue;
                    } else {
                        thriftResult = client.cancel_plan_fragment(rpcParams);
                        isReturnToPool = true;
                    }
                }
            } catch (Exception e) {
                LOG.warn("cancelRemoteFragments fail ip={} port={}", address.hostname, address.port);
                LOG.warn("", e);
            } finally {
                if (isReturnToPool) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
                backendExecState.unlock();
            }
        }
    }

    // fill all the fields in fragmentExecParams
    private void computeFragmentExecParams() throws Exception {
        // fill hosts field in fragmentExecParams
        computeFragmentHosts();

        // assign instance ids
        numBackends = 0;
        for (FragmentExecParams params : fragmentExecParams.values()) {
            LOG.debug("parameter has hosts.{}", params.hosts.size());
            for (int j = 0; j < params.hosts.size(); ++j) {
                // we add instance_num to query_id.lo to create a
                // globally-unique instance id
                TUniqueId instanceId = new TUniqueId();
                instanceId.setHi(queryId.hi);
                instanceId.setLo(queryId.lo + numBackends + 1);
                params.instanceIds.add(instanceId);

                numBackends++;
            }
        }

        // compute destinations and # senders per exchange node
        // (the root fragment doesn't have a destination)
        for (FragmentExecParams params : fragmentExecParams.values()) {
            PlanFragment destFragment = params.fragment.getDestFragment();
            if (destFragment == null) {
                // root plan fragment
                continue;
            }
            FragmentExecParams destParams = fragmentExecParams.get(destFragment.getFragmentId());

            // set # of senders
            DataSink sink = params.fragment.getSink();
            // we can only handle unpartitioned (= broadcast) and
            // hash-partitioned
            // output at the moment

            PlanNodeId exchId = sink.getExchNodeId();
            // we might have multiple fragments sending to this exchange node
            // (distributed MERGE), which is why we need to add up the #senders
            if (destParams.perExchNumSenders.get(exchId.asInt()) == null) {
                destParams.perExchNumSenders.put(exchId.asInt(), params.hosts.size());
            } else {
                destParams.perExchNumSenders.put(exchId.asInt(),
                        params.hosts.size() + destParams.perExchNumSenders.get(exchId.asInt()));
            }

            // add destination host to this fragment's destination
            for (int j = 0; j < destParams.hosts.size(); ++j) {
                TPlanFragmentDestination dest = new TPlanFragmentDestination();
                dest.fragment_instance_id = destParams.instanceIds.get(j);
                dest.server = toRpcHost(destParams.hosts.get(j));
                params.destinations.add(dest);
            }
        }
    }

    private TNetworkAddress toRpcHost(TNetworkAddress host) throws Exception {
        Backend backend = Catalog.getCurrentSystemInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (backend == null) {
            throw new InternalException("there is no scanNode Backend");
        }
        TNetworkAddress dest = new TNetworkAddress(backend.getHost(), backend.getBeRpcPort());
        return dest;
    }

    // For each fragment in fragments, computes hosts on which to run the instances
    // and stores result in fragmentExecParams.hosts.
    private void computeFragmentHosts() throws Exception {
        // compute hosts of producer fragment before those of consumer fragment(s),
        // the latter might inherit the set of hosts from the former
        // compute hosts *bottom up*.
        for (int i = fragments.size() - 1; i >= 0; --i) {
            PlanFragment fragment = fragments.get(i);
            FragmentExecParams params = fragmentExecParams.get(fragment.getFragmentId());

            if (fragment.getDataPartition() == DataPartition.UNPARTITIONED) {
                Reference<Long> backendIdRef = new Reference<Long>();
                TNetworkAddress execHostport = SimpleScheduler.getHost(this.idToBackend, backendIdRef);
                if (execHostport == null) {
                    LOG.warn("DataPartition UNPARTITIONED, no scanNode Backend");
                    throw new InternalException("there is no scanNode Backend");
                }
                this.addressToBackendID.put(execHostport, backendIdRef.getRef());

                params.hosts.add(execHostport);
                continue;
            }

            PlanNode leftMostNode = findLeftmostNode(fragment.getPlanRoot());
            if (!(leftMostNode instanceof ScanNode)) {
                // there is no leftmost scan; we assign the same hosts as those of our
                // leftmost input fragment (so that a partitioned aggregation
                // fragment runs on the hosts that provide the input data)
                PlanFragmentId inputFragmentIdx =
                    fragments.get(i).getChild(0).getFragmentId();
                // AddAll() soft copy()
                params.hosts.addAll(fragmentExecParams.get(inputFragmentIdx).hosts);
                // TODO: switch to unpartitioned/coord execution if our input fragment
                // is executed that way (could have been downgraded from distributed)
                continue;
            }

            Iterator iter = scanRangeAssignment.get(fragment.getFragmentId()).entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                TNetworkAddress key = (TNetworkAddress) entry.getKey();
                params.hosts.add(key);
            }
            if (params.hosts.isEmpty()) {
                Reference<Long> backendIdRef = new Reference<Long>();
                TNetworkAddress execHostport = SimpleScheduler.getHost(this.idToBackend, backendIdRef);
                if (execHostport == null) {
                    throw new InternalException("there is no scanNode Backend");
                }
                this.addressToBackendID.put(execHostport, backendIdRef.getRef());

                params.hosts.add(execHostport);
            }
        }
    }

    // Returns the id of the leftmost node of any of the gives types in 'plan_root',
    // or INVALID_PLAN_NODE_ID if no such node present.
    private PlanNode findLeftmostNode(PlanNode plan) {
        PlanNode newPlan = plan;
        while (newPlan.getChildren().size() != 0) {
            newPlan = newPlan.getChild(0);
        }
        return newPlan;
    }

    private <K, V> V findOrInsert(HashMap<K, V> m, final K key, final V defaultVal) {
        V value = m.get(key);
        if (value == null) {
            m.put(key, defaultVal);
            value = defaultVal;
        }
        return value;
    }

    // weather we can overwrite the first parameter or not?
    private List<TScanRangeParams> findOrInsert(Map<Integer, List<TScanRangeParams>> m, Integer key,
            ArrayList<TScanRangeParams> defaultVal) {
        List<TScanRangeParams> value = m.get(key);
        if (value == null) {
            m.put(key, defaultVal);
            value = defaultVal;
        }
        return value;
    }

    private long getScanRangeLength(final TScanRange scanRange) {
        return 1;
    }

    // Populates scan_range_assignment_.
    // <fragment, <server, nodeId>>
    private void computeScanRangeAssignment() throws Exception {
        // set scan ranges/locations for scan nodes
        for (ScanNode scanNode : scanNodes) {
            // the parameters of getScanRangeLocations may ignore, It dosn't take effect
            List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(0);
            if (locations == null) {
                // only analysis olap scan node
                continue;
            }

            FragmentScanRangeAssignment assignment =
                    scanRangeAssignment.get(scanNode.getFragmentId());
            computeScanRangeAssignment(scanNode.getId(), locations, assignment);
        }
    }

    // Does a scan range assignment (returned in 'assignment') based on a list
    // of scan range locations for a particular node.
    // If exec_at_coord is true, all scan ranges will be assigned to the coord node.
    private void computeScanRangeAssignment(
            final PlanNodeId nodeId,
            final List<TScanRangeLocations> locations,
            FragmentScanRangeAssignment assignment) throws Exception {
        HashMap<TNetworkAddress, Long> assignedBytesPerHost = Maps.newHashMap();
        for (TScanRangeLocations scanRangeLocations : locations) {
            // assign this scan range to the host w/ the fewest assigned bytes
            Long minAssignedBytes = Long.MAX_VALUE;
            TScanRangeLocation minLocation = null;
            for (final TScanRangeLocation location : scanRangeLocations.getLocations()) {
                Long assignedBytes = findOrInsert(assignedBytesPerHost, location.server, 0L);
                if (assignedBytes < minAssignedBytes) {
                    minAssignedBytes = assignedBytes;
                    minLocation = location;
                }
            }
            Long scanRangeLength = getScanRangeLength(scanRangeLocations.scan_range);
            assignedBytesPerHost.put(minLocation.server,
                    assignedBytesPerHost.get(minLocation.server) + scanRangeLength);

            Reference<Long> backendIdRef = new Reference<Long>();
            TNetworkAddress execHostPort = SimpleScheduler.getHost(minLocation.backend_id,
                    scanRangeLocations.getLocations(), this.idToBackend, backendIdRef);
            if (execHostPort == null) {
                throw new InternalException("there is no scanNode Backend");
            }
            this.addressToBackendID.put(execHostPort, backendIdRef.getRef());

            Map<Integer, List<TScanRangeParams>> scanRanges = findOrInsert(assignment, execHostPort,
                new HashMap<Integer, List<TScanRangeParams>>());
            List<TScanRangeParams> scanRangeParamsList =
                findOrInsert(scanRanges, nodeId.asInt(), new ArrayList<TScanRangeParams>());
            // add scan range
            TScanRangeParams scanRangeParams = new TScanRangeParams();
            scanRangeParams.scan_range = scanRangeLocations.scan_range;
            // Volume is is optional, so we need to set the value and the is-set bit
            scanRangeParams.setVolume_id(minLocation.volume_id);
            scanRangeParamsList.add(scanRangeParams);
        }
    }

    public void updateFragmentExecStatus(TReportExecStatusParams params) {
        if (params.backend_num >= backendExecStates.size()) {
            LOG.error("unknown backend number");
            return;
        }

        boolean done = false;
        BackendExecState execState = backendExecStates.get(params.backend_num);
        execState.lock();
        try {
            if (execState.done) {
                // duplicate packet
                return;
            }
            execState.profile.update(params.profile);
            done = params.done;
            execState.done = params.done;
        } finally {
            execState.unlock();
        }

        // print fragment instance profile
        if (LOG.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            execState.profile().prettyPrint(builder, "");
            LOG.debug("profile for query_id={} instance_id={}\n{}",
                    DebugUtil.printId(queryId),
                    DebugUtil.printId(params.getFragment_instance_id()),
                    builder.toString());
        }

        Status status = new Status(params.status);
        // for now, abort the query if we see any error except if the error is cancelled
        // and returned_all_results_ is true.
        // (UpdateStatus() initiates cancellation, if it hasn't already been initiated)
        if (!(returnedAllResults && status.isCancelled()) && !status.ok()) {
            LOG.warn("One instance report fail, query_id={} instance_id={}",
                    DebugUtil.printId(queryId),
                    DebugUtil.printId(params.getFragment_instance_id()));
            updateStatus(status);
        }
        if (done) {
            if (params.isSetDelta_urls()) {
                updateDeltas(params.getDelta_urls());
            }
            if (params.isSetLoad_counters()) {
                updateLoadCounters(params.getLoad_counters());
            }
            if (params.isSetTracking_url()) {
                trackingUrl = params.tracking_url;
            }
            if (params.isSetExport_files()) {
                updateExportFiles(params.export_files);
            }
            profileDoneSignal.countDown();
        }

        return;
    }

    public void endProfile() {
        if (backendExecStates.isEmpty()) {
            return;
        }

        // wait for all backends
        if (needReport) {
            try {
                profileDoneSignal.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e1) {
                LOG.warn("signal await error", e1);
            }
        }

        for (int i = 0; i < backendExecStates.size(); ++i) {
            if (backendExecStates.get(i) == null) {
                continue;
            }
            BackendExecState backendExecState = backendExecStates.get(i);
            backendExecState.profile().computeTimeInProfile();

            int profileFragmentId = backendExecState.profileFragmentId();
            if (profileFragmentId < 0 || profileFragmentId > fragmentProfile.size()) {
                LOG.error("profileFragmentId " + profileFragmentId
                        + " should be in [0," + fragmentProfile.size() + ")");
                return;
            }
            fragmentProfile.get(profileFragmentId).addChild(backendExecState.profile());
        }

        for (int i = 1; i < fragmentProfile.size(); ++i) {
            fragmentProfile.get(i).sortChildren();
        }
    }

    public boolean join(int seconds) {
        try {
            return profileDoneSignal.await(seconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // Do nothing
        }
        return false;
    }

    public boolean isDone() {
        return profileDoneSignal.getCount() == 0;
    }

    // map from an impalad host address to the per-node assigned scan ranges;
    // records scan range assignment for a single fragment
    class FragmentScanRangeAssignment
            extends HashMap<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> {
    }

    // record backend execute state
    // TODO(zhaochun): add profile information and others
    public class BackendExecState {
        TExecPlanFragmentParams rpcParams;
        private PlanFragmentId fragmentId;
        private int            instanceId;
        private boolean initiated;
        private boolean done;
        private boolean hasCanceled;
        private Lock lock = new ReentrantLock();
        private int profileFragmentId;
        RuntimeProfile profile;
        Map<TNetworkAddress, Long> addressToBackendID;

        public int profileFragmentId() {
            return profileFragmentId;
        }

        public boolean initiated() {
            return initiated;
        }

        public RuntimeProfile profile() {
            return profile;
        }

        public void lock() {
            lock.lock();
        }

        public void unlock() {
            lock.unlock();
        }

        public BackendExecState(PlanFragmentId fragmentId, int instanceId, int profileFragmentId,
            TExecPlanFragmentParams rpcParams, Map<TNetworkAddress, Long> addressToBackendID) {
            this.profileFragmentId = profileFragmentId;
            this.fragmentId = fragmentId;
            this.instanceId = instanceId;
            this.rpcParams = rpcParams;
            this.initiated = false;
            this.done = false;
            String name = "Instance " + DebugUtil.printId(fragmentExecParams.get(fragmentId)
                    .instanceIds.get(instanceId)) + " (host=" + getBackendAddress() + ")";
            this.profile = new RuntimeProfile(name);
            this.hasCanceled = false;
            this.addressToBackendID = addressToBackendID;
        }

        public final TNetworkAddress getBackendAddress() {
            return fragmentExecParams.get(fragmentId).hosts.get(instanceId);
        }

        public TUniqueId getFragmentInstanceId() {
            return this.rpcParams.params.getFragment_instance_id();
        }

        public void execRemoteFragment() throws Exception {
            TExecPlanFragmentResult thriftResult = null;
            BackendService.Client client = null;
            TNetworkAddress address = null;
            int execRemoteTimeoutMs = 5 * 1000;
            boolean isReturnToPool = false;
            try {
                try {
                    address = getBackendAddress();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("exec address={} fragmentInstanceId={} rpcParams={}", address,
                                DebugUtil.printId(rpcParams.params.fragment_instance_id), rpcParams);
                    }
                    client = ClientPool.backendPool.borrowObject(address, execRemoteTimeoutMs);
                    thriftResult = client.exec_plan_fragment(rpcParams);
                    isReturnToPool = true;
                } catch (TTransportException e) {
                    LOG.warn("execRemoteFragment TTransporxception address={}", address, e);
                    // retry
                    if (client == null) {
                        LOG.warn("may be connection refuse, try to retry from top");
                        if (address != null) {
                            SimpleScheduler.updateBlacklistBackends(
                                    this.addressToBackendID.get(address));
                        }
                        throw e; // may be connection refuse, we may retry from top 3 retry.
                    }
                    boolean ok = ClientPool.backendPool.reopen(client,
                            execRemoteTimeoutMs);
                    if (!ok) {
                        if (address != null) {
                            SimpleScheduler.updateBlacklistBackends(
                                    this.addressToBackendID.get(address));
                        }
                        LOG.warn("reopen rpc error address=" + address);
                        throw e;
                    }

                    if (e.getType() == TTransportException.TIMED_OUT) {
                        throw e;
                    } else {
                        thriftResult = client.exec_plan_fragment(rpcParams);
                        isReturnToPool = true;
                    }
                }
            } catch (org.apache.thrift.TApplicationException e) {
                SimpleScheduler.updateBlacklistBackends(this.addressToBackendID.get(address));
                LOG.warn("execRemoteFragment Exception ", e);
                throw e;
            } catch (Exception e) {
                LOG.warn("execRemoteFragment Exception " + DebugUtil.getStackTrace(e));
                throw e;
            } finally {
                if (isReturnToPool) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
            if (thriftResult != null) {
                if (!thriftResult.getStatus().getStatus_code().equals(TStatusCode.OK)) {
                    String errMsg = thriftResult.getStatus().getError_msgs().get(0);
                    LOG.warn("exec_plan_fragment get wrong result, err_msg =" + errMsg);
                    throw new Exception(errMsg);
                }
            }

            initiated = true;
        }
    }

    // execution parameters for a single fragment; used to assemble the
    // per-fragment instance TPlanFragmentExecParams;
    // hosts.size() == instance_ids.size()
    protected class FragmentExecParams {
        public PlanFragment fragment;
        public List<TNetworkAddress>          hosts             = Lists.newArrayList();
        public List<TUniqueId>                instanceIds       = Lists.newArrayList();
        public List<TPlanFragmentDestination> destinations      = Lists.newArrayList();
        public Map<Integer, Integer>          perExchNumSenders = Maps.newHashMap();

        public FragmentExecParams(PlanFragment fragment) {
            this.fragment = fragment;
        }

        List<TExecPlanFragmentParams> toThrift(int backendNum) {
            List<TExecPlanFragmentParams> paramsList = Lists.newArrayList();

            int tmpBackendNum = backendNum;
            for (int i = 0; i < instanceIds.size(); ++i) {
                TExecPlanFragmentParams params = new TExecPlanFragmentParams();
                params.setProtocol_version(PaloInternalServiceVersion.V1);
                params.setFragment(fragment.toThrift());
                params.setDesc_tbl(descTable);
                params.setParams(new TPlanFragmentExecParams());
                params.setResource_info(tResourceInfo);
                params.params.setQuery_id(queryId);
                params.params.setFragment_instance_id(instanceIds.get(i));
                TNetworkAddress address = fragmentExecParams.get(fragment.getFragmentId()).hosts.get(i);

                Map<Integer, List<TScanRangeParams>> scanRanges =
                        scanRangeAssignment.get(fragment.getFragmentId()).get(address);
                if (scanRanges == null) {
                    scanRanges = Maps.newHashMap();
                }
                params.params.setPer_node_scan_ranges(scanRanges);
                params.params.setPer_exch_num_senders(perExchNumSenders);
                params.params.setDestinations(destinations);
                params.params.setSender_id(i);
                params.setCoord(coordAddress);
                params.setBackend_num(tmpBackendNum++);
                params.setQuery_globals(queryGlobals);
                params.setQuery_options(queryOptions);

                paramsList.add(params);
            }
            return paramsList;
        }

        // Append range information
        // [tablet_id(version),tablet_id(version)]
        public void appendScanRange(StringBuilder sb, List<TScanRangeParams> params) {
            sb.append("range=[");
            int idx = 0;
            for (TScanRangeParams range : params) {
                TPaloScanRange paloScanRange = range.getScan_range().getPalo_scan_range();
                if (paloScanRange != null) {
                    if (idx++ != 0) {
                        sb.append(",");
                    }
                    sb.append("{tid=").append(paloScanRange.getTablet_id())
                            .append(",ver=").append(paloScanRange.getVersion()).append("}");
                }
            }
            sb.append("]");
        }

        public void appendTo(StringBuilder sb) {
            // append fragment
            sb.append("{plan=");
            fragment.getPlanRoot().appendTrace(sb);
            sb.append(",instance=[");
            // append instance
            for (int i = 0; i < instanceIds.size(); ++i) {
                if (i != 0) {
                    sb.append(",");
                }
                TNetworkAddress address = fragmentExecParams.get(fragment.getFragmentId()).hosts.get(i);
                Map<Integer, List<TScanRangeParams>> scanRanges =
                        scanRangeAssignment.get(fragment.getFragmentId()).get(address);
                sb.append("{");
                sb.append("id=").append(DebugUtil.printId(instanceIds.get(i)));
                sb.append(",host=").append(hosts.get(i).getHostname());
                if (scanRanges == null) {
                    sb.append("}");
                    continue;
                }
                sb.append(",range=[");
                int eIdx = 0;
                for (Map.Entry<Integer, List<TScanRangeParams>> entry : scanRanges.entrySet()) {
                    if (eIdx++ != 0) {
                        sb.append(",");
                    }
                    sb.append("id").append(entry.getKey()).append(",");
                    appendScanRange(sb, entry.getValue());
                }
                sb.append("]");
                sb.append("}");
            }
            sb.append("]"); // end of instances
            sb.append("}");
        }
    }

    private static class ParallelExecutor {
        private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

        public static void exec(List<Coordinator.BackendExecState> args,
                int beginPos,
                int numArgs,
                ExecStatus status)
                throws InterruptedException, TException {
            CountDownLatch latch = new CountDownLatch(numArgs);
            for (int i = 0; i < numArgs; ++i) {
                Runnable r = new RpcThread(args.get(beginPos + i), latch, status);
                EXECUTOR.submit(r);
            }
            // timeOut cancel, no need to lock
            // for after do exec, we can cancel and update
            // thrift rpc default timeout is 5 secs.
            // we make latch wait 10 secs(> 5 sec) to avoid false timeout
            if (!latch.await(10, TimeUnit.SECONDS)) {
                status.setErrCode(TStatusCode.TIMEOUT);
            }
        }

        private static class RpcThread implements Runnable {
            private Coordinator.BackendExecState parameter;
            private CountDownLatch               latch;
            private ExecStatus status;

            public RpcThread(Coordinator.BackendExecState parameter, CountDownLatch latch,
                    ExecStatus status) {
                // store parameter for later user
                this.parameter = parameter;
                this.latch = latch;
                // there is no need to lock needCanceled, because we only set needCanceled true
                this.status = status;
            }

            public void run() {
                try {
                    parameter.execRemoteFragment();
                } catch (TTransportException e) {
                    if (e.getType() == TTransportException.TIMED_OUT) {
                        status.setErrCode(TStatusCode.TIMEOUT);
                        status.setErrAddress(parameter.getBackendAddress());
                    } else {
                        status.setErrCode(TStatusCode.THRIFT_RPC_ERROR);
                        status.setErrAddress(parameter.getBackendAddress());
                    }
                    LOG.warn("ParallelExecutor get exception: {}", parameter.getBackendAddress(), e);
                } catch (Exception e) {
                    status.setErrCode(TStatusCode.INTERNAL_ERROR);
                    status.setErrAddress(parameter.getBackendAddress());
                    LOG.warn("ParallelExecutor get exception: {}", parameter.getBackendAddress(), e);
                }
                latch.countDown();
            }
        }
    }

}
