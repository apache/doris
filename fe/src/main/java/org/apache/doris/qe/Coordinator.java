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

package org.apache.doris.qe;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Reference;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.ListUtil;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.load.LoadErrorHub;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.proto.PExecPlanFragmentResult;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TEsScanRange;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TLoadErrorHubInfo;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TPlanFragmentDestination;
import org.apache.doris.thrift.TPlanFragmentExecParams;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    private Map<PlanFragmentId, FragmentExecParams> fragmentExecParamsMap = Maps.newHashMap();

    private List<PlanFragment> fragments;
    // backend execute state
    private List<BackendExecState> backendExecStates = Lists.newArrayList();
    private ResultReceiver receiver;
    // fragment instance id to backend state
    private ConcurrentMap<TUniqueId, BackendExecState> backendExecStateMap =
            Maps.newConcurrentMap();
    private List<ScanNode> scanNodes;
    // number of instances of this query, equals to
    // number of backends executing plan fragments on behalf of this query;
    // set in computeFragmentExecParams();
    // same as backend_exec_states_.size() after Exec()
    private Set<TUniqueId> instanceIds = Sets.newHashSet();
    // instance id -> dummy value
    private MarkedCountDownLatch<TUniqueId, Long> profileDoneSignal;

    private boolean isBlockQuery;

    private int numReceivedRows = 0;

    private List<String> deltaUrls;
    private Map<String, String> loadCounters;
    private String trackingUrl;

    // for export
    private List<String> exportFiles;

    private List<TTabletCommitInfo> commitInfos = Lists.newArrayList();

    // Input parameter
    private long jobId = -1; // job which this task belongs to
    private TUniqueId queryId;
    private TResourceInfo tResourceInfo;
    private boolean needReport;

    private String clusterName;
    // parallel execute
    private final TUniqueId nextInstanceId;

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
        this.tResourceInfo = new TResourceInfo(context.getQualifiedUser(),
                context.getSessionVariable().getResourceGroup());
        this.needReport = context.getSessionVariable().isReportSucc();
        this.clusterName = context.getClusterName();
        this.nextInstanceId = new TUniqueId();
        nextInstanceId.setHi(queryId.hi);
        nextInstanceId.setLo(queryId.lo + 1);
    }

    // Used for pull load task coordinator
    public Coordinator(Long jobId, TUniqueId queryId, DescriptorTable descTable,
            List<PlanFragment> fragments, List<ScanNode> scanNodes, String cluster) {
        this.isBlockQuery = true;
        this.jobId = jobId;
        this.queryId = queryId;
        this.descTable = descTable.toThrift();
        this.fragments = fragments;
        this.scanNodes = scanNodes;
        this.queryOptions = new TQueryOptions();
        this.queryGlobals.setNow_string(DATE_FORMAT.format(new Date()));
        this.tResourceInfo = new TResourceInfo("", "");
        this.needReport = true;
        this.clusterName = cluster;
        this.nextInstanceId = new TUniqueId();
        nextInstanceId.setHi(queryId.hi);
        nextInstanceId.setLo(queryId.lo + 1);
    }

    public long getJobId() {
        return jobId;
    }

    public TUniqueId getQueryId() {
        return queryId;
    }

    public void setQueryId(TUniqueId queryId) {
        this.queryId = queryId;
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

    public void clearExportStatus() {
        lock.lock();
        try {
            this.backendExecStates.clear();
            this.backendExecStateMap.clear();
            this.queryStatus.setStatus(new Status());
            if (this.exportFiles == null) {
                this.exportFiles = Lists.newArrayList();
            }
            this.exportFiles.clear();
        } finally {
            lock.unlock();
        }
    }

    public List<TTabletCommitInfo> getCommitInfos() {
        return commitInfos;
    }

    // Initiate
    private void prepare() {
        
        for (PlanFragment fragment : fragments) {
            fragmentExecParamsMap.put(fragment.getFragmentId(), new FragmentExecParams(fragment));
        }
    
        // set inputFragments
        for (PlanFragment fragment : fragments) {
            if (!(fragment.getSink() instanceof DataStreamSink)) {
                continue;
            }
            FragmentExecParams params = fragmentExecParamsMap.get(fragment.getDestFragment().getFragmentId());
            params.inputFragments.add(fragment.getFragmentId());

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
        sb.append("query id=").append(DebugUtil.printId(queryId)).append(",");
        sb.append("fragment=[");
        for (Map.Entry<PlanFragmentId, FragmentExecParams> entry : fragmentExecParamsMap.entrySet()) {
            if (idx++ != 0) {
                sb.append(",");
            }
            sb.append(entry.getKey());
            entry.getValue().appendTo(sb);
        }
        sb.append("]");

        LOG.info(sb.toString());
    }

    // Initiate asynchronous execution of query. Returns as soon as all plan fragments
    // have started executing at their respective backends.
    // 'Request' must contain at least a coordinator plan fragment (ie, can't
    // be for a query like 'SELECT 1').
    // A call to Exec() must precede all other member function calls.
    public void exec() throws Exception {
        if (!scanNodes.isEmpty()) {
            LOG.debug("debug: in Coordinator::exec. query id: {}, planNode: {}",
                    DebugUtil.printId(queryId), scanNodes.get(0).treeToThrift());
        }

        if (!fragments.isEmpty()) {
            LOG.debug("debug: in Coordinator::exec. query id: {}, fragment: {}",
                    DebugUtil.printId(queryId), fragments.get(0).toThrift());
        }

        // prepare information
        prepare();
        // compute Fragment Instance
        computeScanRangeAssignment();

        computeFragmentExecParams();

        traceInstance();

        // create result receiver
        PlanFragmentId topId = fragments.get(0).getFragmentId();
        FragmentExecParams topParams = fragmentExecParamsMap.get(topId);
        if (topParams.fragment.getSink() instanceof ResultSink) {
            receiver = new ResultReceiver(
                    topParams.instanceExecParams.get(0).instanceId,
                    addressToBackendID.get(topParams.instanceExecParams.get(0).host),
                    toBrpcHost(topParams.instanceExecParams.get(0).host),
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
        profileDoneSignal = new MarkedCountDownLatch<TUniqueId, Long>(instanceIds.size());
        for (TUniqueId instanceId : instanceIds) {
            profileDoneSignal.addMark(instanceId, -1L /* value is meaningless */);
        }
        lock();
        try {
            // execute all instances from up to bottom
            int backendId = 0;
            int profileFragmentId = 0;
            long memoryLimit = queryOptions.getMem_limit();
            for (PlanFragment fragment : fragments) {
                FragmentExecParams params = fragmentExecParamsMap.get(fragment.getFragmentId());
                
                // set up exec states
                int instanceNum = params.instanceExecParams.size();
                Preconditions.checkState(instanceNum > 0);
                List<TExecPlanFragmentParams> tParams = params.toThrift(backendId);
                List<Pair<BackendExecState, Future<PExecPlanFragmentResult>>> futures = Lists.newArrayList();

                //update memory limit for colocate join
                if (colocateFragmentIds.contains(fragment.getFragmentId().asInt())) {
                    int rate = Math.min(Config.query_colocate_join_memory_limit_penalty_factor, instanceNum);
                    long newmemory = memoryLimit / rate;

                    for (TExecPlanFragmentParams tParam : tParams) {
                        tParam.query_options.setMem_limit(newmemory);
                    }
                }

                int instanceId = 0;
                for (TExecPlanFragmentParams tParam : tParams) {
                    // TODO: pool of pre-formatted BackendExecStates?
                    BackendExecState execState =
                            new BackendExecState(fragment.getFragmentId(), instanceId++,
                                    profileFragmentId, tParam, this.addressToBackendID);
                    backendExecStates.add(execState);
                    backendExecStateMap.put(tParam.params.getFragment_instance_id(), execState);

                    futures.add(Pair.create(execState, execState.execRemoteFragmentAsync()));

                    backendId++;
                }

                for (Pair<BackendExecState, Future<PExecPlanFragmentResult>> pair : futures) {
                    TStatusCode code = TStatusCode.INTERNAL_ERROR;
                    String errMsg = null;
                    try {
                        PExecPlanFragmentResult result = pair.second.get(Config.remote_fragment_exec_timeout_ms,
                                                                         TimeUnit.MILLISECONDS);
                        code = TStatusCode.findByValue(result.status.status_code);
                        if (result.status.error_msgs != null && !result.status.error_msgs.isEmpty()) {
                            errMsg = result.status.error_msgs.get(0);
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
                        if (errMsg == null) {
                            errMsg = "exec rpc error. backend id: " + pair.first.systemBackendId;
                        }
                        queryStatus.setStatus(errMsg);
                        LOG.warn("exec plan fragment failed, errmsg={}, fragmentId={}, backend={}:{}",
                                 errMsg, fragment.getFragmentId(),
                                 pair.first.address.hostname, pair.first.address.port);
                        cancelInternal();
                        switch (code) {
                            case TIMEOUT:
                                throw new UserException("query timeout. backend id: " + pair.first.systemBackendId);
                            case THRIFT_RPC_ERROR:
                                SimpleScheduler.updateBlacklistBackends(pair.first.systemBackendId);
                                throw new RpcException("rpc failed. backend id: " + pair.first.systemBackendId);
                            default:
                                throw new UserException(errMsg);
                        }
                    }
                }
                profileFragmentId += 1;
            }
            attachInstanceProfileToFragmentProfile();
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

    private void updateCommitInfos(List<TTabletCommitInfo> commitInfos) {
        lock.lock();
        try {
            this.commitInfos.addAll(commitInfos);
        } finally {
            lock.unlock();
        }
    }

    private void updateStatus(Status status, TUniqueId instanceId) {
        lock.lock();
        try {
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
            LOG.warn("one instance report fail throw updateStatus(), need cancel. job id: {}, query id: {}, instance id: {}",
                    jobId, DebugUtil.printId(queryId), instanceId != null ? DebugUtil.printId(instanceId) : "NaN");
            cancelInternal();
        } finally {
            lock.unlock();
        }
    }

    public RowBatch getNext() throws Exception {
        if (receiver == null) {
            throw new UserException("There is no receiver.");
        }

        RowBatch resultBatch;
        Status status = new Status();

        resultBatch = receiver.getNext(status);
        if (!status.ok()) {
            LOG.warn("get next fail, need cancel. query id: {}", DebugUtil.printId(queryId));
        }
        updateStatus(status, null /* no instance id */);

        Status copyStatus = null;
        lock();
        try {
            copyStatus = new Status(queryStatus);
        } finally {
            unlock();
        }

        if (!copyStatus.ok()) {
            if (Strings.isNullOrEmpty(copyStatus.getErrorMsg())) {
                copyStatus.rewriteErrorMsg();
            }
            if (copyStatus.isRpcError()) {
                throw new RpcException(copyStatus.getErrorMsg());
            } else {
                String errMsg = copyStatus.getErrorMsg();
                LOG.warn("query failed: {}", errMsg);

                // hide host info
                int hostIndex = errMsg.indexOf("host");
                if (hostIndex != -1) {
                    errMsg = errMsg.substring(0, hostIndex);
                }
                throw new UserException(errMsg);
            }
        }

        if (resultBatch.isEos()) {
            this.returnedAllResults = true;

            // if this query is a block query do not cancel.
            Long numLimitRows  = fragments.get(0).getPlanRoot().getLimit();
            boolean hasLimit = numLimitRows > 0;
            if (!isBlockQuery && instanceIds.size() > 1 && hasLimit && numReceivedRows >= numLimitRows) {
                LOG.debug("no block query, return num >= limit rows, need cancel");
                cancelInternal();
            }
        } else {
            numReceivedRows += resultBatch.getBatch().getRowsSize();
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
        cancelRemoteFragmentsAsync();
        if (profileDoneSignal != null) {
            // count down to zero to notify all objects waiting for this
            profileDoneSignal.countDownToZero();
            LOG.info("unfinished instance: {}", profileDoneSignal.getLeftMarks());
        }
    }

    private void cancelRemoteFragmentsAsync() {
        for (BackendExecState backendExecState : backendExecStates) {
            TNetworkAddress address = backendExecState.getBackendAddress();
            LOG.info("cancelRemoteFragments initiated={} done={} hasCanceled={} ip={} port={} fragment instance id={}",
                    backendExecState.initiated, backendExecState.done, backendExecState.hasCanceled,
                    address.hostname, address.port, DebugUtil.printId(backendExecState.getFragmentInstanceId()));

            backendExecState.lock();
            try {
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
                TNetworkAddress brpcAddress = toBrpcHost(address);

                try {
                    BackendServiceProxy.getInstance().cancelPlanFragmentAsync(
                            brpcAddress, backendExecState.getFragmentInstanceId());
                } catch (RpcException e) {
                    LOG.warn("cancel plan fragment get a exception, address={}:{}",
                            brpcAddress.getHostname(), brpcAddress.getPort());
                    SimpleScheduler.updateBlacklistBackends(addressToBackendID.get(brpcAddress));
                }

                backendExecState.hasCanceled = true;
            } catch (Exception e) {
                LOG.warn("catch a exception", e);
            } finally {
                backendExecState.unlock();
            }
        }
    }

    private void computeFragmentExecParams() throws Exception {
        // fill hosts field in fragmentExecParams
        computeFragmentHosts();

        // assign instance ids
        instanceIds.clear();
        for (FragmentExecParams params : fragmentExecParamsMap.values()) {
            LOG.debug("fragment {} has instances {}", params.fragment.getFragmentId(), params.instanceExecParams.size());
            for (int j = 0; j < params.instanceExecParams.size(); ++j) {
                // we add instance_num to query_id.lo to create a
                // globally-unique instance id
                TUniqueId instanceId = new TUniqueId();
                instanceId.setHi(queryId.hi);
                instanceId.setLo(queryId.lo + instanceIds.size() + 1);
                params.instanceExecParams.get(j).instanceId = instanceId;
                instanceIds.add(instanceId);
            }
        }

        // compute destinations and # senders per exchange node
        // (the root fragment doesn't have a destination)
        for (FragmentExecParams params : fragmentExecParamsMap.values()) {
            PlanFragment destFragment = params.fragment.getDestFragment();
            if (destFragment == null) {
                // root plan fragment
                continue;
            }
            FragmentExecParams destParams = fragmentExecParamsMap.get(destFragment.getFragmentId());

            // set # of senders
            DataSink sink = params.fragment.getSink();
            // we can only handle unpartitioned (= broadcast) and
            // hash-partitioned
            // output at the moment

            PlanNodeId exchId = sink.getExchNodeId();
            // we might have multiple fragments sending to this exchange node
            // (distributed MERGE), which is why we need to add up the #senders
            if (destParams.perExchNumSenders.get(exchId.asInt()) == null) {
                destParams.perExchNumSenders.put(exchId.asInt(), params.instanceExecParams.size());
            } else {
                destParams.perExchNumSenders.put(exchId.asInt(),
                        params.instanceExecParams.size() + destParams.perExchNumSenders.get(exchId.asInt()));
            }

            // add destination host to this fragment's destination
            for (int j = 0; j < destParams.instanceExecParams.size(); ++j) {
                TPlanFragmentDestination dest = new TPlanFragmentDestination();
                dest.fragment_instance_id = destParams.instanceExecParams.get(j).instanceId;
                dest.server = toRpcHost(destParams.instanceExecParams.get(j).host);
                dest.setBrpc_server(toBrpcHost(destParams.instanceExecParams.get(j).host));
                params.destinations.add(dest);
            }
        }
    }

    private TNetworkAddress toRpcHost(TNetworkAddress host) throws Exception {
        Backend backend = Catalog.getCurrentSystemInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (backend == null) {
            throw new UserException("there is no scanNode Backend");
        }
        TNetworkAddress dest = new TNetworkAddress(backend.getHost(), backend.getBeRpcPort());
        return dest;
    }

    private TNetworkAddress toBrpcHost(TNetworkAddress host) throws Exception {
        Backend backend = Catalog.getCurrentSystemInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (backend == null) {
            throw new UserException("there is no scanNode Backend");
        }
        if (backend.getBrpcPort() < 0) {
            return null;
        }
        return new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
    }

    // estimate if this fragment contains UnionNode
    private boolean containsUnionNode(PlanNode node) {
        if (node instanceof UnionNode) {
            return true;
        }

        for (PlanNode child : node.getChildren()) {
            if (child instanceof ExchangeNode) {
                // Ignore other fragment's node
                continue;
            } else if (child instanceof UnionNode) {
                return true;
            } else {
                return containsUnionNode(child);
            }
        }
        return false;
    }

    // For each fragment in fragments, computes hosts on which to run the instances
    // and stores result in fragmentExecParams.hosts.
    private void computeFragmentHosts() throws Exception {
        // compute hosts of producer fragment before those of consumer fragment(s),
        // the latter might inherit the set of hosts from the former
        // compute hosts *bottom up*.
        for (int i = fragments.size() - 1; i >= 0; --i) {
            PlanFragment fragment = fragments.get(i);
            FragmentExecParams params = fragmentExecParamsMap.get(fragment.getFragmentId());

            if (fragment.getDataPartition() == DataPartition.UNPARTITIONED) {
                Reference<Long> backendIdRef = new Reference<Long>();
                TNetworkAddress execHostport = SimpleScheduler.getHost(this.idToBackend, backendIdRef);
                if (execHostport == null) {
                    LOG.warn("DataPartition UNPARTITIONED, no scanNode Backend");
                    throw new UserException("there is no scanNode Backend");
                }
                this.addressToBackendID.put(execHostport, backendIdRef.getRef());
                FInstanceExecParam instanceParam = new FInstanceExecParam(null, execHostport, 
                        0, params);
                params.instanceExecParams.add(instanceParam);
                continue;
            }

            PlanNode leftMostNode = findLeftmostNode(fragment.getPlanRoot());
            // When fragment contains UnionNode, because  the fragment may has child
            // and not all BE will receive the fragment, child fragment's dest must
            // be BE that fragment's scannode locates,  avoid less data.
            // chenhao added
            boolean hasUnionNode = containsUnionNode(fragment.getPlanRoot());
            if (!(leftMostNode instanceof ScanNode) && !hasUnionNode) {
                // there is no leftmost scan; we assign the same hosts as those of our
                // leftmost input fragment (so that a partitioned aggregation
                // fragment runs on the hosts that provide the input data)
                PlanFragmentId inputFragmentIdx =
                    fragments.get(i).getChild(0).getFragmentId();
                // AddAll() soft copy()
                for (FInstanceExecParam execParams 
                        : fragmentExecParamsMap.get(inputFragmentIdx).instanceExecParams) {
                    FInstanceExecParam instanceParam = new FInstanceExecParam(null, execParams.host, 
                            0, params);
                    params.instanceExecParams.add(instanceParam);
                }
                
                // TODO: switch to unpartitioned/coord execution if our input fragment
                // is executed that way (could have been downgraded from distributed)
                continue;
            }

            //for ColocateJoin fragment
            if (bucketSeqToAddress.size() > 0 && isColocateJoin(fragment.getPlanRoot())) {
                for (Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>> scanRanges : bucketSeqToScanRange.entrySet()) {
                    FInstanceExecParam instanceParam = new FInstanceExecParam(null, bucketSeqToAddress.get(scanRanges.getKey()), 0, params);

                    Map<Integer, List<TScanRangeParams>> nodeScanRanges = scanRanges.getValue();
                    for (Map.Entry<Integer, List<TScanRangeParams>> nodeScanRange : nodeScanRanges.entrySet()) {
                        instanceParam.perNodeScanRanges.put(nodeScanRange.getKey(), nodeScanRange.getValue());
                    }

                    params.instanceExecParams.add(instanceParam);
                }
            } else {
                // normal fragment
                Iterator iter = fragmentExecParamsMap.get(fragment.getFragmentId()).scanRangeAssignment.entrySet().iterator();
                int parallelExecInstanceNum = fragment.getParallel_exec_num();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    TNetworkAddress key = (TNetworkAddress) entry.getKey();
                    Map<Integer, List<TScanRangeParams>> value = (Map<Integer, List<TScanRangeParams>>) entry.getValue();

                    for (Integer planNodeId : value.keySet()) {
                        List<TScanRangeParams> perNodeScanRanges = value.get(planNodeId);
                        int expectedInstanceNum = 1;
                        if (parallelExecInstanceNum > 1) {
                            //the scan instance num should not larger than the tablets num
                            expectedInstanceNum = Math.min(perNodeScanRanges.size(), parallelExecInstanceNum);
                        }
                        List<List<TScanRangeParams>> perInstanceScanRanges = ListUtil.splitBySize(perNodeScanRanges,
                                expectedInstanceNum);

                        for (List<TScanRangeParams> scanRangeParams : perInstanceScanRanges) {
                            FInstanceExecParam instanceParam = new FInstanceExecParam(null, key, 0, params);
                            instanceParam.perNodeScanRanges.put(planNodeId, scanRangeParams);
                            params.instanceExecParams.add(instanceParam);
                        }
                    }
                }
            }

            if (params.instanceExecParams.isEmpty()) {
                Reference<Long> backendIdRef = new Reference<Long>();
                TNetworkAddress execHostport = SimpleScheduler.getHost(this.idToBackend, backendIdRef);
                if (execHostport == null) {
                    throw new UserException("there is no scanNode Backend");
                }
                this.addressToBackendID.put(execHostport, backendIdRef.getRef());
                FInstanceExecParam instanceParam = new FInstanceExecParam(null, execHostport, 
                        0, params);
                params.instanceExecParams.add(instanceParam);
            }
        }
    }

    //One fragment could only have one HashJoinNode
    private boolean isColocateJoin(PlanNode node) {
        if (Config.disable_colocate_join) {
            return false;
        }

        // TODO(cmy): some internal process, such as broker load task, do not have ConnectContext.
        // Any configurations needed by the Coordinator should be passed in Coordinator initialization.
        // Refine this later.
        // Currently, just ignore the session variables if ConnectContext does not exist
        if (ConnectContext.get() != null) {
            if (ConnectContext.get().getSessionVariable().isDisableColocateJoin()) {
                return false;
            }
        }

        //cache the colocateFragmentIds
        if (colocateFragmentIds.contains(node.getFragmentId().asInt())) {
            return true;
        }

        if (node instanceof HashJoinNode) {
            HashJoinNode joinNode = (HashJoinNode) node;
            if (joinNode.isColocate()) {
                colocateFragmentIds.add(joinNode.getFragmentId().asInt());
                return true;
            }
        }

        for (PlanNode childNode : node.getChildren()) {
            return isColocateJoin(childNode);
        }

        return false;
    }
    
    // Returns the id of the leftmost node of any of the gives types in 'plan_root',
    // or INVALID_PLAN_NODE_ID if no such node present.
    private PlanNode findLeftmostNode(PlanNode plan) {
        PlanNode newPlan = plan;
        while (newPlan.getChildren().size() != 0 && !(newPlan instanceof ExchangeNode)) {
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
                    fragmentExecParamsMap.get(scanNode.getFragmentId()).scanRangeAssignment;
            if (isColocateJoin(scanNode.getFragment().getPlanRoot())) {
                computeScanRangeAssignmentByColocate((OlapScanNode) scanNode, assignment);
            } else {
                computeScanRangeAssignmentByScheduler(scanNode, locations, assignment);
            }
        }
    }

    //To ensure the same bucketSeq tablet to the same execHostPort
    private void computeScanRangeAssignmentByColocate(
            final OlapScanNode scanNode,
            FragmentScanRangeAssignment assignment) throws Exception {

        for(Integer bucketSeq: scanNode.bucketSeq2locations.keySet()) {
            //fill scanRangeParamsList
            List<TScanRangeLocations> locations = scanNode.bucketSeq2locations.get(bucketSeq);
            if (!bucketSeqToAddress.containsKey(bucketSeq)) {
                getExecHostPortForBucketSeq(locations.get(0), bucketSeq);
            }

            for(TScanRangeLocations location: locations) {
                Map<Integer, List<TScanRangeParams>> scanRanges =
                        findOrInsert(bucketSeqToScanRange, bucketSeq, new HashMap<Integer, List<TScanRangeParams>>());

                List<TScanRangeParams> scanRangeParamsList =
                        findOrInsert(scanRanges, scanNode.getId().asInt(), new ArrayList<TScanRangeParams>());

                // add scan range
                TScanRangeParams scanRangeParams = new TScanRangeParams();
                scanRangeParams.scan_range = location.scan_range;
                scanRangeParamsList.add(scanRangeParams);
            }

        }
    }

    private void getExecHostPortForBucketSeq(TScanRangeLocations seqLocation, Integer bucketSeq) throws Exception {
        int randomLocation = new Random().nextInt(seqLocation.locations.size());
        Reference<Long> backendIdRef = new Reference<Long>();
        TNetworkAddress execHostPort = SimpleScheduler.getHost(seqLocation.locations.get(randomLocation).backend_id, seqLocation.locations, this.idToBackend, backendIdRef);
        if (execHostPort == null) {
            throw new UserException("there is no scanNode Backend");
        }
        this.addressToBackendID.put(execHostPort, backendIdRef.getRef());
        this.bucketSeqToAddress.put(bucketSeq, execHostPort);
    }

    private void computeScanRangeAssignmentByScheduler(
            final ScanNode scanNode,
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
                throw new UserException("there is no scanNode Backend");
            }
            this.addressToBackendID.put(execHostPort, backendIdRef.getRef());

            Map<Integer, List<TScanRangeParams>> scanRanges = findOrInsert(assignment, execHostPort,
                    new HashMap<Integer, List<TScanRangeParams>>());
            List<TScanRangeParams> scanRangeParamsList = findOrInsert(scanRanges, scanNode.getId().asInt(),
                    new ArrayList<TScanRangeParams>());
            // add scan range
            TScanRangeParams scanRangeParams = new TScanRangeParams();
            scanRangeParams.scan_range = scanRangeLocations.scan_range;
            // Volume is optional, so we need to set the value and the is-set bit
            scanRangeParams.setVolume_id(minLocation.volume_id);
            scanRangeParamsList.add(scanRangeParams);
        }
    }

    public void updateFragmentExecStatus(TReportExecStatusParams params) {
        if (params.backend_num >= backendExecStates.size()) {
            LOG.error("unknown backend number: {}, expected less than: {}",
                    params.backend_num, backendExecStates.size());
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
                    DebugUtil.printId(queryId), DebugUtil.printId(params.getFragment_instance_id()));
            updateStatus(status, params.getFragment_instance_id());
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
            if (params.isSetCommitInfos()) {
                updateCommitInfos(params.getCommitInfos());
            }
            profileDoneSignal.markedCountDown(params.getFragment_instance_id(), -1L);
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

    class BucketSeqToScanRange extends HashMap<Integer, Map<Integer, List<TScanRangeParams>>> {

    }

    private BucketSeqToScanRange bucketSeqToScanRange = new BucketSeqToScanRange();
    private Map<Integer, TNetworkAddress> bucketSeqToAddress = Maps.newHashMap();
    private Set<Integer> colocateFragmentIds = new HashSet<>();

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
        TNetworkAddress address;
        Long systemBackendId;

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

        public int getInstanceId() {
            return instanceId;
        }

        public PlanFragmentId getfragmentId() {
            return fragmentId;
        }

        public BackendExecState(PlanFragmentId fragmentId, int instanceId, int profileFragmentId,
            TExecPlanFragmentParams rpcParams, Map<TNetworkAddress, Long> addressToBackendID) {
            this.profileFragmentId = profileFragmentId;
            this.fragmentId = fragmentId;
            this.instanceId = instanceId;
            this.rpcParams = rpcParams;
            this.initiated = false;
            this.done = false;
            this.address = fragmentExecParamsMap.get(fragmentId).instanceExecParams.get(instanceId).host;
            this.systemBackendId = addressToBackendID.get(address);

            String name = "Instance " + DebugUtil.printId(fragmentExecParamsMap.get(fragmentId)
                    .instanceExecParams.get(instanceId).instanceId) + " (host=" + address + ")";
            this.profile = new RuntimeProfile(name);
            this.hasCanceled = false;
        }

        public TNetworkAddress getBackendAddress() {
            return address;
        }

        public TUniqueId getFragmentInstanceId() {
            return this.rpcParams.params.getFragment_instance_id();
        }

        public Future<PExecPlanFragmentResult> execRemoteFragmentAsync() throws TException, RpcException {
            TNetworkAddress brpcAddress = null;
            try {
                brpcAddress = toBrpcHost(address);
            } catch (Exception e) {
                throw new TException(e.getMessage());
            }
            initiated = true;
            try {
                return BackendServiceProxy.getInstance().execPlanFragmentAsync(brpcAddress, rpcParams);
            } catch (RpcException e) {
                SimpleScheduler.updateBlacklistBackends(systemBackendId);
                throw e;
            }
        }
    }

    // execution parameters for a single fragment,
    // per-fragment can have multiple FInstanceExecParam,
    // used to assemble TPlanFragmentExecParas  
    protected class FragmentExecParams {
        public PlanFragment fragment;
        public List<TPlanFragmentDestination> destinations      = Lists.newArrayList();
        public Map<Integer, Integer>          perExchNumSenders = Maps.newHashMap();
        
        public List<PlanFragmentId> inputFragments = Lists.newArrayList();
        public List<FInstanceExecParam> instanceExecParams = Lists.newArrayList();
        public FragmentScanRangeAssignment scanRangeAssignment = new FragmentScanRangeAssignment();
        
        public FragmentExecParams(PlanFragment fragment) {
            this.fragment = fragment;
        }

        List<TExecPlanFragmentParams> toThrift(int backendNum) {
            List<TExecPlanFragmentParams> paramsList = Lists.newArrayList();

            for (int i = 0; i < instanceExecParams.size(); ++i) {
                final FInstanceExecParam instanceExecParam = instanceExecParams.get(i);
                TExecPlanFragmentParams params = new TExecPlanFragmentParams();
                params.setProtocol_version(PaloInternalServiceVersion.V1);
                params.setFragment(fragment.toThrift());
                params.setDesc_tbl(descTable);
                params.setParams(new TPlanFragmentExecParams());
                params.setResource_info(tResourceInfo);
                params.params.setQuery_id(queryId);
                params.params.setFragment_instance_id(instanceExecParam.instanceId);
                Map<Integer, List<TScanRangeParams>> scanRanges = instanceExecParam.perNodeScanRanges;
                if (scanRanges == null) {
                    scanRanges = Maps.newHashMap();
                }

                params.params.setPer_node_scan_ranges(scanRanges);
                params.params.setPer_exch_num_senders(perExchNumSenders);

                params.params.setDestinations(destinations);
                params.params.setSender_id(i);
                params.params.setNum_senders(instanceExecParams.size());
                params.setCoord(coordAddress);
                params.setBackend_num(backendNum++);
                params.setQuery_globals(queryGlobals);
                params.setQuery_options(queryOptions);
                params.params.setSend_query_statistics_with_every_batch(
                        fragment.isTransferQueryStatisticsWithEveryBatch());
                if (queryOptions.getQuery_type() == TQueryType.LOAD) {
                    LoadErrorHub.Param param = Catalog.getCurrentCatalog().getLoadInstance().getLoadErrorHubInfo();
                    if (param != null) {
                        TLoadErrorHubInfo info = param.toThrift();
                        if (info != null) {
                            params.setLoad_error_hub_info(info);
                        }
                    }
                }

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
                TEsScanRange esScanRange = range.getScan_range().getEs_scan_range();
                if (esScanRange != null) {
                    sb.append("{ index=").append(esScanRange.getIndex())
                        .append(", shardid=").append(esScanRange.getShard_id())
                        .append("}");
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
            for (int i = 0; i < instanceExecParams.size(); ++i) {
                if (i != 0) {
                    sb.append(",");
                }
                TNetworkAddress address = instanceExecParams.get(i).host;
                Map<Integer, List<TScanRangeParams>> scanRanges =
                        scanRangeAssignment.get(address);
                sb.append("{");
                sb.append("id=").append(DebugUtil.printId(instanceExecParams.get(i).instanceId));
                sb.append(",host=").append(instanceExecParams.get(i).host);
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

    // fragment instance exec param, it is used to assemble 
    // the per-instance TPlanFragmentExecParas, as a member of 
    // FragmentExecParams
    static class FInstanceExecParam {
        TUniqueId instanceId;
        TNetworkAddress host;
        Map<Integer, List<TScanRangeParams>> perNodeScanRanges = Maps.newHashMap();
        
        int perFragmentInstanceIdx;
        int senderId;
  
        FragmentExecParams fragmentExecParams;
        
        public FInstanceExecParam(TUniqueId id, TNetworkAddress host,
                int perFragmentInstanceIdx, FragmentExecParams fragmentExecParams) {
            this.instanceId = id;
            this.host = host;
            this.perFragmentInstanceIdx = perFragmentInstanceIdx;
            this.fragmentExecParams = fragmentExecParams;
        }
     
        public PlanFragment fragment() {
            return fragmentExecParams.fragment;
        }
    }

    // consistent with EXPLAIN's fragment index
    public List<QueryStatisticsItem.FragmentInstanceInfo> getFragmentInstanceInfos() {
        final List<QueryStatisticsItem.FragmentInstanceInfo> result =
                Lists.newArrayList();
        for (int index = 0; index < fragments.size(); index++) {
            for (Map.Entry<TUniqueId, BackendExecState> entry: backendExecStateMap.entrySet()) {
                final BackendExecState backendExecState = entry.getValue();
                if (fragments.get(index).getFragmentId() != backendExecState.getfragmentId()) {
                    continue;
                }
                final QueryStatisticsItem.FragmentInstanceInfo info
                        = new QueryStatisticsItem.FragmentInstanceInfo.Builder()
                        .instanceId(entry.getValue().getFragmentInstanceId())
                        .fragmentId(String.valueOf(index))
                        .address(backendExecState.getBackendAddress())
                        .build();
                result.add(info);
            }
        }
        return result;
    }

    private void attachInstanceProfileToFragmentProfile() {
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
    }
}
