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
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.Config;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Reference;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.ListUtil;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.LoadErrorHub;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.ExceptNode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.IntersectNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SetOperationNode;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.QueryStatisticsItem.FragmentInstanceInfo;
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
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TUniqueId;

import org.apache.commons.collections.map.HashedMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
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

    // Random is used to shuffle instances of partitioned
    private static Random instanceRandom = new Random();

    // Overall status of the entire query; set to the first reported fragment error
    // status or to CANCELLED, if Cancel() is called.
    Status queryStatus = new Status();

    // save of related backends of this query
    Map<TNetworkAddress, Long> addressToBackendID = Maps.newHashMap();

    private ImmutableMap<Long, Backend> idToBackend = ImmutableMap.of();

    // copied from TQueryExecRequest; constant across all fragments
    private TDescriptorTable descTable;

    private Set<Long> alreadySentBackendIds = Sets.newHashSet();

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
    // backend which state need to be checked when joining this coordinator.
    // It is supposed to be the subset of backendExecStates.
    private List<BackendExecState> needCheckBackendExecStates = Lists.newArrayList();
    private ResultReceiver receiver;
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

    // Used for query/insert
    public Coordinator(ConnectContext context, Analyzer analyzer, Planner planner) {
        this.isBlockQuery = planner.isBlockQuery();
        this.queryId = context.queryId();
        this.fragments = planner.getFragments();
        this.scanNodes = planner.getScanNodes();
        this.descTable = analyzer.getDescTbl().toThrift();
        this.returnedAllResults = false;
        this.queryOptions = context.getSessionVariable().toThrift();
        this.queryGlobals.setNowString(DATE_FORMAT.format(new Date()));
        this.queryGlobals.setTimestampMs(new Date().getTime());
        if (context.getSessionVariable().getTimeZone().equals("CST")) {
            this.queryGlobals.setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
        } else {
            this.queryGlobals.setTimeZone(context.getSessionVariable().getTimeZone());
        }
        this.tResourceInfo = new TResourceInfo(context.getQualifiedUser(),
                context.getSessionVariable().getResourceGroup());
        this.needReport = context.getSessionVariable().isReportSucc();
        this.clusterName = context.getClusterName();
        this.nextInstanceId = new TUniqueId();
        nextInstanceId.setHi(queryId.hi);
        nextInstanceId.setLo(queryId.lo + 1);
    }

    // Used for broker load task/export task coordinator
    public Coordinator(Long jobId, TUniqueId queryId, DescriptorTable descTable,
            List<PlanFragment> fragments, List<ScanNode> scanNodes, String cluster, String timezone) {
        this.isBlockQuery = true;
        this.jobId = jobId;
        this.queryId = queryId;
        this.descTable = descTable.toThrift();
        this.fragments = fragments;
        this.scanNodes = scanNodes;
        this.queryOptions = new TQueryOptions();
        this.queryGlobals.setNowString(DATE_FORMAT.format(new Date()));
        this.queryGlobals.setTimestampMs(new Date().getTime());
        this.queryGlobals.setTimeZone(timezone);
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
        this.queryOptions.setQueryType(type);
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
        this.queryOptions.setMemLimit(execMemoryLimit);
    }

    public void setLoadMemLimit(long loadMemLimit) {
        this.queryOptions.setLoadMemLimit(loadMemLimit);
    }

    public void setTimeout(int timeout) {
        this.queryOptions.setQueryTimeout(timeout);
    }

    public void clearExportStatus() {
        lock.lock();
        try {
            this.backendExecStates.clear();
            this.queryStatus.setStatus(new Status());
            if (this.exportFiles == null) {
                this.exportFiles = Lists.newArrayList();
            }
            this.exportFiles.clear();
            this.needCheckBackendExecStates.clear();
        } finally {
            lock.unlock();
        }
    }

    public List<TTabletCommitInfo> getCommitInfos() {
        return commitInfos;
    }

    // Initialize
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
        for (int i = 0; i < fragmentSize; i++) {
            fragmentProfile.add(new RuntimeProfile("Fragment " + i));
            queryProfile.addChild(fragmentProfile.get(i));
        }

        this.idToBackend = Catalog.getCurrentSystemInfo().getIdToBackend();
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
        if (LOG.isDebugEnabled()) {
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
            LOG.debug(sb.toString());
        }
    }

    // Initiate asynchronous execution of query. Returns as soon as all plan fragments
    // have started executing at their respective backends.
    // 'Request' must contain at least a coordinator plan fragment (ie, can't
    // be for a query like 'SELECT 1').
    // A call to Exec() must precede all other member function calls.
    public void exec() throws Exception {
        if (LOG.isDebugEnabled() && !scanNodes.isEmpty()) {
            LOG.debug("debug: in Coordinator::exec. query id: {}, planNode: {}",
                    DebugUtil.printId(queryId), scanNodes.get(0).treeToThrift());
        }

        if (LOG.isDebugEnabled() && !fragments.isEmpty()) {
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
            TNetworkAddress execBeAddr = topParams.instanceExecParams.get(0).host;
            receiver = new ResultReceiver(
                    topParams.instanceExecParams.get(0).instanceId,
                    addressToBackendID.get(execBeAddr),
                    toBrpcHost(execBeAddr),
                    queryOptions.query_timeout * 1000);

            LOG.info("dispatch query job: {} to {}", DebugUtil.printId(queryId), topParams.instanceExecParams.get(0).host);

            // set the broker address for OUTFILE sink
            ResultSink resultSink = (ResultSink) topParams.fragment.getSink();
            if (resultSink.isOutputFileSink() && resultSink.needBroker()) {
                FsBroker broker = Catalog.getCurrentCatalog().getBrokerMgr().getBroker(resultSink.getBrokerName(),
                        execBeAddr.getHostname());
                resultSink.setBrokerAddr(broker.ip, broker.port);
                LOG.info("OUTFILE through broker: {}:{}", broker.ip, broker.port);
            }

        } else {
            // This is a load process.
            this.queryOptions.setIsReportSuccess(true);
            deltaUrls = Lists.newArrayList();
            loadCounters = Maps.newHashMap();
            List<Long> relatedBackendIds = Lists.newArrayList(addressToBackendID.values());
            Catalog.getCurrentCatalog().getLoadManager().initJobProgress(jobId, queryId, instanceIds,
                    relatedBackendIds);
            LOG.info("dispatch load job: {} to {}", DebugUtil.printId(queryId), addressToBackendID.keySet());
        }

        // to keep things simple, make async Cancel() calls wait until plan fragment
        // execution has been initiated, otherwise we might try to cancel fragment
        // execution at backends where it hasn't even started
        profileDoneSignal = new MarkedCountDownLatch<TUniqueId, Long>(instanceIds.size());
        for (TUniqueId instanceId : instanceIds) {
            profileDoneSignal.addMark(instanceId, -1L /* value is meaningless */);
        }

        sendFragment();
    }

    private void sendFragment() throws TException, RpcException, UserException {
        lock();
        try {
            Multiset<TNetworkAddress> hostCounter = HashMultiset.create();
            for (FragmentExecParams params : fragmentExecParamsMap.values()) {
                for (FInstanceExecParam fi : params.instanceExecParams) {
                    hostCounter.add(fi.host);
                }
            }
            // Execute all instances from up to bottom
            // NOTICE: We must ensure that these fragments are executed sequentially,
            // otherwise the data dependency between the fragments will be destroyed.
            int backendIdx = 0;
            int profileFragmentId = 0;
            long memoryLimit = queryOptions.getMemLimit();
            for (PlanFragment fragment : fragments) {
                FragmentExecParams params = fragmentExecParamsMap.get(fragment.getFragmentId());

                // set up exec states
                int instanceNum = params.instanceExecParams.size();
                Preconditions.checkState(instanceNum > 0);
                List<TExecPlanFragmentParams> tParams = params.toThrift(backendIdx);
                List<Pair<BackendExecState, Future<InternalService.PExecPlanFragmentResult>>> futures =
                        Lists.newArrayList();

                // update memory limit for colocate join
                if (colocateFragmentIds.contains(fragment.getFragmentId().asInt())) {
                    int rate = Math.min(Config.query_colocate_join_memory_limit_penalty_factor, instanceNum);
                    long newmemory = memoryLimit / rate;

                    for (TExecPlanFragmentParams tParam : tParams) {
                        tParam.query_options.setMemLimit(newmemory);
                    }
                }

                boolean needCheckBackendState = false;
                if (queryOptions.getQueryType() == TQueryType.LOAD && profileFragmentId == 0) {
                    // this is a load process, and it is the first fragment.
                    // we should add all BackendExecState of this fragment to needCheckBackendExecStates,
                    // so that we can check these backends' state when joining this Coordinator
                    needCheckBackendState = true;
                }

                int instanceId = 0;
                for (TExecPlanFragmentParams tParam : tParams) {
                    BackendExecState execState = new BackendExecState(fragment.getFragmentId(), instanceId++,
                            profileFragmentId, tParam, this.addressToBackendID);
                    execState.unsetFields();
                    // Each tParam will set the total number of Fragments that need to be executed on the same BE,
                    // and the BE will determine whether all Fragments have been executed based on this information.
                    tParam.setFragmentNumOnHost(hostCounter.count(execState.address));

                    backendExecStates.add(execState);
                    if (needCheckBackendState) {
                        needCheckBackendExecStates.add(execState);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("add need check backend {} for fragment, {} job: {}", execState.backend.getId(),
                                    fragment.getFragmentId().asInt(), jobId);
                        }
                    }
                    futures.add(Pair.create(execState, execState.execRemoteFragmentAsync()));

                    backendIdx++;
                }

                for (Pair<BackendExecState, Future<InternalService.PExecPlanFragmentResult>> pair : futures) {
                    TStatusCode code;
                    String errMsg = null;
                    Exception exception = null;
                    try {
                        InternalService.PExecPlanFragmentResult result = pair.second.get(Config.remote_fragment_exec_timeout_ms,
                                TimeUnit.MILLISECONDS);
                        code = TStatusCode.findByValue(result.getStatus().getStatusCode());
                        if (!result.getStatus().getErrorMsgsList().isEmpty()) {
                            errMsg = result.getStatus().getErrorMsgsList().get(0);
                        }
                    } catch (ExecutionException e) {
                        LOG.warn("catch a execute exception", e);
                        exception = e;
                        code = TStatusCode.THRIFT_RPC_ERROR;
                    } catch (InterruptedException e) {
                        LOG.warn("catch a interrupt exception", e);
                        exception = e;
                        code = TStatusCode.INTERNAL_ERROR;
                    } catch (TimeoutException e) {
                        LOG.warn("catch a timeout exception", e);
                        exception = e;
                        code = TStatusCode.TIMEOUT;
                    }

                    if (code != TStatusCode.OK) {
                        if (exception != null) {
                            errMsg = exception.getMessage();
                        }

                        if (errMsg == null) {
                            errMsg = "exec rpc error. backend id: " + pair.first.backend.getId();
                        }
                        queryStatus.setStatus(errMsg);
                        LOG.warn("exec plan fragment failed, errmsg={}, code: {}, fragmentId={}, backend={}:{}",
                                errMsg, code, fragment.getFragmentId(),
                                pair.first.address.hostname, pair.first.address.port);
                        cancelInternal(InternalService.PPlanFragmentCancelReason.INTERNAL_ERROR);
                        switch (code) {
                            case TIMEOUT:
                                throw new UserException("query timeout. backend id: " + pair.first.backend.getId());
                            case THRIFT_RPC_ERROR:
                                SimpleScheduler.addToBlacklist(pair.first.backend.getId(), errMsg);
                                throw new RpcException(pair.first.backend.getHost(), "rpc failed");
                            default:
                                throw new UserException(errMsg);
                        }
                    }

                    // succeed to send the plan fragment, update the "alreadySentBackendIds"
                    alreadySentBackendIds.add(pair.first.backend.getId());
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
            long numRowsUnselected = 0L;
            value = this.loadCounters.get(LoadJob.UNSELECTED_ROWS);
            if (value != null) {
                numRowsUnselected = Long.valueOf(value);
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
            value = newLoadCounters.get(LoadJob.UNSELECTED_ROWS);
            if (value != null) {
                numRowsUnselected += Long.valueOf(value);
            }

            this.loadCounters.put(LoadEtlTask.DPP_NORMAL_ALL, "" + numRowsNormal);
            this.loadCounters.put(LoadEtlTask.DPP_ABNORMAL_ALL, "" + numRowsAbnormal);
            this.loadCounters.put(LoadJob.UNSELECTED_ROWS, "" + numRowsUnselected);
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
            cancelInternal(InternalService.PPlanFragmentCancelReason.INTERNAL_ERROR);
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
                throw new RpcException(null, copyStatus.getErrorMsg());
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
            Long numLimitRows = fragments.get(0).getPlanRoot().getLimit();
            boolean hasLimit = numLimitRows > 0;
            if (!isBlockQuery && instanceIds.size() > 1 && hasLimit && numReceivedRows >= numLimitRows) {
                LOG.debug("no block query, return num >= limit rows, need cancel");
                cancelInternal(InternalService.PPlanFragmentCancelReason.LIMIT_REACH);
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
            cancelInternal(InternalService.PPlanFragmentCancelReason.USER_CANCEL);
        } finally {
            unlock();
        }
    }

    private void cancelInternal(InternalService.PPlanFragmentCancelReason cancelReason) {
        if (null != receiver) {
            receiver.cancel();
        }
        cancelRemoteFragmentsAsync(cancelReason);
        if (profileDoneSignal != null) {
            // count down to zero to notify all objects waiting for this
            profileDoneSignal.countDownToZero(new Status());
            LOG.info("unfinished instance: {}", profileDoneSignal.getLeftMarks().stream().map(e -> DebugUtil.printId(e.getKey())).toArray());
        }
    }

    private void cancelRemoteFragmentsAsync(InternalService.PPlanFragmentCancelReason cancelReason) {
        for (BackendExecState backendExecState : backendExecStates) {
            backendExecState.cancelFragmentInstance(cancelReason);
        }
    }

    private void computeFragmentExecParams() throws Exception {
        // fill hosts field in fragmentExecParams
        computeFragmentHosts();

        // assign instance ids
        instanceIds.clear();
        for (FragmentExecParams params : fragmentExecParamsMap.values()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("fragment {} has instances {}", params.fragment.getFragmentId(), params.instanceExecParams.size());
            }

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

            if (bucketShuffleJoinController.isBucketShuffleJoin(destFragment.getFragmentId().asInt())) {
                int bucketSeq = 0;
                int bucketNum = bucketShuffleJoinController.getFragmentBucketNum(destFragment.getFragmentId());
                TNetworkAddress dummyServer = new TNetworkAddress("0.0.0.0", 0);

                while (bucketSeq < bucketNum) {
                    TPlanFragmentDestination dest = new TPlanFragmentDestination();

                    dest.fragment_instance_id = new TUniqueId(-1, -1);
                    dest.server = dummyServer;
                    dest.setBrpcServer(dummyServer);

                    for (FInstanceExecParam instanceExecParams : destParams.instanceExecParams) {
                        if (instanceExecParams.bucketSeqSet.contains(bucketSeq)) {
                            dest.fragment_instance_id = instanceExecParams.instanceId;
                            dest.server = toRpcHost(instanceExecParams.host);
                            dest.setBrpcServer(toBrpcHost(instanceExecParams.host));
                            break;
                        }
                    }

                    bucketSeq++;
                    params.destinations.add(dest);
                }
            } else {
                // add destination host to this fragment's destination
                for (int j = 0; j < destParams.instanceExecParams.size(); ++j) {
                    TPlanFragmentDestination dest = new TPlanFragmentDestination();
                    dest.fragment_instance_id = destParams.instanceExecParams.get(j).instanceId;
                    dest.server = toRpcHost(destParams.instanceExecParams.get(j).host);
                    dest.setBrpcServer(toBrpcHost(destParams.instanceExecParams.get(j).host));
                    params.destinations.add(dest);
                }
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

    // estimate if this fragment contains IntersectNode
    private boolean containsIntersectNode(PlanNode node) {
        if (node instanceof IntersectNode) {
            return true;
        }

        for (PlanNode child : node.getChildren()) {
            if (child instanceof ExchangeNode) {
                // Ignore other fragment's node
                continue;
            } else if (child instanceof IntersectNode) {
                return true;
            } else {
                return containsIntersectNode(child);
            }
        }
        return false;
    }

    // estimate if this fragment contains ExceptNode
    private boolean containsExceptNode(PlanNode node) {
        if (node instanceof ExceptNode) {
            return true;
        }

        for (PlanNode child : node.getChildren()) {
            if (child instanceof ExchangeNode) {
                // Ignore other fragment's node
                continue;
            } else if (child instanceof ExceptNode) {
                return true;
            } else {
                return containsExceptNode(child);
            }
        }
        return false;
    }

    // estimate if this fragment contains SetOperationNode
    private boolean containsSetOperationNode(PlanNode node) {
        if (node instanceof SetOperationNode) {
            return true;
        }

        for (PlanNode child : node.getChildren()) {
            if (child instanceof ExchangeNode) {
                // Ignore other fragment's node
                continue;
            } else if (child instanceof SetOperationNode) {
                return true;
            } else {
                return containsSetOperationNode(child);
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

            Pair<PlanNode, PlanNode> pairNodes = findLeftmostNode(fragment.getPlanRoot());
            PlanNode fatherNode = pairNodes.first;
            PlanNode leftMostNode = pairNodes.second;

            /*
             * Case A:
             *      if the left most is ScanNode, which means there is no child fragment,
             *      we should assign fragment instances on every scan node hosts.
             * Case B:
             *      if not, there should be exchange nodes to collect all data from child fragments(input fragments),
             *      so we should assign fragment instances corresponding to the child fragments' host
             */
            if (!(leftMostNode instanceof ScanNode)) {
                // (Case B)
                // there is no leftmost scan; we assign the same hosts as those of our
                //  input fragment which has a higher instance_number

                int inputFragmentIndex = 0;
                int maxParallelism = 0;
                // If the fragment has three children, then the first child and the second child are the child(both exchange node) of shuffle HashJoinNode,
                // and the third child is the right child(ExchangeNode) of broadcast HashJoinNode.
                // We only need to pay attention to the maximum parallelism among the two ExchangeNodes of shuffle HashJoinNode.
                int childrenCount = (fatherNode != null) ? fatherNode.getChildren().size() : 1;
                for (int j = 0; j < childrenCount; j++) {
                    int currentChildFragmentParallelism = fragmentExecParamsMap.get(fragment.getChild(j).getFragmentId()).instanceExecParams.size();
                    if (currentChildFragmentParallelism > maxParallelism) {
                        maxParallelism = currentChildFragmentParallelism;
                        inputFragmentIndex = j;
                    }
                }

                PlanFragmentId inputFragmentId = fragment.getChild(inputFragmentIndex).getFragmentId();
                // AddAll() soft copy()
                int exchangeInstances = -1;
                if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
                    exchangeInstances = ConnectContext.get().getSessionVariable().getExchangeInstanceParallel();
                }
                if (exchangeInstances > 0 && fragmentExecParamsMap.get(inputFragmentId).instanceExecParams.size() > exchangeInstances) {
                    // random select some instance
                    // get distinct host,  when parallel_fragment_exec_instance_num > 1, single host may execute several instances
                    Set<TNetworkAddress> hostSet = Sets.newHashSet();
                    for (FInstanceExecParam execParams : fragmentExecParamsMap.get(inputFragmentId).instanceExecParams) {
                        hostSet.add(execParams.host);
                    }
                    List<TNetworkAddress> hosts = Lists.newArrayList(hostSet);
                    Collections.shuffle(hosts, instanceRandom);
                    for (int index = 0; index < exchangeInstances; index++) {
                        FInstanceExecParam instanceParam = new FInstanceExecParam(null, hosts.get(index % hosts.size()), 0, params);
                        params.instanceExecParams.add(instanceParam);
                    }
                } else {
                    for (FInstanceExecParam execParams : fragmentExecParamsMap.get(inputFragmentId).instanceExecParams) {
                        FInstanceExecParam instanceParam = new FInstanceExecParam(null, execParams.host, 0, params);
                        params.instanceExecParams.add(instanceParam);
                    }
                }

                // When group by cardinality is smaller than number of backend, only some backends always
                // process while other has no data to process.
                // So we shuffle instances to make different backends handle different queries.
                Collections.shuffle(params.instanceExecParams, instanceRandom);

                // TODO: switch to unpartitioned/coord execution if our input fragment
                // is executed that way (could have been downgraded from distributed)
                continue;
            }

            int parallelExecInstanceNum = fragment.getParallelExecNum();
            //for ColocateJoin fragment
            if ((isColocateJoin(fragment.getPlanRoot()) && fragmentIdToSeqToAddressMap.containsKey(fragment.getFragmentId())
                    && fragmentIdToSeqToAddressMap.get(fragment.getFragmentId()).size() > 0)) {
                computeColocateJoinInstanceParam(fragment.getFragmentId(), parallelExecInstanceNum, params);
            } else if (bucketShuffleJoinController.isBucketShuffleJoin(fragment.getFragmentId().asInt())) {
                bucketShuffleJoinController.computeInstanceParam(fragment.getFragmentId(), parallelExecInstanceNum, params);
            } else {
                // case A
                Iterator iter = fragmentExecParamsMap.get(fragment.getFragmentId()).scanRangeAssignment.entrySet().iterator();
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

                        LOG.debug("scan range number per instance is: {}", perInstanceScanRanges.size());

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

    // One fragment could only have one HashJoinNode
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
    private Pair<PlanNode, PlanNode> findLeftmostNode(PlanNode plan) {
        PlanNode newPlan = plan;
        PlanNode fatherPlan = null;
        while (newPlan.getChildren().size() != 0 && !(newPlan instanceof ExchangeNode)) {
            fatherPlan = newPlan;
            newPlan = newPlan.getChild(0);
        }
        return new Pair<PlanNode, PlanNode>(fatherPlan, newPlan);
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

    private void computeColocateJoinInstanceParam(PlanFragmentId fragmentId, int parallelExecInstanceNum, FragmentExecParams params) {
        Map<Integer, TNetworkAddress> bucketSeqToAddress = fragmentIdToSeqToAddressMap.get(fragmentId);
        Set<Integer> scanNodeIds = fragmentIdToScanNodeIds.get(fragmentId);

        // 1. count each node in one fragment should scan how many tablet, gather them in one list
        Map<TNetworkAddress, List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>>> addressToScanRanges = Maps.newHashMap();
        for (Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>> scanRanges : bucketSeqToScanRange.entrySet()) {
            TNetworkAddress address = bucketSeqToAddress.get(scanRanges.getKey());
            Map<Integer, List<TScanRangeParams>> nodeScanRanges = scanRanges.getValue();

            // We only care about the node scan ranges of scan nodes which belong to this fragment
            Map<Integer, List<TScanRangeParams>> filteredNodeScanRanges = Maps.newHashMap();
            for (Integer scanNodeId : nodeScanRanges.keySet()) {
                if (scanNodeIds.contains(scanNodeId)) {
                    filteredNodeScanRanges.put(scanNodeId, nodeScanRanges.get(scanNodeId));
                }
            }

            // set bucket for scanRange, the pair is <bucket_num, map<scanNode_id, list<scanRange>>>>
            // we should make sure
            // 1. same bucket in some address be
            // 2. different scanNode id scan different scanRange which belong to the scanNode id
            // 3. split how many scanRange one instance should scan, same bucket do not spilt to different instance
            Pair<Integer, Map<Integer, List<TScanRangeParams>>> filteredScanRanges = Pair.create(scanRanges.getKey(), filteredNodeScanRanges);

            if (!addressToScanRanges.containsKey(address)) {
                addressToScanRanges.put(address, Lists.newArrayList());
            }
            addressToScanRanges.get(address).add(filteredScanRanges);
        }
        FragmentScanRangeAssignment assignment = params.scanRangeAssignment;
        for (Map.Entry<TNetworkAddress, List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>>> addressScanRange : addressToScanRanges.entrySet()) {
            List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>> scanRange = addressScanRange.getValue();
            Map<Integer, List<TScanRangeParams>> range = findOrInsert(assignment, addressScanRange.getKey(), new HashMap<Integer, List<TScanRangeParams>>());
            int expectedInstanceNum = 1;
            if (parallelExecInstanceNum > 1) {
                //the scan instance num should not larger than the tablets num
                expectedInstanceNum = Math.min(scanRange.size(), parallelExecInstanceNum);
            }

            // 2.split how many scanRange one instance should scan
            List<List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>>> perInstanceScanRanges = ListUtil.splitBySize(scanRange,
                    expectedInstanceNum);

            // 3.construct instanceExecParam add the scanRange should be scan by instance
            for (List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>> perInstanceScanRange : perInstanceScanRanges) {
                FInstanceExecParam instanceParam = new FInstanceExecParam(null, addressScanRange.getKey(), 0, params);

                for (Pair<Integer, Map<Integer, List<TScanRangeParams>>> nodeScanRangeMap : perInstanceScanRange) {
                    instanceParam.bucketSeqSet.add(nodeScanRangeMap.first);
                    for (Map.Entry<Integer, List<TScanRangeParams>> nodeScanRange : nodeScanRangeMap.second.entrySet()) {
                        if (!instanceParam.perNodeScanRanges.containsKey(nodeScanRange.getKey())) {
                            range.put(nodeScanRange.getKey(), Lists.newArrayList());
                            instanceParam.perNodeScanRanges.put(nodeScanRange.getKey(), Lists.newArrayList());
                        }
                        range.get(nodeScanRange.getKey()).addAll(nodeScanRange.getValue());
                        instanceParam.perNodeScanRanges.get(nodeScanRange.getKey()).addAll(nodeScanRange.getValue());
                    }
                }
                params.instanceExecParams.add(instanceParam);
            }
        }
    }

    // Populates scan_range_assignment_.
    // <fragment, <server, nodeId>>
    private void computeScanRangeAssignment() throws Exception {
        HashMap<TNetworkAddress, Long> assignedBytesPerHost = Maps.newHashMap();
        // set scan ranges/locations for scan nodes
        for (ScanNode scanNode : scanNodes) {
            // the parameters of getScanRangeLocations may ignore, It dosn't take effect
            List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(0);
            if (locations == null) {
                // only analysis olap scan node
                continue;
            }

            Set<Integer> scanNodeIds = fragmentIdToScanNodeIds.get(scanNode.getFragmentId());
            if (scanNodeIds == null) {
                scanNodeIds = Sets.newHashSet();
                fragmentIdToScanNodeIds.put(scanNode.getFragmentId(), scanNodeIds);
            }
            scanNodeIds.add(scanNode.getId().asInt());

            FragmentScanRangeAssignment assignment = fragmentExecParamsMap.get(scanNode.getFragmentId()).scanRangeAssignment;
            boolean fragmentContainsColocateJoin = isColocateJoin(scanNode.getFragment().getPlanRoot());
            boolean fragmentContainsBucketShuffleJoin = bucketShuffleJoinController.isBucketShuffleJoin(scanNode.getFragmentId().asInt(), scanNode.getFragment().getPlanRoot());

            // A fragment may contain both colocate join and bucket shuffle join
            // on need both compute scanRange to init basic data for query coordinator
            if (fragmentContainsColocateJoin) {
                computeScanRangeAssignmentByColocate((OlapScanNode) scanNode);
            }
            if (fragmentContainsBucketShuffleJoin) {
                bucketShuffleJoinController.computeScanRangeAssignmentByBucket((OlapScanNode) scanNode, idToBackend, addressToBackendID);
            }
            if (!(fragmentContainsColocateJoin | fragmentContainsBucketShuffleJoin)) {
                computeScanRangeAssignmentByScheduler(scanNode, locations, assignment, assignedBytesPerHost);
            }
        }
    }

    // To ensure the same bucketSeq tablet to the same execHostPort
    private void computeScanRangeAssignmentByColocate(
            final OlapScanNode scanNode) throws Exception {
        if (!fragmentIdToSeqToAddressMap.containsKey(scanNode.getFragmentId())) {
            fragmentIdToSeqToAddressMap.put(scanNode.getFragmentId(), new HashedMap());
        }
        Map<Integer, TNetworkAddress> bucketSeqToAddress = fragmentIdToSeqToAddressMap.get(scanNode.getFragmentId());
        HashMap<TNetworkAddress, Long> assignedBytesPerHost = Maps.newHashMap();
        for (Integer bucketSeq : scanNode.bucketSeq2locations.keySet()) {
            //fill scanRangeParamsList
            List<TScanRangeLocations> locations = scanNode.bucketSeq2locations.get(bucketSeq);
            if (!bucketSeqToAddress.containsKey(bucketSeq)) {
                getExecHostPortForFragmentIDAndBucketSeq(locations.get(0), scanNode.getFragmentId(), bucketSeq, assignedBytesPerHost);
            }

            for (TScanRangeLocations location : locations) {
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

    //ensure bucket sequence distribued to every host evenly
    private void getExecHostPortForFragmentIDAndBucketSeq(TScanRangeLocations seqLocation, PlanFragmentId fragmentId, Integer bucketSeq,
                                                          HashMap<TNetworkAddress, Long> assignedBytesPerHost) throws Exception {
        Reference<Long> backendIdRef = new Reference<Long>();
        selectBackendsByRoundRobin(seqLocation, assignedBytesPerHost, backendIdRef);
        Backend backend = this.idToBackend.get(backendIdRef.getRef());
        TNetworkAddress execHostPort = new TNetworkAddress(backend.getHost(), backend.getBePort());
        this.addressToBackendID.put(execHostPort, backendIdRef.getRef());
        this.fragmentIdToSeqToAddressMap.get(fragmentId).put(bucketSeq, execHostPort);
    }

    public TScanRangeLocation selectBackendsByRoundRobin(TScanRangeLocations seqLocation,
                                          HashMap<TNetworkAddress, Long> assignedBytesPerHost,
                                          Reference<Long> backendIdRef) throws UserException {
        Long minAssignedBytes = Long.MAX_VALUE;
        TScanRangeLocation minLocation = null;
        Long step = 1L;
        for (final TScanRangeLocation location : seqLocation.getLocations()) {
            Long assignedBytes = findOrInsert(assignedBytesPerHost, location.server, 0L);
            if (assignedBytes < minAssignedBytes) {
                minAssignedBytes = assignedBytes;
                minLocation = location;
            }
        }
        TScanRangeLocation location = SimpleScheduler.getLocation(minLocation, seqLocation.locations, this.idToBackend, backendIdRef);
        if (assignedBytesPerHost.containsKey(location.server)) {
            assignedBytesPerHost.put(location.server,
                    assignedBytesPerHost.get(location.server) + step);
        } else {
            assignedBytesPerHost.put(location.server, step);
        }
        return location;
    }

    private void computeScanRangeAssignmentByScheduler(
            final ScanNode scanNode,
            final List<TScanRangeLocations> locations,
            FragmentScanRangeAssignment assignment,
            HashMap<TNetworkAddress, Long> assignedBytesPerHost) throws Exception {
        for (TScanRangeLocations scanRangeLocations : locations) {
            Reference<Long> backendIdRef = new Reference<Long>();
            TScanRangeLocation minLocation = selectBackendsByRoundRobin(scanRangeLocations, assignedBytesPerHost, backendIdRef);
            Backend backend = this.idToBackend.get(backendIdRef.getRef());
            TNetworkAddress execHostPort = new TNetworkAddress(backend.getHost(), backend.getBePort());
            this.addressToBackendID.put(execHostPort, backendIdRef.getRef());

            Map<Integer, List<TScanRangeParams>> scanRanges = findOrInsert(assignment, execHostPort,
                    new HashMap<Integer, List<TScanRangeParams>>());
            List<TScanRangeParams> scanRangeParamsList = findOrInsert(scanRanges, scanNode.getId().asInt(),
                    new ArrayList<TScanRangeParams>());
            // add scan range
            TScanRangeParams scanRangeParams = new TScanRangeParams();
            scanRangeParams.scan_range = scanRangeLocations.scan_range;
            // Volume is optional, so we need to set the value and the is-set bit
            scanRangeParams.setVolumeId(minLocation.volume_id);
            scanRangeParamsList.add(scanRangeParams);
        }
    }

    public void updateFragmentExecStatus(TReportExecStatusParams params) {
        if (params.backend_num >= backendExecStates.size()) {
            LOG.warn("unknown backend number: {}, expected less than: {}",
                    params.backend_num, backendExecStates.size());
            return;
        }

        BackendExecState execState = backendExecStates.get(params.backend_num);
        if (!execState.updateProfile(params)) {
            return;
        }

        // print fragment instance profile
        if (LOG.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            execState.printProfile(builder);
            LOG.debug("profile for query_id={} instance_id={}\n{}",
                    DebugUtil.printId(queryId),
                    DebugUtil.printId(params.getFragmentInstanceId()),
                    builder.toString());
        }

        Status status = new Status(params.status);
        // for now, abort the query if we see any error except if the error is cancelled
        // and returned_all_results_ is true.
        // (UpdateStatus() initiates cancellation, if it hasn't already been initiated)
        if (!(returnedAllResults && status.isCancelled()) && !status.ok()) {
            LOG.warn("one instance report fail, query_id={} instance_id={}",
                    DebugUtil.printId(queryId), DebugUtil.printId(params.getFragmentInstanceId()));
            updateStatus(status, params.getFragmentInstanceId());
        }
        if (execState.done) {
            if (params.isSetDeltaUrls()) {
                updateDeltas(params.getDeltaUrls());
            }
            if (params.isSetLoadCounters()) {
                updateLoadCounters(params.getLoadCounters());
            }
            if (params.isSetTrackingUrl()) {
                trackingUrl = params.getTrackingUrl();
            }
            if (params.isSetExportFiles()) {
                updateExportFiles(params.getExportFiles());
            }
            if (params.isSetCommitInfos()) {
                updateCommitInfos(params.getCommitInfos());
            }
            profileDoneSignal.markedCountDown(params.getFragmentInstanceId(), -1L);
        }

        if (params.isSetLoadedRows()) {
            Catalog.getCurrentCatalog().getLoadManager().updateJobProgress(
                    jobId, params.backend_id, params.query_id, params.fragment_instance_id, params.loaded_rows,
                    params.done);
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

    /*
     * Waiting the coordinator finish executing.
     * return false if waiting timeout.
     * return true otherwise.
     * NOTICE: return true does not mean that coordinator executed success,
     * the caller should check queryStatus for result.
     *
     * We divide the entire waiting process into multiple rounds,
     * with a maximum of 30 seconds per round. And after each round of waiting,
     * check the status of the BE. If the BE status is abnormal, the wait is ended
     * and the result is returned. Otherwise, continue to the next round of waiting.
     * This method mainly avoids the problem that the Coordinator waits for a long time
     * after some BE can no long return the result due to some exception, such as BE is down.
     */
    public boolean join(int timeoutS) {
        final long fixedMaxWaitTime = 30;

        long leftTimeoutS = timeoutS;
        while (leftTimeoutS > 0) {
            long waitTime = Math.min(leftTimeoutS, fixedMaxWaitTime);
            boolean awaitRes = false;
            try {
                awaitRes = profileDoneSignal.await(waitTime, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // Do nothing
            }
            if (awaitRes) {
                return true;
            }

            if (!checkBackendState()) {
                return true;
            }

            leftTimeoutS -= waitTime;
        }
        return false;
    }

    /*
     * Check the state of backends in needCheckBackendExecStates.
     * return true if all of them are OK. Otherwise, return false.
     */
    private boolean checkBackendState() {
        for (BackendExecState backendExecState : needCheckBackendExecStates) {
            if (!backendExecState.isBackendStateHealthy()) {
                queryStatus = new Status(TStatusCode.INTERNAL_ERROR, "backend " + backendExecState.backend.getId() + " is down");
                return false;
            }
        }
        return true;
    }

    public boolean isDone() {
        return profileDoneSignal.getCount() == 0;
    }

    // map from an impalad host address to the per-node assigned scan ranges;
    // records scan range assignment for a single fragment
    class FragmentScanRangeAssignment
            extends HashMap<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> {
    }

    // Bucket sequence -> (scan node id -> list of TScanRangeParams)
    class BucketSeqToScanRange extends HashMap<Integer, Map<Integer, List<TScanRangeParams>>> {

    }

    class BucketShuffleJoinController {
        // fragment_id -> < bucket_seq -> < scannode_id -> scan_range_params >>
        private Map<PlanFragmentId, BucketSeqToScanRange> fragmentIdBucketSeqToScanRangeMap = Maps.newHashMap();
        // fragment_id -> < bucket_seq -> be_addresss >
        private Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentIdToSeqToAddressMap = Maps.newHashMap();
        // fragment_id -> < be_id -> bucket_count >
        private Map<PlanFragmentId, Map<Long, Integer>> fragmentIdToBuckendIdBucketCountMap = Maps.newHashMap();
        // fragment_id -> bucket_num
        private Map<PlanFragmentId, Integer> fragmentIdToBucketNumMap = Maps.newHashMap();

        // cache the bucketShuffleFragmentIds
        private Set<Integer> bucketShuffleFragmentIds = new HashSet<>();

        private Map<PlanFragmentId, Set<Integer>> fragmentIdToScanNodeIds;

        // TODO(cmy): Should refactor this Controller to unify bucket shuffle join and colocate join
        public BucketShuffleJoinController(Map<PlanFragmentId, Set<Integer>> fragmentIdToScanNodeIds) {
            this.fragmentIdToScanNodeIds = fragmentIdToScanNodeIds;
        }

        // check whether the node fragment is bucket shuffle join fragment
        private boolean isBucketShuffleJoin(int fragmentId, PlanNode node) {
            if (ConnectContext.get() != null) {
                if (!ConnectContext.get().getSessionVariable().isEnableBucketShuffleJoin()) {
                    return false;
                }
            }

            // check the node is be the part of the fragment
            if (fragmentId != node.getFragmentId().asInt()) {
                return false;
            }

            if (bucketShuffleFragmentIds.contains(fragmentId)) {
                return true;
            }

            // One fragment could only have one HashJoinNode
            if (node instanceof HashJoinNode) {
                HashJoinNode joinNode = (HashJoinNode) node;
                if (joinNode.isBucketShuffle()) {
                    bucketShuffleFragmentIds.add(joinNode.getFragmentId().asInt());
                    return true;
                }
            }

            for (PlanNode childNode : node.getChildren()) {
                return isBucketShuffleJoin(fragmentId, childNode);
            }

            return false;
        }

        private boolean isBucketShuffleJoin(int fragmentId) {
            return bucketShuffleFragmentIds.contains(fragmentId);
        }

        private int getFragmentBucketNum(PlanFragmentId fragmentId) {
            return fragmentIdToBucketNumMap.get(fragmentId);
        }

        // make sure each host have average bucket to scan
        private void getExecHostPortForFragmentIDAndBucketSeq(TScanRangeLocations seqLocation, PlanFragmentId fragmentId, Integer bucketSeq,
            ImmutableMap<Long, Backend> idToBackend, Map<TNetworkAddress, Long> addressToBackendID) throws Exception {
            Map<Long, Integer> buckendIdToBucketCountMap = fragmentIdToBuckendIdBucketCountMap.get(fragmentId);
            int maxBucketNum = Integer.MAX_VALUE;
            long buckendId = Long.MAX_VALUE;
            for (TScanRangeLocation location : seqLocation.locations) {
                if (buckendIdToBucketCountMap.containsKey(location.backend_id)) {
                    if (buckendIdToBucketCountMap.get(location.backend_id) < maxBucketNum) {
                        maxBucketNum = buckendIdToBucketCountMap.get(location.backend_id);
                        buckendId = location.backend_id;
                    }
                } else {
                    maxBucketNum = 0;
                    buckendId = location.backend_id;
                    buckendIdToBucketCountMap.put(buckendId, 0);
                    break;
                }
            }
            Reference<Long> backendIdRef = new Reference<Long>();
            TNetworkAddress execHostPort = SimpleScheduler.getHost(buckendId, seqLocation.locations, idToBackend, backendIdRef);
            if (execHostPort == null) {
                throw new UserException("there is no scanNode Backend");
            }
            //the backend with buckendId is not alive, chose another new backend
            if (backendIdRef.getRef() != buckendId) {
                //buckendIdToBucketCountMap does not contain the new backend, insert into it
                if (!buckendIdToBucketCountMap.containsKey(backendIdRef.getRef())) {
                    buckendIdToBucketCountMap.put(backendIdRef.getRef(), 1);
                } else { //buckendIdToBucketCountMap contains the new backend, update it
                    buckendIdToBucketCountMap.put(backendIdRef.getRef(), buckendIdToBucketCountMap.get(backendIdRef.getRef()) + 1);
                }
            } else { //the backend with buckendId is alive, update buckendIdToBucketCountMap directly
                buckendIdToBucketCountMap.put(buckendId, buckendIdToBucketCountMap.get(buckendId) + 1);
            }
            addressToBackendID.put(execHostPort, backendIdRef.getRef());
            this.fragmentIdToSeqToAddressMap.get(fragmentId).put(bucketSeq, execHostPort);
        }

        // to ensure the same bucketSeq tablet to the same execHostPort
        private void computeScanRangeAssignmentByBucket(
                final OlapScanNode scanNode, ImmutableMap<Long, Backend> idToBackend, Map<TNetworkAddress, Long> addressToBackendID) throws Exception {
            if (!fragmentIdToSeqToAddressMap.containsKey(scanNode.getFragmentId())) {
                fragmentIdToBucketNumMap.put(scanNode.getFragmentId(), scanNode.getOlapTable().getDefaultDistributionInfo().getBucketNum());
                fragmentIdToSeqToAddressMap.put(scanNode.getFragmentId(), new HashedMap());
                fragmentIdBucketSeqToScanRangeMap.put(scanNode.getFragmentId(), new BucketSeqToScanRange());
                fragmentIdToBuckendIdBucketCountMap.put(scanNode.getFragmentId(), new HashMap<>());
            }
            Map<Integer, TNetworkAddress> bucketSeqToAddress = fragmentIdToSeqToAddressMap.get(scanNode.getFragmentId());
            BucketSeqToScanRange bucketSeqToScanRange = fragmentIdBucketSeqToScanRangeMap.get(scanNode.getFragmentId());

            for (Integer bucketSeq : scanNode.bucketSeq2locations.keySet()) {
                //fill scanRangeParamsList
                List<TScanRangeLocations> locations = scanNode.bucketSeq2locations.get(bucketSeq);
                if (!bucketSeqToAddress.containsKey(bucketSeq)) {
                    getExecHostPortForFragmentIDAndBucketSeq(locations.get(0), scanNode.getFragmentId(), bucketSeq, idToBackend, addressToBackendID);
                }

                for (TScanRangeLocations location : locations) {
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

        private void computeInstanceParam(PlanFragmentId fragmentId, int parallelExecInstanceNum, FragmentExecParams params) {
            Map<Integer, TNetworkAddress> bucketSeqToAddress = fragmentIdToSeqToAddressMap.get(fragmentId);
            BucketSeqToScanRange bucketSeqToScanRange = fragmentIdBucketSeqToScanRangeMap.get(fragmentId);
            Set<Integer> scanNodeIds = fragmentIdToScanNodeIds.get(fragmentId);

            // 1. count each node in one fragment should scan how many tablet, gather them in one list
            Map<TNetworkAddress, List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>>> addressToScanRanges = Maps.newHashMap();
            for (Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>> scanRanges : bucketSeqToScanRange.entrySet()) {
                TNetworkAddress address = bucketSeqToAddress.get(scanRanges.getKey());
                Map<Integer, List<TScanRangeParams>> nodeScanRanges = scanRanges.getValue();
                // We only care about the node scan ranges of scan nodes which belong to this fragment
                Map<Integer, List<TScanRangeParams>> filteredNodeScanRanges = Maps.newHashMap();
                for (Integer scanNodeId : nodeScanRanges.keySet()) {
                    if (scanNodeIds.contains(scanNodeId)) {
                        filteredNodeScanRanges.put(scanNodeId, nodeScanRanges.get(scanNodeId));
                    }
                }
                Pair<Integer, Map<Integer, List<TScanRangeParams>>> filteredScanRanges = Pair.create(scanRanges.getKey(), filteredNodeScanRanges);

                if (!addressToScanRanges.containsKey(address)) {
                    addressToScanRanges.put(address, Lists.newArrayList());
                }
                addressToScanRanges.get(address).add(filteredScanRanges);
            }
            FragmentScanRangeAssignment assignment = params.scanRangeAssignment;
            for (Map.Entry<TNetworkAddress, List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>>> addressScanRange : addressToScanRanges.entrySet()) {
                List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>> scanRange = addressScanRange.getValue();
                Map<Integer, List<TScanRangeParams>> range = findOrInsert(assignment, addressScanRange.getKey(), new HashMap<Integer, List<TScanRangeParams>>());
                int expectedInstanceNum = 1;
                if (parallelExecInstanceNum > 1) {
                    //the scan instance num should not larger than the tablets num
                    expectedInstanceNum = Math.min(scanRange.size(), parallelExecInstanceNum);
                }

                // 2. split how many scanRange one instance should scan
                List<List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>>> perInstanceScanRanges = ListUtil.splitBySize(scanRange,
                        expectedInstanceNum);

                // 3.construct instanceExecParam add the scanRange should be scan by instance
                for (List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>> perInstanceScanRange : perInstanceScanRanges) {
                    FInstanceExecParam instanceParam = new FInstanceExecParam(null, addressScanRange.getKey(), 0, params);

                    for (Pair<Integer, Map<Integer, List<TScanRangeParams>>> nodeScanRangeMap : perInstanceScanRange) {
                        instanceParam.addBucketSeq(nodeScanRangeMap.first);
                        for (Map.Entry<Integer, List<TScanRangeParams>> nodeScanRange : nodeScanRangeMap.second.entrySet()) {
                            if (!instanceParam.perNodeScanRanges.containsKey(nodeScanRange.getKey())) {
                                range.put(nodeScanRange.getKey(), Lists.newArrayList());
                                instanceParam.perNodeScanRanges.put(nodeScanRange.getKey(), Lists.newArrayList());
                            }
                            range.get(nodeScanRange.getKey()).addAll(nodeScanRange.getValue());
                            instanceParam.perNodeScanRanges.get(nodeScanRange.getKey()).addAll(nodeScanRange.getValue());
                        }
                    }
                    params.instanceExecParams.add(instanceParam);
                }
            }
        }
    }

    private BucketSeqToScanRange bucketSeqToScanRange = new BucketSeqToScanRange();
    private Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentIdToSeqToAddressMap = Maps.newHashMap();
    // cache the fragment id to its scan node ids. Used for colocate join.
    private Map<PlanFragmentId, Set<Integer>> fragmentIdToScanNodeIds = Maps.newHashMap();
    private Set<Integer> colocateFragmentIds = new HashSet<>();
    private BucketShuffleJoinController bucketShuffleJoinController = new BucketShuffleJoinController(fragmentIdToScanNodeIds);

    // record backend execute state
    // TODO(zhaochun): add profile information and others
    public class BackendExecState {
        TExecPlanFragmentParams rpcParams;
        PlanFragmentId fragmentId;
        int instanceId;
        boolean initiated;
        boolean done;
        boolean hasCanceled;
        int profileFragmentId;
        RuntimeProfile profile;
        TNetworkAddress address;
        Backend backend;
        long lastMissingHeartbeatTime = -1;

        public BackendExecState(PlanFragmentId fragmentId, int instanceId, int profileFragmentId,
            TExecPlanFragmentParams rpcParams, Map<TNetworkAddress, Long> addressToBackendID) {
            this.profileFragmentId = profileFragmentId;
            this.fragmentId = fragmentId;
            this.instanceId = instanceId;
            this.rpcParams = rpcParams;
            this.initiated = false;
            this.done = false;
            FInstanceExecParam fi = fragmentExecParamsMap.get(fragmentId).instanceExecParams.get(instanceId);
            this.address = fi.host;
            this.backend = idToBackend.get(addressToBackendID.get(address));

            String name = "Instance " + DebugUtil.printId(fi.instanceId) + " (host=" + address + ")";
            this.profile = new RuntimeProfile(name);
            this.hasCanceled = false;
            this.lastMissingHeartbeatTime = backend.getLastMissingHeartbeatTime();
        }

        /**
         * Some information common to all Fragments does not need to be sent repeatedly.
         * Therefore, when we confirm that a certain BE has accepted the information,
         * we will delete the information in the subsequent Fragment to avoid repeated sending.
         * This information can be obtained from the cache of BE.
         */
        public void unsetFields() {
            if (alreadySentBackendIds.contains(backend.getId())) {
                this.rpcParams.unsetDescTbl();
                this.rpcParams.unsetCoord();
                this.rpcParams.unsetQueryGlobals();
                this.rpcParams.unsetResourceInfo();
                this.rpcParams.setIsSimplifiedParam(true);
            } else {
                this.rpcParams.setIsSimplifiedParam(false);
            }
        }

        // update profile.
        // return true if profile is updated. Otherwise, return false.
        public synchronized boolean updateProfile(TReportExecStatusParams params) {
            if (this.done) {
                // duplicate packet
                return false;
            }
            if (params.isSetProfile()) {
                profile.update(params.profile);
            }
            this.done = params.done;
            return true;
        }

        public synchronized void printProfile(StringBuilder builder) {
            this.profile.computeTimeInProfile();
            this.profile.prettyPrint(builder, "");
        }

        // cancel the fragment instance.
        // return true if cancel success. Otherwise, return false
        public synchronized boolean cancelFragmentInstance(InternalService.PPlanFragmentCancelReason cancelReason) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("cancelRemoteFragments initiated={} done={} hasCanceled={} backend: {}, fragment instance id={}, reason: {}",
                        this.initiated, this.done, this.hasCanceled, backend.getId(),
                        DebugUtil.printId(fragmentInstanceId()), cancelReason.name());
            }
            try {
                if (!this.initiated) {
                    return false;
                }
                // don't cancel if it is already finished
                if (this.done) {
                    return false;
                }
                if (this.hasCanceled) {
                    return false;
                }
                TNetworkAddress brpcAddress = toBrpcHost(address);

                try {
                    BackendServiceProxy.getInstance().cancelPlanFragmentAsync(brpcAddress,
                            fragmentInstanceId(), cancelReason);
                } catch (RpcException e) {
                    LOG.warn("cancel plan fragment get a exception, address={}:{}", brpcAddress.getHostname(),
                            brpcAddress.getPort());
                    SimpleScheduler.addToBlacklist(addressToBackendID.get(brpcAddress), e.getMessage());
                }

                this.hasCanceled = true;
            } catch (Exception e) {
                LOG.warn("catch a exception", e);
                return false;
            }
            return true;
        }

        public synchronized boolean computeTimeInProfile(int maxFragmentId) {
            if (this.profileFragmentId < 0 || this.profileFragmentId > maxFragmentId) {
                LOG.warn("profileFragmentId {} should be in [0, {})", profileFragmentId, maxFragmentId);
                return false;
            }
            profile.computeTimeInProfile();
            return true;
        }

        public boolean isBackendStateHealthy() {
            if (backend.getLastMissingHeartbeatTime() > lastMissingHeartbeatTime) {
                LOG.warn("backend {} is down while joining the coordinator. job id: {}", backend.getId(), jobId);
                return false;
            }
            return true;
        }

        public Future<InternalService.PExecPlanFragmentResult> execRemoteFragmentAsync() throws TException, RpcException {
            TNetworkAddress brpcAddress = null;
            try {
                brpcAddress = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
            } catch (Exception e) {
                throw new TException(e.getMessage());
            }
            this.initiated = true;
            try {
                return BackendServiceProxy.getInstance().execPlanFragmentAsync(brpcAddress, rpcParams);
            } catch (RpcException e) {
                // DO NOT throw exception here, return a complete future with error code,
                // so that the following logic will cancel the fragment.
                return new Future<InternalService.PExecPlanFragmentResult>() {
                    @Override
                    public boolean cancel(boolean mayInterruptIfRunning) {
                        return false;
                    }

                    @Override
                    public boolean isCancelled() {
                        return false;
                    }

                    @Override
                    public boolean isDone() {
                        return true;
                    }

                    @Override
                    public InternalService.PExecPlanFragmentResult get() {
                        InternalService.PExecPlanFragmentResult result = InternalService.PExecPlanFragmentResult
                                .newBuilder()
                                .setStatus(org.apache.doris.proto.Status.PStatus.newBuilder()
                                        .addErrorMsgs(e.getMessage())
                                        .setStatusCode(TStatusCode.THRIFT_RPC_ERROR.getValue())
                                        .build())
                                .build();
                        return result;
                    }

                    @Override
                    public InternalService.PExecPlanFragmentResult get(long timeout, TimeUnit unit) {
                        return get();
                    }
                };
            }
        }

        public FragmentInstanceInfo buildFragmentInstanceInfo() {
            return new QueryStatisticsItem.FragmentInstanceInfo.Builder()
                    .instanceId(fragmentInstanceId()).fragmentId(String.valueOf(fragmentId)).address(this.address)
                    .build();
        }

        private TUniqueId fragmentInstanceId() {
            return this.rpcParams.params.getFragmentInstanceId();
        }
    }

    // execution parameters for a single fragment,
    // per-fragment can have multiple FInstanceExecParam,
    // used to assemble TPlanFragmentExecParas
    protected class FragmentExecParams {
        public PlanFragment fragment;
        public List<TPlanFragmentDestination> destinations = Lists.newArrayList();
        public Map<Integer, Integer> perExchNumSenders = Maps.newHashMap();

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
                params.setProtocolVersion(PaloInternalServiceVersion.V1);
                params.setFragment(fragment.toThrift());
                params.setDescTbl(descTable);
                params.setParams(new TPlanFragmentExecParams());
                params.setResourceInfo(tResourceInfo);
                params.params.setQueryId(queryId);
                params.params.setFragmentInstanceId(instanceExecParam.instanceId);
                Map<Integer, List<TScanRangeParams>> scanRanges = instanceExecParam.perNodeScanRanges;
                if (scanRanges == null) {
                    scanRanges = Maps.newHashMap();
                }

                params.params.setPerNodeScanRanges(scanRanges);
                params.params.setPerExchNumSenders(perExchNumSenders);

                params.params.setDestinations(destinations);
                params.params.setSenderId(i);
                params.params.setNumSenders(instanceExecParams.size());
                params.setCoord(coordAddress);
                params.setBackendNum(backendNum++);
                params.setQueryGlobals(queryGlobals);
                params.setQueryOptions(queryOptions);
                params.params.setSendQueryStatisticsWithEveryBatch(
                        fragment.isTransferQueryStatisticsWithEveryBatch());
                if (queryOptions.getQueryType() == TQueryType.LOAD) {
                    LoadErrorHub.Param param = Catalog.getCurrentCatalog().getLoadInstance().getLoadErrorHubInfo();
                    if (param != null) {
                        TLoadErrorHubInfo info = param.toThrift();
                        if (info != null) {
                            params.setLoadErrorHubInfo(info);
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
                TPaloScanRange paloScanRange = range.getScanRange().getPaloScanRange();
                if (paloScanRange != null) {
                    if (idx++ != 0) {
                        sb.append(",");
                    }
                    sb.append("{tid=").append(paloScanRange.getTabletId())
                            .append(",ver=").append(paloScanRange.getVersion()).append("}");
                }
                TEsScanRange esScanRange = range.getScanRange().getEsScanRange();
                if (esScanRange != null) {
                    sb.append("{ index=").append(esScanRange.getIndex())
                            .append(", shardid=").append(esScanRange.getShardId())
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

        Set<Integer> bucketSeqSet = Sets.newHashSet();

        FragmentExecParams fragmentExecParams;

        public void addBucketSeq(int bucketSeq) {
            this.bucketSeqSet.add(bucketSeq);
        }

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
            for (BackendExecState backendExecState: backendExecStates) {
                if (fragments.get(index).getFragmentId() != backendExecState.fragmentId) {
                    continue;
                }
                final QueryStatisticsItem.FragmentInstanceInfo info = backendExecState.buildFragmentInstanceInfo();
                result.add(info);
            }
        }
        return result;
    }

    private void attachInstanceProfileToFragmentProfile() {
        for (BackendExecState backendExecState : backendExecStates) {
            if (!backendExecState.computeTimeInProfile(fragmentProfile.size())) {
                return;
            }
            fragmentProfile.get(backendExecState.profileFragmentId).addChild(backendExecState.profile);
        }
    }
}


