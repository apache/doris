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
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.Config;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Reference;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.telemetry.ScopedSpan;
import org.apache.doris.common.telemetry.Telemetry;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.ListUtil;
import org.apache.doris.common.util.ProfileWriter;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.VectorizedUtil;
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
import org.apache.doris.planner.ResultFileSink;
import org.apache.doris.planner.ResultSink;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.planner.SetOperationNode;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PExecPlanFragmentResult;
import org.apache.doris.proto.InternalService.PExecPlanFragmentStartRequest;
import org.apache.doris.proto.Types;
import org.apache.doris.proto.Types.PUniqueId;
import org.apache.doris.qe.QueryStatisticsItem.FragmentInstanceInfo;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TErrorTabletInfo;
import org.apache.doris.thrift.TEsScanRange;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TExecPlanFragmentParamsList;
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
import org.apache.doris.thrift.TResourceLimit;
import org.apache.doris.thrift.TRuntimeFilterParams;
import org.apache.doris.thrift.TRuntimeFilterTargetParams;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

    private static final String localIP = FrontendOptions.getLocalHostAddress();

    // Random is used to shuffle instances of partitioned
    private static final Random instanceRandom = new Random();

    // Overall status of the entire query; set to the first reported fragment error
    // status or to CANCELLED, if Cancel() is called.
    Status queryStatus = new Status();

    // save of related backends of this query
    Map<TNetworkAddress, Long> addressToBackendID = Maps.newHashMap();

    private ImmutableMap<Long, Backend> idToBackend = ImmutableMap.of();

    // copied from TQueryExecRequest; constant across all fragments
    private final TDescriptorTable descTable;

    // Why do we use query global?
    // When `NOW()` function is in sql, we need only one now(),
    // but, we execute `NOW()` distributed.
    // So we make a query global value here to make one `now()` value in one query process.
    private final TQueryGlobals queryGlobals = new TQueryGlobals();
    private TQueryOptions queryOptions;
    private TNetworkAddress coordAddress;

    // protects all fields below
    private final Lock lock = new ReentrantLock();

    // If true, the query is done returning all results.  It is possible that the
    // coordinator still needs to wait for cleanup on remote fragments (e.g. queries
    // with limit)
    // Once this is set to true, errors from remote fragments are ignored.
    private boolean returnedAllResults;

    private RuntimeProfile queryProfile;

    private List<RuntimeProfile> fragmentProfile;

    private ProfileWriter profileWriter;

    // populated in computeFragmentExecParams()
    private final Map<PlanFragmentId, FragmentExecParams> fragmentExecParamsMap = Maps.newHashMap();

    private final List<PlanFragment> fragments;
    // backend execute state
    private final List<BackendExecState> backendExecStates = Lists.newArrayList();
    // backend which state need to be checked when joining this coordinator.
    // It is supposed to be the subset of backendExecStates.
    private final List<BackendExecState> needCheckBackendExecStates = Lists.newArrayList();
    private ResultReceiver receiver;
    private final List<ScanNode> scanNodes;
    // number of instances of this query, equals to
    // number of backends executing plan fragments on behalf of this query;
    // set in computeFragmentExecParams();
    // same as backend_exec_states_.size() after Exec()
    private final Set<TUniqueId> instanceIds = Sets.newHashSet();
    // instance id -> dummy value
    private MarkedCountDownLatch<TUniqueId, Long> profileDoneSignal;

    private final boolean isBlockQuery;

    private int numReceivedRows = 0;

    private List<String> deltaUrls;
    private Map<String, String> loadCounters;
    private String trackingUrl;

    // for export
    private List<String> exportFiles;

    private final List<TTabletCommitInfo> commitInfos = Lists.newArrayList();
    private final List<TErrorTabletInfo> errorTabletInfos = Lists.newArrayList();

    // Input parameter
    private long jobId = -1; // job which this task belongs to
    private TUniqueId queryId;
    private final TResourceInfo tResourceInfo;
    private final boolean needReport;

    // parallel execute
    private final TUniqueId nextInstanceId;

    // a timestamp represent the absolute timeout
    // eg, System.currentTimeMillis() + query_timeout * 1000
    private long timeoutDeadline;

    // Runtime filter merge instance address and ID
    public TNetworkAddress runtimeFilterMergeAddr;
    public TUniqueId runtimeFilterMergeInstanceId;
    // Runtime filter ID to the target instance address of the fragment,
    // that is expected to use this runtime filter, the instance address is not repeated
    public Map<RuntimeFilterId, List<FRuntimeFilterTargetParam>> ridToTargetParam = Maps.newHashMap();
    // The runtime filter that expects the instance to be used
    public List<RuntimeFilter> assignedRuntimeFilters = new ArrayList<>();
    // Runtime filter ID to the builder instance number
    public Map<RuntimeFilterId, Integer> ridToBuilderNum = Maps.newHashMap();


    // Used for query/insert
    public Coordinator(ConnectContext context, Analyzer analyzer, Planner planner) {
        this.isBlockQuery = planner.isBlockQuery();
        this.queryId = context.queryId();
        this.fragments = planner.getFragments();
        this.scanNodes = planner.getScanNodes();
        this.descTable = planner.getDescTable().toThrift();
        this.returnedAllResults = false;
        initQueryOptions(context);

        setFromUserProperty(analyzer);

        this.queryGlobals.setNowString(DATE_FORMAT.format(new Date()));
        this.queryGlobals.setTimestampMs(System.currentTimeMillis());
        this.queryGlobals.setNanoSeconds(LocalDateTime.now().getNano());
        this.queryGlobals.setLoadZeroTolerance(false);
        if (context.getSessionVariable().getTimeZone().equals("CST")) {
            this.queryGlobals.setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
        } else {
            this.queryGlobals.setTimeZone(context.getSessionVariable().getTimeZone());
        }
        this.tResourceInfo = new TResourceInfo(context.getQualifiedUser(),
                context.getSessionVariable().getResourceGroup());
        this.needReport = context.getSessionVariable().enableProfile();
        this.nextInstanceId = new TUniqueId();
        nextInstanceId.setHi(queryId.hi);
        nextInstanceId.setLo(queryId.lo + 1);
        this.assignedRuntimeFilters = planner.getRuntimeFilters();
    }

    // Used for broker load task/export task/update coordinator
    public Coordinator(Long jobId, TUniqueId queryId, DescriptorTable descTable,
                       List<PlanFragment> fragments, List<ScanNode> scanNodes, String timezone,
                       boolean loadZeroTolerance) {
        this.isBlockQuery = true;
        this.jobId = jobId;
        this.queryId = queryId;
        this.descTable = descTable.toThrift();
        this.fragments = fragments;
        this.scanNodes = scanNodes;
        this.queryOptions = new TQueryOptions();
        this.queryGlobals.setNowString(DATE_FORMAT.format(new Date()));
        this.queryGlobals.setTimestampMs(System.currentTimeMillis());
        this.queryGlobals.setTimeZone(timezone);
        this.queryGlobals.setLoadZeroTolerance(loadZeroTolerance);
        this.tResourceInfo = new TResourceInfo("", "");
        this.needReport = true;
        this.nextInstanceId = new TUniqueId();
        nextInstanceId.setHi(queryId.hi);
        nextInstanceId.setLo(queryId.lo + 1);
    }

    private void setFromUserProperty(Analyzer analyzer) {
        String qualifiedUser = analyzer.getQualifiedUser();
        // set cpu resource limit
        int cpuLimit = Env.getCurrentEnv().getAuth().getCpuResourceLimit(qualifiedUser);
        if (cpuLimit > 0) {
            // overwrite the cpu resource limit from session variable;
            TResourceLimit resourceLimit = new TResourceLimit();
            resourceLimit.setCpuLimit(cpuLimit);
            this.queryOptions.setResourceLimit(resourceLimit);
        }
        // set exec mem limit
        long memLimit = Env.getCurrentEnv().getAuth().getExecMemLimit(qualifiedUser);
        if (memLimit > 0) {
            // overwrite the exec_mem_limit from session variable;
            this.queryOptions.setMemLimit(memLimit);
            this.queryOptions.setMaxReservation(memLimit);
            this.queryOptions.setInitialReservationTotalClaims(memLimit);
            this.queryOptions.setBufferPoolLimit(memLimit);
        }
    }

    private void initQueryOptions(ConnectContext context) {
        this.queryOptions = context.getSessionVariable().toThrift();
        this.queryOptions.setEnableVectorizedEngine(VectorizedUtil.isVectorized());
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

    public void setExecVecEngine(boolean vec) {
        this.queryOptions.setEnableVectorizedEngine(vec);
    }

    public Status getExecStatus() {
        return queryStatus;
    }

    public RuntimeProfile getQueryProfile() {
        return queryProfile;
    }

    public ProfileWriter getProfileWriter() {
        return profileWriter;
    }

    public void setProfileWriter(ProfileWriter profileWriter) {
        this.profileWriter = profileWriter;
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

    public void setLoadZeroTolerance(boolean loadZeroTolerance) {
        this.queryGlobals.setLoadZeroTolerance(loadZeroTolerance);
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

    public List<TErrorTabletInfo> getErrorTabletInfos() {
        return errorTabletInfos;
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

        this.idToBackend = Env.getCurrentSystemInfo().getIdToBackend();
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

        QeProcessorImpl.INSTANCE.registerInstances(queryId, instanceIds.size());

        // create result receiver
        PlanFragmentId topId = fragments.get(0).getFragmentId();
        FragmentExecParams topParams = fragmentExecParamsMap.get(topId);
        DataSink topDataSink = topParams.fragment.getSink();
        this.timeoutDeadline = System.currentTimeMillis() + queryOptions.query_timeout * 1000L;
        if (topDataSink instanceof ResultSink || topDataSink instanceof ResultFileSink) {
            TNetworkAddress execBeAddr = topParams.instanceExecParams.get(0).host;
            receiver = new ResultReceiver(topParams.instanceExecParams.get(0).instanceId,
                    addressToBackendID.get(execBeAddr), toBrpcHost(execBeAddr), this.timeoutDeadline);
            if (LOG.isDebugEnabled()) {
                LOG.debug("dispatch query job: {} to {}", DebugUtil.printId(queryId),
                        topParams.instanceExecParams.get(0).host);
            }

            if (topDataSink instanceof ResultFileSink
                    && ((ResultFileSink) topDataSink).getStorageType() == StorageBackend.StorageType.BROKER) {
                // set the broker address for OUTFILE sink
                ResultFileSink topResultFileSink = (ResultFileSink) topDataSink;
                FsBroker broker = Env.getCurrentEnv().getBrokerMgr()
                        .getBroker(topResultFileSink.getBrokerName(), execBeAddr.getHostname());
                topResultFileSink.setBrokerAddr(broker.ip, broker.port);
            }
        } else {
            // This is a load process.
            this.queryOptions.setIsReportSuccess(true);
            deltaUrls = Lists.newArrayList();
            loadCounters = Maps.newHashMap();
            List<Long> relatedBackendIds = Lists.newArrayList(addressToBackendID.values());
            Env.getCurrentEnv().getLoadManager().initJobProgress(jobId, queryId, instanceIds,
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

    /**
     * The logic for sending query plan fragments is as follows:
     * First, plan fragments are dependent. According to the order in "fragments" list,
     * it must be ensured that on the BE side, the next fragment instance can be executed
     * only after the previous fragment instance is ready,
     * <p>
     * In the previous logic, we will send fragment instances in sequence through RPC,
     * and will wait for the RPC of the previous fragment instance to return successfully
     * before sending the next one. But for some complex queries, this may lead to too many RPCs.
     * <p>
     * The optimized logic is as follows:
     * 1. If the number of fragment instance is <= 2, the original logic is still used
     * to complete the sending of fragments through at most 2 RPCs.
     * 2. If the number of fragment instance is >= 3, first group all fragments by BE,
     * and send all fragment instances to the corresponding BE node through the FIRST rpc,
     * but these fragment instances will only perform the preparation phase but will not be actually executed.
     * After that, the execution logic of all fragment instances is started through the SECOND RPC.
     * <p>
     * After optimization, a query on a BE node will only send two RPCs at most.
     * Thereby reducing the "send fragment timeout" error caused by too many RPCs and BE unable to process in time.
     *
     * @throws TException
     * @throws RpcException
     * @throws UserException
     */
    private void sendFragment() throws TException, RpcException, UserException {
        lock();
        try {
            Multiset<TNetworkAddress> hostCounter = HashMultiset.create();
            for (FragmentExecParams params : fragmentExecParamsMap.values()) {
                for (FInstanceExecParam fi : params.instanceExecParams) {
                    hostCounter.add(fi.host);
                }
            }

            int backendIdx = 0;
            int profileFragmentId = 0;
            long memoryLimit = queryOptions.getMemLimit();
            Map<Long, BackendExecStates> beToExecStates = Maps.newHashMap();
            // If #fragments >=2, use twoPhaseExecution with exec_plan_fragments_prepare and exec_plan_fragments_start,
            // else use exec_plan_fragments directly.
            // we choose #fragments >=2 because in some cases
            // we need ensure that A fragment is already prepared to receive data before B fragment sends data.
            // For example: select * from numbers("10","w") will generate ExchangeNode and TableValuedFunctionScanNode,
            // we should ensure TableValuedFunctionScanNode does not send data until ExchangeNode is ready to receive.
            boolean twoPhaseExecution = fragments.size() >= 2;
            for (PlanFragment fragment : fragments) {
                FragmentExecParams params = fragmentExecParamsMap.get(fragment.getFragmentId());

                // 1. set up exec states
                int instanceNum = params.instanceExecParams.size();
                Preconditions.checkState(instanceNum > 0);
                List<TExecPlanFragmentParams> tParams = params.toThrift(backendIdx);

                // 2. update memory limit for colocate join
                if (colocateFragmentIds.contains(fragment.getFragmentId().asInt())) {
                    int rate = Math.min(Config.query_colocate_join_memory_limit_penalty_factor, instanceNum);
                    long newMemory = memoryLimit / rate;
                    // TODO(zxy): The meaning of mem limit in query_options has become the real once query mem limit.
                    // The logic to modify mem_limit here needs to be modified or deleted.
                    for (TExecPlanFragmentParams tParam : tParams) {
                        tParam.query_options.setMemLimit(newMemory);
                    }
                }

                boolean needCheckBackendState = false;
                if (queryOptions.getQueryType() == TQueryType.LOAD && profileFragmentId == 0) {
                    // this is a load process, and it is the first fragment.
                    // we should add all BackendExecState of this fragment to needCheckBackendExecStates,
                    // so that we can check these backends' state when joining this Coordinator
                    needCheckBackendState = true;
                }

                // 3. group BackendExecState by BE. So that we can use one RPC to send all fragment instances of a BE.
                int instanceId = 0;
                for (TExecPlanFragmentParams tParam : tParams) {
                    BackendExecState execState =
                            new BackendExecState(fragment.getFragmentId(), instanceId++,
                                    profileFragmentId, tParam, this.addressToBackendID);
                    // Each tParam will set the total number of Fragments that need to be executed on the same BE,
                    // and the BE will determine whether all Fragments have been executed based on this information.
                    tParam.setFragmentNumOnHost(hostCounter.count(execState.address));
                    tParam.setBackendId(execState.backend.getId());
                    tParam.setNeedWaitExecutionTrigger(twoPhaseExecution);

                    backendExecStates.add(execState);
                    if (needCheckBackendState) {
                        needCheckBackendExecStates.add(execState);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("add need check backend {} for fragment, {} job: {}",
                                    execState.backend.getId(), fragment.getFragmentId().asInt(), jobId);
                        }
                    }

                    BackendExecStates states = beToExecStates.get(execState.backend.getId());
                    if (states == null) {
                        states = new BackendExecStates(execState.backend.getId(), execState.brpcAddress,
                                twoPhaseExecution);
                        beToExecStates.putIfAbsent(execState.backend.getId(), states);
                    }
                    states.addState(execState);
                    ++backendIdx;
                }
                profileFragmentId += 1;
            } // end for fragments

            // 4. send and wait fragments rpc
            List<Pair<BackendExecStates, Future<InternalService.PExecPlanFragmentResult>>> futures =
                    Lists.newArrayList();
            Context parentSpanContext = Context.current();
            for (BackendExecStates states : beToExecStates.values()) {
                Span span = Telemetry.getNoopSpan();
                if (ConnectContext.get() != null) {
                    span = ConnectContext.get().getTracer().spanBuilder("execRemoteFragmentsAsync")
                            .setParent(parentSpanContext).setSpanKind(SpanKind.CLIENT).startSpan();
                }
                states.scopedSpan = new ScopedSpan(span);
                states.unsetFields();
                futures.add(Pair.of(states, states.execRemoteFragmentsAsync()));
            }
            waitRpc(futures, this.timeoutDeadline - System.currentTimeMillis(), "send fragments");

            if (twoPhaseExecution) {
                // 5. send and wait execution start rpc
                futures.clear();
                for (BackendExecStates states : beToExecStates.values()) {
                    Span span = Telemetry.getNoopSpan();
                    if (ConnectContext.get() != null) {
                        span = ConnectContext.get().getTracer().spanBuilder("execPlanFragmentStartAsync")
                                .setParent(parentSpanContext).setSpanKind(SpanKind.CLIENT).startSpan();
                    }
                    states.scopedSpan = new ScopedSpan(span);
                    futures.add(Pair.of(states, states.execPlanFragmentStartAsync()));
                }
                waitRpc(futures, this.timeoutDeadline - System.currentTimeMillis(), "send execution start");
            }

            attachInstanceProfileToFragmentProfile();
        } finally {
            unlock();
        }
    }

    private void waitRpc(List<Pair<BackendExecStates, Future<PExecPlanFragmentResult>>> futures, long leftTimeMs,
            String operation) throws RpcException, UserException {
        if (leftTimeMs <= 0) {
            throw new UserException("timeout before waiting for " + operation + " RPC. Elapse(sec): " + (
                    (System.currentTimeMillis() - timeoutDeadline) / 1000 + queryOptions.query_timeout));
        }

        long timeoutMs = Math.min(leftTimeMs, Config.remote_fragment_exec_timeout_ms);
        for (Pair<BackendExecStates, Future<PExecPlanFragmentResult>> pair : futures) {
            TStatusCode code;
            String errMsg = null;
            Exception exception = null;
            Span span = pair.first.scopedSpan.getSpan();
            try {
                PExecPlanFragmentResult result = pair.second.get(timeoutMs, TimeUnit.MILLISECONDS);
                code = TStatusCode.findByValue(result.getStatus().getStatusCode());
                if (code != TStatusCode.OK) {
                    if (!result.getStatus().getErrorMsgsList().isEmpty()) {
                        errMsg = result.getStatus().getErrorMsgsList().get(0);
                    } else {
                        errMsg = operation + " failed. backend id: " + pair.first.beId;
                    }
                }
            } catch (ExecutionException e) {
                exception = e;
                code = TStatusCode.THRIFT_RPC_ERROR;
                BackendServiceProxy.getInstance().removeProxy(pair.first.brpcAddr);
            } catch (InterruptedException e) {
                exception = e;
                code = TStatusCode.INTERNAL_ERROR;
            } catch (TimeoutException e) {
                exception = e;
                errMsg = "timeout when waiting for " + operation + " RPC. Wait(sec): " + timeoutMs / 1000;
                code = TStatusCode.TIMEOUT;
            }

            try {
                if (code != TStatusCode.OK) {
                    if (exception != null && errMsg == null) {
                        errMsg = operation + " failed. " + exception.getMessage();
                    }
                    queryStatus.setStatus(errMsg);
                    cancelInternal(Types.PPlanFragmentCancelReason.INTERNAL_ERROR);
                    switch (code) {
                        case TIMEOUT:
                            throw new RpcException(pair.first.brpcAddr.hostname, errMsg, exception);
                        case THRIFT_RPC_ERROR:
                            SimpleScheduler.addToBlacklist(pair.first.beId, errMsg);
                            throw new RpcException(pair.first.brpcAddr.hostname, errMsg, exception);
                        default:
                            throw new UserException(errMsg, exception);
                    }
                }
            } catch (Exception e) {
                span.recordException(e);
                throw e;
            } finally {
                pair.first.scopedSpan.endSpan();
            }
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
                numRowsNormal = Long.parseLong(value);
            }
            long numRowsAbnormal = 0L;
            value = this.loadCounters.get(LoadEtlTask.DPP_ABNORMAL_ALL);
            if (value != null) {
                numRowsAbnormal = Long.parseLong(value);
            }
            long numRowsUnselected = 0L;
            value = this.loadCounters.get(LoadJob.UNSELECTED_ROWS);
            if (value != null) {
                numRowsUnselected = Long.parseLong(value);
            }

            // new load counters
            value = newLoadCounters.get(LoadEtlTask.DPP_NORMAL_ALL);
            if (value != null) {
                numRowsNormal += Long.parseLong(value);
            }
            value = newLoadCounters.get(LoadEtlTask.DPP_ABNORMAL_ALL);
            if (value != null) {
                numRowsAbnormal += Long.parseLong(value);
            }
            value = newLoadCounters.get(LoadJob.UNSELECTED_ROWS);
            if (value != null) {
                numRowsUnselected += Long.parseLong(value);
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

    private void updateErrorTabletInfos(List<TErrorTabletInfo> errorTabletInfos) {
        lock.lock();
        try {
            this.errorTabletInfos.addAll(errorTabletInfos);
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
            LOG.warn("one instance report fail throw updateStatus(), need cancel. job id: {},"
                            + " query id: {}, instance id: {}",
                    jobId, DebugUtil.printId(queryId), instanceId != null ? DebugUtil.printId(instanceId) : "NaN");
            cancelInternal(Types.PPlanFragmentCancelReason.INTERNAL_ERROR);
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
                cancelInternal(Types.PPlanFragmentCancelReason.LIMIT_REACH);
            }
        } else if (resultBatch.getBatch() != null) {
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
            cancelInternal(Types.PPlanFragmentCancelReason.USER_CANCEL);
        } finally {
            unlock();
        }
    }

    private void cancelInternal(Types.PPlanFragmentCancelReason cancelReason) {
        if (null != receiver) {
            receiver.cancel();
        }
        cancelRemoteFragmentsAsync(cancelReason);
        if (profileDoneSignal != null) {
            // count down to zero to notify all objects waiting for this
            profileDoneSignal.countDownToZero(new Status());
            LOG.info("unfinished instance: {}", profileDoneSignal.getLeftMarks()
                    .stream().map(e -> DebugUtil.printId(e.getKey())).toArray());
        }
    }

    private void cancelRemoteFragmentsAsync(Types.PPlanFragmentCancelReason cancelReason) {
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
                LOG.debug("fragment {} has instances {}",
                        params.fragment.getFragmentId(), params.instanceExecParams.size());
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

        // assign runtime filter merge addr and target addr
        assignRuntimeFilterAddr();

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

            if (sink.getOutputPartition() != null
                    && sink.getOutputPartition().isBucketShuffleHashPartition()) {
                // the destFragment must be bucket shuffle
                Preconditions.checkState(bucketShuffleJoinController
                        .isBucketShuffleJoin(destFragment.getFragmentId().asInt()), "Sink is"
                        + "Bucket Shuffle Partition, The destFragment must have bucket shuffle join node ");

                int bucketSeq = 0;
                int bucketNum = bucketShuffleJoinController.getFragmentBucketNum(destFragment.getFragmentId());

                // when left table is empty, it's bucketset is empty.
                // set right table destination address to the address of left table
                if (destParams.instanceExecParams.size() == 1
                        && destParams.instanceExecParams.get(0).bucketSeqSet.isEmpty()) {
                    bucketNum = 1;
                    destParams.instanceExecParams.get(0).bucketSeqSet.add(0);
                }
                // process bucket shuffle join on fragment without scan node
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
        Backend backend = Env.getCurrentSystemInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (backend == null) {
            throw new UserException(SystemInfoService.NO_SCAN_NODE_BACKEND_AVAILABLE_MSG);
        }
        TNetworkAddress dest = new TNetworkAddress(backend.getHost(), backend.getBeRpcPort());
        return dest;
    }

    private TNetworkAddress toBrpcHost(TNetworkAddress host) throws Exception {
        Backend backend = Env.getCurrentSystemInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (backend == null) {
            throw new UserException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG);
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
                TNetworkAddress execHostport;
                if (ConnectContext.get() != null && ConnectContext.get().isResourceTagsSet()
                        && !addressToBackendID.isEmpty()) {
                    // In this case, we only use the BE where the replica selected by the tag is located to execute
                    // this query. Otherwise, except for the scan node, the rest of the execution nodes of the query
                    // can be executed on any BE. addressToBackendID can be empty when this is a constant
                    // select stmt like:
                    //      SELECT  @@session.auto_increment_increment AS auto_increment_increment;
                    execHostport = SimpleScheduler.getHostByCurrentBackend(addressToBackendID);
                } else {
                    execHostport = SimpleScheduler.getHost(this.idToBackend, backendIdRef);
                }
                if (execHostport == null) {
                    LOG.warn("DataPartition UNPARTITIONED, no scanNode Backend available");
                    throw new UserException(SystemInfoService.NO_SCAN_NODE_BACKEND_AVAILABLE_MSG);
                }
                if (backendIdRef.getRef() != null) {
                    // backendIdRef can be null is we call getHostByCurrentBackend() before
                    this.addressToBackendID.put(execHostport, backendIdRef.getRef());
                }
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
                // If the fragment has three children, then the first child and the second child are
                // the children(both exchange node) of shuffle HashJoinNode,
                // and the third child is the right child(ExchangeNode) of broadcast HashJoinNode.
                // We only need to pay attention to the maximum parallelism among
                // the two ExchangeNodes of shuffle HashJoinNode.
                int childrenCount = (fatherNode != null) ? fatherNode.getChildren().size() : 1;
                for (int j = 0; j < childrenCount; j++) {
                    int currentChildFragmentParallelism
                            = fragmentExecParamsMap.get(fragment.getChild(j).getFragmentId()).instanceExecParams.size();
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
                if (exchangeInstances > 0 && fragmentExecParamsMap.get(inputFragmentId)
                        .instanceExecParams.size() > exchangeInstances) {
                    // random select some instance
                    // get distinct host, when parallel_fragment_exec_instance_num > 1,
                    // single host may execute several instances
                    Set<TNetworkAddress> hostSet = Sets.newHashSet();
                    for (FInstanceExecParam execParams :
                            fragmentExecParamsMap.get(inputFragmentId).instanceExecParams) {
                        hostSet.add(execParams.host);
                    }
                    List<TNetworkAddress> hosts = Lists.newArrayList(hostSet);
                    Collections.shuffle(hosts, instanceRandom);
                    for (int index = 0; index < exchangeInstances; index++) {
                        FInstanceExecParam instanceParam = new FInstanceExecParam(null,
                                hosts.get(index % hosts.size()), 0, params);
                        params.instanceExecParams.add(instanceParam);
                    }
                } else {
                    for (FInstanceExecParam execParams
                            : fragmentExecParamsMap.get(inputFragmentId).instanceExecParams) {
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
            if ((isColocateFragment(fragment, fragment.getPlanRoot())
                    && fragmentIdToSeqToAddressMap.containsKey(fragment.getFragmentId())
                    && fragmentIdToSeqToAddressMap.get(fragment.getFragmentId()).size() > 0)) {
                computeColocateJoinInstanceParam(fragment.getFragmentId(), parallelExecInstanceNum, params);
            } else if (bucketShuffleJoinController.isBucketShuffleJoin(fragment.getFragmentId().asInt())) {
                bucketShuffleJoinController.computeInstanceParam(fragment.getFragmentId(),
                        parallelExecInstanceNum, params);
            } else {
                // case A
                for (Entry<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> entry : fragmentExecParamsMap.get(
                        fragment.getFragmentId()).scanRangeAssignment.entrySet()) {
                    TNetworkAddress key = entry.getKey();
                    Map<Integer, List<TScanRangeParams>> value = entry.getValue();

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
                TNetworkAddress execHostport;
                if (ConnectContext.get() != null
                        && !ConnectContext.get().isResourceTagsSet()
                        && !addressToBackendID.isEmpty()) {
                    // In this case, we only use the BE where the replica selected by the tag is located to
                    // execute this query. Otherwise, except for the scan node, the rest of the execution nodes
                    // of the query can be executed on any BE. addressToBackendID can be empty when this is a constant
                    // select stmt like:
                    //      SELECT  @@session.auto_increment_increment AS auto_increment_increment;
                    execHostport = SimpleScheduler.getHostByCurrentBackend(addressToBackendID);
                } else {
                    execHostport = SimpleScheduler.getHost(this.idToBackend, backendIdRef);
                }
                if (execHostport == null) {
                    throw new UserException(SystemInfoService.NO_SCAN_NODE_BACKEND_AVAILABLE_MSG);
                }
                if (backendIdRef.getRef() != null) {
                    // backendIdRef can be null is we call getHostByCurrentBackend() before
                    this.addressToBackendID.put(execHostport, backendIdRef.getRef());
                }
                FInstanceExecParam instanceParam = new FInstanceExecParam(null, execHostport,
                        0, params);
                params.instanceExecParams.add(instanceParam);
            }
        }
    }

    // Traverse the expected runtimeFilterID in each fragment, and establish the corresponding relationship
    // between runtimeFilterID and fragment instance addr and select the merge instance of runtimeFilter
    private void assignRuntimeFilterAddr() throws Exception {
        for (PlanFragment fragment : fragments) {
            FragmentExecParams params = fragmentExecParamsMap.get(fragment.getFragmentId());
            // Transform <fragment, runtimeFilterId> to <runtimeFilterId, fragment>
            for (RuntimeFilterId rid : fragment.getTargetRuntimeFilterIds()) {
                List<FRuntimeFilterTargetParam> targetFragments =
                        ridToTargetParam.computeIfAbsent(rid, k -> new ArrayList<>());
                for (final FInstanceExecParam instance : params.instanceExecParams) {
                    targetFragments.add(new FRuntimeFilterTargetParam(instance.instanceId, toBrpcHost(instance.host)));
                }
            }

            for (RuntimeFilterId rid : fragment.getBuilderRuntimeFilterIds()) {
                ridToBuilderNum.merge(rid, params.instanceExecParams.size(), Integer::sum);
            }
        }
        // Use the uppermost fragment as a merged node, the uppermost fragment has one and only one instance
        FragmentExecParams uppermostParams = fragmentExecParamsMap.get(fragments.get(0).getFragmentId());
        runtimeFilterMergeAddr = toBrpcHost(uppermostParams.instanceExecParams.get(0).host);
        runtimeFilterMergeInstanceId = uppermostParams.instanceExecParams.get(0).instanceId;
    }

    // If fragment has colocated plan node, it will return true.
    private boolean isColocateFragment(PlanFragment planFragment, PlanNode node) {
        // TODO(cmy): some internal process, such as broker load task, do not have ConnectContext.
        // Any configurations needed by the Coordinator should be passed in Coordinator initialization.
        // Refine this later.
        // Currently, just ignore the session variables if ConnectContext does not exist
        if (ConnectContext.get() != null) {
            if (ConnectContext.get().getSessionVariable().isDisableColocatePlan()) {
                return false;
            }
        }

        //cache the colocateFragmentIds
        if (colocateFragmentIds.contains(node.getFragmentId().asInt())) {
            return true;
        }

        if (planFragment.hasColocatePlanNode()) {
            colocateFragmentIds.add(planFragment.getId().asInt());
            return true;
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
        return Pair.of(fatherPlan, newPlan);
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

    private void computeColocateJoinInstanceParam(PlanFragmentId fragmentId,
            int parallelExecInstanceNum, FragmentExecParams params) {
        Map<Integer, TNetworkAddress> bucketSeqToAddress = fragmentIdToSeqToAddressMap.get(fragmentId);
        BucketSeqToScanRange bucketSeqToScanRange = fragmentIdTobucketSeqToScanRangeMap.get(fragmentId);
        Set<Integer> scanNodeIds = fragmentIdToScanNodeIds.get(fragmentId);

        // 1. count each node in one fragment should scan how many tablet, gather them in one list
        Map<TNetworkAddress, List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>>> addressToScanRanges
                = Maps.newHashMap();
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
            // 3. split how many scanRange one instance should scan, same bucket do not split to different instance
            Pair<Integer, Map<Integer, List<TScanRangeParams>>> filteredScanRanges
                    = Pair.of(scanRanges.getKey(), filteredNodeScanRanges);

            if (!addressToScanRanges.containsKey(address)) {
                addressToScanRanges.put(address, Lists.newArrayList());
            }
            addressToScanRanges.get(address).add(filteredScanRanges);
        }
        FragmentScanRangeAssignment assignment = params.scanRangeAssignment;
        for (Map.Entry<TNetworkAddress, List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>>> addressScanRange
                : addressToScanRanges.entrySet()) {
            List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>> scanRange = addressScanRange.getValue();
            Map<Integer, List<TScanRangeParams>> range
                    = findOrInsert(assignment, addressScanRange.getKey(), new HashMap<>());
            int expectedInstanceNum = 1;
            if (parallelExecInstanceNum > 1) {
                //the scan instance num should not larger than the tablets num
                expectedInstanceNum = Math.min(scanRange.size(), parallelExecInstanceNum);
            }

            // 2.split how many scanRange one instance should scan
            List<List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>>> perInstanceScanRanges
                    = ListUtil.splitBySize(scanRange, expectedInstanceNum);

            // 3.construct instanceExecParam add the scanRange should be scan by instance
            for (List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>> perInstanceScanRange
                    : perInstanceScanRanges) {
                FInstanceExecParam instanceParam = new FInstanceExecParam(null, addressScanRange.getKey(), 0, params);

                for (Pair<Integer, Map<Integer, List<TScanRangeParams>>> nodeScanRangeMap : perInstanceScanRange) {
                    instanceParam.bucketSeqSet.add(nodeScanRangeMap.first);
                    for (Map.Entry<Integer, List<TScanRangeParams>> nodeScanRange
                            : nodeScanRangeMap.second.entrySet()) {
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
            // the parameters of getScanRangeLocations may ignore, It doesn't take effect
            List<TScanRangeLocations> locations = scanNode.getScanRangeLocations(0);
            if (locations == null) {
                // only analysis olap scan node
                continue;
            }

            Set<Integer> scanNodeIds = fragmentIdToScanNodeIds.computeIfAbsent(scanNode.getFragmentId(),
                    k -> Sets.newHashSet());
            scanNodeIds.add(scanNode.getId().asInt());

            FragmentScanRangeAssignment assignment
                    = fragmentExecParamsMap.get(scanNode.getFragmentId()).scanRangeAssignment;
            boolean fragmentContainsColocateJoin = isColocateFragment(scanNode.getFragment(),
                    scanNode.getFragment().getPlanRoot());
            boolean fragmentContainsBucketShuffleJoin = bucketShuffleJoinController
                    .isBucketShuffleJoin(scanNode.getFragmentId().asInt(), scanNode.getFragment().getPlanRoot());

            // A fragment may contain both colocate join and bucket shuffle join
            // on need both compute scanRange to init basic data for query coordinator
            if (fragmentContainsColocateJoin) {
                computeScanRangeAssignmentByColocate((OlapScanNode) scanNode);
            }
            if (fragmentContainsBucketShuffleJoin) {
                bucketShuffleJoinController.computeScanRangeAssignmentByBucket((OlapScanNode) scanNode,
                        idToBackend, addressToBackendID);
            }
            if (!(fragmentContainsColocateJoin || fragmentContainsBucketShuffleJoin)) {
                computeScanRangeAssignmentByScheduler(scanNode, locations, assignment, assignedBytesPerHost);
            }
        }
    }

    // To ensure the same bucketSeq tablet to the same execHostPort
    private void computeScanRangeAssignmentByColocate(
            final OlapScanNode scanNode) throws Exception {
        if (!fragmentIdToSeqToAddressMap.containsKey(scanNode.getFragmentId())) {
            fragmentIdToSeqToAddressMap.put(scanNode.getFragmentId(), new HashMap<>());
            fragmentIdTobucketSeqToScanRangeMap.put(scanNode.getFragmentId(), new BucketSeqToScanRange());
        }
        Map<Integer, TNetworkAddress> bucketSeqToAddress = fragmentIdToSeqToAddressMap.get(scanNode.getFragmentId());
        BucketSeqToScanRange bucketSeqToScanRange = fragmentIdTobucketSeqToScanRangeMap.get(scanNode.getFragmentId());

        HashMap<TNetworkAddress, Long> assignedBytesPerHost = Maps.newHashMap();
        for (Integer bucketSeq : scanNode.bucketSeq2locations.keySet()) {
            //fill scanRangeParamsList
            List<TScanRangeLocations> locations = scanNode.bucketSeq2locations.get(bucketSeq);
            if (!bucketSeqToAddress.containsKey(bucketSeq)) {
                getExecHostPortForFragmentIDAndBucketSeq(locations.get(0),
                        scanNode.getFragmentId(), bucketSeq, assignedBytesPerHost);
            }

            for (TScanRangeLocations location : locations) {
                Map<Integer, List<TScanRangeParams>> scanRanges =
                        findOrInsert(bucketSeqToScanRange, bucketSeq, new HashMap<>());

                List<TScanRangeParams> scanRangeParamsList =
                        findOrInsert(scanRanges, scanNode.getId().asInt(), new ArrayList<>());

                // add scan range
                TScanRangeParams scanRangeParams = new TScanRangeParams();
                scanRangeParams.scan_range = location.scan_range;
                scanRangeParamsList.add(scanRangeParams);
            }
        }
    }

    //ensure bucket sequence distribued to every host evenly
    private void getExecHostPortForFragmentIDAndBucketSeq(TScanRangeLocations seqLocation,
            PlanFragmentId fragmentId, Integer bucketSeq, HashMap<TNetworkAddress, Long> assignedBytesPerHost)
            throws Exception {
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
        if (!Config.enable_local_replica_selection) {
            return selectBackendsByRoundRobin(seqLocation.getLocations(), assignedBytesPerHost, backendIdRef);
        }

        List<TScanRangeLocation> localLocations = new ArrayList<>();
        List<TScanRangeLocation> nonlocalLocations = new ArrayList<>();
        long localBeId = Env.getCurrentSystemInfo().getBackendIdByHost(FrontendOptions.getLocalHostAddress());
        for (final TScanRangeLocation location : seqLocation.getLocations()) {
            if (location.backend_id == localBeId) {
                localLocations.add(location);
            } else {
                nonlocalLocations.add(location);
            }
        }

        try {
            return selectBackendsByRoundRobin(localLocations, assignedBytesPerHost, backendIdRef);
        } catch (UserException ue) {
            if (!Config.enable_local_replica_selection_fallback) {
                throw ue;
            }
            return selectBackendsByRoundRobin(nonlocalLocations, assignedBytesPerHost, backendIdRef);
        }
    }

    public TScanRangeLocation selectBackendsByRoundRobin(List<TScanRangeLocation> locations,
            HashMap<TNetworkAddress, Long> assignedBytesPerHost, Reference<Long> backendIdRef) throws UserException {
        Long minAssignedBytes = Long.MAX_VALUE;
        TScanRangeLocation minLocation = null;
        Long step = 1L;
        for (final TScanRangeLocation location : locations) {
            Long assignedBytes = findOrInsert(assignedBytesPerHost, location.server, 0L);
            if (assignedBytes < minAssignedBytes) {
                minAssignedBytes = assignedBytes;
                minLocation = location;
            }
        }
        TScanRangeLocation location = SimpleScheduler.getLocation(minLocation, locations,
                this.idToBackend, backendIdRef);
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
            TScanRangeLocation minLocation = selectBackendsByRoundRobin(scanRangeLocations,
                    assignedBytesPerHost, backendIdRef);
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
            if (params.isSetErrorTabletInfos()) {
                updateErrorTabletInfos(params.getErrorTabletInfos());
            }
            profileDoneSignal.markedCountDown(params.getFragmentInstanceId(), -1L);
        }

        if (params.isSetLoadedRows()) {
            Env.getCurrentEnv().getLoadManager().updateJobProgress(
                    jobId, params.getBackendId(), params.getQueryId(), params.getFragmentInstanceId(),
                    params.getLoadedRows(), params.getLoadedBytes(), params.isDone());
        }
    }

    public void endProfile() {
        endProfile(true);
    }

    public void endProfile(boolean waitProfileDone) {
        if (backendExecStates.isEmpty()) {
            return;
        }

        // Wait for all backends to finish reporting when writing profile last time.
        if (waitProfileDone && needReport) {
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
                queryStatus = new Status(TStatusCode.INTERNAL_ERROR, "backend "
                        + backendExecState.backend.getId() + " is down");
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
        private final Map<PlanFragmentId, BucketSeqToScanRange> fragmentIdBucketSeqToScanRangeMap = Maps.newHashMap();
        // fragment_id -> < bucket_seq -> be_addresss >
        private final Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentIdToSeqToAddressMap
                = Maps.newHashMap();
        // fragment_id -> < be_id -> bucket_count >
        private final Map<PlanFragmentId, Map<Long, Integer>> fragmentIdToBuckendIdBucketCountMap = Maps.newHashMap();
        // fragment_id -> bucket_num
        private final Map<PlanFragmentId, Integer> fragmentIdToBucketNumMap = Maps.newHashMap();

        // cache the bucketShuffleFragmentIds
        private final Set<Integer> bucketShuffleFragmentIds = new HashSet<>();

        private final Map<PlanFragmentId, Set<Integer>> fragmentIdToScanNodeIds;

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

            if (node instanceof HashJoinNode) {
                HashJoinNode joinNode = (HashJoinNode) node;
                if (joinNode.isBucketShuffle()) {
                    bucketShuffleFragmentIds.add(joinNode.getFragmentId().asInt());
                    return true;
                }
            }

            for (PlanNode childNode : node.getChildren()) {
                if (isBucketShuffleJoin(fragmentId, childNode)) {
                    return true;
                }
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
        private void getExecHostPortForFragmentIDAndBucketSeq(TScanRangeLocations seqLocation,
                PlanFragmentId fragmentId, Integer bucketSeq, ImmutableMap<Long, Backend> idToBackend,
                Map<TNetworkAddress, Long> addressToBackendID) throws Exception {
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
            TNetworkAddress execHostPort = SimpleScheduler.getHost(buckendId,
                    seqLocation.locations, idToBackend, backendIdRef);
            if (execHostPort == null) {
                throw new UserException(SystemInfoService.NO_SCAN_NODE_BACKEND_AVAILABLE_MSG);
            }
            //the backend with buckendId is not alive, chose another new backend
            if (backendIdRef.getRef() != buckendId) {
                //buckendIdToBucketCountMap does not contain the new backend, insert into it
                if (!buckendIdToBucketCountMap.containsKey(backendIdRef.getRef())) {
                    buckendIdToBucketCountMap.put(backendIdRef.getRef(), 1);
                } else { //buckendIdToBucketCountMap contains the new backend, update it
                    buckendIdToBucketCountMap.put(backendIdRef.getRef(),
                            buckendIdToBucketCountMap.get(backendIdRef.getRef()) + 1);
                }
            } else { //the backend with buckendId is alive, update buckendIdToBucketCountMap directly
                buckendIdToBucketCountMap.put(buckendId, buckendIdToBucketCountMap.get(buckendId) + 1);
            }
            addressToBackendID.put(execHostPort, backendIdRef.getRef());
            this.fragmentIdToSeqToAddressMap.get(fragmentId).put(bucketSeq, execHostPort);
        }

        // to ensure the same bucketSeq tablet to the same execHostPort
        private void computeScanRangeAssignmentByBucket(
                final OlapScanNode scanNode, ImmutableMap<Long, Backend> idToBackend,
                Map<TNetworkAddress, Long> addressToBackendID) throws Exception {
            if (!fragmentIdToSeqToAddressMap.containsKey(scanNode.getFragmentId())) {
                // In bucket shuffle join, we have 2 situation.
                // 1. Only one partition: in this case, we use scanNode.getTotalTabletsNum() to get the right bucket num
                //    because when table turn on dynamic partition, the bucket number in default distribution info
                //    is not correct.
                // 2. Table is colocated: in this case, table could have more than one partition, but all partition's
                //    bucket number must be same, so we use default bucket num is ok.
                int bucketNum = 0;
                if (scanNode.getOlapTable().isColocateTable()) {
                    bucketNum = scanNode.getOlapTable().getDefaultDistributionInfo().getBucketNum();
                } else {
                    bucketNum = (int) (scanNode.getTotalTabletsNum());
                }
                fragmentIdToBucketNumMap.put(scanNode.getFragmentId(), bucketNum);
                fragmentIdToSeqToAddressMap.put(scanNode.getFragmentId(), new HashMap<>());
                fragmentIdBucketSeqToScanRangeMap.put(scanNode.getFragmentId(), new BucketSeqToScanRange());
                fragmentIdToBuckendIdBucketCountMap.put(scanNode.getFragmentId(), new HashMap<>());
            }
            Map<Integer, TNetworkAddress> bucketSeqToAddress
                    = fragmentIdToSeqToAddressMap.get(scanNode.getFragmentId());
            BucketSeqToScanRange bucketSeqToScanRange = fragmentIdBucketSeqToScanRangeMap.get(scanNode.getFragmentId());

            for (Integer bucketSeq : scanNode.bucketSeq2locations.keySet()) {
                //fill scanRangeParamsList
                List<TScanRangeLocations> locations = scanNode.bucketSeq2locations.get(bucketSeq);
                if (!bucketSeqToAddress.containsKey(bucketSeq)) {
                    getExecHostPortForFragmentIDAndBucketSeq(locations.get(0), scanNode.getFragmentId(),
                            bucketSeq, idToBackend, addressToBackendID);
                }

                for (TScanRangeLocations location : locations) {
                    Map<Integer, List<TScanRangeParams>> scanRanges =
                            findOrInsert(bucketSeqToScanRange, bucketSeq, new HashMap<>());

                    List<TScanRangeParams> scanRangeParamsList =
                            findOrInsert(scanRanges, scanNode.getId().asInt(), new ArrayList<>());

                    // add scan range
                    TScanRangeParams scanRangeParams = new TScanRangeParams();
                    scanRangeParams.scan_range = location.scan_range;
                    scanRangeParamsList.add(scanRangeParams);
                }
            }
        }

        private void computeInstanceParam(PlanFragmentId fragmentId,
                int parallelExecInstanceNum, FragmentExecParams params) {
            Map<Integer, TNetworkAddress> bucketSeqToAddress = fragmentIdToSeqToAddressMap.get(fragmentId);
            BucketSeqToScanRange bucketSeqToScanRange = fragmentIdBucketSeqToScanRangeMap.get(fragmentId);
            Set<Integer> scanNodeIds = fragmentIdToScanNodeIds.get(fragmentId);

            // 1. count each node in one fragment should scan how many tablet, gather them in one list
            Map<TNetworkAddress, List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>>> addressToScanRanges
                    = Maps.newHashMap();
            for (Map.Entry<Integer, Map<Integer, List<TScanRangeParams>>> scanRanges
                    : bucketSeqToScanRange.entrySet()) {
                TNetworkAddress address = bucketSeqToAddress.get(scanRanges.getKey());
                Map<Integer, List<TScanRangeParams>> nodeScanRanges = scanRanges.getValue();
                // We only care about the node scan ranges of scan nodes which belong to this fragment
                Map<Integer, List<TScanRangeParams>> filteredNodeScanRanges = Maps.newHashMap();
                for (Integer scanNodeId : nodeScanRanges.keySet()) {
                    if (scanNodeIds.contains(scanNodeId)) {
                        filteredNodeScanRanges.put(scanNodeId, nodeScanRanges.get(scanNodeId));
                    }
                }
                Pair<Integer, Map<Integer, List<TScanRangeParams>>> filteredScanRanges
                        = Pair.of(scanRanges.getKey(), filteredNodeScanRanges);

                if (!addressToScanRanges.containsKey(address)) {
                    addressToScanRanges.put(address, Lists.newArrayList());
                }
                addressToScanRanges.get(address).add(filteredScanRanges);
            }
            FragmentScanRangeAssignment assignment = params.scanRangeAssignment;
            for (Map.Entry<TNetworkAddress, List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>>> addressScanRange
                    : addressToScanRanges.entrySet()) {
                List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>> scanRange = addressScanRange.getValue();
                Map<Integer, List<TScanRangeParams>> range
                        = findOrInsert(assignment, addressScanRange.getKey(), new HashMap<>());
                int expectedInstanceNum = 1;
                if (parallelExecInstanceNum > 1) {
                    //the scan instance num should not larger than the tablets num
                    expectedInstanceNum = Math.min(scanRange.size(), parallelExecInstanceNum);
                }

                // 2. split how many scanRange one instance should scan
                List<List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>>> perInstanceScanRanges
                        = ListUtil.splitBySize(scanRange, expectedInstanceNum);

                // 3.construct instanceExecParam add the scanRange should be scan by instance
                for (List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>> perInstanceScanRange
                        : perInstanceScanRanges) {
                    FInstanceExecParam instanceParam = new FInstanceExecParam(
                            null, addressScanRange.getKey(), 0, params);

                    for (Pair<Integer, Map<Integer, List<TScanRangeParams>>> nodeScanRangeMap : perInstanceScanRange) {
                        instanceParam.addBucketSeq(nodeScanRangeMap.first);
                        for (Map.Entry<Integer, List<TScanRangeParams>> nodeScanRange
                                : nodeScanRangeMap.second.entrySet()) {
                            if (!instanceParam.perNodeScanRanges.containsKey(nodeScanRange.getKey())) {
                                range.put(nodeScanRange.getKey(), Lists.newArrayList());
                                instanceParam.perNodeScanRanges.put(nodeScanRange.getKey(), Lists.newArrayList());
                            }
                            range.get(nodeScanRange.getKey()).addAll(nodeScanRange.getValue());
                            instanceParam.perNodeScanRanges.get(nodeScanRange.getKey())
                                    .addAll(nodeScanRange.getValue());
                        }
                    }
                    params.instanceExecParams.add(instanceParam);
                }
            }
        }
    }

    private final Map<PlanFragmentId, BucketSeqToScanRange> fragmentIdTobucketSeqToScanRangeMap = Maps.newHashMap();
    private final Map<PlanFragmentId, Map<Integer, TNetworkAddress>> fragmentIdToSeqToAddressMap = Maps.newHashMap();
    // cache the fragment id to its scan node ids. Used for colocate join.
    private final Map<PlanFragmentId, Set<Integer>> fragmentIdToScanNodeIds = Maps.newHashMap();
    private final Set<Integer> colocateFragmentIds = new HashSet<>();
    private final BucketShuffleJoinController bucketShuffleJoinController
            = new BucketShuffleJoinController(fragmentIdToScanNodeIds);

    // record backend execute state
    // TODO(zhaochun): add profile information and others
    public class BackendExecState {
        TExecPlanFragmentParams rpcParams;
        PlanFragmentId fragmentId;
        boolean initiated;
        volatile boolean done;
        boolean hasCanceled;
        int profileFragmentId;
        RuntimeProfile profile;
        TNetworkAddress brpcAddress;
        TNetworkAddress address;
        Backend backend;
        long lastMissingHeartbeatTime = -1;
        TUniqueId instanceId;

        public BackendExecState(PlanFragmentId fragmentId, int instanceId, int profileFragmentId,
                                TExecPlanFragmentParams rpcParams, Map<TNetworkAddress, Long> addressToBackendID) {
            this.profileFragmentId = profileFragmentId;
            this.fragmentId = fragmentId;
            this.rpcParams = rpcParams;
            this.initiated = false;
            this.done = false;
            FInstanceExecParam fi = fragmentExecParamsMap.get(fragmentId).instanceExecParams.get(instanceId);
            this.instanceId = fi.instanceId;
            this.address = fi.host;
            this.backend = idToBackend.get(addressToBackendID.get(address));
            this.brpcAddress = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());

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
            this.rpcParams.unsetDescTbl();
            this.rpcParams.unsetCoord();
            this.rpcParams.unsetQueryGlobals();
            this.rpcParams.unsetResourceInfo();
            this.rpcParams.setIsSimplifiedParam(true);
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
        public synchronized boolean cancelFragmentInstance(Types.PPlanFragmentCancelReason cancelReason) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("cancelRemoteFragments initiated={} done={} hasCanceled={} backend: {},"
                                + " fragment instance id={}, reason: {}",
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

                Span span = ConnectContext.get() != null
                        ? ConnectContext.get().getTracer().spanBuilder("cancelPlanFragmentAsync")
                        .setParent(Context.current()).setSpanKind(SpanKind.CLIENT).startSpan()
                        : Telemetry.getNoopSpan();
                try (Scope scope = span.makeCurrent()) {
                    BackendServiceProxy.getInstance().cancelPlanFragmentAsync(brpcAddress,
                            fragmentInstanceId(), cancelReason);
                } catch (RpcException e) {
                    span.recordException(e);
                    LOG.warn("cancel plan fragment get a exception, address={}:{}", brpcAddress.getHostname(),
                            brpcAddress.getPort());
                    SimpleScheduler.addToBlacklist(addressToBackendID.get(brpcAddress), e.getMessage());
                } finally {
                    span.end();
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
                LOG.warn("backend {} is down while joining the coordinator. job id: {}",
                        backend.getId(), jobId);
                return false;
            }
            return true;
        }

        public FragmentInstanceInfo buildFragmentInstanceInfo() {
            return new QueryStatisticsItem.FragmentInstanceInfo.Builder().instanceId(fragmentInstanceId())
                    .fragmentId(String.valueOf(fragmentId)).address(this.address).build();
        }

        private TUniqueId fragmentInstanceId() {
            return this.rpcParams.params.getFragmentInstanceId();
        }
    }

    /**
     * A set of BackendExecState for same Backend
     */
    public class BackendExecStates {
        long beId;
        TNetworkAddress brpcAddr;
        List<BackendExecState> states = Lists.newArrayList();
        boolean twoPhaseExecution = false;
        ScopedSpan scopedSpan = new ScopedSpan();

        public BackendExecStates(long beId, TNetworkAddress brpcAddr, boolean twoPhaseExecution) {
            this.beId = beId;
            this.brpcAddr = brpcAddr;
            this.twoPhaseExecution = twoPhaseExecution;
        }

        public void addState(BackendExecState state) {
            this.states.add(state);
        }

        /**
         * The BackendExecState in states are all send to the same BE.
         * So only the first BackendExecState need to carry some common fields, such as DescriptorTbl,
         * the other BackendExecState does not need those fields. Unset them to reduce size.
         */
        public void unsetFields() {
            boolean first = true;
            for (BackendExecState state : states) {
                if (first) {
                    first = false;
                    continue;
                }
                state.unsetFields();
            }
        }

        public Future<InternalService.PExecPlanFragmentResult> execRemoteFragmentsAsync() throws TException {
            try {
                TExecPlanFragmentParamsList paramsList = new TExecPlanFragmentParamsList();
                for (BackendExecState state : states) {
                    state.initiated = true;
                    paramsList.addToParamsList(state.rpcParams);
                }
                return BackendServiceProxy.getInstance()
                        .execPlanFragmentsAsync(brpcAddr, paramsList, twoPhaseExecution);
            } catch (RpcException e) {
                // DO NOT throw exception here, return a complete future with error code,
                // so that the following logic will cancel the fragment.
                return futureWithException(e);
            }
        }

        public Future<InternalService.PExecPlanFragmentResult> execPlanFragmentStartAsync() throws TException {
            try {
                PExecPlanFragmentStartRequest.Builder builder = PExecPlanFragmentStartRequest.newBuilder();
                PUniqueId qid = PUniqueId.newBuilder().setHi(queryId.hi).setLo(queryId.lo).build();
                builder.setQueryId(qid);
                return BackendServiceProxy.getInstance().execPlanFragmentStartAsync(brpcAddr, builder.build());
            } catch (RpcException e) {
                // DO NOT throw exception here, return a complete future with error code,
                // so that the following logic will cancel the fragment.
                return futureWithException(e);
            }
        }

        @NotNull
        private Future<PExecPlanFragmentResult> futureWithException(RpcException e) {
            return new Future<PExecPlanFragmentResult>() {
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
                public PExecPlanFragmentResult get() {
                    PExecPlanFragmentResult result = PExecPlanFragmentResult.newBuilder().setStatus(
                            Types.PStatus.newBuilder().addErrorMsgs(e.getMessage())
                                    .setStatusCode(TStatusCode.THRIFT_RPC_ERROR.getValue()).build()).build();
                    return result;
                }

                @Override
                public PExecPlanFragmentResult get(long timeout, TimeUnit unit) {
                    return get();
                }
            };
        }
    }

    // execution parameters for a single fragment,
    // per-fragment can have multiple FInstanceExecParam,
    // used to assemble TPlanFragmentExecParams
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
                params.params.setRuntimeFilterParams(new TRuntimeFilterParams());
                params.params.runtime_filter_params.setRuntimeFilterMergeAddr(runtimeFilterMergeAddr);
                if (instanceExecParam.instanceId.equals(runtimeFilterMergeInstanceId)) {
                    for (Map.Entry<RuntimeFilterId, List<FRuntimeFilterTargetParam>> entry
                            : ridToTargetParam.entrySet()) {
                        List<TRuntimeFilterTargetParams> targetParams = Lists.newArrayList();
                        for (FRuntimeFilterTargetParam targetParam : entry.getValue()) {
                            targetParams.add(new TRuntimeFilterTargetParams(targetParam.targetFragmentInstanceId,
                                    targetParam.targetFragmentInstanceAddr));
                        }
                        params.params.runtime_filter_params.putToRidToTargetParam(entry.getKey().asInt(), targetParams);
                    }
                    for (Map.Entry<RuntimeFilterId, Integer> entry : ridToBuilderNum.entrySet()) {
                        params.params.runtime_filter_params.putToRuntimeFilterBuilderNum(
                                entry.getKey().asInt(), entry.getValue());
                    }
                    for (RuntimeFilter rf : assignedRuntimeFilters) {
                        params.params.runtime_filter_params.putToRidToRuntimeFilter(
                                rf.getFilterId().asInt(), rf.toThrift());
                    }
                }
                if (queryOptions.getQueryType() == TQueryType.LOAD) {
                    LoadErrorHub.Param param = Env.getCurrentEnv().getLoadInstance().getLoadErrorHubInfo();
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
    // the per-instance TPlanFragmentExecParams, as a member of
    // FragmentExecParams
    static class FInstanceExecParam {
        TUniqueId instanceId;
        TNetworkAddress host;
        Map<Integer, List<TScanRangeParams>> perNodeScanRanges = Maps.newHashMap();

        int perFragmentInstanceIdx;

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
        lock();
        try {
            for (int index = 0; index < fragments.size(); index++) {
                for (BackendExecState backendExecState : backendExecStates) {
                    if (fragments.get(index).getFragmentId() != backendExecState.fragmentId) {
                        continue;
                    }
                    final QueryStatisticsItem.FragmentInstanceInfo info = backendExecState.buildFragmentInstanceInfo();
                    result.add(info);
                }
            }
        } finally {
            unlock();
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

    // Runtime filter target fragment instance param
    static class FRuntimeFilterTargetParam {
        public TUniqueId targetFragmentInstanceId;

        public TNetworkAddress targetFragmentInstanceAddr;

        public FRuntimeFilterTargetParam(TUniqueId id, TNetworkAddress host) {
            this.targetFragmentInstanceId = id;
            this.targetFragmentInstanceAddr = host;
        }
    }
}

