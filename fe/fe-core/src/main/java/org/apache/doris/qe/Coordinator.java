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
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.ListUtil;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.ExternalScanNode;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.DataStreamSink;
import org.apache.doris.planner.ExceptNode;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.HashJoinNode;
import org.apache.doris.planner.IntersectNode;
import org.apache.doris.planner.MultiCastDataSink;
import org.apache.doris.planner.MultiCastPlanFragment;
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
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.QueryStatisticsItem.FragmentInstanceInfo;
import org.apache.doris.resource.workloadgroup.QueryQueue;
import org.apache.doris.resource.workloadgroup.QueueToken;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TBrokerScanRange;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TErrorTabletInfo;
import org.apache.doris.thrift.TEsScanRange;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TExecPlanFragmentParamsList;
import org.apache.doris.thrift.TExternalScanRange;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TPlanFragment;
import org.apache.doris.thrift.TPlanFragmentDestination;
import org.apache.doris.thrift.TPlanFragmentExecParams;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TResourceLimit;
import org.apache.doris.thrift.TRuntimeFilterParams;
import org.apache.doris.thrift.TRuntimeFilterTargetParams;
import org.apache.doris.thrift.TRuntimeFilterTargetParamsV2;
import org.apache.doris.thrift.TScanRange;
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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;

import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Coordinator implements CoordInterface {
    private static final Logger LOG = LogManager.getLogger(Coordinator.class);

    private static final String localIP = FrontendOptions.getLocalHostAddress();

    // Random is used to shuffle instances of partitioned
    private static final Random instanceRandom = new SecureRandom();

    private static ExecutorService backendRpcCallbackExecutor = ThreadPoolManager.newDaemonProfileThreadPool(32, 100,
            "backend-rpc-callback", true);

    // Overall status of the entire query; set to the first reported fragment error
    // status or to CANCELLED, if Cancel() is called.
    Status queryStatus = new Status();

    // save of related backends of this query
    Map<TNetworkAddress, Long> addressToBackendID = Maps.newHashMap();

    protected ImmutableMap<Long, Backend> idToBackend = ImmutableMap.of();

    // copied from TQueryExecRequest; constant across all fragments
    private final TDescriptorTable descTable;

    // scan node id -> TFileScanRangeParams
    private Map<Integer, TFileScanRangeParams> fileScanRangeParamsMap = Maps.newHashMap();

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
    private boolean returnedAllResults = false;

    // populated in computeFragmentExecParams()
    private final Map<PlanFragmentId, FragmentExecParams> fragmentExecParamsMap = Maps.newHashMap();

    private final List<PlanFragment> fragments;

    private Map<Long, BackendExecStates> beToExecStates = Maps.newHashMap();
    private Map<Long, PipelineExecContexts> beToPipelineExecCtxs = Maps.newHashMap();

    // backend execute state
    private final List<BackendExecState> backendExecStates = Lists.newArrayList();
    private final Map<Pair<Integer, Long>, PipelineExecContext> pipelineExecContexts = new HashMap<>();
    // backend which state need to be checked when joining this coordinator.
    // It is supposed to be the subset of backendExecStates.
    private final List<BackendExecState> needCheckBackendExecStates = Lists.newArrayList();
    private final List<PipelineExecContext> needCheckPipelineExecContexts = Lists.newArrayList();
    private ResultReceiver receiver;
    protected final List<ScanNode> scanNodes;
    private int scanRangeNum = 0;
    // number of instances of this query, equals to
    // number of backends executing plan fragments on behalf of this query;
    // set in computeFragmentExecParams();
    // same as backend_exec_states_.size() after Exec()
    private final Set<TUniqueId> instanceIds = Sets.newHashSet();

    private final boolean isBlockQuery;

    private int numReceivedRows = 0;

    private List<String> deltaUrls;
    private Map<String, String> loadCounters;
    private String trackingUrl;

    // for export
    private List<String> exportFiles;

    private final List<TTabletCommitInfo> commitInfos = Lists.newArrayList();
    private final List<TErrorTabletInfo> errorTabletInfos = Lists.newArrayList();

    // Collect all hivePartitionUpdates obtained from be
    Consumer<List<THivePartitionUpdate>> hivePartitionUpdateFunc;

    // Input parameter
    private long jobId = -1; // job which this task belongs to
    private TUniqueId queryId;

    // parallel execute
    private final TUniqueId nextInstanceId;

    // a timestamp represent the absolute timeout
    // eg, System.currentTimeMillis() + executeTimeoutS * 1000
    private long timeoutDeadline;

    private boolean enableShareHashTableForBroadcastJoin = false;

    private boolean enablePipelineEngine = false;
    private boolean enablePipelineXEngine = false;
    private boolean useNereids = false;

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
    private ConnectContext context;

    private PointQueryExec pointExec = null;

    private StatsErrorEstimator statsErrorEstimator;

    // A countdown latch to mark the completion of each instance.
    // use for old pipeline
    // instance id -> dummy value
    private MarkedCountDownLatch<TUniqueId, Long> instancesDoneLatch = null;

    // A countdown latch to mark the completion of each fragment. use for pipelineX
    // fragmentid -> backendid
    private MarkedCountDownLatch<Integer, Long> fragmentsDoneLatch = null;

    public void setTWorkloadGroups(List<TPipelineWorkloadGroup> tWorkloadGroups) {
        this.tWorkloadGroups = tWorkloadGroups;
    }

    public List<TPipelineWorkloadGroup> gettWorkloadGroups() {
        return tWorkloadGroups;
    }

    private List<TPipelineWorkloadGroup> tWorkloadGroups = Lists.newArrayList();

    private final ExecutionProfile executionProfile;

    private volatile QueueToken queueToken = null;
    private QueryQueue queryQueue = null;

    public ExecutionProfile getExecutionProfile() {
        return executionProfile;
    }

    // True if all scan node are ExternalScanNode.
    private boolean isAllExternalScan = true;

    // Used for query/insert
    public Coordinator(ConnectContext context, Analyzer analyzer, Planner planner,
            StatsErrorEstimator statsErrorEstimator) {
        this(context, analyzer, planner);
        this.statsErrorEstimator = statsErrorEstimator;
    }

    // Used for query/insert/test
    public Coordinator(ConnectContext context, Analyzer analyzer, Planner planner) {
        this.context = context;
        this.isBlockQuery = planner.isBlockQuery();
        this.queryId = context.queryId();
        this.fragments = planner.getFragments();
        this.scanNodes = planner.getScanNodes();
        this.descTable = planner.getDescTable().toThrift();

        this.returnedAllResults = false;
        this.enableShareHashTableForBroadcastJoin = context.getSessionVariable().enableShareHashTableForBroadcastJoin;
        // Only enable pipeline query engine in query, not load
        this.enablePipelineEngine = context.getSessionVariable().getEnablePipelineEngine()
                && (fragments.size() > 0);
        this.enablePipelineXEngine = context.getSessionVariable().getEnablePipelineXEngine()
                && (fragments.size() > 0);

        initQueryOptions(context);
        useNereids = planner instanceof NereidsPlanner;
        if (!useNereids) {
            // Enable local shuffle on pipelineX engine only if Nereids planner is applied.
            queryOptions.setEnableLocalShuffle(false);
        }

        setFromUserProperty(context);

        this.queryGlobals.setNowString(TimeUtils.DATETIME_FORMAT.format(LocalDateTime.now()));
        this.queryGlobals.setTimestampMs(System.currentTimeMillis());
        this.queryGlobals.setNanoSeconds(LocalDateTime.now().getNano());
        this.queryGlobals.setLoadZeroTolerance(false);
        if (context.getSessionVariable().getTimeZone().equals("CST")) {
            this.queryGlobals.setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
        } else {
            this.queryGlobals.setTimeZone(context.getSessionVariable().getTimeZone());
        }
        this.nextInstanceId = new TUniqueId();
        nextInstanceId.setHi(queryId.hi);
        nextInstanceId.setLo(queryId.lo + 1);
        this.assignedRuntimeFilters = planner.getRuntimeFilters();
        this.executionProfile = new ExecutionProfile(queryId, fragments);

    }

    // Used for broker load task/export task/update coordinator
    public Coordinator(Long jobId, TUniqueId queryId, DescriptorTable descTable, List<PlanFragment> fragments,
            List<ScanNode> scanNodes, String timezone, boolean loadZeroTolerance) {
        this.isBlockQuery = true;
        this.jobId = jobId;
        this.queryId = queryId;
        this.descTable = descTable.toThrift();
        this.fragments = fragments;
        this.scanNodes = scanNodes;
        this.queryOptions = new TQueryOptions();
        this.queryGlobals.setNowString(TimeUtils.DATETIME_FORMAT.format(LocalDateTime.now()));
        this.queryGlobals.setTimestampMs(System.currentTimeMillis());
        this.queryGlobals.setTimeZone(timezone);
        this.queryGlobals.setLoadZeroTolerance(loadZeroTolerance);
        this.queryOptions.setBeExecVersion(Config.be_exec_version);
        this.nextInstanceId = new TUniqueId();
        nextInstanceId.setHi(queryId.hi);
        nextInstanceId.setLo(queryId.lo + 1);
        this.executionProfile = new ExecutionProfile(queryId, fragments);
    }

    private void setFromUserProperty(ConnectContext connectContext) {
        String qualifiedUser = connectContext.getQualifiedUser();
        // set cpu resource limit
        int cpuLimit = Env.getCurrentEnv().getAuth().getCpuResourceLimit(qualifiedUser);
        if (cpuLimit > 0) {
            // overwrite the cpu resource limit from session variable;
            TResourceLimit resourceLimit = new TResourceLimit();
            resourceLimit.setCpuLimit(cpuLimit);
            this.queryOptions.setResourceLimit(resourceLimit);
        }
        // set exec mem limit
        long maxExecMemByte = connectContext.getSessionVariable().getMaxExecMemByte();
        long memLimit = maxExecMemByte > 0 ? maxExecMemByte :
                Env.getCurrentEnv().getAuth().getExecMemLimit(qualifiedUser);
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
        this.queryOptions.setEnablePipelineEngine(SessionVariable.enablePipelineEngine());
        this.queryOptions.setBeExecVersion(Config.be_exec_version);
        this.queryOptions.setQueryTimeout(context.getExecTimeout());
        this.queryOptions.setExecutionTimeout(context.getExecTimeout());
        if (this.queryOptions.getExecutionTimeout() < 1) {
            LOG.info("try set timeout less than 1", new RuntimeException(""));
        }
        this.queryOptions.setEnableScanNodeRunSerial(context.getSessionVariable().isEnableScanRunSerial());
        this.queryOptions.setFeProcessUuid(ExecuteEnv.getInstance().getProcessUUID());
        this.queryOptions.setWaitFullBlockScheduleTimes(context.getSessionVariable().getWaitFullBlockScheduleTimes());
    }

    public ConnectContext getConnectContext() {
        return context;
    }

    public long getJobId() {
        return jobId;
    }

    public TUniqueId getQueryId() {
        return queryId;
    }

    public int getScanRangeNum() {
        return scanRangeNum;
    }

    public TQueryOptions getQueryOptions() {
        return this.queryOptions;
    }

    public void setQueryId(TUniqueId queryId) {
        this.queryId = queryId;
    }

    public void setQueryType(TQueryType type) {
        this.queryOptions.setQueryType(type);
    }

    public void setExecPipEngine(boolean vec) {
        this.queryOptions.setEnablePipelineEngine(vec);
    }

    public Status getExecStatus() {
        return queryStatus;
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
        this.queryOptions.setExecutionTimeout(timeout);
        if (this.queryOptions.getExecutionTimeout() < 1) {
            LOG.info("try set timeout less than 1", new RuntimeException(""));
        }
    }

    public void setLoadZeroTolerance(boolean loadZeroTolerance) {
        this.queryGlobals.setLoadZeroTolerance(loadZeroTolerance);
    }

    public void clearExportStatus() {
        lock.lock();
        try {
            this.backendExecStates.clear();
            this.pipelineExecContexts.clear();
            this.queryStatus.setStatus(new Status());
            if (this.exportFiles == null) {
                this.exportFiles = Lists.newArrayList();
            }
            this.exportFiles.clear();
            this.needCheckBackendExecStates.clear();
            this.needCheckPipelineExecContexts.clear();
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

    public Map<String, Integer> getBeToInstancesNum() {
        Map<String, Integer> result = Maps.newTreeMap();
        if (enablePipelineEngine) {
            for (PipelineExecContexts ctxs : beToPipelineExecCtxs.values()) {
                result.put(ctxs.brpcAddr.hostname.concat(":").concat("" + ctxs.brpcAddr.port),
                        ctxs.getInstanceNumber());
            }
        } else {
            for (BackendExecStates states : beToExecStates.values()) {
                result.put(states.brpcAddr.hostname.concat(":").concat("" + states.brpcAddr.port),
                        states.states.size());
            }
        }
        return result;
    }

    // Initialize
    protected void prepare() throws UserException {
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

        this.idToBackend = Env.getCurrentSystemInfo().getBackendsWithIdByCurrentCluster();

        if (LOG.isDebugEnabled()) {
            int backendNum = idToBackend.size();
            StringBuilder backendInfos = new StringBuilder("backends info:");
            for (Map.Entry<Long, Backend> entry : idToBackend.entrySet()) {
                Long backendID = entry.getKey();
                Backend backend = entry.getValue();
                backendInfos.append(' ').append(backendID).append("-")
                            .append(backend.getHost()).append("-")
                            .append(backend.getBePort()).append("-")
                            .append(backend.getProcessEpoch());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("query {}, backend size: {}, {}",
                        DebugUtil.printId(queryId), backendNum, backendInfos.toString());
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
            if (LOG.isDebugEnabled()) {
                LOG.debug(sb.toString());
            }
        }
    }

    protected void processFragmentAssignmentAndParams() throws Exception {
        // prepare information
        prepare();
        // compute Fragment Instance
        computeScanRangeAssignment();

        computeFragmentExecParams();
    }


    public TExecPlanFragmentParams getStreamLoadPlan() throws Exception {
        processFragmentAssignmentAndParams();

        // This is a load process.
        List<Long> relatedBackendIds = Lists.newArrayList(addressToBackendID.values());
        Env.getCurrentEnv().getLoadManager().initJobProgress(jobId, queryId, instanceIds,
                relatedBackendIds);
        Env.getCurrentEnv().getProgressManager().addTotalScanNums(String.valueOf(jobId), scanRangeNum);
        LOG.info("dispatch load job: {} to {}", DebugUtil.printId(queryId), addressToBackendID.keySet());

        List<TExecPlanFragmentParams> tExecPlanFragmentParams
                = ((FragmentExecParams) this.fragmentExecParamsMap.values().toArray()[0]).toThrift(0);
        TExecPlanFragmentParams fragmentParams = tExecPlanFragmentParams.get(0);
        return fragmentParams;
    }

    // Initiate asynchronous execution of query. Returns as soon as all plan fragments
    // have started executing at their respective backends.
    // 'Request' must contain at least a coordinator plan fragment (ie, can't
    // be for a query like 'SELECT 1').
    // A call to Exec() must precede all other member function calls.
    @Override
    public void exec() throws Exception {
        // LoadTask does not have context, not controlled by queue now
        if (context != null) {
            if (Config.enable_workload_group) {
                this.setTWorkloadGroups(context.getEnv().getWorkloadGroupMgr().getWorkloadGroup(context));
                boolean shouldQueue = Config.enable_query_queue && !context.getSessionVariable()
                        .getBypassWorkloadGroup() && !isQueryCancelled();
                if (shouldQueue) {
                    queryQueue = context.getEnv().getWorkloadGroupMgr().getWorkloadGroupQueryQueue(context);
                    if (queryQueue == null) {
                        // This logic is actually useless, because when could not find query queue, it will
                        // throw exception during workload group manager.
                        throw new UserException("could not find query queue");
                    }
                    queueToken = queryQueue.getToken();
                    if (!queueToken.waitSignal(this.queryOptions.getExecutionTimeout() * 1000)) {
                        LOG.error("query (id=" + DebugUtil.printId(queryId) + ") " + queueToken.getOfferResultDetail());
                        queryQueue.returnToken(queueToken);
                        throw new UserException(queueToken.getOfferResultDetail());
                    }
                }
            } else {
                context.setWorkloadGroupName("");
            }
        }
        execInternal();
    }

    @Override
    public void close() {
        if (queryQueue != null && queueToken != null) {
            try {
                queryQueue.returnToken(queueToken);
            } catch (Throwable t) {
                LOG.error("error happens when coordinator close ", t);
            }
        }
    }

    private void execInternal() throws Exception {
        if (LOG.isDebugEnabled() && !scanNodes.isEmpty()) {
            LOG.debug("debug: in Coordinator::exec. query id: {}, planNode: {}",
                    DebugUtil.printId(queryId), scanNodes.get(0).treeToThrift());
        }

        if (LOG.isDebugEnabled() && !fragments.isEmpty()) {
            LOG.debug("debug: in Coordinator::exec. query id: {}, fragment: {}",
                    DebugUtil.printId(queryId), fragments.get(0).toThrift());
        }

        processFragmentAssignmentAndParams();

        traceInstance();

        QeProcessorImpl.INSTANCE.registerInstances(queryId, instanceIds.size());

        // create result receiver
        PlanFragmentId topId = fragments.get(0).getFragmentId();
        FragmentExecParams topParams = fragmentExecParamsMap.get(topId);
        DataSink topDataSink = topParams.fragment.getSink();
        this.timeoutDeadline = System.currentTimeMillis() + queryOptions.getExecutionTimeout() * 1000L;
        if (topDataSink instanceof ResultSink || topDataSink instanceof ResultFileSink) {
            TNetworkAddress execBeAddr = topParams.instanceExecParams.get(0).host;
            receiver = new ResultReceiver(queryId, topParams.instanceExecParams.get(0).instanceId,
                    addressToBackendID.get(execBeAddr), toBrpcHost(execBeAddr), this.timeoutDeadline,
                    context.getSessionVariable().getMaxMsgSizeOfResultReceiver());

            if (!context.isReturnResultFromLocal()) {
                Preconditions.checkState(context.getConnectType().equals(ConnectType.ARROW_FLIGHT_SQL));
                context.setFinstId(topParams.instanceExecParams.get(0).instanceId);
                context.setResultFlightServerAddr(toArrowFlightHost(execBeAddr));
                context.setResultInternalServiceAddr(toBrpcHost(execBeAddr));
                context.setResultOutputExprs(fragments.get(0).getOutputExprs());
            }

            LOG.info("dispatch result sink of query {} to {}", DebugUtil.printId(queryId),
                    topParams.instanceExecParams.get(0).host);

            if (topDataSink instanceof ResultFileSink
                    && ((ResultFileSink) topDataSink).getStorageType() == StorageBackend.StorageType.BROKER) {
                // set the broker address for OUTFILE sink
                ResultFileSink topResultFileSink = (ResultFileSink) topDataSink;
                FsBroker broker = Env.getCurrentEnv().getBrokerMgr()
                        .getBroker(topResultFileSink.getBrokerName(), execBeAddr.getHostname());
                topResultFileSink.setBrokerAddr(broker.host, broker.port);
            }
        } else {
            // This is a load process.
            this.queryOptions.setIsReportSuccess(true);
            deltaUrls = Lists.newArrayList();
            loadCounters = Maps.newHashMap();
            List<Long> relatedBackendIds = Lists.newArrayList(addressToBackendID.values());
            Env.getCurrentEnv().getLoadManager().initJobProgress(jobId, queryId, instanceIds,
                    relatedBackendIds);
            Env.getCurrentEnv().getProgressManager().addTotalScanNums(String.valueOf(jobId), scanRangeNum);
            LOG.info("dispatch load job: {} to {}", DebugUtil.printId(queryId), addressToBackendID.keySet());
        }

        if (enablePipelineEngine) {
            sendPipelineCtx();
        } else {
            sendFragment();
        }
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
            Map<Long, Integer> numSinkOnBackend = Maps.newHashMap();
            beToExecStates.clear();
            // If #fragments >=2, use twoPhaseExecution with exec_plan_fragments_prepare and exec_plan_fragments_start,
            // else use exec_plan_fragments directly.
            // we choose #fragments >=2 because in some cases
            // we need ensure that A fragment is already prepared to receive data before B fragment sends data.
            // For example: select * from numbers("number"="10") will generate ExchangeNode and
            // TableValuedFunctionScanNode, we should ensure TableValuedFunctionScanNode does
            // not send data until ExchangeNode is ready to receive.
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
                                    tParam, this.addressToBackendID, executionProfile);
                    // Each tParam will set the total number of Fragments that need to be executed on the same BE,
                    // and the BE will determine whether all Fragments have been executed based on this information.
                    // Notice. load fragment has a small probability that FragmentNumOnHost is 0, for unknown reasons.
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
                                twoPhaseExecution, execState.backend.getProcessEpoch());
                        beToExecStates.putIfAbsent(execState.backend.getId(), states);
                    }
                    states.addState(execState);
                    if (tParam.getFragment().getOutputSink() != null
                            && tParam.getFragment().getOutputSink().getType() == TDataSinkType.OLAP_TABLE_SINK) {
                        numSinkOnBackend.merge(execState.backend.getId(), 1, Integer::sum);
                    }
                    ++backendIdx;
                }
                int loadStreamPerNode = 1;
                if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
                    loadStreamPerNode = ConnectContext.get().getSessionVariable().getLoadStreamPerNode();
                }
                for (TExecPlanFragmentParams tParam : tParams) {
                    if (tParam.getFragment().getOutputSink() != null
                            && tParam.getFragment().getOutputSink().getType() == TDataSinkType.OLAP_TABLE_SINK) {
                        tParam.setLoadStreamPerNode(loadStreamPerNode);
                        tParam.setTotalLoadStreams(numSinkOnBackend.size() * loadStreamPerNode);
                        tParam.setNumLocalSink(numSinkOnBackend.get(tParam.getBackendId()));
                        LOG.info("num local sink for backend {} is {}", tParam.getBackendId(),
                                numSinkOnBackend.get(tParam.getBackendId()));
                    }
                }
                profileFragmentId += 1;
            } // end for fragments

            // 4. send and wait fragments rpc
            List<Triple<BackendExecStates, BackendServiceProxy, Future<InternalService.PExecPlanFragmentResult>>>
                    futures = Lists.newArrayList();

            for (BackendExecStates states : beToExecStates.values()) {
                states.unsetFields();
                BackendServiceProxy proxy = BackendServiceProxy.getInstance();
                futures.add(ImmutableTriple.of(states, proxy, states.execRemoteFragmentsAsync(proxy)));
            }
            waitRpc(futures, this.timeoutDeadline - System.currentTimeMillis(), "send fragments");

            if (twoPhaseExecution) {
                // 5. send and wait execution start rpc
                futures.clear();
                for (BackendExecStates states : beToExecStates.values()) {
                    BackendServiceProxy proxy = BackendServiceProxy.getInstance();
                    futures.add(ImmutableTriple.of(states, proxy, states.execPlanFragmentStartAsync(proxy)));
                }
                waitRpc(futures, this.timeoutDeadline - System.currentTimeMillis(), "send execution start");
            }
        } finally {
            unlock();
        }
    }

    private void sendPipelineCtx() throws TException, RpcException, UserException {
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
            beToPipelineExecCtxs.clear();
            // fragment:backend
            List<Pair<PlanFragmentId, Long>> backendFragments = Lists.newArrayList();
            // If #fragments >=2, use twoPhaseExecution with exec_plan_fragments_prepare and exec_plan_fragments_start,
            // else use exec_plan_fragments directly.
            // we choose #fragments > 1 because in some cases
            // we need ensure that A fragment is already prepared to receive data before B fragment sends data.
            // For example: select * from numbers("number"="10") will generate ExchangeNode and
            // TableValuedFunctionScanNode, we should ensure TableValuedFunctionScanNode does not
            // send data until ExchangeNode is ready to receive.
            boolean twoPhaseExecution = ConnectContext.get() != null
                    && ConnectContext.get().getSessionVariable().isEnableSinglePhaseExecutionCommitOpt()
                    ? fragments.size() > 1 && addressToBackendID.size() > 1 : fragments.size() > 1;
            for (PlanFragment fragment : fragments) {
                FragmentExecParams params = fragmentExecParamsMap.get(fragment.getFragmentId());

                // 1. set up exec states
                int instanceNum = params.instanceExecParams.size();
                Preconditions.checkState(instanceNum > 0);
                Map<TNetworkAddress, TPipelineFragmentParams> tParams = params.toTPipelineParams(backendIdx);

                boolean needCheckBackendState = false;
                if (queryOptions.getQueryType() == TQueryType.LOAD && profileFragmentId == 0) {
                    // this is a load process, and it is the first fragment.
                    // we should add all BackendExecState of this fragment to needCheckBackendExecStates,
                    // so that we can check these backends' state when joining this Coordinator
                    needCheckBackendState = true;
                }

                Map<TUniqueId, Boolean> fragmentInstancesMap = new HashMap<TUniqueId, Boolean>();
                for (Map.Entry<TNetworkAddress, TPipelineFragmentParams> entry : tParams.entrySet()) {
                    for (TPipelineInstanceParams instanceParam : entry.getValue().local_params) {
                        fragmentInstancesMap.put(instanceParam.fragment_instance_id, false);
                    }
                }

                int numBackendsWithSink = 0;
                // 3. group PipelineExecContext by BE.
                // So that we can use one RPC to send all fragment instances of a BE.
                for (Map.Entry<TNetworkAddress, TPipelineFragmentParams> entry : tParams.entrySet()) {
                    Long backendId = this.addressToBackendID.get(entry.getKey());
                    backendFragments.add(Pair.of(fragment.getFragmentId(), backendId));
                    PipelineExecContext pipelineExecContext = new PipelineExecContext(fragment.getFragmentId(),
                            entry.getValue(), backendId, fragmentInstancesMap,
                            this.enablePipelineXEngine, executionProfile);
                    // Each tParam will set the total number of Fragments that need to be executed on the same BE,
                    // and the BE will determine whether all Fragments have been executed based on this information.
                    // Notice. load fragment has a small probability that FragmentNumOnHost is 0, for unknown reasons.
                    entry.getValue().setFragmentNumOnHost(hostCounter.count(pipelineExecContext.address));
                    entry.getValue().setBackendId(pipelineExecContext.backend.getId());
                    entry.getValue().setNeedWaitExecutionTrigger(twoPhaseExecution);
                    entry.getValue().setFragmentId(fragment.getFragmentId().asInt());

                    pipelineExecContexts.put(Pair.of(fragment.getFragmentId().asInt(), backendId), pipelineExecContext);
                    if (needCheckBackendState) {
                        needCheckPipelineExecContexts.add(pipelineExecContext);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("add need check backend {} for fragment, {} job: {}",
                                    pipelineExecContext.backend.getId(), fragment.getFragmentId().asInt(), jobId);
                        }
                    }

                    PipelineExecContexts ctxs = beToPipelineExecCtxs.get(pipelineExecContext.backend.getId());
                    if (ctxs == null) {
                        ctxs = new PipelineExecContexts(pipelineExecContext.backend.getId(),
                                pipelineExecContext.brpcAddress, twoPhaseExecution,
                                entry.getValue().getFragmentNumOnHost());
                        beToPipelineExecCtxs.putIfAbsent(pipelineExecContext.backend.getId(), ctxs);
                    }
                    ctxs.addContext(pipelineExecContext);

                    if (entry.getValue().getFragment().getOutputSink() != null
                            && entry.getValue().getFragment().getOutputSink().getType()
                            == TDataSinkType.OLAP_TABLE_SINK) {
                        numBackendsWithSink++;
                    }
                    ++backendIdx;
                }
                int loadStreamPerNode = 1;
                if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable() != null) {
                    loadStreamPerNode = ConnectContext.get().getSessionVariable().getLoadStreamPerNode();
                }
                for (Map.Entry<TNetworkAddress, TPipelineFragmentParams> entry : tParams.entrySet()) {
                    if (entry.getValue().getFragment().getOutputSink() != null
                            && entry.getValue().getFragment().getOutputSink().getType()
                            == TDataSinkType.OLAP_TABLE_SINK) {
                        entry.getValue().setLoadStreamPerNode(loadStreamPerNode);
                        entry.getValue().setTotalLoadStreams(numBackendsWithSink * loadStreamPerNode);
                        entry.getValue().setNumLocalSink(entry.getValue().getLocalParams().size());
                        LOG.info("num local sink for backend {} is {}", entry.getValue().getBackendId(),
                                entry.getValue().getNumLocalSink());
                    }
                }

                profileFragmentId += 1;
            } // end for fragments

            // Init the mark done in order to track the finished state of the query
            if (this.enablePipelineXEngine) {
                fragmentsDoneLatch = new MarkedCountDownLatch<>(backendFragments.size());
                for (Pair<PlanFragmentId, Long> pair : backendFragments) {
                    fragmentsDoneLatch.addMark(pair.first.asInt(), pair.second);
                }
            }

            // 4. send and wait fragments rpc
            List<Triple<PipelineExecContexts, BackendServiceProxy, Future<InternalService.PExecPlanFragmentResult>>>
                    futures = Lists.newArrayList();

            for (PipelineExecContexts ctxs : beToPipelineExecCtxs.values()) {
                if (LOG.isDebugEnabled()) {
                    String infos = "";
                    for (PipelineExecContext pec : ctxs.ctxs) {
                        infos += pec.fragmentId + " ";
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("query {}, sending pipeline fragments: {} to be {} bprc address {}",
                                DebugUtil.printId(queryId), infos, ctxs.beId, ctxs.brpcAddr.toString());
                    }
                }

                ctxs.unsetFields();
                BackendServiceProxy proxy = BackendServiceProxy.getInstance();
                futures.add(ImmutableTriple.of(ctxs, proxy, ctxs.execRemoteFragmentsAsync(proxy)));
            }
            waitPipelineRpc(futures, this.timeoutDeadline - System.currentTimeMillis(), "send fragments");

            if (twoPhaseExecution) {
                // 5. send and wait execution start rpc
                futures.clear();
                for (PipelineExecContexts ctxs : beToPipelineExecCtxs.values()) {
                    BackendServiceProxy proxy = BackendServiceProxy.getInstance();
                    futures.add(ImmutableTriple.of(ctxs, proxy, ctxs.execPlanFragmentStartAsync(proxy)));
                }
                waitPipelineRpc(futures, this.timeoutDeadline - System.currentTimeMillis(), "send execution start");
            }
        } finally {
            unlock();
        }
    }

    private void waitRpc(List<Triple<BackendExecStates, BackendServiceProxy, Future<PExecPlanFragmentResult>>> futures,
                         long leftTimeMs,
            String operation) throws RpcException, UserException {
        if (leftTimeMs <= 0) {
            long currentTimeMillis = System.currentTimeMillis();
            long elapsed = (currentTimeMillis - timeoutDeadline) / 1000 + queryOptions.getExecutionTimeout();
            String msg = String.format(
                    "timeout before waiting %s rpc, query timeout:%d, already elapsed:%d, left for this:%d",
                    operation, queryOptions.getExecutionTimeout(), elapsed, leftTimeMs);
            LOG.warn("Query {} {}", DebugUtil.printId(queryId), msg);
            if (!queryOptions.isSetExecutionTimeout() || !queryOptions.isSetQueryTimeout()) {
                LOG.warn("Query {} does not set timeout info, execution timeout: is_set:{}, value:{}"
                                + ", query timeout: is_set:{}, value: {}, "
                                + "coordinator timeout deadline {}, cur time millis: {}",
                        DebugUtil.printId(queryId),
                        queryOptions.isSetExecutionTimeout(), queryOptions.getExecutionTimeout(),
                        queryOptions.isSetQueryTimeout(), queryOptions.getQueryTimeout(),
                        timeoutDeadline, currentTimeMillis);
            }
            throw new UserException(msg);
        }

        long timeoutMs = Math.min(leftTimeMs, Config.remote_fragment_exec_timeout_ms);
        for (Triple<BackendExecStates, BackendServiceProxy, Future<PExecPlanFragmentResult>> triple : futures) {
            TStatusCode code;
            String errMsg = null;
            Exception exception = null;

            try {
                PExecPlanFragmentResult result = triple.getRight().get(timeoutMs, TimeUnit.MILLISECONDS);
                code = TStatusCode.findByValue(result.getStatus().getStatusCode());
                if (code != TStatusCode.OK) {
                    if (!result.getStatus().getErrorMsgsList().isEmpty()) {
                        errMsg = result.getStatus().getErrorMsgsList().get(0);
                    } else {
                        errMsg = operation + " failed. backend id: " + triple.getLeft().beId;
                    }
                }
            } catch (ExecutionException e) {
                exception = e;
                code = TStatusCode.THRIFT_RPC_ERROR;
                triple.getMiddle().removeProxy(triple.getLeft().brpcAddr);
            } catch (InterruptedException e) {
                exception = e;
                code = TStatusCode.INTERNAL_ERROR;
            } catch (TimeoutException e) {
                exception = e;
                errMsg = String.format(
                    "timeout when waiting for %s rpc, query timeout:%d, left timeout for this operation:%d",
                    operation, queryOptions.getExecutionTimeout(), timeoutMs / 1000);
                LOG.warn("Query {} {}", DebugUtil.printId(queryId), errMsg);
                code = TStatusCode.TIMEOUT;
            }

            if (code != TStatusCode.OK) {
                if (exception != null && errMsg == null) {
                    errMsg = operation + " failed. " + exception.getMessage();
                }
                queryStatus.setStatus(errMsg);
                cancelInternal(Types.PPlanFragmentCancelReason.INTERNAL_ERROR);
                switch (code) {
                    case TIMEOUT:
                        MetricRepo.BE_COUNTER_QUERY_RPC_FAILED.getOrAdd(triple.getLeft().brpcAddr.hostname)
                                .increase(1L);
                        throw new RpcException(triple.getLeft().brpcAddr.hostname, errMsg, exception);
                    case THRIFT_RPC_ERROR:
                        MetricRepo.BE_COUNTER_QUERY_RPC_FAILED.getOrAdd(triple.getLeft().brpcAddr.hostname)
                                .increase(1L);
                        SimpleScheduler.addToBlacklist(triple.getLeft().beId, errMsg);
                        throw new RpcException(triple.getLeft().brpcAddr.hostname, errMsg, exception);
                    default:
                        throw new UserException(errMsg, exception);
                }
            }
        }
    }

    private void waitPipelineRpc(List<Triple<PipelineExecContexts, BackendServiceProxy,
            Future<PExecPlanFragmentResult>>> futures, long leftTimeMs,
            String operation) throws RpcException, UserException {
        if (leftTimeMs <= 0) {
            long currentTimeMillis = System.currentTimeMillis();
            long elapsed = (currentTimeMillis - timeoutDeadline) / 1000 + queryOptions.getExecutionTimeout();
            String msg = String.format(
                    "timeout before waiting %s rpc, query timeout:%d, already elapsed:%d, left for this:%d",
                    operation, queryOptions.getExecutionTimeout(), elapsed, leftTimeMs);
            LOG.warn("Query {} {}", DebugUtil.printId(queryId), msg);
            if (!queryOptions.isSetExecutionTimeout() || !queryOptions.isSetQueryTimeout()) {
                LOG.warn("Query {} does not set timeout info, execution timeout: is_set:{}, value:{}"
                                + ", query timeout: is_set:{}, value: {}, "
                                + "coordinator timeout deadline {}, cur time millis: {}",
                        DebugUtil.printId(queryId),
                        queryOptions.isSetExecutionTimeout(), queryOptions.getExecutionTimeout(),
                        queryOptions.isSetQueryTimeout(), queryOptions.getQueryTimeout(),
                        timeoutDeadline, currentTimeMillis);
            }
            throw new UserException(msg);
        }

        long timeoutMs = Math.min(leftTimeMs, Config.remote_fragment_exec_timeout_ms);
        for (Triple<PipelineExecContexts, BackendServiceProxy, Future<PExecPlanFragmentResult>> triple : futures) {
            TStatusCode code;
            String errMsg = null;
            Exception exception = null;

            try {
                PExecPlanFragmentResult result = triple.getRight().get(timeoutMs, TimeUnit.MILLISECONDS);
                code = TStatusCode.findByValue(result.getStatus().getStatusCode());
                if (code == null) {
                    code = TStatusCode.INTERNAL_ERROR;
                }

                if (code != TStatusCode.OK) {
                    if (!result.getStatus().getErrorMsgsList().isEmpty()) {
                        errMsg = result.getStatus().getErrorMsgsList().get(0);
                    } else {
                        errMsg = operation + " failed. backend id: " + triple.getLeft().beId;
                    }
                }
            } catch (ExecutionException e) {
                exception = e;
                code = TStatusCode.THRIFT_RPC_ERROR;
                triple.getMiddle().removeProxy(triple.getLeft().brpcAddr);
            } catch (InterruptedException e) {
                exception = e;
                code = TStatusCode.INTERNAL_ERROR;
            } catch (TimeoutException e) {
                exception = e;
                errMsg = String.format(
                    "timeout when waiting for %s rpc, query timeout:%d, left timeout for this operation:%d",
                                            operation, queryOptions.getExecutionTimeout(), timeoutMs / 1000);
                LOG.warn("Query {} {}", DebugUtil.printId(queryId), errMsg);
                code = TStatusCode.TIMEOUT;
            }

            if (code != TStatusCode.OK) {
                if (exception != null && errMsg == null) {
                    errMsg = operation + " failed. " + exception.getMessage();
                }
                queryStatus.setStatus(errMsg);
                cancelInternal(Types.PPlanFragmentCancelReason.INTERNAL_ERROR);
                switch (code) {
                    case TIMEOUT:
                        MetricRepo.BE_COUNTER_QUERY_RPC_FAILED.getOrAdd(triple.getLeft().brpcAddr.hostname)
                                .increase(1L);
                        throw new RpcException(triple.getLeft().brpcAddr.hostname, errMsg, exception);
                    case THRIFT_RPC_ERROR:
                        MetricRepo.BE_COUNTER_QUERY_RPC_FAILED.getOrAdd(triple.getLeft().brpcAddr.hostname)
                                .increase(1L);
                        SimpleScheduler.addToBlacklist(triple.getLeft().beId, errMsg);
                        throw new RpcException(triple.getLeft().brpcAddr.hostname, errMsg, exception);
                    default:
                        throw new UserException(errMsg, exception);
                }
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
            if (this.errorTabletInfos.size() <= Config.max_error_tablet_of_broker_load) {
                this.errorTabletInfos.addAll(errorTabletInfos.stream().limit(Config.max_error_tablet_of_broker_load
                        - this.errorTabletInfos.size()).collect(Collectors.toList()));
            }
        } finally {
            lock.unlock();
        }
    }

    private void updateStatus(Status status) {
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
            if (status.getErrorCode() == TStatusCode.TIMEOUT) {
                cancelInternal(Types.PPlanFragmentCancelReason.TIMEOUT);
            } else {
                cancelInternal(Types.PPlanFragmentCancelReason.INTERNAL_ERROR);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public RowBatch getNext() throws Exception {
        if (receiver == null) {
            throw new UserException("There is no receiver.");
        }

        RowBatch resultBatch;
        Status status = new Status();
        resultBatch = receiver.getNext(status);
        if (!status.ok()) {
            LOG.warn("Query {} coordinator get next fail, {}, need cancel.",
                    DebugUtil.printId(queryId), status.toString());
        }

        updateStatus(status);

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

                // hide host info exclude localhost
                if (errMsg.contains("localhost")) {
                    throw new UserException(errMsg);
                }
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
                if (LOG.isDebugEnabled()) {
                    LOG.debug("no block query, return num >= limit rows, need cancel");
                }
                cancelInternal(Types.PPlanFragmentCancelReason.LIMIT_REACH);
            }
            if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().dryRunQuery) {
                numReceivedRows = 0;
                numReceivedRows += resultBatch.getQueryStatistics().getReturnedRows();
            }
        } else if (resultBatch.getBatch() != null) {
            numReceivedRows += resultBatch.getBatch().getRowsSize();
        }

        return resultBatch;
    }


    // We use a very conservative cancel strategy.
    // 0. If backends has zero process epoch, do not cancel. Zero process epoch usually arises in cluster upgrading.
    // 1. If process epoch is same, do not cancel. Means backends does not restart or die.
    public boolean shouldCancel(List<Backend> currentBackends) {
        Map<Long, Backend> curBeMap = Maps.newHashMap();
        for (Backend be : currentBackends) {
            curBeMap.put(be.getId(), be);
        }

        try {
            lock();

            if (queryOptions.isEnablePipelineEngine()) {
                for (PipelineExecContext pipelineExecContext : pipelineExecContexts.values()) {
                    Backend be = curBeMap.get(pipelineExecContext.backend.getId());
                    if (be == null || !be.isAlive()) {
                        LOG.warn("Backend {} not exists or dead, query {} should be cancelled",
                                pipelineExecContext.backend.toString(), DebugUtil.printId(queryId));
                        return true;
                    }

                    // Backend process epoch changed, indicates that this be restarts, query should be cancelled.
                    // Check zero since during upgrading, older version oplog will not persistent be start time
                    // so newer version follower will get zero epoch when replaying oplog or snapshot
                    if (pipelineExecContext.beProcessEpoch != be.getProcessEpoch() && be.getProcessEpoch() != 0) {
                        LOG.warn("Backend process epoch changed, previous {} now {}, "
                                        + "means this be has already restarted, should cancel this coordinator,"
                                        + " query id {}",
                                        pipelineExecContext.beProcessEpoch, be.getProcessEpoch(),
                                        DebugUtil.printId(queryId));
                        return true;
                    } else if (be.getProcessEpoch() == 0) {
                        LOG.warn("Backend {} has zero process epoch, maybe we are upgrading cluster?",
                                be.toString());
                    }
                }
            } else {
                // beToExecStates will be updated only in non-pipeline query.
                for (BackendExecStates beExecState : beToExecStates.values()) {
                    Backend be = curBeMap.get(beExecState.beId);
                    if (be == null || !be.isAlive()) {
                        LOG.warn("Backend {} not exists or dead, query {} should be cancelled.",
                                beExecState.beId, DebugUtil.printId(queryId));
                        return true;
                    }

                    if (beExecState.beProcessEpoch != be.getProcessEpoch() && be.getProcessEpoch() != 0) {
                        LOG.warn("Process epoch changed, previous {} now {}, means this be has already restarted, "
                                        + "should cancel this coordinator, query id {}",
                                beExecState.beProcessEpoch, be.getProcessEpoch(), DebugUtil.printId(queryId));
                        return true;
                    } else if (be.getProcessEpoch() == 0) {
                        LOG.warn("Backend {} has zero process epoch, maybe we are upgrading cluster?", be.toString());
                    }
                }
            }

            return false;
        } finally {
            unlock();
        }
    }

    // Cancel execution of query. This includes the execution of the local plan
    // fragment,
    // if any, as well as all plan fragments on remote nodes.
    public void cancel() {
        cancel(Types.PPlanFragmentCancelReason.USER_CANCEL);
        if (queueToken != null) {
            queueToken.signalForCancel();
        }
    }

    @Override
    public void cancel(Types.PPlanFragmentCancelReason cancelReason) {
        lock();
        try {
            if (!queryStatus.ok()) {
                // Print an error stack here to know why send cancel again.
                LOG.warn("Query {} already in abnormal status {}, but received cancel again,"
                        + "so that send cancel to BE again",
                        DebugUtil.printId(queryId), queryStatus.toString(), new Exception());
            } else {
                queryStatus.setStatus(Status.CANCELLED);
            }
            LOG.warn("Cancel execution of query {}, this is a outside invoke", DebugUtil.printId(queryId));
            cancelInternal(cancelReason);
        } finally {
            unlock();
        }
    }

    public boolean isQueryCancelled() {
        lock();
        try {
            return queryStatus.isCancelled();
        } finally {
            unlock();
        }
    }

    private void cancelLatch() {
        if (instancesDoneLatch != null) {
            instancesDoneLatch.countDownToZero(new Status());
        }
        if (fragmentsDoneLatch != null) {
            fragmentsDoneLatch.countDownToZero(new Status());
        }
    }

    private void cancelInternal(Types.PPlanFragmentCancelReason cancelReason) {
        if (null != receiver) {
            receiver.cancel(cancelReason.toString());
        }
        if (null != pointExec) {
            pointExec.cancel();
            return;
        }
        cancelRemoteFragmentsAsync(cancelReason);
        cancelLatch();
    }

    private void cancelRemoteFragmentsAsync(Types.PPlanFragmentCancelReason cancelReason) {
        if (enablePipelineEngine) {
            for (PipelineExecContext ctx : pipelineExecContexts.values()) {
                ctx.cancelFragmentInstance(cancelReason);
            }
        } else {
            for (BackendExecState backendExecState : backendExecStates) {
                backendExecState.cancelFragmentInstance(cancelReason);
            }
        }
    }

    private void computeFragmentExecParams() throws Exception {
        // fill hosts field in fragmentExecParams
        computeFragmentHosts();

        // assign instance ids
        instanceIds.clear();
        for (FragmentExecParams params : fragmentExecParamsMap.values()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Query {} fragment {} has {} instances.",
                        DebugUtil.printId(queryId), params.fragment.getFragmentId(),
                        params.instanceExecParams.size());
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

        // Init instancesDoneLatch, it will be used to track if the instances has finished for insert stmt
        instancesDoneLatch = new MarkedCountDownLatch<>(instanceIds.size());
        for (TUniqueId instanceId : instanceIds) {
            instancesDoneLatch.addMark(instanceId, -1L /* value is meaningless */);
        }

        // compute multi cast fragment params
        computeMultiCastFragmentParams();

        // assign runtime filter merge addr and target addr
        assignRuntimeFilterAddr();

        // compute destinations and # senders per exchange node
        // (the root fragment doesn't have a destination)
        for (FragmentExecParams params : fragmentExecParamsMap.values()) {
            if (params.fragment instanceof MultiCastPlanFragment) {
                continue;
            }
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
            PlanNode exchNode = PlanNode.findPlanNodeFromPlanNodeId(destFragment.getPlanRoot(), exchId);
            Preconditions.checkState(exchNode != null, "exchNode is null");
            Preconditions.checkState(exchNode instanceof ExchangeNode,
                    "exchNode is not ExchangeNode" + exchNode.getId().toString());
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
                if (destParams.instanceExecParams.size() == 1 && (bucketNum == 0
                        || destParams.instanceExecParams.get(0).bucketSeqSet.isEmpty())) {
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

                    Set<TNetworkAddress> hostSet = new HashSet<>();
                    for (int insIdx = 0; insIdx < destParams.instanceExecParams.size(); insIdx++) {
                        FInstanceExecParam instanceExecParams = destParams.instanceExecParams.get(insIdx);
                        if (destParams.ignoreDataDistribution
                                && hostSet.contains(instanceExecParams.host)) {
                            continue;
                        }
                        hostSet.add(instanceExecParams.host);
                        if (instanceExecParams.bucketSeqSet.contains(bucketSeq)) {
                            dest.fragment_instance_id = instanceExecParams.instanceId;
                            dest.server = toRpcHost(instanceExecParams.host);
                            dest.setBrpcServer(toBrpcHost(instanceExecParams.host));
                            instanceExecParams.recvrId = params.destinations.size();
                            break;
                        }
                    }

                    bucketSeq++;
                    params.destinations.add(dest);
                }
            } else {
                if (enablePipelineEngine && enableShareHashTableForBroadcastJoin
                        && ((ExchangeNode) exchNode).isRightChildOfBroadcastHashJoin()) {
                    // here choose the first instance to build hash table.
                    Map<TNetworkAddress, FInstanceExecParam> destHosts = new HashMap<>();
                    destParams.instanceExecParams.forEach(param -> {
                        if (destHosts.containsKey(param.host)) {
                            destHosts.get(param.host).instancesSharingHashTable.add(param.instanceId);
                        } else {
                            destHosts.put(param.host, param);
                            param.buildHashTableForBroadcastJoin = true;
                            TPlanFragmentDestination dest = new TPlanFragmentDestination();
                            param.recvrId = params.destinations.size();
                            dest.fragment_instance_id = param.instanceId;
                            try {
                                dest.server = toRpcHost(param.host);
                                dest.setBrpcServer(toBrpcHost(param.host));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            params.destinations.add(dest);
                        }
                    });
                } else {
                    Set<TNetworkAddress> hostSet = new HashSet<>();
                    // add destination host to this fragment's destination
                    for (int j = 0; j < destParams.instanceExecParams.size(); ++j) {
                        if (destParams.ignoreDataDistribution
                                && hostSet.contains(destParams.instanceExecParams.get(j).host)) {
                            continue;
                        }
                        hostSet.add(destParams.instanceExecParams.get(j).host);
                        TPlanFragmentDestination dest = new TPlanFragmentDestination();
                        dest.fragment_instance_id = destParams.instanceExecParams.get(j).instanceId;
                        dest.server = toRpcHost(destParams.instanceExecParams.get(j).host);
                        dest.setBrpcServer(toBrpcHost(destParams.instanceExecParams.get(j).host));
                        destParams.instanceExecParams.get(j).recvrId = params.destinations.size();
                        params.destinations.add(dest);
                    }
                }
            }
        }
    }

    private void computeMultiCastFragmentParams() throws Exception {
        for (FragmentExecParams params : fragmentExecParamsMap.values()) {
            if (!(params.fragment instanceof MultiCastPlanFragment)) {
                continue;
            }

            MultiCastPlanFragment multi = (MultiCastPlanFragment) params.fragment;
            Preconditions.checkState(multi.getSink() instanceof MultiCastDataSink);
            MultiCastDataSink multiSink = (MultiCastDataSink) multi.getSink();

            for (int i = 0; i < multi.getDestFragmentList().size(); i++) {
                PlanFragment destFragment = multi.getDestFragmentList().get(i);
                DataStreamSink sink = multiSink.getDataStreamSinks().get(i);

                if (destFragment == null) {
                    continue;
                }
                FragmentExecParams destParams = fragmentExecParamsMap.get(destFragment.getFragmentId());
                multi.getDestFragmentList().get(i).setOutputPartition(params.fragment.getOutputPartition());

                PlanNodeId exchId = sink.getExchNodeId();
                PlanNode exchNode = PlanNode.findPlanNodeFromPlanNodeId(destFragment.getPlanRoot(), exchId);
                Preconditions.checkState(!destParams.perExchNumSenders.containsKey(exchId.asInt()));
                Preconditions.checkState(exchNode != null, "exchNode is null");
                Preconditions.checkState(exchNode instanceof ExchangeNode,
                        "exchNode is not ExchangeNode" + exchNode.getId().toString());
                if (destParams.perExchNumSenders.get(exchId.asInt()) == null) {
                    destParams.perExchNumSenders.put(exchId.asInt(), params.instanceExecParams.size());
                } else {
                    destParams.perExchNumSenders.put(exchId.asInt(),
                            params.instanceExecParams.size() + destParams.perExchNumSenders.get(exchId.asInt()));
                }

                List<TPlanFragmentDestination> destinations = multiSink.getDestinations().get(i);
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
                    if (destParams.instanceExecParams.size() == 1 && (bucketNum == 0
                            || destParams.instanceExecParams.get(0).bucketSeqSet.isEmpty())) {
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

                        Set<TNetworkAddress> hostSet = new HashSet<>();
                        for (int insIdx = 0; insIdx < destParams.instanceExecParams.size(); insIdx++) {
                            FInstanceExecParam instanceExecParams = destParams.instanceExecParams.get(insIdx);
                            if (destParams.ignoreDataDistribution
                                    && hostSet.contains(instanceExecParams.host)) {
                                continue;
                            }
                            hostSet.add(instanceExecParams.host);
                            if (instanceExecParams.bucketSeqSet.contains(bucketSeq)) {
                                dest.fragment_instance_id = instanceExecParams.instanceId;
                                dest.server = toRpcHost(instanceExecParams.host);
                                dest.setBrpcServer(toBrpcHost(instanceExecParams.host));
                                instanceExecParams.recvrId = params.destinations.size();
                                break;
                            }
                        }

                        bucketSeq++;
                        destinations.add(dest);
                    }
                } else if (enablePipelineEngine && enableShareHashTableForBroadcastJoin
                        && ((ExchangeNode) exchNode).isRightChildOfBroadcastHashJoin()) {
                    // here choose the first instance to build hash table.
                    Map<TNetworkAddress, FInstanceExecParam> destHosts = new HashMap<>();

                    destParams.instanceExecParams.forEach(param -> {
                        if (destHosts.containsKey(param.host)) {
                            destHosts.get(param.host).instancesSharingHashTable.add(param.instanceId);
                        } else {
                            destHosts.put(param.host, param);
                            param.buildHashTableForBroadcastJoin = true;
                            TPlanFragmentDestination dest = new TPlanFragmentDestination();
                            dest.fragment_instance_id = param.instanceId;
                            param.recvrId = params.destinations.size();
                            try {
                                dest.server = toRpcHost(param.host);
                                dest.setBrpcServer(toBrpcHost(param.host));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            destinations.add(dest);
                        }
                    });
                } else {
                    Set<TNetworkAddress> hostSet = new HashSet<>();
                    for (int j = 0; j < destParams.instanceExecParams.size(); ++j) {
                        if (destParams.ignoreDataDistribution
                                && hostSet.contains(destParams.instanceExecParams.get(j).host)) {
                            continue;
                        }
                        hostSet.add(destParams.instanceExecParams.get(j).host);
                        TPlanFragmentDestination dest = new TPlanFragmentDestination();
                        dest.fragment_instance_id = destParams.instanceExecParams.get(j).instanceId;
                        dest.server = toRpcHost(destParams.instanceExecParams.get(j).host);
                        dest.brpc_server = toBrpcHost(destParams.instanceExecParams.get(j).host);
                        destParams.instanceExecParams.get(j).recvrId = params.destinations.size();
                        destinations.add(dest);
                    }
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

    private TNetworkAddress toArrowFlightHost(TNetworkAddress host) throws Exception {
        Backend backend = Env.getCurrentSystemInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (backend == null) {
            throw new UserException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG);
        }
        if (backend.getArrowFlightSqlPort() < 0) {
            throw new UserException("be arrow_flight_sql_port cannot be empty.");
        }
        return backend.getArrowFlightAddress();
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
                if (((ConnectContext.get() != null && ConnectContext.get().isResourceTagsSet()) || (isAllExternalScan
                        && Config.prefer_compute_node_for_external_table)) && !addressToBackendID.isEmpty()) {
                    // 2 cases:
                    // case 1: user set resource tag, we need to use the BE with the specified resource tags.
                    // case 2: All scan nodes are external scan node,
                    //         and prefer_compute_node_for_external_table is true, we should only select BE which scan
                    //         nodes are used.
                    // Otherwise, except for the scan node, the rest of the execution nodes of the query
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
                // when we use nested loop join do right outer / semi / anti join, the instance must be 1.
                if (leftMostNode.getNumInstances() == 1) {
                    exchangeInstances = 1;
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
                        List<List<TScanRangeParams>> perInstanceScanRanges = Lists.newArrayList();

                        Optional<ScanNode> node = scanNodes.stream().filter(scanNode -> {
                            return scanNode.getId().asInt() == planNodeId;
                        }).findFirst();

                        /**
                         * Ignore storage data distribution iff:
                         * 1. `parallelExecInstanceNum * numBackends` is larger than scan ranges.
                         * 2. Use Nereids planner.
                         */
                        boolean sharedScan = true;
                        int expectedInstanceNum = Math.min(parallelExecInstanceNum,
                                leftMostNode.getNumInstances());
                        boolean forceToLocalShuffle = context != null
                                && context.getSessionVariable().isForceToLocalShuffle();
                        boolean ignoreStorageDataDistribution = forceToLocalShuffle || (scanNodes.stream()
                                .allMatch(scanNode -> scanNode.ignoreStorageDataDistribution(context,
                                        fragmentExecParamsMap.get(scanNode.getFragment().getFragmentId())
                                                .scanRangeAssignment.size())) && useNereids);
                        if (node.isPresent() && (!node.get().shouldDisableSharedScan(context)
                                || ignoreStorageDataDistribution)) {
                            expectedInstanceNum = Math.max(expectedInstanceNum, 1);
                            // if have limit and no conjuncts, only need 1 instance to save cpu and
                            // mem resource
                            if (node.get().shouldUseOneInstance()) {
                                expectedInstanceNum = 1;
                            }

                            perInstanceScanRanges = Collections.nCopies(expectedInstanceNum, perNodeScanRanges);
                        } else {
                            expectedInstanceNum = 1;
                            if (parallelExecInstanceNum > 1) {
                                //the scan instance num should not larger than the tablets num
                                expectedInstanceNum = Math.min(perNodeScanRanges.size(), parallelExecInstanceNum);
                            }
                            // if have limit and no conjuncts, only need 1 instance to save cpu and
                            // mem resource
                            if (node.get().shouldUseOneInstance()) {
                                expectedInstanceNum = 1;
                            }

                            perInstanceScanRanges = ListUtil.splitBySize(perNodeScanRanges,
                                    expectedInstanceNum);
                            sharedScan = false;
                        }

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("scan range number per instance is: {}", perInstanceScanRanges.size());
                        }

                        for (int j = 0; j < perInstanceScanRanges.size(); j++) {
                            List<TScanRangeParams> scanRangeParams = perInstanceScanRanges.get(j);

                            FInstanceExecParam instanceParam = new FInstanceExecParam(null, key, 0, params);
                            instanceParam.perNodeScanRanges.put(planNodeId, scanRangeParams);
                            instanceParam.perNodeSharedScans.put(planNodeId, sharedScan);
                            params.instanceExecParams.add(instanceParam);
                        }
                        params.ignoreDataDistribution = sharedScan && enablePipelineXEngine;
                        params.parallelTasksNum = params.ignoreDataDistribution ? 1 : params.instanceExecParams.size();
                    }
                }
            }

            if (params.instanceExecParams.isEmpty()) {
                Reference<Long> backendIdRef = new Reference<Long>();
                TNetworkAddress execHostport;
                if (ConnectContext.get() != null && ConnectContext.get().isResourceTagsSet()
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
                FInstanceExecParam instanceParam = new FInstanceExecParam(null, execHostport, 0, params);
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
                List<FRuntimeFilterTargetParam> targetFragments = ridToTargetParam.computeIfAbsent(rid,
                        k -> new ArrayList<>());
                for (final FInstanceExecParam instance : params.instanceExecParams) {
                    targetFragments.add(new FRuntimeFilterTargetParam(instance.instanceId, toBrpcHost(instance.host)));
                }
            }

            for (RuntimeFilterId rid : fragment.getBuilderRuntimeFilterIds()) {
                ridToBuilderNum.merge(rid,
                        (int) params.instanceExecParams.stream().map(ins -> ins.host).distinct().count(), Integer::sum);
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

    private <K, V> V findOrInsert(Map<K, V> m, final K key, final V defaultVal) {
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
        assignScanRanges(fragmentId, parallelExecInstanceNum, params, fragmentIdTobucketSeqToScanRangeMap,
                fragmentIdToSeqToAddressMap, fragmentIdToScanNodeIds);
    }

    private Map<TNetworkAddress, Long> getReplicaNumPerHostForOlapTable() {
        Map<TNetworkAddress, Long> replicaNumPerHost = Maps.newHashMap();
        for (ScanNode scanNode : scanNodes) {
            List<TScanRangeLocations> locationsList = scanNode.getScanRangeLocations(0);
            for (TScanRangeLocations locations : locationsList) {
                for (TScanRangeLocation location : locations.locations) {
                    if (replicaNumPerHost.containsKey(location.server)) {
                        replicaNumPerHost.put(location.server, replicaNumPerHost.get(location.server) + 1L);
                    } else {
                        replicaNumPerHost.put(location.server, 1L);
                    }
                }

            }
        }
        return replicaNumPerHost;
    }

    // Populates scan_range_assignment_.
    // <fragment, <server, nodeId>>
    protected void computeScanRangeAssignment() throws Exception {
        Map<TNetworkAddress, Long> assignedBytesPerHost = Maps.newHashMap();
        Map<TNetworkAddress, Long> replicaNumPerHost = getReplicaNumPerHostForOlapTable();
        Collections.shuffle(scanNodes);
        // set scan ranges/locations for scan nodes
        for (ScanNode scanNode : scanNodes) {
            if (!(scanNode instanceof ExternalScanNode)) {
                isAllExternalScan = false;
            }
            List<TScanRangeLocations> locations;
            // the parameters of getScanRangeLocations may ignore, It doesn't take effect
            locations = scanNode.getScanRangeLocations(0);
            if (locations == null) {
                // only analysis olap scan node
                continue;
            }
            Set<Integer> scanNodeIds = fragmentIdToScanNodeIds.computeIfAbsent(scanNode.getFragmentId(),
                    k -> Sets.newHashSet());
            scanNodeIds.add(scanNode.getId().asInt());

            if (scanNode instanceof FileQueryScanNode) {
                fileScanRangeParamsMap.put(
                        scanNode.getId().asInt(), ((FileQueryScanNode) scanNode).getFileScanRangeParams());
            }

            FragmentScanRangeAssignment assignment
                    = fragmentExecParamsMap.get(scanNode.getFragmentId()).scanRangeAssignment;
            boolean fragmentContainsColocateJoin = isColocateFragment(scanNode.getFragment(),
                    scanNode.getFragment().getPlanRoot());
            boolean fragmentContainsBucketShuffleJoin = bucketShuffleJoinController
                    .isBucketShuffleJoin(scanNode.getFragmentId().asInt(), scanNode.getFragment().getPlanRoot());

            // A fragment may contain both colocate join and bucket shuffle join
            // on need both compute scanRange to init basic data for query coordinator
            if (fragmentContainsColocateJoin) {
                computeScanRangeAssignmentByColocate((OlapScanNode) scanNode, assignedBytesPerHost, replicaNumPerHost);
            }
            if (fragmentContainsBucketShuffleJoin) {
                bucketShuffleJoinController.computeScanRangeAssignmentByBucket((OlapScanNode) scanNode,
                        idToBackend, addressToBackendID, replicaNumPerHost);
            }
            if (!(fragmentContainsColocateJoin || fragmentContainsBucketShuffleJoin)) {
                computeScanRangeAssignmentByScheduler(scanNode, locations, assignment, assignedBytesPerHost,
                        replicaNumPerHost);
            }
        }
    }

    // To ensure the same bucketSeq tablet to the same execHostPort
    private void computeScanRangeAssignmentByColocate(
            final OlapScanNode scanNode, Map<TNetworkAddress, Long> assignedBytesPerHost,
            Map<TNetworkAddress, Long> replicaNumPerHost) throws Exception {
        if (!fragmentIdToSeqToAddressMap.containsKey(scanNode.getFragmentId())) {
            fragmentIdToSeqToAddressMap.put(scanNode.getFragmentId(), new HashMap<>());
            fragmentIdTobucketSeqToScanRangeMap.put(scanNode.getFragmentId(), new BucketSeqToScanRange());

            // Same as bucket shuffle.
            int bucketNum = 0;
            if (scanNode.getOlapTable().isColocateTable()) {
                bucketNum = scanNode.getOlapTable().getDefaultDistributionInfo().getBucketNum();
            } else {
                bucketNum = (int) (scanNode.getTotalTabletsNum());
            }
            scanNode.getFragment().setBucketNum(bucketNum);
        }
        Map<Integer, TNetworkAddress> bucketSeqToAddress = fragmentIdToSeqToAddressMap.get(scanNode.getFragmentId());
        BucketSeqToScanRange bucketSeqToScanRange = fragmentIdTobucketSeqToScanRangeMap.get(scanNode.getFragmentId());
        for (Integer bucketSeq : scanNode.bucketSeq2locations.keySet()) {
            //fill scanRangeParamsList
            List<TScanRangeLocations> locations = scanNode.bucketSeq2locations.get(bucketSeq);
            if (!bucketSeqToAddress.containsKey(bucketSeq)) {
                getExecHostPortForFragmentIDAndBucketSeq(locations.get(0),
                        scanNode.getFragmentId(), bucketSeq, assignedBytesPerHost, replicaNumPerHost);
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
                updateScanRangeNumByScanRange(scanRangeParams);
            }
        }
    }

    //ensure bucket sequence distribued to every host evenly
    private void getExecHostPortForFragmentIDAndBucketSeq(TScanRangeLocations seqLocation,
            PlanFragmentId fragmentId, Integer bucketSeq, Map<TNetworkAddress, Long> assignedBytesPerHost,
            Map<TNetworkAddress, Long> replicaNumPerHost)
            throws Exception {
        Reference<Long> backendIdRef = new Reference<Long>();
        selectBackendsByRoundRobin(seqLocation, assignedBytesPerHost, replicaNumPerHost, backendIdRef);
        Backend backend = this.idToBackend.get(backendIdRef.getRef());
        TNetworkAddress execHostPort = new TNetworkAddress(backend.getHost(), backend.getBePort());
        this.addressToBackendID.put(execHostPort, backendIdRef.getRef());
        this.fragmentIdToSeqToAddressMap.get(fragmentId).put(bucketSeq, execHostPort);
    }

    public TScanRangeLocation selectBackendsByRoundRobin(TScanRangeLocations seqLocation,
                                                         Map<TNetworkAddress, Long> assignedBytesPerHost,
                                                         Map<TNetworkAddress, Long> replicaNumPerHost,
                                                         Reference<Long> backendIdRef) throws UserException {
        if (!Config.enable_local_replica_selection) {
            return selectBackendsByRoundRobin(seqLocation.getLocations(), assignedBytesPerHost, replicaNumPerHost,
                    backendIdRef);
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
            return selectBackendsByRoundRobin(localLocations, assignedBytesPerHost, replicaNumPerHost, backendIdRef);
        } catch (UserException ue) {
            if (!Config.enable_local_replica_selection_fallback) {
                throw ue;
            }
            return selectBackendsByRoundRobin(nonlocalLocations, assignedBytesPerHost, replicaNumPerHost, backendIdRef);
        }
    }

    public TScanRangeLocation selectBackendsByRoundRobin(List<TScanRangeLocation> locations,
            Map<TNetworkAddress, Long> assignedBytesPerHost, Map<TNetworkAddress, Long> replicaNumPerHost,
            Reference<Long> backendIdRef) throws UserException {
        Long minAssignedBytes = Long.MAX_VALUE;
        Long minReplicaNum = Long.MAX_VALUE;
        TScanRangeLocation minLocation = null;
        Long step = 1L;
        for (final TScanRangeLocation location : locations) {
            Long assignedBytes = findOrInsert(assignedBytesPerHost, location.server, 0L);
            if (assignedBytes < minAssignedBytes || (assignedBytes.equals(minAssignedBytes)
                    && replicaNumPerHost.get(location.server) < minReplicaNum)) {
                minAssignedBytes = assignedBytes;
                minReplicaNum = replicaNumPerHost.get(location.server);
                minLocation = location;
            }
        }
        for (TScanRangeLocation location : locations) {
            replicaNumPerHost.put(location.server, replicaNumPerHost.get(location.server) - 1);
        }
        TScanRangeLocation location = SimpleScheduler.getLocation(minLocation, locations,
                this.idToBackend, backendIdRef);
        assignedBytesPerHost.put(location.server, assignedBytesPerHost.get(location.server) + step);

        return location;
    }

    private void computeScanRangeAssignmentByScheduler(
            final ScanNode scanNode,
            final List<TScanRangeLocations> locations,
            FragmentScanRangeAssignment assignment,
            Map<TNetworkAddress, Long> assignedBytesPerHost,
            Map<TNetworkAddress, Long> replicaNumPerHost) throws Exception {
        // Type of locations is List, it could have elements that have same "location"
        // and we do have this situation for some scan node.
        // The duplicate "location" will NOT be filtered by FragmentScanRangeAssignment,
        // since FragmentScanRangeAssignment use List<TScanRangeParams> as its value type,
        // duplicate "locations" will be converted to list.
        for (TScanRangeLocations scanRangeLocations : locations) {
            Reference<Long> backendIdRef = new Reference<Long>();
            TScanRangeLocation minLocation = selectBackendsByRoundRobin(scanRangeLocations,
                    assignedBytesPerHost, replicaNumPerHost, backendIdRef);
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
            updateScanRangeNumByScanRange(scanRangeParams);
        }
    }

    private void updateScanRangeNumByScanRange(TScanRangeParams param) {
        TScanRange scanRange = param.getScanRange();
        if (scanRange == null) {
            return;
        }
        TBrokerScanRange brokerScanRange = scanRange.getBrokerScanRange();
        if (brokerScanRange != null) {
            scanRangeNum += brokerScanRange.getRanges().size();
        }
        TExternalScanRange externalScanRange = scanRange.getExtScanRange();
        if (externalScanRange != null) {
            TFileScanRange fileScanRange = externalScanRange.getFileScanRange();
            if (fileScanRange != null) {
                scanRangeNum += fileScanRange.getRanges().size();
            }
        }
        TPaloScanRange paloScanRange = scanRange.getPaloScanRange();
        if (paloScanRange != null) {
            scanRangeNum = scanRangeNum + 1;
        }
        // TODO: more ranges?
    }

    public void setHivePartitionUpdateFunc(Consumer<List<THivePartitionUpdate>> hivePartitionUpdateFunc) {
        this.hivePartitionUpdateFunc = hivePartitionUpdateFunc;
    }

    // update job progress from BE
    public void updateFragmentExecStatus(TReportExecStatusParams params) {
        if (enablePipelineXEngine) {
            PipelineExecContext ctx = pipelineExecContexts.get(Pair.of(params.getFragmentId(), params.getBackendId()));
            if (ctx == null || !ctx.updatePipelineStatus(params)) {
                return;
            }

            Status status = new Status(params.status);
            // for now, abort the query if we see any error except if the error is cancelled
            // and returned_all_results_ is true.
            // (UpdateStatus() initiates cancellation, if it hasn't already been initiated)
            if (!status.ok()) {
                if (returnedAllResults && status.isCancelled()) {
                    LOG.warn("Query {} has returned all results, fragment_id={} instance_id={}, be={}"
                            + " is reporting failed status {}",
                            DebugUtil.printId(queryId), params.getFragmentId(),
                            DebugUtil.printId(params.getFragmentInstanceId()),
                            params.getBackendId(),
                            status.toString());
                } else {
                    LOG.warn("one instance report fail, query_id={} fragment_id={} instance_id={}, be={},"
                                    + " error message: {}",
                            DebugUtil.printId(queryId), params.getFragmentId(),
                            DebugUtil.printId(params.getFragmentInstanceId()),
                            params.getBackendId(), status.toString());
                    updateStatus(status);
                }
            }
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
            if (params.isSetHivePartitionUpdates() && hivePartitionUpdateFunc != null) {
                hivePartitionUpdateFunc.accept(params.getHivePartitionUpdates());
            }

            Preconditions.checkArgument(params.isSetDetailedReport());
            if (ctx.done) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Query {} fragment {} is marked done",
                            DebugUtil.printId(queryId), ctx.fragmentId);
                }
                fragmentsDoneLatch.markedCountDown(params.getFragmentId(), params.getBackendId());
            }
        } else if (enablePipelineEngine) {
            PipelineExecContext ctx = pipelineExecContexts.get(Pair.of(params.getFragmentId(), params.getBackendId()));
            if (ctx == null || !ctx.updatePipelineStatus(params)) {
                return;
            }

            Status status = new Status(params.status);
            // for now, abort the query if we see any error except if the error is cancelled
            // and returned_all_results_ is true.
            // (UpdateStatus() initiates cancellation, if it hasn't already been initiated)
            if (!status.ok()) {
                if (returnedAllResults && status.isCancelled()) {
                    LOG.warn("Query {} has returned all results, fragment_id={} instance_id={}, be={}"
                            + " is reporting failed status {}",
                            DebugUtil.printId(queryId), params.getFragmentId(),
                            DebugUtil.printId(params.getFragmentInstanceId()),
                            params.getBackendId(),
                            status.toString());
                } else {
                    LOG.warn("one instance report fail, query_id={} fragment_id={} instance_id={}, be={},"
                                    + " error message: {}",
                            DebugUtil.printId(queryId), params.getFragmentId(),
                            DebugUtil.printId(params.getFragmentInstanceId()),
                            params.getBackendId(), status.toString());
                    updateStatus(status);
                }
            }

            // params.isDone() should be promised.
            // There are some periodic reports during the load process,
            // and the reports from the intermediate process may be concurrent with the last report.
            // The last report causes the counter to decrease to zero,
            // but it is possible that the report without commit-info triggered the commit operation,
            // resulting in the data not being published.
            if (ctx.fragmentInstancesMap.get(params.fragment_instance_id) && params.isDone()) {
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
                if (params.isSetHivePartitionUpdates() && hivePartitionUpdateFunc != null) {
                    hivePartitionUpdateFunc.accept(params.getHivePartitionUpdates());
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Query {} instance {} is marked done",
                            DebugUtil.printId(queryId), DebugUtil.printId(params.getFragmentInstanceId()));
                }
                instancesDoneLatch.markedCountDown(params.getFragmentInstanceId(), -1L);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Query {} instance {} is not marked done",
                            DebugUtil.printId(queryId), DebugUtil.printId(params.getFragmentInstanceId()));
                }
            }
        } else {
            if (params.backend_num >= backendExecStates.size()) {
                LOG.warn("Query {} instance {} unknown backend number: {}, expected less than: {}",
                        DebugUtil.printId(queryId), DebugUtil.printId(params.getFragmentInstanceId()),
                        params.backend_num, backendExecStates.size());
                return;
            }
            BackendExecState execState = backendExecStates.get(params.backend_num);
            if (!execState.updateInstanceStatus(params)) {
                // Has to return here, to avoid out of order report messages. For example,
                // the first message is done, then we update commit messages, but the new
                // message is running, then we will also update commit messages. It will
                // lead to data corrupt.
                return;
            }

            Status status = new Status(params.status);
            // for now, abort the query if we see any error except if the error is cancelled
            // and returned_all_results_ is true.
            // (UpdateStatus() initiates cancellation, if it hasn't already been initiated)
            if (!status.ok()) {
                if (status.isCancelled() && returnedAllResults) {
                    LOG.warn("Query {} has returned all results, its instance {} is reporting failed status {}",
                            DebugUtil.printId(queryId), DebugUtil.printId(params.getFragmentInstanceId()),
                            status.toString());
                } else {
                    LOG.warn("Instance {} of query {} report failed status, error msg: {}",
                            DebugUtil.printId(queryId), DebugUtil.printId(params.getFragmentInstanceId()),
                            status.toString());
                    updateStatus(status);
                }
            }

            // params.isDone() should be promised.
            // There are some periodic reports during the load process,
            // and the reports from the intermediate process may be concurrent with the last report.
            // The last report causes the counter to decrease to zero,
            // but it is possible that the report without commit-info triggered the commit operation,
            // resulting in the data not being published.
            if (execState.done && params.isDone()) {
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
                if (params.isSetHivePartitionUpdates() && hivePartitionUpdateFunc != null) {
                    hivePartitionUpdateFunc.accept(params.getHivePartitionUpdates());
                }
                instancesDoneLatch.markedCountDown(params.getFragmentInstanceId(), -1L);
            }
        }

        if (params.isSetLoadedRows() && jobId != -1) {
            Env.getCurrentEnv().getLoadManager().updateJobProgress(
                    jobId, params.getBackendId(), params.getQueryId(), params.getFragmentInstanceId(),
                    params.getLoadedRows(), params.getLoadedBytes(), params.isDone());
            Env.getCurrentEnv().getProgressManager().updateProgress(String.valueOf(jobId),
                    params.getQueryId(), params.getFragmentInstanceId(), params.getFinishedScanRanges());
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
                if (fragmentsDoneLatch != null) {
                    awaitRes = fragmentsDoneLatch.await(waitTime, TimeUnit.SECONDS);
                } else {
                    awaitRes = instancesDoneLatch.await(waitTime, TimeUnit.SECONDS);
                }
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
        if (enablePipelineEngine) {
            for (PipelineExecContext ctx : needCheckPipelineExecContexts) {
                if (!ctx.isBackendStateHealthy()) {
                    queryStatus = new Status(TStatusCode.INTERNAL_ERROR, "backend "
                            + ctx.backend.getId() + " is down");
                    return false;
                }
            }
        } else {
            for (BackendExecState backendExecState : needCheckBackendExecStates) {
                if (!backendExecState.isBackendStateHealthy()) {
                    queryStatus = new Status(TStatusCode.INTERNAL_ERROR, "backend "
                            + backendExecState.backend.getId() + " is down");
                    return false;
                }
            }
        }
        return true;
    }

    public boolean isDone() {
        if (fragmentsDoneLatch != null) {
            return fragmentsDoneLatch.getCount() == 0;
        } else {
            return instancesDoneLatch.getCount() == 0;
        }
    }

    public void setMemTableOnSinkNode(boolean enableMemTableOnSinkNode) {
        this.queryOptions.setEnableMemtableOnSinkNode(enableMemTableOnSinkNode);
    }

    // map from a BE host address to the per-node assigned scan ranges;
    // records scan range assignment for a single fragment
    static class FragmentScanRangeAssignment
            extends HashMap<TNetworkAddress, Map<Integer, List<TScanRangeParams>>> {
    }

    // Bucket sequence -> (scan node id -> list of TScanRangeParams)
    static class BucketSeqToScanRange extends HashMap<Integer, Map<Integer, List<TScanRangeParams>>> {

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
                if (!ConnectContext.get().getSessionVariable().isEnableBucketShuffleJoin()
                        && !ConnectContext.get().getSessionVariable().isEnableNereidsPlanner()) {
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
                Map<TNetworkAddress, Long> addressToBackendID,
                Map<TNetworkAddress, Long> replicaNumPerHost) throws Exception {
            Map<Long, Integer> buckendIdToBucketCountMap = fragmentIdToBuckendIdBucketCountMap.get(fragmentId);
            int maxBucketNum = Integer.MAX_VALUE;
            long buckendId = Long.MAX_VALUE;
            Long minReplicaNum = Long.MAX_VALUE;
            for (TScanRangeLocation location : seqLocation.locations) {
                if (buckendIdToBucketCountMap.getOrDefault(location.backend_id, 0) < maxBucketNum) {
                    maxBucketNum = buckendIdToBucketCountMap.getOrDefault(location.backend_id, 0);
                    buckendId = location.backend_id;
                    minReplicaNum = replicaNumPerHost.get(location.server);
                } else if (buckendIdToBucketCountMap.getOrDefault(location.backend_id, 0) == maxBucketNum
                        && replicaNumPerHost.get(location.server) < minReplicaNum) {
                    buckendId = location.backend_id;
                    minReplicaNum = replicaNumPerHost.get(location.server);
                }
            }
            Reference<Long> backendIdRef = new Reference<>();
            TNetworkAddress execHostPort = SimpleScheduler.getHost(buckendId,
                    seqLocation.locations, idToBackend, backendIdRef);
            //the backend with buckendId is not alive, chose another new backend
            if (backendIdRef.getRef() != buckendId) {
                buckendIdToBucketCountMap.put(backendIdRef.getRef(),
                        buckendIdToBucketCountMap.getOrDefault(backendIdRef.getRef(), 0) + 1);
            } else { //the backend with buckendId is alive, update buckendIdToBucketCountMap directly
                buckendIdToBucketCountMap.put(buckendId, buckendIdToBucketCountMap.getOrDefault(buckendId, 0) + 1);
            }
            for (TScanRangeLocation location : seqLocation.locations) {
                replicaNumPerHost.put(location.server, replicaNumPerHost.get(location.server) - 1);
            }
            addressToBackendID.put(execHostPort, backendIdRef.getRef());
            this.fragmentIdToSeqToAddressMap.get(fragmentId).put(bucketSeq, execHostPort);
        }

        // to ensure the same bucketSeq tablet to the same execHostPort
        private void computeScanRangeAssignmentByBucket(
                final OlapScanNode scanNode, ImmutableMap<Long, Backend> idToBackend,
                Map<TNetworkAddress, Long> addressToBackendID,
                Map<TNetworkAddress, Long> replicaNumPerHost) throws Exception {
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
                scanNode.getFragment().setBucketNum(bucketNum);
            }
            Map<Integer, TNetworkAddress> bucketSeqToAddress
                    = fragmentIdToSeqToAddressMap.get(scanNode.getFragmentId());
            BucketSeqToScanRange bucketSeqToScanRange = fragmentIdBucketSeqToScanRangeMap.get(scanNode.getFragmentId());

            for (Integer bucketSeq : scanNode.bucketSeq2locations.keySet()) {
                //fill scanRangeParamsList
                List<TScanRangeLocations> locations = scanNode.bucketSeq2locations.get(bucketSeq);
                if (!bucketSeqToAddress.containsKey(bucketSeq)) {
                    getExecHostPortForFragmentIDAndBucketSeq(locations.get(0), scanNode.getFragmentId(),
                            bucketSeq, idToBackend, addressToBackendID, replicaNumPerHost);
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
                    updateScanRangeNumByScanRange(scanRangeParams);
                }
            }
        }

        private void computeInstanceParam(PlanFragmentId fragmentId,
                int parallelExecInstanceNum, FragmentExecParams params) {
            assignScanRanges(fragmentId, parallelExecInstanceNum, params, fragmentIdBucketSeqToScanRangeMap,
                    fragmentIdToSeqToAddressMap, fragmentIdToScanNodeIds);
        }
    }

    private void assignScanRanges(PlanFragmentId fragmentId, int parallelExecInstanceNum, FragmentExecParams params,
            Map<PlanFragmentId, BucketSeqToScanRange> fragmentIdBucketSeqToScanRangeMap,
            Map<PlanFragmentId, Map<Integer, TNetworkAddress>> curFragmentIdToSeqToAddressMap,
            Map<PlanFragmentId, Set<Integer>> fragmentIdToScanNodeIds) {
        Map<Integer, TNetworkAddress> bucketSeqToAddress = curFragmentIdToSeqToAddressMap.get(fragmentId);
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

        /**
         * Ignore storage data distribution iff:
         * 1. `parallelExecInstanceNum * numBackends` is larger than scan ranges.
         * 2. Use Nereids planner.
         */
        boolean forceToLocalShuffle = context != null
                && context.getSessionVariable().isForceToLocalShuffle();
        boolean ignoreStorageDataDistribution = forceToLocalShuffle || (scanNodes.stream()
                .allMatch(node -> node.ignoreStorageDataDistribution(context,
                        fragmentExecParamsMap.get(node.getFragment().getFragmentId())
                                .scanRangeAssignment.size()))
                && addressToScanRanges.entrySet().stream().allMatch(addressScanRange -> {
                    return addressScanRange.getValue().size() < parallelExecInstanceNum;
                }) && useNereids);

        FragmentScanRangeAssignment assignment = params.scanRangeAssignment;
        for (Map.Entry<TNetworkAddress, List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>>> addressScanRange
                : addressToScanRanges.entrySet()) {
            List<Pair<Integer, Map<Integer, List<TScanRangeParams>>>> scanRange = addressScanRange.getValue();
            Map<Integer, List<TScanRangeParams>> range
                    = findOrInsert(assignment, addressScanRange.getKey(), new HashMap<>());

            if (ignoreStorageDataDistribution) {
                FInstanceExecParam instanceParam = new FInstanceExecParam(
                        null, addressScanRange.getKey(), 0, params);

                for (Pair<Integer, Map<Integer, List<TScanRangeParams>>> nodeScanRangeMap : scanRange) {
                    instanceParam.addBucketSeq(nodeScanRangeMap.first);
                    for (Map.Entry<Integer, List<TScanRangeParams>> nodeScanRange
                            : nodeScanRangeMap.second.entrySet()) {
                        if (!instanceParam.perNodeScanRanges.containsKey(nodeScanRange.getKey())) {
                            range.put(nodeScanRange.getKey(), Lists.newArrayList());
                            instanceParam.perNodeScanRanges.put(nodeScanRange.getKey(), Lists.newArrayList());
                            instanceParam.perNodeSharedScans.put(nodeScanRange.getKey(), true);
                        }
                        range.get(nodeScanRange.getKey()).addAll(nodeScanRange.getValue());
                        instanceParam.perNodeScanRanges.get(nodeScanRange.getKey())
                                .addAll(nodeScanRange.getValue());
                    }
                }
                params.instanceExecParams.add(instanceParam);
                for (int i = 1; i < parallelExecInstanceNum; i++) {
                    params.instanceExecParams.add(new FInstanceExecParam(
                            null, addressScanRange.getKey(), 0, params));
                }
            } else {
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
        params.ignoreDataDistribution = ignoreStorageDataDistribution && enablePipelineXEngine;
        params.parallelTasksNum = params.ignoreDataDistribution ? 1 : params.instanceExecParams.size();
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
        TNetworkAddress brpcAddress;
        TNetworkAddress address;
        Backend backend;
        long lastMissingHeartbeatTime = -1;
        TUniqueId instanceId;
        private boolean hasCancelled = false;
        private boolean cancelInProcess = false;

        public BackendExecState(PlanFragmentId fragmentId, int instanceId,
                                TExecPlanFragmentParams rpcParams, Map<TNetworkAddress, Long> addressToBackendID,
                                ExecutionProfile executionProfile) {
            this.fragmentId = fragmentId;
            this.rpcParams = rpcParams;
            this.initiated = false;
            this.done = false;
            FInstanceExecParam fi = fragmentExecParamsMap.get(fragmentId).instanceExecParams.get(instanceId);
            this.instanceId = fi.instanceId;
            this.address = fi.host;
            this.backend = idToBackend.get(addressToBackendID.get(address));
            this.brpcAddress = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
            this.lastMissingHeartbeatTime = backend.getLastMissingHeartbeatTime();
            String profileName = "Instance " + DebugUtil.printId(
                    fi.instanceId) + " (host=" + this.backend.getHeartbeatAddress() + ")";
            RuntimeProfile instanceProfile = new RuntimeProfile(profileName);
            executionProfile.addInstanceProfile(fragmentId, fi.instanceId, instanceProfile);
        }

        /**
         * Some information common to all Fragments does not need to be sent repeatedly.
         * Therefore, when we confirm that a certain BE has accepted the information,
         * we will delete the information in the subsequent Fragment to avoid repeated sending.
         * This information can be obtained from the cache of BE.
         */
        public void unsetFields() {
            this.rpcParams.unsetDescTbl();
            this.rpcParams.unsetFileScanParams();
            this.rpcParams.unsetCoord();
            this.rpcParams.unsetQueryGlobals();
            this.rpcParams.unsetResourceInfo();
            this.rpcParams.setIsSimplifiedParam(true);
        }

        // update the instance status, if it is already finished, then not update any more.
        public synchronized boolean updateInstanceStatus(TReportExecStatusParams params) {
            if (this.done) {
                // duplicate packet
                return false;
            }
            this.done = params.done;
            if (statsErrorEstimator != null) {
                statsErrorEstimator.updateExactReturnedRows(params);
            }
            return true;
        }

        // cancel the fragment instance.
        // return true if cancel success. Otherwise, return false
        public synchronized void cancelFragmentInstance(Types.PPlanFragmentCancelReason cancelReason) {
            LOG.warn("cancelRemoteFragments initiated={} done={} backend: {},"
                            + " fragment instance id={}, reason: {}",
                    this.initiated, this.done, backend.getId(),
                    DebugUtil.printId(fragmentInstanceId()), cancelReason.name());
            try {
                if (!this.initiated) {
                    return;
                }
                // don't cancel if it is already finished
                if (this.done) {
                    return;
                }
                if (this.hasCancelled || this.cancelInProcess) {
                    LOG.info("Fragment instance has already been cancelled {} or under cancel {}."
                            + " initiated={} done={} backend: {},"
                            + "fragment instance id={}, reason: {}",
                            this.hasCancelled, this.cancelInProcess,
                            this.initiated, this.done, backend.getId(),
                            DebugUtil.printId(fragmentInstanceId()), cancelReason.name());
                    return;
                }
                try {
                    ListenableFuture<InternalService.PCancelPlanFragmentResult> cancelResult =
                            BackendServiceProxy.getInstance().cancelPlanFragmentAsync(brpcAddress,
                                    fragmentInstanceId(), cancelReason);
                    Futures.addCallback(cancelResult, new FutureCallback<InternalService.PCancelPlanFragmentResult>() {
                        public void onSuccess(InternalService.PCancelPlanFragmentResult result) {
                            cancelInProcess = false;
                            if (result.hasStatus()) {
                                Status status = new Status();
                                status.setPstatus(result.getStatus());
                                if (status.getErrorCode() == TStatusCode.OK) {
                                    hasCancelled = true;
                                } else {
                                    LOG.warn("Failed to cancel query {} instance initiated={} done={} backend: {},"
                                            + "fragment instance id={}, reason: {}",
                                            DebugUtil.printId(queryId), initiated, done, backend.getId(),
                                            DebugUtil.printId(fragmentInstanceId()), status.toString());
                                }
                            }
                            LOG.warn("Failed to cancel query {} instance initiated={} done={} backend: {},"
                                    + "fragment instance id={}, reason: {}",
                                    DebugUtil.printId(queryId), initiated, done, backend.getId(),
                                    DebugUtil.printId(fragmentInstanceId()), "without status");
                        }

                        public void onFailure(Throwable t) {
                            cancelInProcess = false;
                            LOG.warn("Failed to cancel query {} instance initiated={} done={} backend: {},"
                                    + "fragment instance id={}, reason: {}",
                                    DebugUtil.printId(queryId), initiated, done, backend.getId(),
                                    DebugUtil.printId(fragmentInstanceId()), cancelReason.name(), t);
                        }
                    }, backendRpcCallbackExecutor);
                    cancelInProcess = true;
                } catch (RpcException e) {
                    LOG.warn("cancel plan fragment get a exception, address={}:{}", brpcAddress.getHostname(),
                            brpcAddress.getPort());
                    SimpleScheduler.addToBlacklist(addressToBackendID.get(brpcAddress), e.getMessage());
                }

            } catch (Exception e) {
                LOG.warn("catch a exception", e);
                return;
            }
            return;
        }

        public boolean isBackendStateHealthy() {
            if (backend.getLastMissingHeartbeatTime() > lastMissingHeartbeatTime && !backend.isAlive()) {
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

    public class PipelineExecContext {
        TPipelineFragmentParams rpcParams;
        PlanFragmentId fragmentId;
        boolean initiated;
        boolean done;
        // use for pipeline
        Map<TUniqueId, Boolean> fragmentInstancesMap;
        // use for pipelineX

        boolean enablePipelineX;
        TNetworkAddress brpcAddress;
        TNetworkAddress address;
        Backend backend;
        long lastMissingHeartbeatTime = -1;
        long profileReportProgress = 0;
        long beProcessEpoch = 0;
        private final int numInstances;
        private boolean hasCancelled = false;
        private boolean cancelInProcess = false;

        public PipelineExecContext(PlanFragmentId fragmentId,
                TPipelineFragmentParams rpcParams, Long backendId,
                Map<TUniqueId, Boolean> fragmentInstancesMap,
                boolean enablePipelineX, ExecutionProfile executionProfile) {
            this.fragmentId = fragmentId;
            this.rpcParams = rpcParams;
            this.numInstances = rpcParams.local_params.size();
            this.fragmentInstancesMap = fragmentInstancesMap;

            this.initiated = false;
            this.done = false;

            this.backend = idToBackend.get(backendId);
            this.address = new TNetworkAddress(backend.getHost(), backend.getBePort());
            this.brpcAddress = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
            this.beProcessEpoch = backend.getProcessEpoch();

            this.lastMissingHeartbeatTime = backend.getLastMissingHeartbeatTime();
            this.enablePipelineX = enablePipelineX;
            if (this.enablePipelineX) {
                executionProfile.addFragmentBackend(fragmentId, backendId);
            } else {
                for (TPipelineInstanceParams instanceParam : rpcParams.local_params) {
                    String profileName = "Instance " + DebugUtil.printId(instanceParam.fragment_instance_id)
                            + " (host=" + address + ")";
                    executionProfile.addInstanceProfile(fragmentId, instanceParam.fragment_instance_id,
                        new RuntimeProfile(profileName));
                }
            }
        }

        /**
         * Some information common to all Fragments does not need to be sent repeatedly.
         * Therefore, when we confirm that a certain BE has accepted the information,
         * we will delete the information in the subsequent Fragment to avoid repeated
         * sending.
         * This information can be obtained from the cache of BE.
         */
        public void unsetFields() {
            this.rpcParams.unsetDescTbl();
            this.rpcParams.unsetFileScanParams();
            this.rpcParams.unsetCoord();
            this.rpcParams.unsetQueryGlobals();
            this.rpcParams.unsetResourceInfo();
            this.rpcParams.setIsSimplifiedParam(true);
        }

        // update profile.
        // return true if profile is updated. Otherwise, return false.
        // Has to use synchronized to ensure there are not concurrent update threads. Or the done
        // state maybe update wrong and will lose data. see https://github.com/apache/doris/pull/29802/files.
        public synchronized boolean updatePipelineStatus(TReportExecStatusParams params) {
            // The fragment or instance is not finished, not need update
            if (!params.done) {
                return false;
            }
            if (enablePipelineX) {
                if (this.done) {
                    // duplicate packet
                    return false;
                }
                this.done = true;
                return true;
            } else {
                // could not find the related instances, not update and return false, to indicate
                // that the caller should not update any more.
                if (!fragmentInstancesMap.containsKey(params.fragment_instance_id)) {
                    return false;
                }
                Boolean instanceDone = fragmentInstancesMap.get(params.fragment_instance_id);
                if (instanceDone) {
                    // duplicate packet
                    return false;
                }
                fragmentInstancesMap.put(params.fragment_instance_id, true);
                profileReportProgress++;
                if (profileReportProgress == numInstances) {
                    this.done = true;
                }
                return true;
            }
        }

        // Just send the cancel message to BE, not care about the result, because there is no retry
        // logic in upper logic.
        private synchronized void cancelFragment(Types.PPlanFragmentCancelReason cancelReason) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("cancelRemoteFragments initiated={} done={} backend: {},"
                        + " fragment id={} query={}, reason: {}",
                        this.initiated, this.done, backend.getId(),
                        this.fragmentId,
                        DebugUtil.printId(queryId), cancelReason.name());
            }

            if (this.hasCancelled || this.cancelInProcess) {
                LOG.info("Frangment has already been cancelled. Query {} backend: {}, fragment id={}",
                        DebugUtil.printId(queryId), backend.getId(), this.fragmentId);
                return;
            }
            try {
                try {
                    ListenableFuture<InternalService.PCancelPlanFragmentResult> cancelResult =
                            BackendServiceProxy.getInstance().cancelPipelineXPlanFragmentAsync(brpcAddress,
                            this.fragmentId, queryId, cancelReason);
                    Futures.addCallback(cancelResult, new FutureCallback<InternalService.PCancelPlanFragmentResult>() {
                        public void onSuccess(InternalService.PCancelPlanFragmentResult result) {
                            cancelInProcess = false;
                            if (result.hasStatus()) {
                                Status status = new Status();
                                status.setPstatus(result.getStatus());
                                if (status.getErrorCode() == TStatusCode.OK) {
                                    hasCancelled = true;
                                } else {
                                    LOG.warn("Failed to cancel query {} instance initiated={} done={} backend: {},"
                                            + "fragment id={}, reason: {}",
                                            DebugUtil.printId(queryId), initiated, done, backend.getId(),
                                            fragmentId, status.toString());
                                }
                            }
                            LOG.warn("Failed to cancel query {} instance initiated={} done={} backend: {},"
                                    + "fragment id={}, reason: {}",
                                    DebugUtil.printId(queryId), initiated, done, backend.getId(),
                                    fragmentId, "without status");
                        }

                        public void onFailure(Throwable t) {
                            cancelInProcess = false;
                            LOG.warn("Failed to cancel query {} instance initiated={} done={} backend: {},"
                                    + "fragment id={}, reason: {}",
                                    DebugUtil.printId(queryId), initiated, done, backend.getId(),
                                    fragmentId, cancelReason.name(), t);
                        }
                    }, backendRpcCallbackExecutor);
                    cancelInProcess = true;
                } catch (RpcException e) {
                    LOG.warn("cancel plan fragment get a exception, address={}:{}", brpcAddress.getHostname(),
                            brpcAddress.getPort());
                    SimpleScheduler.addToBlacklist(addressToBackendID.get(brpcAddress), e.getMessage());
                }
            } catch (Exception e) {
                LOG.warn("catch a exception", e);
                return;
            }
            return;
        }

        // Just send the cancel logic to BE, not care about the result, and there is no retry logic
        // in upper logic.
        private synchronized void cancelInstance(Types.PPlanFragmentCancelReason cancelReason) {
            for (TPipelineInstanceParams localParam : rpcParams.local_params) {
                LOG.warn("cancelRemoteFragments initiated={} done={} backend:{},"
                        + " fragment instance id={} query={}, reason: {}",
                        this.initiated, this.done, backend.getId(),
                        DebugUtil.printId(localParam.fragment_instance_id),
                        DebugUtil.printId(queryId), cancelReason.name());
                if (this.hasCancelled || this.cancelInProcess) {
                    LOG.info("fragment instance has already been cancelled {} or in process {}. "
                            + "initiated={} done={} backend:{},"
                            + " fragment instance id={} query={}, reason: {}",
                            this.hasCancelled, this.cancelInProcess,
                            this.initiated, this.done, backend.getId(),
                            DebugUtil.printId(localParam.fragment_instance_id),
                            DebugUtil.printId(queryId), cancelReason.name());
                    return;
                }
                try {
                    ListenableFuture<InternalService.PCancelPlanFragmentResult> cancelResult =
                            BackendServiceProxy.getInstance().cancelPlanFragmentAsync(brpcAddress,
                                    localParam.fragment_instance_id, cancelReason);
                    Futures.addCallback(cancelResult, new FutureCallback<InternalService.PCancelPlanFragmentResult>() {
                        public void onSuccess(InternalService.PCancelPlanFragmentResult result) {
                            cancelInProcess = false;
                            if (result.hasStatus()) {
                                Status status = new Status();
                                status.setPstatus(result.getStatus());
                                if (status.getErrorCode() == TStatusCode.OK) {
                                    hasCancelled = true;
                                } else {
                                    LOG.warn("Failed to cancel query {} instance initiated={} done={} backend: {},"
                                            + "fragment instance id={}, reason: {}",
                                            DebugUtil.printId(queryId), initiated, done, backend.getId(),
                                            DebugUtil.printId(localParam.fragment_instance_id), status.toString());
                                }
                            }
                            LOG.warn("Failed to cancel query {} instance initiated={} done={} backend: {},"
                                    + "fragment instance id={}, reason: {}",
                                    DebugUtil.printId(queryId), initiated, done, backend.getId(),
                                    DebugUtil.printId(localParam.fragment_instance_id), "without status");
                        }

                        public void onFailure(Throwable t) {
                            cancelInProcess = false;
                            LOG.warn("Failed to cancel query {} instance initiated={} done={} backend: {},"
                                    + "fragment instance id={}, reason: {}",
                                    DebugUtil.printId(queryId), initiated, done, backend.getId(),
                                    DebugUtil.printId(localParam.fragment_instance_id), cancelReason.name(), t);
                        }
                    }, backendRpcCallbackExecutor);
                    cancelInProcess = true;
                } catch (Exception e) {
                    LOG.warn("catch a exception", e);
                    return;
                }
            }
            return;
        }

        /// TODO: refactor rpcParams
        public synchronized void cancelFragmentInstance(Types.PPlanFragmentCancelReason cancelReason) {
            if (!this.initiated) {
                LOG.warn("Query {}, ccancel before initiated", DebugUtil.printId(queryId));
                return;
            }
            // don't cancel if it is already finished
            if (this.done) {
                LOG.warn("Query {}, cancel after finished", DebugUtil.printId(queryId));
                return;
            }

            if (this.enablePipelineX) {
                cancelFragment(cancelReason);
                return;
            } else {
                cancelInstance(cancelReason);
                return;
            }
        }

        public boolean isBackendStateHealthy() {
            if (backend.getLastMissingHeartbeatTime() > lastMissingHeartbeatTime && !backend.isAlive()) {
                LOG.warn("backend {} is down while joining the coordinator. job id: {}",
                        backend.getId(), jobId);
                return false;
            }
            return true;
        }

        public List<QueryStatisticsItem.FragmentInstanceInfo> buildFragmentInstanceInfo() {
            return this.rpcParams.local_params.stream().map(it -> new FragmentInstanceInfo.Builder()
                    .instanceId(it.fragment_instance_id).fragmentId(String.valueOf(fragmentId))
                    .address(this.address).build()).collect(Collectors.toList());
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
        long beProcessEpoch = 0;

        public BackendExecStates(long beId, TNetworkAddress brpcAddr, boolean twoPhaseExecution, long beProcessEpoch) {
            this.beId = beId;
            this.brpcAddr = brpcAddr;
            this.twoPhaseExecution = twoPhaseExecution;
            this.beProcessEpoch = beProcessEpoch;
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

        public Future<InternalService.PExecPlanFragmentResult> execRemoteFragmentsAsync(BackendServiceProxy proxy)
                throws TException {
            try {
                TExecPlanFragmentParamsList paramsList = new TExecPlanFragmentParamsList();
                for (BackendExecState state : states) {
                    state.initiated = true;
                    paramsList.addToParamsList(state.rpcParams);
                }
                return proxy.execPlanFragmentsAsync(brpcAddr, paramsList, twoPhaseExecution);
            } catch (RpcException e) {
                // DO NOT throw exception here, return a complete future with error code,
                // so that the following logic will cancel the fragment.
                return futureWithException(e);
            }
        }

        public Future<InternalService.PExecPlanFragmentResult> execPlanFragmentStartAsync(BackendServiceProxy proxy)
                throws TException {
            try {
                PExecPlanFragmentStartRequest.Builder builder = PExecPlanFragmentStartRequest.newBuilder();
                PUniqueId qid = PUniqueId.newBuilder().setHi(queryId.hi).setLo(queryId.lo).build();
                builder.setQueryId(qid);
                return proxy.execPlanFragmentStartAsync(brpcAddr, builder.build());
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

    public class PipelineExecContexts {
        long beId;
        TNetworkAddress brpcAddr;
        List<PipelineExecContext> ctxs = Lists.newArrayList();
        boolean twoPhaseExecution = false;
        int instanceNumber;

        public PipelineExecContexts(long beId, TNetworkAddress brpcAddr, boolean twoPhaseExecution,
                int instanceNumber) {
            this.beId = beId;
            this.brpcAddr = brpcAddr;
            this.twoPhaseExecution = twoPhaseExecution;
            this.instanceNumber = instanceNumber;
        }

        public void addContext(PipelineExecContext ctx) {
            this.ctxs.add(ctx);
        }

        public int getInstanceNumber() {
            return instanceNumber;
        }

        /**
         * The BackendExecState in states are all send to the same BE.
         * So only the first BackendExecState need to carry some common fields, such as DescriptorTbl,
         * the other BackendExecState does not need those fields. Unset them to reduce size.
         */
        public void unsetFields() {
            boolean first = true;
            for (PipelineExecContext ctx : ctxs) {
                if (first) {
                    first = false;
                    continue;
                }
                ctx.unsetFields();
            }
        }

        public Future<InternalService.PExecPlanFragmentResult> execRemoteFragmentsAsync(BackendServiceProxy proxy)
                throws TException {
            try {
                TPipelineFragmentParamsList paramsList = new TPipelineFragmentParamsList();
                for (PipelineExecContext cts : ctxs) {
                    cts.initiated = true;
                    paramsList.addToParamsList(cts.rpcParams);
                }
                return proxy.execPlanFragmentsAsync(brpcAddr, paramsList, twoPhaseExecution);
            } catch (RpcException e) {
                // DO NOT throw exception here, return a complete future with error code,
                // so that the following logic will cancel the fragment.
                return futureWithException(e);
            }
        }

        public Future<InternalService.PExecPlanFragmentResult> execPlanFragmentStartAsync(BackendServiceProxy proxy)
                throws TException {
            try {
                PExecPlanFragmentStartRequest.Builder builder = PExecPlanFragmentStartRequest.newBuilder();
                PUniqueId qid = PUniqueId.newBuilder().setHi(queryId.hi).setLo(queryId.lo).build();
                builder.setQueryId(qid);
                return proxy.execPlanFragmentStartAsync(brpcAddr, builder.build());
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
        public int parallelTasksNum = 0;
        public boolean ignoreDataDistribution = false;
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
            Set<Integer> topnFilterSources = scanNodes.stream()
                    .filter(scanNode -> scanNode instanceof OlapScanNode)
                    .flatMap(scanNode -> ((OlapScanNode) scanNode).getTopnFilterSortNodes().stream())
                    .map(sort -> sort.getId().asInt()).collect(Collectors.toSet());
            for (int i = 0; i < instanceExecParams.size(); ++i) {
                final FInstanceExecParam instanceExecParam = instanceExecParams.get(i);
                TExecPlanFragmentParams params = new TExecPlanFragmentParams();
                params.setIsNereids(context != null ? context.getState().isNereids() : false);
                params.setProtocolVersion(PaloInternalServiceVersion.V1);
                params.setFragment(fragment.toThrift());
                params.setDescTbl(descTable);
                params.setParams(new TPlanFragmentExecParams());
                params.setBuildHashTableForBroadcastJoin(instanceExecParam.buildHashTableForBroadcastJoin);
                params.params.setQueryId(queryId);
                params.params.setFragmentInstanceId(instanceExecParam.instanceId);

                Map<Integer, List<TScanRangeParams>> scanRanges = instanceExecParam.perNodeScanRanges;
                if (scanRanges == null) {
                    scanRanges = Maps.newHashMap();
                }
                if (!topnFilterSources.isEmpty()) {
                    // topn_filter_source_node_ids is used by nereids not by legacy planner.
                    // if there is no topnFilterSources, do not set it.
                    // topn_filter_source_node_ids=null means legacy planner
                    params.params.topn_filter_source_node_ids = Lists.newArrayList(topnFilterSources);
                }
                params.params.setPerNodeScanRanges(scanRanges);
                params.params.setPerExchNumSenders(perExchNumSenders);

                if (tWorkloadGroups != null) {
                    params.setWorkloadGroups(tWorkloadGroups);
                }
                params.params.setDestinations(destinations);
                params.params.setSenderId(i);
                params.params.setNumSenders(instanceExecParams.size());
                params.setCoord(coordAddress);
                params.setBackendNum(backendNum++);
                params.setQueryGlobals(queryGlobals);
                params.setQueryOptions(queryOptions);
                params.query_options.setEnablePipelineEngine(false);
                params.params.setSendQueryStatisticsWithEveryBatch(
                        fragment.isTransferQueryStatisticsWithEveryBatch());
                params.params.setRuntimeFilterParams(new TRuntimeFilterParams());
                params.params.runtime_filter_params.setRuntimeFilterMergeAddr(runtimeFilterMergeAddr);
                if (instanceExecParam.instanceId.equals(runtimeFilterMergeInstanceId)) {
                    Set<Integer> broadCastRf = assignedRuntimeFilters.stream().filter(RuntimeFilter::isBroadcast)
                            .map(r -> r.getFilterId().asInt()).collect(Collectors.toSet());

                    for (RuntimeFilter rf : assignedRuntimeFilters) {
                        if (!ridToTargetParam.containsKey(rf.getFilterId())) {
                            continue;
                        }
                        List<FRuntimeFilterTargetParam> fParams = ridToTargetParam.get(rf.getFilterId());
                        if (rf.hasRemoteTargets()) {
                            Map<TNetworkAddress, TRuntimeFilterTargetParamsV2> targetParamsV2 = new HashMap<>();
                            for (FRuntimeFilterTargetParam targetParam : fParams) {
                                if (targetParamsV2.containsKey(targetParam.targetFragmentInstanceAddr)) {
                                    targetParamsV2.get(targetParam.targetFragmentInstanceAddr)
                                            .target_fragment_instance_ids
                                            .add(targetParam.targetFragmentInstanceId);
                                } else {
                                    targetParamsV2.put(targetParam.targetFragmentInstanceAddr,
                                            new TRuntimeFilterTargetParamsV2());
                                    targetParamsV2.get(targetParam.targetFragmentInstanceAddr)
                                            .target_fragment_instance_addr
                                            = targetParam.targetFragmentInstanceAddr;
                                    targetParamsV2.get(targetParam.targetFragmentInstanceAddr)
                                            .target_fragment_instance_ids
                                            = new ArrayList<>();
                                    targetParamsV2.get(targetParam.targetFragmentInstanceAddr)
                                            .target_fragment_instance_ids
                                            .add(targetParam.targetFragmentInstanceId);
                                }
                            }
                            params.params.runtime_filter_params.putToRidToTargetParamv2(rf.getFilterId().asInt(),
                                    new ArrayList<TRuntimeFilterTargetParamsV2>(targetParamsV2.values()));
                        } else {
                            List<TRuntimeFilterTargetParams> targetParams = Lists.newArrayList();
                            for (FRuntimeFilterTargetParam targetParam : fParams) {
                                targetParams.add(new TRuntimeFilterTargetParams(targetParam.targetFragmentInstanceId,
                                        targetParam.targetFragmentInstanceAddr));
                            }
                            params.params.runtime_filter_params.putToRidToTargetParam(rf.getFilterId().asInt(),
                                    targetParams);
                        }
                    }
                    for (Map.Entry<RuntimeFilterId, Integer> entry : ridToBuilderNum.entrySet()) {
                        params.params.runtime_filter_params.putToRuntimeFilterBuilderNum(
                                entry.getKey().asInt(), broadCastRf.contains(entry.getKey().asInt())
                                        ? 1 : entry.getValue());
                    }
                    for (RuntimeFilter rf : assignedRuntimeFilters) {
                        params.params.runtime_filter_params.putToRidToRuntimeFilter(
                                rf.getFilterId().asInt(), rf.toThrift());
                    }
                }
                params.setFileScanParams(fileScanRangeParamsMap);
                paramsList.add(params);
            }
            return paramsList;
        }

        Map<TNetworkAddress, TPipelineFragmentParams> toTPipelineParams(int backendNum) {
            long memLimit = queryOptions.getMemLimit();
            // 2. update memory limit for colocate join
            if (colocateFragmentIds.contains(fragment.getFragmentId().asInt())) {
                int rate = Math.min(Config.query_colocate_join_memory_limit_penalty_factor, instanceExecParams.size());
                memLimit = queryOptions.getMemLimit() / rate;
            }
            Set<Integer> topnFilterSources = scanNodes.stream()
                    .filter(scanNode -> scanNode instanceof OlapScanNode)
                    .flatMap(scanNode -> ((OlapScanNode) scanNode).getTopnFilterSortNodes().stream())
                    .map(sort -> sort.getId().asInt()).collect(Collectors.toSet());
            Map<TNetworkAddress, TPipelineFragmentParams> res = new HashMap();
            Map<TNetworkAddress, Integer> instanceIdx = new HashMap();
            TPlanFragment fragmentThrift = fragment.toThrift();
            for (int i = 0; i < instanceExecParams.size(); ++i) {
                final FInstanceExecParam instanceExecParam = instanceExecParams.get(i);
                Map<Integer, List<TScanRangeParams>> scanRanges = instanceExecParam.perNodeScanRanges;
                Map<Integer, Boolean> perNodeSharedScans = instanceExecParam.perNodeSharedScans;
                if (scanRanges == null) {
                    scanRanges = Maps.newHashMap();
                    perNodeSharedScans = Maps.newHashMap();
                }
                if (!res.containsKey(instanceExecParam.host)) {
                    TPipelineFragmentParams params = new TPipelineFragmentParams();

                    // Set global param
                    params.setIsNereids(context != null ? context.getState().isNereids() : false);
                    params.setProtocolVersion(PaloInternalServiceVersion.V1);
                    params.setDescTbl(descTable);
                    params.setQueryId(queryId);
                    params.setPerExchNumSenders(perExchNumSenders);
                    params.setDestinations(destinations);
                    params.setNumSenders(instanceExecParams.size());
                    params.setCoord(coordAddress);
                    params.setQueryGlobals(queryGlobals);
                    params.setQueryOptions(queryOptions);
                    params.query_options.setEnablePipelineEngine(true);
                    params.query_options.setMemLimit(memLimit);
                    params.setSendQueryStatisticsWithEveryBatch(
                            fragment.isTransferQueryStatisticsWithEveryBatch());
                    params.setFragment(fragmentThrift);
                    params.setLocalParams(Lists.newArrayList());
                    if (tWorkloadGroups != null) {
                        params.setWorkloadGroups(tWorkloadGroups);
                    }

                    params.setFileScanParams(fileScanRangeParamsMap);
                    params.setNumBuckets(fragment.getBucketNum());
                    params.setPerNodeSharedScans(perNodeSharedScans);
                    params.setTotalInstances(instanceExecParams.size());
                    if (ignoreDataDistribution) {
                        params.setParallelInstances(parallelTasksNum);
                    }
                    res.put(instanceExecParam.host, params);
                    res.get(instanceExecParam.host).setBucketSeqToInstanceIdx(new HashMap<Integer, Integer>());
                    res.get(instanceExecParam.host).setShuffleIdxToInstanceIdx(new HashMap<Integer, Integer>());
                    instanceIdx.put(instanceExecParam.host, 0);
                }
                // Set each bucket belongs to which instance on this BE.
                // This is used for LocalExchange(BUCKET_HASH_SHUFFLE).
                int instanceId = instanceIdx.get(instanceExecParam.host);

                for (int bucket : instanceExecParam.bucketSeqSet) {
                    res.get(instanceExecParam.host).getBucketSeqToInstanceIdx().put(bucket, instanceId);
                }
                instanceIdx.replace(instanceExecParam.host, ++instanceId);
                TPipelineFragmentParams params = res.get(instanceExecParam.host);
                res.get(instanceExecParam.host).getShuffleIdxToInstanceIdx().put(instanceExecParam.recvrId,
                        params.getLocalParams().size());
                TPipelineInstanceParams localParams = new TPipelineInstanceParams();

                localParams.setBuildHashTableForBroadcastJoin(instanceExecParam.buildHashTableForBroadcastJoin);
                localParams.setFragmentInstanceId(instanceExecParam.instanceId);
                localParams.setPerNodeScanRanges(scanRanges);
                localParams.setPerNodeSharedScans(perNodeSharedScans);
                localParams.setSenderId(i);
                localParams.setBackendNum(backendNum++);
                localParams.setRuntimeFilterParams(new TRuntimeFilterParams());
                localParams.runtime_filter_params.setRuntimeFilterMergeAddr(runtimeFilterMergeAddr);
                if (!topnFilterSources.isEmpty()) {
                    // topn_filter_source_node_ids is used by nereids not by legacy planner.
                    // if there is no topnFilterSources, do not set it.
                    // topn_filter_source_node_ids=null means legacy planner
                    localParams.topn_filter_source_node_ids = Lists.newArrayList(topnFilterSources);
                }
                if (instanceExecParam.instanceId.equals(runtimeFilterMergeInstanceId)) {
                    Set<Integer> broadCastRf = assignedRuntimeFilters.stream().filter(RuntimeFilter::isBroadcast)
                            .map(r -> r.getFilterId().asInt()).collect(Collectors.toSet());

                    for (RuntimeFilter rf : assignedRuntimeFilters) {
                        if (!ridToTargetParam.containsKey(rf.getFilterId())) {
                            continue;
                        }
                        List<FRuntimeFilterTargetParam> fParams = ridToTargetParam.get(rf.getFilterId());
                        if (rf.hasRemoteTargets()) {
                            Map<TNetworkAddress, TRuntimeFilterTargetParamsV2> targetParamsV2 = new HashMap<>();
                            for (FRuntimeFilterTargetParam targetParam : fParams) {
                                if (targetParamsV2.containsKey(targetParam.targetFragmentInstanceAddr)) {
                                    targetParamsV2.get(targetParam.targetFragmentInstanceAddr)
                                            .target_fragment_instance_ids
                                            .add(targetParam.targetFragmentInstanceId);
                                } else {
                                    targetParamsV2.put(targetParam.targetFragmentInstanceAddr,
                                            new TRuntimeFilterTargetParamsV2());
                                    targetParamsV2.get(targetParam.targetFragmentInstanceAddr)
                                            .target_fragment_instance_addr
                                            = targetParam.targetFragmentInstanceAddr;
                                    targetParamsV2.get(targetParam.targetFragmentInstanceAddr)
                                            .target_fragment_instance_ids
                                            = new ArrayList<>();
                                    targetParamsV2.get(targetParam.targetFragmentInstanceAddr)
                                            .target_fragment_instance_ids
                                            .add(targetParam.targetFragmentInstanceId);
                                }
                            }

                            localParams.runtime_filter_params.putToRidToTargetParamv2(rf.getFilterId().asInt(),
                                    new ArrayList<TRuntimeFilterTargetParamsV2>(targetParamsV2.values()));
                        } else {
                            List<TRuntimeFilterTargetParams> targetParams = Lists.newArrayList();
                            for (FRuntimeFilterTargetParam targetParam : fParams) {
                                targetParams.add(new TRuntimeFilterTargetParams(targetParam.targetFragmentInstanceId,
                                        targetParam.targetFragmentInstanceAddr));
                            }
                            localParams.runtime_filter_params.putToRidToTargetParam(rf.getFilterId().asInt(),
                                    targetParams);
                        }
                    }
                    for (Map.Entry<RuntimeFilterId, Integer> entry : ridToBuilderNum.entrySet()) {
                        localParams.runtime_filter_params.putToRuntimeFilterBuilderNum(
                                entry.getKey().asInt(), broadCastRf.contains(entry.getKey().asInt()) ? 1 :
                                        entry.getValue());
                    }
                    for (RuntimeFilter rf : assignedRuntimeFilters) {
                        localParams.runtime_filter_params.putToRidToRuntimeFilter(
                                rf.getFilterId().asInt(), rf.toThrift());
                    }
                }
                params.getLocalParams().add(localParams);
            }

            return res;
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

    public QueueToken getQueueToken() {
        return queueToken;
    }

    // fragment instance exec param, it is used to assemble
    // the per-instance TPlanFragmentExecParams, as a member of
    // FragmentExecParams
    static class FInstanceExecParam {
        TUniqueId instanceId;
        TNetworkAddress host;
        Map<Integer, List<TScanRangeParams>> perNodeScanRanges = Maps.newHashMap();
        Map<Integer, Boolean> perNodeSharedScans = Maps.newHashMap();

        int perFragmentInstanceIdx;

        Set<Integer> bucketSeqSet = Sets.newHashSet();

        FragmentExecParams fragmentExecParams;

        boolean buildHashTableForBroadcastJoin = false;

        int recvrId = -1;

        List<TUniqueId> instancesSharingHashTable = Lists.newArrayList();

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
            if (enablePipelineEngine) {
                for (int index = 0; index < fragments.size(); index++) {
                    for (PipelineExecContext ctx : pipelineExecContexts.values()) {
                        if (fragments.get(index).getFragmentId() != ctx.fragmentId) {
                            continue;
                        }
                        final List<QueryStatisticsItem.FragmentInstanceInfo> info = ctx.buildFragmentInstanceInfo();
                        result.addAll(info);
                    }
                }
            } else {
                for (int index = 0; index < fragments.size(); index++) {
                    for (BackendExecState backendExecState : backendExecStates) {
                        if (fragments.get(index).getFragmentId() != backendExecState.fragmentId) {
                            continue;
                        }
                        final QueryStatisticsItem.FragmentInstanceInfo info =
                                backendExecState.buildFragmentInstanceInfo();
                        result.add(info);
                    }
                }
            }
        } finally {
            unlock();
        }
        return result;
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

