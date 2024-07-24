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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryProfile;
import org.apache.doris.thrift.TReportExecStatusParams;
import org.apache.doris.thrift.TReportExecStatusResult;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public final class QeProcessorImpl implements QeProcessor {

    private static final Logger LOG = LogManager.getLogger(QeProcessorImpl.class);
    private Map<TUniqueId, QueryInfo> coordinatorMap;

    private Map<TUniqueId, Integer> queryToInstancesNum;
    private Map<String, AtomicInteger> userToInstancesCount;
    private ExecutorService writeProfileExecutor;

    public static final QeProcessor INSTANCE;

    static {
        INSTANCE = new QeProcessorImpl();
    }

    private QeProcessorImpl() {
        coordinatorMap = new ConcurrentHashMap<>();
        queryToInstancesNum = new ConcurrentHashMap<>();
        userToInstancesCount = new ConcurrentHashMap<>();
        // write profile to ProfileManager when query is running.
        writeProfileExecutor = ThreadPoolManager.newDaemonProfileThreadPool(3, 100,
                "profile-write-pool", true);
    }

    private Status processQueryProfile(TQueryProfile profile, TNetworkAddress address, boolean isDone) {
        LOG.info("New profile processing API, query {}", DebugUtil.printId(profile.query_id));

        ExecutionProfile executionProfile = ProfileManager.getInstance().getExecutionProfile(profile.query_id);
        if (executionProfile == null) {
            LOG.warn("Could not find execution profile with query id {}", DebugUtil.printId(profile.query_id));
            return new Status(TStatusCode.NOT_FOUND, "Could not find execution profile with query id "
                    + DebugUtil.printId(profile.query_id));
        }

        // Update profile may cost a lot of time, use a seperate pool to deal with it.
        writeProfileExecutor.submit(new Runnable() {
            @Override
            public void run() {
                executionProfile.updateProfile(profile, address, isDone);
            }
        });

        return Status.OK;
    }

    @Override
    public Coordinator getCoordinator(TUniqueId queryId) {
        QueryInfo queryInfo = coordinatorMap.get(queryId);
        if (queryInfo != null) {
            return queryInfo.getCoord();
        }
        return null;
    }

    @Override
    public List<Coordinator> getAllCoordinators() {
        List<Coordinator> res = new ArrayList<>();

        for (QueryInfo co : coordinatorMap.values()) {
            res.add(co.coord);
        }
        return res;
    }

    @Override
    public void registerQuery(TUniqueId queryId, QueryInfo info) throws UserException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("register query id = " + DebugUtil.printId(queryId) + ", job: " + info.getCoord().getJobId());
        }
        final QueryInfo result = coordinatorMap.putIfAbsent(queryId, info);
        if (result != null) {
            throw new UserException("queryId " + queryId + " already exists");
        }
        // Should add the execution profile to profile manager, BE will report the profile to FE and FE
        // will update it in ProfileManager
        if (info.coord.getQueryOptions().enable_profile) {
            ProfileManager.getInstance().addExecutionProfile(info.getCoord().getExecutionProfile());
        }
    }

    @Override
    public void registerInstances(TUniqueId queryId, Integer instancesNum) throws UserException {
        if (!coordinatorMap.containsKey(queryId)) {
            throw new UserException("query not exists in coordinatorMap:" + DebugUtil.printId(queryId));
        }
        QueryInfo queryInfo = coordinatorMap.get(queryId);
        if (queryInfo.getConnectContext() != null
                && !Strings.isNullOrEmpty(queryInfo.getConnectContext().getQualifiedUser())
        ) {
            String user = queryInfo.getConnectContext().getQualifiedUser();
            long maxQueryInstances = queryInfo.getConnectContext().getEnv().getAuth().getMaxQueryInstances(user);
            if (maxQueryInstances <= 0) {
                maxQueryInstances = Config.default_max_query_instances;
            }
            if (maxQueryInstances > 0) {
                AtomicInteger currentCount = userToInstancesCount
                        .computeIfAbsent(user, ignored -> new AtomicInteger(0));
                // Many query can reach here.
                if (instancesNum + currentCount.get() > maxQueryInstances) {
                    throw new UserException("reach max_query_instances " + maxQueryInstances);
                }
            }
            queryToInstancesNum.put(queryId, instancesNum);
            userToInstancesCount.computeIfAbsent(user, ignored -> new AtomicInteger(0)).addAndGet(instancesNum);
            MetricRepo.USER_COUNTER_QUERY_INSTANCE_BEGIN.getOrAdd(user).increase(instancesNum.longValue());
        }
    }

    public Map<String, Integer> getInstancesNumPerUser() {
        return Maps.transformEntries(userToInstancesCount, (ignored, value) -> value != null ? value.get() : 0);
    }

    @Override
    public void unregisterQuery(TUniqueId queryId) {
        QueryInfo queryInfo = coordinatorMap.remove(queryId);
        if (queryInfo != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Deregister query id {}", DebugUtil.printId(queryId));
            }

            // Here we shuold use query option instead of ConnectContext,
            // because for the coordinator of load task, it does not have ConnectContext.
            if (queryInfo.getCoord().getQueryOptions().enable_profile) {
                ProfileManager.getInstance().markExecutionProfileFinished(queryId);
            }

            if (queryInfo.getConnectContext() != null
                    && !Strings.isNullOrEmpty(queryInfo.getConnectContext().getQualifiedUser())
            ) {
                Integer num = queryToInstancesNum.remove(queryId);
                if (num != null) {
                    String user = queryInfo.getConnectContext().getQualifiedUser();
                    AtomicInteger instancesNum = userToInstancesCount.get(user);
                    if (instancesNum == null) {
                        LOG.warn("WTF?? query {} in queryToInstancesNum but not in userToInstancesCount",
                                DebugUtil.printId(queryId)
                        );
                    } else {
                        instancesNum.addAndGet(-num);
                    }
                }
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("not found query {} when unregisterQuery", DebugUtil.printId(queryId));
            }
        }

        // commit hive tranaction if needed
        Env.getCurrentHiveTransactionMgr().deregister(DebugUtil.printId(queryId));
    }

    @Override
    public Map<String, QueryStatisticsItem> getQueryStatistics() {
        final Map<String, QueryStatisticsItem> querySet = Maps.newHashMap();
        for (Map.Entry<TUniqueId, QueryInfo> entry : coordinatorMap.entrySet()) {
            final QueryInfo info = entry.getValue();
            final ConnectContext context = info.getConnectContext();
            if (info.sql == null || context == null) {
                continue;
            }
            final String queryIdStr = DebugUtil.printId(info.getConnectContext().queryId());
            final QueryStatisticsItem item = new QueryStatisticsItem.Builder().queryId(queryIdStr)
                    .queryStartTime(info.getStartExecTime()).sql(info.getSql()).user(context.getQualifiedUser())
                    .connId(String.valueOf(context.getConnectionId())).db(context.getDatabase())
                    .catalog(context.getDefaultCatalog())
                    .fragmentInstanceInfos(info.getCoord().getFragmentInstanceInfos())
                    .profile(info.getCoord().getExecutionProfile().getRoot())
                    .isReportSucc(context.getSessionVariable().enableProfile()).build();
            querySet.put(queryIdStr, item);
        }
        return querySet;
    }

    @Override
    public TReportExecStatusResult reportExecStatus(TReportExecStatusParams params, TNetworkAddress beAddr) {
        if (params.isSetQueryProfile()) {
            // Why not return response when process new profile failed?
            // First of all, we will do a refactor for report exec status in the future.
            // In that refactor, we will combine the report of exec status with query profile in a single rpc.
            // If we return error response in this pr, we will have problem when doing cluster upgrading.
            // For example, FE will return directly if it receives profile, but BE actually report exec status
            // with profile in a single rpc, this will make FE ignore the exec status and may lead to bug in query
            // like insert into select.
            if (params.isSetBackendId() && params.isSetDone()) {
                Backend backend = Env.getCurrentSystemInfo().getBackend(params.getBackendId());
                boolean isDone = params.isDone();
                if (backend != null) {
                    // the process status is ignored by design.
                    // actually be does not care the process status of profile on fe.
                    processQueryProfile(params.getQueryProfile(), backend.getHeartbeatAddress(), isDone);
                }
            } else {
                LOG.warn("Invalid report profile req, this is a logical error, BE must set backendId and isDone"
                            + " at same time, query id: {}", DebugUtil.printId(params.query_id));
            }
        }

        if (params.isSetProfile() || params.isSetLoadChannelProfile()) {
            LOG.info("ReportExecStatus(): fragment_instance_id={}, query id={}, backend num: {}, ip: {}",
                    DebugUtil.printId(params.fragment_instance_id), DebugUtil.printId(params.query_id),
                    params.backend_num, beAddr);
            if (LOG.isDebugEnabled()) {
                LOG.debug("params: {}", params);
            }
            ExecutionProfile executionProfile = ProfileManager.getInstance().getExecutionProfile(params.query_id);
            if (executionProfile != null) {
                // Update profile may cost a lot of time, use a seperate pool to deal with it.
                writeProfileExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        executionProfile.updateProfile(params);
                    }
                });
            } else {
                LOG.info("Could not find execution profile with query id {}", DebugUtil.printId(params.query_id));
            }
        }
        final TReportExecStatusResult result = new TReportExecStatusResult();

        if (params.isSetReportWorkloadRuntimeStatus()) {
            Env.getCurrentEnv().getWorkloadRuntimeStatusMgr().updateBeQueryStats(params.report_workload_runtime_status);
            if (!params.isSetQueryId()) {
                result.setStatus(new TStatus(TStatusCode.OK));
                return result;
            }
        }

        final QueryInfo info = coordinatorMap.get(params.query_id);
        result.setStatus(new TStatus(TStatusCode.OK));
        if (info == null) {
            // Currently, the execution of query is splited from the exec status process.
            // So, it is very likely that when exec status arrived on FE asynchronously, coordinator
            // has been removed from coordinatorMap.
            return result;
        }
        try {
            info.getCoord().updateFragmentExecStatus(params);
        } catch (Exception e) {
            LOG.warn("Exception during handle report, response: {}, query: {}, instance: {}", result.toString(),
                    DebugUtil.printId(params.query_id), DebugUtil.printId(params.fragment_instance_id), e);
            return result;
        }
        result.setStatus(new TStatus(TStatusCode.OK));
        return result;
    }

    @Override
    public String getCurrentQueryByQueryId(TUniqueId queryId) {
        QueryInfo info = coordinatorMap.get(queryId);
        if (info != null && info.sql != null) {
            return info.sql;
        }
        return "";
    }

    public Map<String, QueryInfo> getQueryInfoMap() {
        Map<String, QueryInfo> retQueryInfoMap = Maps.newHashMap();
        Set<TUniqueId> queryIdSet = coordinatorMap.keySet();
        for (TUniqueId qid : queryIdSet) {
            QueryInfo queryInfo = coordinatorMap.get(qid);
            if (queryInfo != null) {
                retQueryInfoMap.put(DebugUtil.printId(qid), queryInfo);
            }
        }
        return retQueryInfoMap;
    }

    public static final class QueryInfo {
        private final ConnectContext connectContext;
        private final Coordinator coord;
        private final String sql;
        private long registerTimeMs = 0L;

        // from Export, Pull load, Insert
        public QueryInfo(Coordinator coord) {
            this(null, null, coord);
        }

        // from query
        public QueryInfo(ConnectContext connectContext, String sql, Coordinator coord) {
            this.connectContext = connectContext;
            this.coord = coord;
            this.sql = sql;
            this.registerTimeMs = System.currentTimeMillis();
        }

        public ConnectContext getConnectContext() {
            return connectContext;
        }

        public Coordinator getCoord() {
            return coord;
        }

        public String getSql() {
            return sql;
        }

        public long getStartExecTime() {
            if (coord.getQueueToken() != null) {
                return coord.getQueueToken().getQueueEndTime();
            }
            return registerTimeMs;
        }

        public long getQueueStartTime() {
            if (coord.getQueueToken() != null) {
                return coord.getQueueToken().getQueueStartTime();
            }
            return -1;
        }

        public long getQueueEndTime() {
            if (coord.getQueueToken() != null) {
                return coord.getQueueToken().getQueueEndTime();
            }
            return -1;
        }

        public String getQueueStatus() {
            if (coord.getQueueToken() != null) {
                return coord.getQueueToken().getQueueMsg();
            }
            return "";
        }
    }
}
