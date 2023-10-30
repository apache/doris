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
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryType;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public final class QeProcessorImpl implements QeProcessor {

    private static final Logger LOG = LogManager.getLogger(QeProcessorImpl.class);
    private Map<TUniqueId, QueryInfo> coordinatorMap;

    private Map<TUniqueId, Integer> queryToInstancesNum;
    private Map<String, AtomicInteger> userToInstancesCount;

    public static final QeProcessor INSTANCE;

    static {
        INSTANCE = new QeProcessorImpl();
    }

    private ExecutorService writeProfileExecutor;

    private QeProcessorImpl() {
        coordinatorMap = new ConcurrentHashMap<>();
        // write profile to ProfileManager when query is running.
        writeProfileExecutor = ThreadPoolManager.newDaemonProfileThreadPool(1, 100,
                "profile-write-pool", true);
        queryToInstancesNum = new ConcurrentHashMap<>();
        userToInstancesCount = new ConcurrentHashMap<>();
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
    public void registerQuery(TUniqueId queryId, Coordinator coord) throws UserException {
        registerQuery(queryId, new QueryInfo(coord));
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
            LOG.info("Deregister query id {}", DebugUtil.printId(queryId));

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
            LOG.warn("not found query {} when unregisterQuery", DebugUtil.printId(queryId));
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
                    .profile(info.getCoord().getExecutionProfile().getExecutionProfile())
                    .isReportSucc(context.getSessionVariable().enableProfile()).build();
            querySet.put(queryIdStr, item);
        }
        return querySet;
    }

    @Override
    public TReportExecStatusResult reportExecStatus(TReportExecStatusParams params, TNetworkAddress beAddr) {
        LOG.info("Processing report exec status, query {} instance {} from {}",
                DebugUtil.printId(params.query_id), DebugUtil.printId(params.fragment_instance_id),
                beAddr.toString());

        if (params.isSetProfile()) {
            LOG.info("ReportExecStatus(): fragment_instance_id={}, query id={}, backend num: {}, ip: {}",
                    DebugUtil.printId(params.fragment_instance_id), DebugUtil.printId(params.query_id),
                    params.backend_num, beAddr);
            LOG.debug("params: {}", params);
        }
        final TReportExecStatusResult result = new TReportExecStatusResult();
        final QueryInfo info = coordinatorMap.get(params.query_id);

        if (info == null) {
            // There is no QueryInfo for StreamLoad, so we return OK
            if (params.query_type == TQueryType.LOAD) {
                result.setStatus(new TStatus(TStatusCode.OK));
            } else {
                result.setStatus(new TStatus(TStatusCode.RUNTIME_ERROR));
            }
            LOG.warn("ReportExecStatus() runtime error, query {} with type {} does not exist",
                    DebugUtil.printId(params.query_id), params.query_type);
            return result;
        }
        try {
            info.getCoord().updateFragmentExecStatus(params);
            if (params.isSetProfile()) {
                writeProfileExecutor.submit(new WriteProfileTask(params, info));
            }
        } catch (Exception e) {
            LOG.warn("Report response: {}, query: {}, instance: {}", result.toString(),
                    DebugUtil.printId(params.query_id), DebugUtil.printId(params.fragment_instance_id));
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

    public static final class QueryInfo {
        private final ConnectContext connectContext;
        private final Coordinator coord;
        private final String sql;
        private final long startExecTime;

        // from Export, Pull load, Insert
        public QueryInfo(Coordinator coord) {
            this(null, null, coord);
        }

        // from query
        public QueryInfo(ConnectContext connectContext, String sql, Coordinator coord) {
            this.connectContext = connectContext;
            this.coord = coord;
            this.sql = sql;
            this.startExecTime = System.currentTimeMillis();
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
            return startExecTime;
        }
    }

    private class WriteProfileTask implements Runnable {
        private TReportExecStatusParams params;

        private QueryInfo queryInfo;

        WriteProfileTask(TReportExecStatusParams params, QueryInfo queryInfo) {
            this.params = params;
            this.queryInfo = queryInfo;
        }

        @Override
        public void run() {
            QueryInfo info = coordinatorMap.get(params.query_id);
            if (info == null) {
                return;
            }

            ExecutionProfile executionProfile = info.getCoord().getExecutionProfile();
            executionProfile.update(-1, false);
        }
    }
}
