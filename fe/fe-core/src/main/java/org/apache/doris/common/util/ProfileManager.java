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

package org.apache.doris.common.util;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.profile.MultiProfileTreeBuilder;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.profile.ProfileTreeBuilder;
import org.apache.doris.common.profile.ProfileTreeNode;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.qe.CoordInterface;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TGetRealtimeExecStatusRequest;
import org.apache.doris.thrift.TGetRealtimeExecStatusResponse;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/*
 * if you want to visit the attribute(such as queryID,defaultDb)
 * you can use profile.getInfoStrings("queryId")
 * All attributes can be seen from the above.
 *
 * why the element in the finished profile array is not RuntimeProfile,
 * the purpose is let coordinator can destruct earlier(the fragment profile is in Coordinator)
 *
 */
public class ProfileManager {
    private static final Logger LOG = LogManager.getLogger(ProfileManager.class);
    private static volatile ProfileManager INSTANCE = null;

    public enum ProfileType {
        QUERY,
        LOAD,
    }

    public static class ProfileElement {
        public ProfileElement(Profile profile) {
            this.profile = profile;
        }

        private final Profile profile;
        public Map<String, String> infoStrings = Maps.newHashMap();
        public MultiProfileTreeBuilder builder = null;
        public String errMsg = "";

        public StatsErrorEstimator statsErrorEstimator;

        // lazy load profileContent because sometimes profileContent is very large
        public String getProfileContent() {
            // Not cache the profile content because it may change during insert
            // into select statement, we need use this to check process.
            // And also, cache the content will double usage of the memory in FE.
            return profile.getProfileByLevel();
        }

        public String getProfileBrief() {
            return profile.getProfileBrief();
        }

        public double getError() {
            return statsErrorEstimator.getQError();
        }

        public void setStatsErrorEstimator(StatsErrorEstimator statsErrorEstimator) {
            this.statsErrorEstimator = statsErrorEstimator;
        }
    }

    // only protect queryIdDeque; queryIdToProfileMap is concurrent, no need to protect
    private ReentrantReadWriteLock lock;
    private ReadLock readLock;
    private WriteLock writeLock;

    // record the order of profiles by queryId
    private Deque<String> queryIdDeque;
    private Map<String, ProfileElement> queryIdToProfileMap; // from QueryId to RuntimeProfile
    // Sometimes one Profile is related with multiple execution profiles(Brokerload), so that
    // execution profile's query id is not related with Profile's query id.
    private Map<TUniqueId, ExecutionProfile> queryIdToExecutionProfiles;

    private final ExecutorService fetchRealTimeProfileExecutor;

    public static ProfileManager getInstance() {
        if (INSTANCE == null) {
            synchronized (ProfileManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ProfileManager();
                }
            }
        }
        return INSTANCE;
    }

    private ProfileManager() {
        lock = new ReentrantReadWriteLock(true);
        readLock = lock.readLock();
        writeLock = lock.writeLock();
        queryIdDeque = new LinkedList<>();
        queryIdToProfileMap = new ConcurrentHashMap<>();
        queryIdToExecutionProfiles = Maps.newHashMap();
        fetchRealTimeProfileExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                10, 100, "fetch-realtime-profile-pool", true);
    }

    private ProfileElement createElement(Profile profile) {
        ProfileElement element = new ProfileElement(profile);
        element.infoStrings.putAll(profile.getSummaryProfile().getAsInfoStings());
        // Not init builder any more, we will not maintain it since 2.1.0, because the structure
        // assume that the execution profiles structure is already known before execution. But in
        // PipelineX Engine, it will changed during execution.
        return element;
    }

    public void addExecutionProfile(ExecutionProfile executionProfile) {
        if (executionProfile == null) {
            return;
        }
        writeLock.lock();
        try {
            if (queryIdToExecutionProfiles.containsKey(executionProfile.getQueryId())) {
                return;
            }
            queryIdToExecutionProfiles.put(executionProfile.getQueryId(), executionProfile);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Add execution profile {} to profile manager",
                        DebugUtil.printId(executionProfile.getQueryId()));
            }
            // This branch has two purposes:
            // 1. discard profile collecting if its collection not finished in 5 seconds after query finished.
            // 2. prevent execution profile from leakage. If we have too many execution profiles in memory,
            // we will remove execution profiles of query that has finished in 5 seconds ago.
            if (queryIdToExecutionProfiles.size() > 2 * Config.max_query_profile_num) {
                List<ExecutionProfile> finishOrExpireExecutionProfiles = Lists.newArrayList();
                for (ExecutionProfile tmpProfile : queryIdToExecutionProfiles.values()) {
                    boolean queryFinishedLongEnough = tmpProfile.getQueryFinishTime() > 0
                            && System.currentTimeMillis() - tmpProfile.getQueryFinishTime()
                            > Config.profile_async_collect_expire_time_secs * 1000;

                    if (queryFinishedLongEnough) {
                        finishOrExpireExecutionProfiles.add(tmpProfile);
                    }
                }
                StringBuilder stringBuilder = new StringBuilder();
                for (ExecutionProfile tmp : finishOrExpireExecutionProfiles) {
                    stringBuilder.append(DebugUtil.printId(tmp.getQueryId())).append(",");
                    queryIdToExecutionProfiles.remove(tmp.getQueryId());
                }
                LOG.warn("Remove expired execution profiles {}", stringBuilder.toString());
            }
        } finally {
            writeLock.unlock();
        }
    }

    public ExecutionProfile getExecutionProfile(TUniqueId queryId) {
        return this.queryIdToExecutionProfiles.get(queryId);
    }

    public void pushProfile(Profile profile) {
        if (profile == null) {
            return;
        }

        ProfileElement element = createElement(profile);
        // 'insert into' does have job_id, put all profiles key with query_id
        String key = element.profile.getSummaryProfile().getProfileId();
        // check when push in, which can ensure every element in the list has QUERY_ID column,
        // so there is no need to check when remove element from list.
        if (Strings.isNullOrEmpty(key)) {
            LOG.warn("the key or value of Map is null, "
                    + "may be forget to insert 'QUERY_ID' or 'JOB_ID' column into infoStrings");
        }
        writeLock.lock();
        // a profile may be updated multiple times in queryIdToProfileMap,
        // and only needs to be inserted into the queryIdDeque for the first time.
        queryIdToProfileMap.put(key, element);
        try {
            if (!queryIdDeque.contains(key)) {
                if (queryIdDeque.size() >= Config.max_query_profile_num) {
                    ProfileElement profileElementRemoved = queryIdToProfileMap.remove(queryIdDeque.getFirst());
                    // If the Profile object is removed from manager, then related execution profile is also useless.
                    if (profileElementRemoved != null) {
                        StringBuilder sb = new StringBuilder();
                        for (ExecutionProfile executionProfile : profileElementRemoved.profile.getExecutionProfiles()) {
                            sb.append(executionProfile.getQueryId()).append(",");
                            this.queryIdToExecutionProfiles.remove(executionProfile.getQueryId());
                        }
                        LOG.warn("Remove expired profile {}, execution profiles {},"
                                    + " queryIdDeque size {}, profile count {},"
                                    + " execution profile count {} max_query_profile_num {}",
                                    profileElementRemoved.profile.getSummaryProfile().getProfileId(),
                                    sb.toString(), queryIdDeque.size(), queryIdToProfileMap.size(),
                                    queryIdToExecutionProfiles.size(), Config.max_query_profile_num);
                    }
                    queryIdDeque.removeFirst();
                }
                queryIdDeque.addLast(key);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void removeProfile(String profileId) {
        writeLock.lock();
        try {
            ProfileElement profileElementRemoved = queryIdToProfileMap.remove(profileId);
            // If the Profile object is removed from manager, then related execution profile is also useless.
            if (profileElementRemoved != null) {
                for (ExecutionProfile executionProfile : profileElementRemoved.profile.getExecutionProfiles()) {
                    this.queryIdToExecutionProfiles.remove(executionProfile.getQueryId());
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public List<List<String>> getAllQueries() {
        return getQueryWithType(null);
    }

    public List<List<String>> getQueryWithType(ProfileType type) {
        List<List<String>> result = Lists.newArrayList();
        readLock.lock();
        try {
            Iterator reverse = queryIdDeque.descendingIterator();
            while (reverse.hasNext()) {
                String queryId = (String) reverse.next();
                ProfileElement profileElement = queryIdToProfileMap.get(queryId);
                if (profileElement == null) {
                    continue;
                }
                Map<String, String> infoStrings = profileElement.infoStrings;
                if (type != null && !infoStrings.get(SummaryProfile.TASK_TYPE).equalsIgnoreCase(type.name())) {
                    continue;
                }

                List<String> row = Lists.newArrayList();
                for (String str : SummaryProfile.SUMMARY_KEYS) {
                    row.add(infoStrings.get(str));
                }
                result.add(row);
            }
        } finally {
            readLock.unlock();
        }
        return result;
    }

    private static TGetRealtimeExecStatusResponse getRealtimeQueryProfile(
            TUniqueId queryID, TNetworkAddress targetBackend) {
        TGetRealtimeExecStatusResponse resp = null;
        BackendService.Client client = null;

        try {
            client = ClientPool.backendPool.borrowObject(targetBackend);
        } catch (Exception e) {
            LOG.warn("Fetch a agent client failed, address: {}", targetBackend.toString());
            return resp;
        }

        try {
            TGetRealtimeExecStatusRequest req = new TGetRealtimeExecStatusRequest();
            req.setId(queryID);
            resp = client.getRealtimeExecStatus(req);
        } catch (TException e) {
            LOG.warn("Got exception when getRealtimeExecStatus, query {} backend {}",
                    DebugUtil.printId(queryID), targetBackend.toString(), e);
            ClientPool.backendPool.invalidateObject(targetBackend, client);
        } finally {
            ClientPool.backendPool.returnObject(targetBackend, client);
        }

        if (!resp.isSetStatus()) {
            LOG.warn("Broken GetRealtimeExecStatusResponse response, query {}",
                    DebugUtil.printId(queryID));
            return null;
        }

        if (resp.getStatus().status_code != TStatusCode.OK) {
            LOG.warn("Failed to get realtime query exec status, query {} error msg {}",
                    DebugUtil.printId(queryID), resp.getStatus().toString());
            return null;
        }

        if (!resp.isSetReportExecStatusParams()) {
            LOG.warn("Invalid GetRealtimeExecStatusResponse, query {}",
                    DebugUtil.printId(queryID));
            return null;
        }

        return resp;
    }

    private List<Future<TGetRealtimeExecStatusResponse>> createFetchRealTimeProfileTasks(String id) {
        // For query, id is queryId, for load, id is LoadLoadingTaskId
        class QueryIdAndAddress {
            public TUniqueId id;
            public TNetworkAddress beAddress;
        }

        List<Future<TGetRealtimeExecStatusResponse>> futures = Lists.newArrayList();
        TUniqueId queryId = null;
        try {
            queryId = DebugUtil.parseTUniqueIdFromString(id);
        } catch (NumberFormatException e) {
            LOG.warn("Failed to parse TUniqueId from string {} when fetch profile", id);
        }
        List<QueryIdAndAddress> involvedBackends = Lists.newArrayList();

        if (queryId != null) {
            CoordInterface coor = QeProcessorImpl.INSTANCE.getCoordinator(queryId);

            if (coor != null) {
                for (TNetworkAddress addr : coor.getInvolvedBackends()) {
                    QueryIdAndAddress tmp = new QueryIdAndAddress();
                    tmp.id = queryId;
                    tmp.beAddress = addr;
                    involvedBackends.add(tmp);
                }
            }
        } else {
            Long loadJobId = (long) -1;
            try {
                loadJobId = Long.parseLong(id);
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid profile id: " + id);
            }

            LoadJob loadJob = Env.getCurrentEnv().getLoadManager().getLoadJob(loadJobId);
            if (loadJob == null) {
                throw new RuntimeException("Profile " + id + " not found");
            }

            if (loadJob.getLoadTaskIds() == null) {
                LOG.warn("Load job {} has no task ids", loadJobId);
                return futures;
            }

            for (TUniqueId taskId : loadJob.getLoadTaskIds()) {
                CoordInterface coor = QeProcessorImpl.INSTANCE.getCoordinator(taskId);
                if (coor != null) {
                    if (coor.getInvolvedBackends() != null) {
                        for (TNetworkAddress beAddress : coor.getInvolvedBackends()) {
                            QueryIdAndAddress tmp = new QueryIdAndAddress();
                            tmp.id = taskId;
                            tmp.beAddress = beAddress;
                            involvedBackends.add(tmp);
                        }
                    } else {
                        LOG.warn("Involved backends is null, load job {}, task {}", id, DebugUtil.printId(taskId));
                    }
                }
            }
        }

        for (QueryIdAndAddress idAndAddress : involvedBackends) {
            Callable<TGetRealtimeExecStatusResponse> task = () -> {
                return getRealtimeQueryProfile(idAndAddress.id, idAndAddress.beAddress);
            };
            Future<TGetRealtimeExecStatusResponse> future = fetchRealTimeProfileExecutor.submit(task);
            futures.add(future);
        }

        return futures;
    }

    public String getProfile(String id) {
        List<Future<TGetRealtimeExecStatusResponse>> futures = createFetchRealTimeProfileTasks(id);
        // beAddr of reportExecStatus of QeProcessorImpl is meaningless, so assign a dummy address
        // to avoid compile failing.
        TNetworkAddress dummyAddr = new TNetworkAddress();
        for (Future<TGetRealtimeExecStatusResponse> future : futures) {
            try {
                TGetRealtimeExecStatusResponse resp = future.get(5, TimeUnit.SECONDS);
                if (resp != null) {
                    QeProcessorImpl.INSTANCE.reportExecStatus(resp.getReportExecStatusParams(), dummyAddr);
                }
            } catch (Exception e) {
                LOG.warn("Failed to get real-time profile, id {}, error: {}", id, e.getMessage(), e);
            }
        }

        if (!futures.isEmpty()) {
            LOG.info("Get real-time exec status finished, id {}", id);
        }

        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(id);
            if (element == null) {
                return null;
            }

            return element.getProfileContent();
        } finally {
            readLock.unlock();
        }
    }

    public String getProfileBrief(String queryID) {
        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(queryID);
            if (element == null) {
                return null;
            }
            return element.getProfileBrief();
        } finally {
            readLock.unlock();
        }
    }

    public ProfileElement findProfileElementObject(String queryId) {
        return queryIdToProfileMap.get(queryId);
    }

    /**
     * Check if the query with specific query id is queried by specific user.
     *
     * @param user
     * @param queryId
     * @throws DdlException
     */
    public void checkAuthByUserAndQueryId(String user, String queryId) throws AuthenticationException {
        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(queryId);
            if (element == null) {
                throw new AuthenticationException("query with id " + queryId + " not found");
            }
            if (!element.infoStrings.get(SummaryProfile.USER).equals(user)) {
                throw new AuthenticationException("Access deny to view query with id: " + queryId);
            }
        } finally {
            readLock.unlock();
        }
    }

    public ProfileTreeNode getFragmentProfileTree(String queryID, String executionId) throws AnalysisException {
        MultiProfileTreeBuilder builder;
        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(queryID);
            if (element == null || element.builder == null) {
                throw new AnalysisException("failed to get fragment profile tree. err: "
                        + (element == null ? "not found" : element.errMsg));
            }
            builder = element.builder;
        } finally {
            readLock.unlock();
        }
        return builder.getFragmentTreeRoot(executionId);
    }

    public List<Triple<String, String, Long>> getFragmentInstanceList(String queryID,
            String executionId, String fragmentId)
            throws AnalysisException {
        MultiProfileTreeBuilder builder;
        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(queryID);
            if (element == null || element.builder == null) {
                throw new AnalysisException("failed to get instance list. err: "
                        + (element == null ? "not found" : element.errMsg));
            }
            builder = element.builder;
        } finally {
            readLock.unlock();
        }

        return builder.getInstanceList(executionId, fragmentId);
    }

    public ProfileTreeNode getInstanceProfileTree(String queryID, String executionId,
            String fragmentId, String instanceId)
            throws AnalysisException {
        MultiProfileTreeBuilder builder;
        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(queryID);
            if (element == null || element.builder == null) {
                throw new AnalysisException("failed to get instance profile tree. err: "
                        + (element == null ? "not found" : element.errMsg));
            }
            builder = element.builder;
        } finally {
            readLock.unlock();
        }

        return builder.getInstanceTreeRoot(executionId, fragmentId, instanceId);
    }

    // Return the tasks info of the specified load job
    // Columns: TaskId, ActiveTime
    public List<List<String>> getLoadJobTaskList(String jobId) throws AnalysisException {
        MultiProfileTreeBuilder builder = getMultiProfileTreeBuilder(jobId);
        return builder.getSubTaskInfo();
    }

    public List<ProfileTreeBuilder.FragmentInstances> getFragmentsAndInstances(String queryId)
            throws AnalysisException {
        return getMultiProfileTreeBuilder(queryId).getFragmentInstances(queryId);
    }

    private MultiProfileTreeBuilder getMultiProfileTreeBuilder(String jobId) throws AnalysisException {
        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(jobId);
            if (element == null || element.builder == null) {
                throw new AnalysisException("failed to get task ids. err: "
                        + (element == null ? "not found" : element.errMsg));
            }
            return element.builder;
        } finally {
            readLock.unlock();
        }
    }

    public String getQueryIdByTraceId(String traceId) {
        readLock.lock();
        try {
            for (Map.Entry<String, ProfileElement> entry : queryIdToProfileMap.entrySet()) {
                if (entry.getValue().infoStrings.getOrDefault(SummaryProfile.TRACE_ID, "").equals(traceId)) {
                    return entry.getKey();
                }
            }
            return "";
        } finally {
            readLock.unlock();
        }
    }

    public void setStatsErrorEstimator(String queryId, StatsErrorEstimator statsErrorEstimator) {
        ProfileElement profileElement = findProfileElementObject(queryId);
        if (profileElement != null) {
            profileElement.setStatsErrorEstimator(statsErrorEstimator);
        }
    }

    public void cleanProfile() {
        writeLock.lock();
        try {
            queryIdToProfileMap.clear();
            queryIdDeque.clear();
        } finally {
            writeLock.unlock();
        }
    }
}
