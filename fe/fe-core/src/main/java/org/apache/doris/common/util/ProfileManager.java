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
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
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
public class ProfileManager extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(ProfileManager.class);
    private static volatile ProfileManager INSTANCE = null;
    private static final String PROFILE_STORAGE_PATH = Config.spilled_profile_storage_path;

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

        // Store profile to path
        public void writeToStorage(String profileStoragePath) {
            profile.writeToStorage(profileStoragePath);
        }

        // Remove profile from storage
        public void deleteFromStorage() {
            profile.deleteFromStorage();
        }
    }

    // this variable is assgiened to true the first time the profile is loaded from storage
    // no futher write operaiton, so no data race
    boolean isProfileLoaded = false;

    // only protect queryIdDeque; queryIdToProfileMap is concurrent, no need to protect
    private ReentrantReadWriteLock lock;
    private ReadLock readLock;
    private WriteLock writeLock;

    // profile id is long string for brocker load
    // is TUniqueId for others.
    private Map<String, ProfileElement> queryIdToProfileMap;
    // Sometimes one Profile is related with multiple execution profiles(Brokerload), so that
    // execution profile's query id is not related with Profile's query id.
    private Map<TUniqueId, ExecutionProfile> queryIdToExecutionProfiles;

    private final ExecutorService fetchRealTimeProfileExecutor;
    private final ExecutorService profileIOExecutor;

    public static ProfileManager getInstance() {
        if (INSTANCE == null) {
            synchronized (ProfileManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ProfileManager();
                    INSTANCE.start();
                }
            }
        }
        return INSTANCE;
    }

    // The visiablity of ProfileManager() is package level, so that we can write ut for it.
    ProfileManager() {
        lock = new ReentrantReadWriteLock(true);
        readLock = lock.readLock();
        writeLock = lock.writeLock();
        queryIdToProfileMap = Maps.newHashMap();
        queryIdToExecutionProfiles = Maps.newHashMap();
        fetchRealTimeProfileExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                10, 100, "fetch-realtime-profile-pool", true);
        profileIOExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                20, 100, "profile-io-thread-pool", true);
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
        String key = profile.getSummaryProfile().getProfileId();
        // check when push in, which can ensure every element in the list has QUERY_ID column,
        // so there is no need to check when remove element from list.
        if (Strings.isNullOrEmpty(key)) {
            LOG.warn("the key or value of Map is null, "
                    + "may be forget to insert 'QUERY_ID' or 'JOB_ID' column into infoStrings");
        }

        writeLock.lock();
        try {
            // a profile may be updated multiple times in queryIdToProfileMap,
            // and only needs to be inserted into the queryIdDeque for the first time.
            queryIdToProfileMap.put(key, element);
        } finally {
            writeLock.unlock();
        }
    }

    public List<List<String>> getAllQueries() {
        List<List<String>> result = Lists.newArrayList();
        readLock.lock();
        try {
            PriorityQueue<ProfileElement> queueIdDeque = getProfileOrderByQueryFinishTime();
            while (!queueIdDeque.isEmpty()) {
                ProfileElement profileElement = queueIdDeque.poll();
                Map<String, String> infoStrings = profileElement.infoStrings;
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
            queryIdToExecutionProfiles.clear();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        loadProfilesFromStorageIfFirstTime();
        writeProfileToStorage();
        deleteBrokenProfiles();
        deleteOutdatedProfilesFromStorage();
    }

    // List PROFILE_STORAGE_PATH and return all dir names
    // string will contain profile id and its storage timestamp
    private List<String> getOnStorageProfileInfos() {
        List<String> res = Lists.newArrayList();
        try {
            File profileDir = new File(PROFILE_STORAGE_PATH);
            if (!profileDir.exists()) {
                LOG.warn("Profile storage directory {} does not exist", PROFILE_STORAGE_PATH);
                return res;
            }

            File[] files = profileDir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    res.add(file.getAbsolutePath());
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to get profile meta from storage", e);
        }

        return res;
    }

    // read profile file on storage
    // deserialize to an object Profile
    // push them to memory structure of ProfileManager for index
    private void loadProfilesFromStorageIfFirstTime() {
        if (this.isProfileLoaded) {
            return;
        }

        try {
            LOG.info("Reading profile from {}", PROFILE_STORAGE_PATH);
            List<String> profileDirAbsPaths = getOnStorageProfileInfos();
            // Thread safe list
            List<Profile> profiles = Collections.synchronizedList(new ArrayList<>());
            // List of profile io futures
            List<Future<?>> profileIOfutures = Lists.newArrayList();
            // Creatre and add task to executor
            for (String profileDirAbsPath : profileDirAbsPaths) {
                Thread thread = new Thread(() -> {
                    Profile profile = Profile.read(profileDirAbsPath);
                    if (profile != null) {
                        profiles.add(profile);
                    }
                });
                profileIOfutures.add(profileIOExecutor.submit(thread));
            }

            // Wait for all submitted futures to complete
            for (Future<?> future : profileIOfutures) {
                try {
                    future.get();
                } catch (Exception e) {
                    LOG.warn("Failed to read profile from storage", e);
                }
            }

            LOG.info("There are {} profiles loaded into memory", profiles.size());

            // there may already has some queries running before this thread running
            // so we should not clear current memory structure

            for (Profile profile : profiles) {
                this.pushProfile(profile);
            }

            this.isProfileLoaded = true;
        } catch (Exception e) {
            LOG.error("Failed to load query profile from storage", e);
        }
    }

    private void createProfileStorageDirIfNecessary() {
        File profileDir = new File(PROFILE_STORAGE_PATH);
        if (profileDir.exists()) {
            return;
        }

        // create query_id directory
        if (!profileDir.mkdir()) {
            LOG.warn("create profile directory {} failed", profileDir.getAbsolutePath());
        } else {
            LOG.info("Create profile storage {} succeed", PROFILE_STORAGE_PATH);
        }
    }

    private List<ProfileElement> getProfilesNeedStore() {
        List<ProfileElement> profilesToBeStored = Lists.newArrayList();

        queryIdToProfileMap.forEach((queryId, profileElement) -> {
            if (profileElement.profile.shouldStoreToStorage()) {
                profilesToBeStored.add(profileElement);
            }
        });

        return profilesToBeStored;
    }

    // Collect profiles that need to be stored to storage
    // Store them to storage
    // Release the memory
    private void writeProfileToStorage() {
        try {
            if (Strings.isNullOrEmpty(PROFILE_STORAGE_PATH)) {
                LOG.error("Logical error, PROFILE_STORAGE_PATH is empty");
                return;
            }

            createProfileStorageDirIfNecessary();
            List<ProfileElement> profilesToBeStored = Lists.newArrayList();

            readLock.lock();
            try {
                profilesToBeStored = getProfilesNeedStore();
            } finally {
                readLock.unlock();
            }

            // Store profile to storage in parallel
            List<Future<?>> profileWriteFutures = Lists.newArrayList();

            for (ProfileElement profileElement : profilesToBeStored) {
                Thread thread = new Thread(() -> {
                    profileElement.writeToStorage(PROFILE_STORAGE_PATH);
                });
                profileWriteFutures.add(profileIOExecutor.submit(thread));
            }

            for (Future<?> future : profileWriteFutures) {
                try {
                    future.get();
                } catch (Exception e) {
                    LOG.warn("Failed to write profile to storage", e);
                }
            }

            // After profile is stored to storage, the executoin profile must be ejected from memory
            // or the memory will be exhausted

            writeLock.lock();
            try {
                for (ProfileElement profileElement : profilesToBeStored) {
                    for (ExecutionProfile executionProfile : profileElement.profile.getExecutionProfiles()) {
                        this.queryIdToExecutionProfiles.remove(executionProfile.getQueryId());
                    }
                    profileElement.profile.releaseExecutionProfile();
                }
            } finally {
                writeLock.unlock();
            }
        } catch (Exception e) {
            LOG.error("Failed to remove query profile", e);
        }
    }

    private List<ProfileElement> getProfilesToBeRemoved() {
        // By order of query finish timestamp
        // The profile with the least storage timestamp will be on the top of heap
        PriorityQueue<ProfileElement> profileDeque = new PriorityQueue<>(Comparator.comparingLong(
                (ProfileElement profileElement) -> profileElement.profile.getQueryFinishTimestamp()));

        long totalProfileSize = 0;

        // Collect all profiles that has been stored to storage
        for (ProfileElement profileElement : queryIdToProfileMap.values()) {
            if (profileElement.profile.profileHasBeenStored()) {
                totalProfileSize += profileElement.profile.getProfileSize();
                profileDeque.add(profileElement);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("{} profiles size on storage: {}", profileDeque.size(),
                        DebugUtil.printByteWithUnit(totalProfileSize));
        }

        final int maxSpilledProfileNum = Config.max_spilled_profile_num;
        final long spilledProfileLimitBytes = Config.spilled_profile_storage_limit_bytes;
        List<ProfileElement> queryIdToBeRemoved = Lists.newArrayList();

        while (profileDeque.size() > maxSpilledProfileNum || totalProfileSize >= spilledProfileLimitBytes) {
            // First profile is the oldest profile
            ProfileElement profileElement = profileDeque.poll();
            totalProfileSize -= profileElement.profile.getProfileSize();
            queryIdToBeRemoved.add(profileElement);
        }

        return queryIdToBeRemoved;
    }

    // We can not store all profiles on storage, because the storage space is limited
    // So we need to remove the outdated profiles
    private void deleteOutdatedProfilesFromStorage() {
        try {
            List<ProfileElement> queryIdToBeRemoved = Lists.newArrayList();
            readLock.lock();
            try {
                queryIdToBeRemoved = getProfilesToBeRemoved();
            } finally {
                readLock.unlock();
            }

            List<Thread> iothreads = Lists.newArrayList();

            for (ProfileElement profileElement : queryIdToBeRemoved) {
                Thread thread = new Thread(() -> {
                    profileElement.deleteFromStorage();
                });
                thread.start();
                iothreads.add(thread);
            }

            try {
                for (Thread thread : iothreads) {
                    thread.join();
                }
            } catch (InterruptedException e) {
                LOG.error("Failed to remove outdated query profile", e);
            }

            writeLock.lock();
            try {
                for (ProfileElement profileElement : queryIdToBeRemoved) {
                    queryIdToProfileMap.remove(profileElement.profile.getSummaryProfile().getProfileId());
                    TUniqueId thriftQueryId = DebugUtil.parseTUniqueIdFromString(
                            profileElement.profile.getSummaryProfile().getProfileId());
                    queryIdToExecutionProfiles.remove(thriftQueryId);
                }
            } finally {
                writeLock.unlock();
            }

            if (queryIdToBeRemoved.size() != 0 && LOG.isDebugEnabled()) {
                StringBuilder builder = new StringBuilder();
                for (ProfileElement profileElement : queryIdToBeRemoved) {
                    builder.append(profileElement.profile.getSummaryProfile().getProfileId()).append(",");
                }
                LOG.debug("Remove outdated profile: {}", builder.toString());
            }
        } catch (Exception e) {
            LOG.error("Failed to remove outdated query profile", e);
        }
    }

    private List<String> getBrokenProfiles() {
        List<String> profilesOnStorage = getOnStorageProfileInfos();
        List<String> brokenProfiles = Lists.newArrayList();

        for (String profileDirAbsPath : profilesOnStorage) {
            int separatorIdx = profileDirAbsPath.lastIndexOf(File.separator);
            if (separatorIdx == -1) {
                LOG.warn("Invalid profile path {}", profileDirAbsPath);
                brokenProfiles.add(profileDirAbsPath);
                continue;
            }

            String profileId = "";

            try {
                String timeStampAndId = profileDirAbsPath.substring(separatorIdx + 1);
                String[] parsed = Profile.parseProfileFileName(timeStampAndId);
                if (parsed == null) {
                    LOG.warn("Invalid profile directory path: {}", profileDirAbsPath);
                    brokenProfiles.add(profileDirAbsPath);
                    continue;
                } else {
                    profileId = parsed[1];
                }
            } catch (Exception e) {
                LOG.error("Failed to get profile id from path: {}", profileDirAbsPath, e);
                brokenProfiles.add(profileDirAbsPath);
                continue;
            }

            readLock.lock();
            try {
                if (!queryIdToProfileMap.containsKey(profileId)) {
                    LOG.debug("Wild profile {}, need to be removed.", profileDirAbsPath);
                    brokenProfiles.add(profileDirAbsPath);
                }
            } finally {
                readLock.unlock();
            }
        }

        return brokenProfiles;
    }

    private void deleteBrokenProfiles() {
        List<String> brokenProfiles = getBrokenProfiles();
        List<Future<?>> profileDeleteFutures = Lists.newArrayList();

        for (String brokenProfile : brokenProfiles) {
            Thread iothread = new Thread(() -> {
                try {
                    File profileFile = new File(brokenProfile);
                    if (!profileFile.isFile()) {
                        LOG.warn("Profile path {} is not a file, can not delete.", brokenProfile);
                        return;
                    }

                    FileUtils.deleteQuietly(profileFile);
                    LOG.debug("Delete broken profile: {}", brokenProfile);
                } catch (Exception e) {
                    LOG.error("Failed to delete broken profile: {}", brokenProfile, e);
                }
            });
            profileDeleteFutures.add(profileIOExecutor.submit(iothread));
        }

        for (Future<?> future : profileDeleteFutures) {
            try {
                future.get();
            } catch (Exception e) {
                LOG.error("Failed to remove broken profile", e);
            }
        }
    }

    // The init value of query finish time of profile is MAX_VALUE
    // So more recent query will be on the top of heap.
    private PriorityQueue<ProfileElement> getProfileOrderByQueryFinishTime() {
        PriorityQueue<ProfileElement> queryIdDeque = new PriorityQueue<>(Comparator.comparingLong(
                (ProfileElement profileElement) -> profileElement.profile.getQueryFinishTimestamp()).reversed());

        queryIdToProfileMap.forEach((queryId, profileElement) -> {
            queryIdDeque.add(profileElement);
        });

        return queryIdDeque;
    }

    // When the query is finished, the execution profile should be marked as finished
    // For load task, one of its execution profile is finished.
    public void markExecutionProfileFinished(TUniqueId queryId) {
        readLock.lock();
        try {
            ExecutionProfile execProfile = queryIdToExecutionProfiles.get(queryId);
            if (execProfile == null) {
                LOG.debug("Profile {} does not exist, already finished or does not enable profile",
                        DebugUtil.printId(queryId));
                return;
            }
            execProfile.setQueryFinishTime(System.currentTimeMillis());
        } catch (Exception e) {
            LOG.error("Failed to mark query {} finished", DebugUtil.printId(queryId), e);
        } finally {
            readLock.unlock();
        }
    }

    public String getLastProfileId() {
        PriorityQueue<ProfileElement> queueIdDeque = getProfileOrderByQueryFinishTime();
        ProfileElement profileElement = queueIdDeque.poll();
        return profileElement.profile.getSummaryProfile().getProfileId();
    }
}
