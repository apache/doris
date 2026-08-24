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

package org.apache.doris.common.profile;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.qe.CoordInterface;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TGetRealtimeExecStatusRequest;
import org.apache.doris.thrift.TGetRealtimeExecStatusResponse;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryStatistics;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/*
 * if you want to visit the attribute(such as queryID,defaultDb)
 * you can use profile.getInfoStrings("queryId")
 * All attributes can be seen from the above.
 *
 * why the element in the finished profile array is not RuntimeProfile,
 * the purpose is let coordinator can destruct earlier (the fragment profile is in Coordinator)
 *
 */
public class ProfileManager extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(ProfileManager.class);
    private static volatile ProfileManager INSTANCE = null;
    static String PROFILE_STORAGE_PATH = Config.spilled_profile_storage_path;
    private static final int BATCH_SIZE = 10; // Number of profiles to process in each batch

    // Archive cleanup interval: 24 hours
    private static final long ARCHIVE_CLEANUP_INTERVAL_MS = 6 * 3600 * 1000L;
    private volatile long lastArchiveCleanupTime = 0;

    public enum ProfileType {
        QUERY,
        LOAD,
    }

    public static class ProfileElement {
        public ProfileElement(Profile profile) {
            this.profile = profile;
        }

        final Profile profile;
        public Map<String, String> infoStrings = Maps.newHashMap();
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

    // Lifecycle of the one-time cold load of profiles from storage into memory.
    //   NOT_LOADED -> the load has not completed; also the retryable state after a transient
    //                 failure (a backoff timer, nextLoadRetryTimeMs, gates re-entry).
    //   LOADING    -> a loader thread currently owns the load; CAS into this state is the single
    //                 guard that prevents spawning multiple profile-loader threads.
    //   LOADED     -> the on-disk index is complete and therefore AUTHORITATIVE:
    //                 deleteBrokenProfiles()/deleteOutdatedProfilesFromStorage() are enabled and
    //                 will delete stored profiles missing from the in-memory index.
    //   FAILED     -> terminal: the load failed MAX_LOAD_RETRY times in a row and is not retried
    //                 until the FE restarts. This is deliberately distinct from LOADED so a
    //                 partial/failed index NEVER enables the destructive cleanup above.
    private enum LoadState { NOT_LOADED, LOADING, LOADED, FAILED }

    private final AtomicReference<LoadState> loadState = new AtomicReference<>(LoadState.NOT_LOADED);
    // A cold load can fail transiently -- e.g. the bounded profileIOExecutor rejects a submit under
    // saturation (BlockedPolicy -> RejectedExecutionException), which is exactly the pressure the
    // LOADING guard targets. We must not respawn a loader every scheduler tick on failure, but we
    // also must not give up permanently on the first failure: a permanent latch would keep the
    // state out of LOADED for the life of the FE, disabling profile disk cleanup while
    // writeProfileToStorage() keeps writing -- trading a thread leak for an unbounded disk leak.
    // Instead we back off exponentially between retries and only move to FAILED after
    // MAX_LOAD_RETRY consecutive failures, so a transient failure self-heals.
    private final AtomicInteger consecutiveLoadFailures = new AtomicInteger(0);
    private final AtomicLong nextLoadRetryTimeMs = new AtomicLong(0);
    private static final int MAX_LOAD_RETRY = 10;
    private static final long MAX_LOAD_BACKOFF_MS = 300_000L;
    // Upper bound for a synchronous caller blocking on an in-flight load. A stuck loader (submit()
    // blocked up to 60s under BlockedPolicy, or an unbounded future.get()) must not hang the caller
    // forever -- exactly the scenario this change targets.
    private static final long PROFILE_LOAD_WAIT_TIMEOUT_MS = 120_000L;
    // While in the terminal FAILED state, re-emit the "cleanup disabled" warning at most this often
    // so an operator who missed the single failure log can still discover the condition.
    private static final long FAILED_STATE_REWARN_INTERVAL_MS = 600_000L;
    private final AtomicLong nextFailedRewarnTimeMs = new AtomicLong(0);

    // only protect queryIdDeque; queryIdToProfileMap is concurrent, no need to protect
    private ReentrantReadWriteLock lock;
    private ReadLock readLock;
    private WriteLock writeLock;

    // profile id is long string for broker load
    // is TUniqueId for others.
    final Map<String, ProfileElement> queryIdToProfileMap;
    // Sometimes one Profile is related with multiple execution profiles(Broker-load), so that
    // execution profile's query id is not related with Profile's query id.
    final Map<TUniqueId, ExecutionProfile> queryIdToExecutionProfiles;

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

    protected ProfileManager() {
        super("profile-manager", Config.profile_manager_gc_interval_seconds * 1000);
        lock = new ReentrantReadWriteLock(true);
        readLock = lock.readLock();
        writeLock = lock.writeLock();
        queryIdToProfileMap = Maps.newHashMap();
        queryIdToExecutionProfiles = Maps.newHashMap();
        fetchRealTimeProfileExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                10, 100, "fetch-realtime-profile-pool", true);

        int iothreads = Math.max(20, Runtime.getRuntime().availableProcessors());
        profileIOExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
            iothreads, 100, "profile-io-thread-pool", true);
    }

    // Shut down the thread pools this instance owns. The production singleton lives for the FE's
    // lifetime and never needs this, but tests that construct throwaway ProfileManager instances
    // must release the pools; otherwise each instance leaks ~30 live threads and its pool names
    // collide in ThreadPoolManager's global name map.
    @VisibleForTesting
    void shutdown() {
        fetchRealTimeProfileExecutor.shutdownNow();
        profileIOExecutor.shutdownNow();
    }

    private ProfileElement createElement(Profile profile) {
        ProfileElement element = new ProfileElement(profile);
        element.infoStrings.putAll(profile.getSummaryProfile().getAsInfoStings());
        // Not init builder anymore, we will not maintain it since 2.1.0, because the structure
        // assume that the execution profiles structure is already known before execution. But in
        // PipelineX Engine, it will be changed during execution.
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
        } finally {
            writeLock.unlock();
        }
    }

    public ExecutionProfile getExecutionProfile(TUniqueId queryId) {
        readLock.lock();
        try {
            return this.queryIdToExecutionProfiles.get(queryId);
        } finally {
            readLock.unlock();
        }
    }

    public void pushProfile(Profile profile) {
        if (profile == null) {
            return;
        }

        writeLock.lock();
        try {
            if (!queryIdToProfileMap.containsKey(profile.getId())) {
                deleteOutdatedProfilesFromMemory(1);
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

            // a profile may be updated multiple times in queryIdToProfileMap,
            // and only needs to be inserted into the queryIdDeque for the first time.
            queryIdToProfileMap.put(key, element);
        } finally {
            writeLock.unlock();
        }
    }

    public List<List<String>> getAllQueries() {
        return getQueryInfoByColumnNameList(SummaryProfile.SUMMARY_KEYS);
    }

    private String getProfileInfoString(ProfileElement profileElement, String columnName) {
        if (SummaryProfile.PROFILE_COMPLETION_STATE.equals(columnName)) {
            return profileElement.profile.getProfileCompletionState();
        }
        return profileElement.infoStrings.get(columnName);
    }

    public List<List<String>> getQueryInfoByColumnNameList(List<String> columnNameList) {
        List<List<String>> result = Lists.newArrayList();
        readLock.lock();
        try {
            PriorityQueue<ProfileElement> queueIdDeque = getProfileOrderByQueryFinishTimeDesc();
            while (!queueIdDeque.isEmpty()) {
                ProfileElement profileElement = queueIdDeque.poll();
                List<String> row = Lists.newArrayList();
                for (String str : columnNameList) {
                    row.add(getProfileInfoString(profileElement, str));
                }
                result.add(row);
            }
        } finally {
            readLock.unlock();
        }
        return result;
    }

    private static TGetRealtimeExecStatusResponse getRealtimeQueryProfile(
            TUniqueId queryID, String reqType, TNetworkAddress targetBackend) {
        TGetRealtimeExecStatusResponse resp = null;
        BackendService.Client client = null;

        try {
            client = ClientPool.backendPool.borrowObject(targetBackend);
        } catch (Exception e) {
            LOG.warn("Fetch a agent client failed, address: {}", targetBackend.toString());
            ClientPool.backendPool.invalidateObject(targetBackend, client);
            return resp;
        }
        boolean ok = true;
        try {
            TGetRealtimeExecStatusRequest req = new TGetRealtimeExecStatusRequest();
            req.setId(queryID);
            req.setReqType(reqType);
            resp = client.getRealtimeExecStatus(req);
        } catch (TException e) {
            LOG.warn("Got exception when getRealtimeExecStatus, query {} backend {}",
                    DebugUtil.printId(queryID), targetBackend.toString(), e);
            ok = false;
        } finally {
            if (ok) {
                ClientPool.backendPool.returnObject(targetBackend, client);
            } else {
                ClientPool.backendPool.invalidateObject(targetBackend, client);
            }
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

        if (!resp.isSetReportExecStatusParams() && !resp.isSetQueryStats()) {
            LOG.warn("Invalid GetRealtimeExecStatusResponse, missing both exec status and query stats. query {}",
                    DebugUtil.printId(queryID));
            return null;
        }

        return resp;
    }

    private List<Future<TGetRealtimeExecStatusResponse>> createFetchRealTimeProfileTasks(String id, String reqType) {
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
            CoordInterface coord = QeProcessorImpl.INSTANCE.getCoordinator(queryId);
            if (coord != null) {
                for (TNetworkAddress addr : coord.getInvolvedBackends()) {
                    QueryIdAndAddress tmp = new QueryIdAndAddress();
                    tmp.id = queryId;
                    tmp.beAddress = addr;
                    involvedBackends.add(tmp);
                }
            } else {
                LOG.warn("Coordinator is null, query id {}", id);
                return futures;
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
                CoordInterface coord = QeProcessorImpl.INSTANCE.getCoordinator(taskId);
                if (coord != null) {
                    if (coord.getInvolvedBackends() != null) {
                        for (TNetworkAddress beAddress : coord.getInvolvedBackends()) {
                            QueryIdAndAddress tmp = new QueryIdAndAddress();
                            tmp.id = taskId;
                            tmp.beAddress = beAddress;
                            involvedBackends.add(tmp);
                        }
                    } else {
                        LOG.warn("Involved backends is null, load job {}, task {}", id, DebugUtil.printId(taskId));
                    }
                } else {
                    LOG.warn("Coordinator is null, load job {}, task {}", id, DebugUtil.printId(taskId));
                }
            }
        }

        for (QueryIdAndAddress idAndAddress : involvedBackends) {
            Callable<TGetRealtimeExecStatusResponse> task = () -> getRealtimeQueryProfile(idAndAddress.id,
                    reqType, idAndAddress.beAddress);
            Future<TGetRealtimeExecStatusResponse> future = fetchRealTimeProfileExecutor.submit(task);
            futures.add(future);
        }
        if (futures.isEmpty()) {
            LOG.warn("No involved backend found for query id {}", id);
        }

        return futures;
    }

    public TQueryStatistics getQueryStatistic(String queryId) throws Exception {
        List<Future<TGetRealtimeExecStatusResponse>> futures = createFetchRealTimeProfileTasks(queryId,
                "stats");
        List<TQueryStatistics> queryStatisticsList = Lists.newArrayList();
        for (Future<TGetRealtimeExecStatusResponse> future : futures) {
            try {
                TGetRealtimeExecStatusResponse resp = future.get(5, TimeUnit.SECONDS);
                if (resp != null && resp.getStatus().status_code == TStatusCode.OK && resp.isSetQueryStats()) {
                    queryStatisticsList.add(resp.getQueryStats());
                } else {
                    LOG.warn("Failed to get real-time query stats, id {}, resp is {}",
                            queryId, resp == null ? "null" : resp.toString());
                    throw new Exception("Failed to get realtime query stats: "
                            + (resp == null ? "null" : resp.toString()));
                }
            } catch (Exception e) {
                LOG.warn("Failed to get real-time query stats, id {}, error: {}", queryId, e.getMessage(), e);
                throw new Exception("Failed to get realtime query stats: " + e.getMessage());
            }
        }
        Preconditions.checkState(queryStatisticsList.size() == futures.size(),
                String.format("Failed to get real-time stats, id %s, "
                                + "queryStatisticsList size %d != futures size %d",
                        queryId, queryStatisticsList.size(), futures.size()));

        TQueryStatistics summary = new TQueryStatistics();
        for (TQueryStatistics queryStats : queryStatisticsList) {
            // sum all the statistics
            summary.setScanRows(summary.getScanRows() + queryStats.getScanRows());
            summary.setScanBytes(summary.getScanBytes() + queryStats.getScanBytes());
            summary.setReturnedRows(summary.getReturnedRows() + queryStats.getReturnedRows());
            summary.setCpuMs(summary.getCpuMs() + queryStats.getCpuMs());
            summary.setMaxPeakMemoryBytes(Math.max(summary.getMaxPeakMemoryBytes(),
                    queryStats.getMaxPeakMemoryBytes()));
            summary.setCurrentUsedMemoryBytes(Math.max(summary.getCurrentUsedMemoryBytes(),
                    queryStats.getCurrentUsedMemoryBytes()));
            summary.setShuffleSendBytes(summary.getShuffleSendBytes() + queryStats.getShuffleSendBytes());
            summary.setShuffleSendRows(summary.getShuffleSendRows() + queryStats.getShuffleSendRows());
            summary.setScanBytesFromLocalStorage(
                    summary.getScanBytesFromLocalStorage() + queryStats.getScanBytesFromLocalStorage());
            summary.setScanBytesFromRemoteStorage(
                    summary.getScanBytesFromRemoteStorage() + queryStats.getScanBytesFromRemoteStorage());
            summary.setSpillWriteBytesToLocalStorage(
                    summary.getSpillWriteBytesToLocalStorage() + queryStats.getSpillWriteBytesToLocalStorage());
            summary.setSpillReadBytesFromLocalStorage(
                    summary.getSpillReadBytesFromLocalStorage() + queryStats.getSpillReadBytesFromLocalStorage());
        }
        return summary;
    }

    public String getProfile(String id) {
        List<Future<TGetRealtimeExecStatusResponse>> futures = createFetchRealTimeProfileTasks(id, "profile");
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
        loadProfilesFromStorageIfFirstTime(false);
        warnIfLoadTerminallyFailed();
        writeProfileToStorage();
        deleteBrokenProfiles();
        deleteOutdatedProfilesFromStorage();
        preventExecutionProfileLeakage();

        // Archive-related periodic tasks
        if (Config.enable_profile_archive) {
            // Task 1: Periodically check pending directory
            checkAndArchivePendingProfilesPeriodically();

            // Task 2: Clean old archives
            long currentTime = System.currentTimeMillis();
            long duration = currentTime - lastArchiveCleanupTime;
            if (duration >= ARCHIVE_CLEANUP_INTERVAL_MS
                    || (Config.profile_archive_retention_seconds > 0
                            && duration >= Config.profile_archive_retention_seconds * 1000 / 2)) {
                cleanOldArchivedProfiles();
                lastArchiveCleanupTime = currentTime;
            }
        }
    }

    // List PROFILE_STORAGE_PATH and return all dir names
    // string will contain profile id and its storage timestamp
    protected List<String> getOnStorageProfileInfos() {
        List<String> res = Lists.newArrayList();
        try {
            File profileDir = new File(PROFILE_STORAGE_PATH);
            if (!profileDir.exists()) {
                LOG.warn("Profile storage directory {} does not exist", PROFILE_STORAGE_PATH);
                return res;
            }

            File[] files = profileDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        res.add(file.getAbsolutePath());
                    }
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
    protected void loadProfilesFromStorageIfFirstTime(boolean sync) {
        // A single CAS NOT_LOADED -> LOADING both wins loader ownership and rules out a redundant
        // second cold load: once a loader is LOADING (or the state is LOADED/FAILED), no other
        // caller can win the CAS, so the "recheck after acquiring ownership" dance is unnecessary.
        if (!tryBeginLoad()) {
            if (sync && loadState.get() == LoadState.LOADING) {
                waitForProfileLoadFinish(PROFILE_LOAD_WAIT_TIMEOUT_MS);
            }
            return;
        }

        Runnable loadTask = () -> {
            long startTime = System.currentTimeMillis();
            int readFailures = -1;
            try {
                readFailures = loadProfilesFromStorage();
                LOG.info("Load profiles into memory finished, readFailures={}, costMs={}",
                        readFailures, System.currentTimeMillis() - startTime);
            } catch (Exception e) {
                LOG.error("Failed to load query profile from storage", e);
            } finally {
                // A load only counts as successful when every stored profile was read without error
                // (readFailures == 0). A partial read must NOT mark the disk index authoritative,
                // because downstream cleanup deletes stored profiles missing from the in-memory
                // index; treat it as a failure so cleanup stays disabled until a clean load succeeds.
                if (readFailures == 0) {
                    finishLoadSuccess();
                } else {
                    finishLoadFailure();
                }
            }
        };

        if (sync) {
            loadTask.run();
        } else {
            Thread loadThread = new Thread(loadTask, "profile-loader");
            loadThread.setDaemon(true);
            try {
                loadThread.start();
            } catch (Throwable t) {
                // Native-thread allocation / start() can fail under the resource pressure this
                // guard targets. loadTask never runs, so release ownership and record the failure
                // here; otherwise the state stays LOADING forever and later cycles skip the load.
                finishLoadFailure();
                LOG.warn("Failed to start profile-loader thread, will retry after backoff", t);
            }
        }
    }

    // Attempt to move NOT_LOADED -> LOADING. Returns true only if this caller now owns the load.
    // Suppresses the attempt while already loaded, terminally failed, in-flight, or still inside the
    // exponential-backoff window after a transient failure.
    private boolean tryBeginLoad() {
        LoadState state = loadState.get();
        if (state != LoadState.NOT_LOADED) {
            return false;
        }
        if (System.currentTimeMillis() < nextLoadRetryTimeMs.get()) {
            return false;
        }
        return loadState.compareAndSet(LoadState.NOT_LOADED, LoadState.LOADING);
    }

    private void finishLoadSuccess() {
        consecutiveLoadFailures.set(0);
        nextLoadRetryTimeMs.set(0);
        // Only a fully indexed load may mark the disk index authoritative; downstream cleanup
        // deletes stored profiles missing from the in-memory index.
        loadState.set(LoadState.LOADED);
    }

    // Record a failed/partial load attempt and schedule the next retry with exponential backoff
    // (1s, 2s, 4s ... capped at MAX_LOAD_BACKOFF_MS). After MAX_LOAD_RETRY consecutive failures we
    // transition to the terminal FAILED state and stop retrying until the FE restarts. In either
    // case the state leaves LOADING but does NOT become LOADED, so destructive cleanup stays off.
    private void finishLoadFailure() {
        int failures = consecutiveLoadFailures.incrementAndGet();
        long backoffMs = Math.min(1000L << Math.min(failures, 8), MAX_LOAD_BACKOFF_MS);
        nextLoadRetryTimeMs.set(System.currentTimeMillis() + backoffMs);
        if (failures >= MAX_LOAD_RETRY) {
            loadState.set(LoadState.FAILED);
            nextFailedRewarnTimeMs.set(System.currentTimeMillis() + FAILED_STATE_REWARN_INTERVAL_MS);
            LOG.warn("Profile cold load failed {} times, giving up until FE restarts; "
                    + "profile disk cleanup stays disabled, loadedProfileCount={}",
                    failures, queryIdToProfileMap.size());
        } else {
            loadState.set(LoadState.NOT_LOADED);
            LOG.warn("Profile cold load failed, attempt={}, nextRetryInMs={}, loadedProfileCount={}",
                    failures, backoffMs, queryIdToProfileMap.size());
        }
    }

    // Re-emit the "profile disk cleanup is disabled" warning periodically while in the terminal
    // FAILED state, so an operator who missed the one-time failure log can still discover it.
    private void warnIfLoadTerminallyFailed() {
        if (loadState.get() != LoadState.FAILED) {
            return;
        }
        long now = System.currentTimeMillis();
        long due = nextFailedRewarnTimeMs.get();
        if (now >= due && nextFailedRewarnTimeMs.compareAndSet(due, now + FAILED_STATE_REWARN_INTERVAL_MS)) {
            LOG.warn("Profile cold load has terminally failed; profile disk cleanup and quota "
                    + "enforcement (max_spilled_profile_num / spilled_profile_storage_limit_bytes) "
                    + "remain DISABLED until the FE restarts. loadedProfileCount={}",
                    queryIdToProfileMap.size());
        }
    }

    @VisibleForTesting
    String getProfileLoadState() {
        return loadState.get().name();
    }

    @VisibleForTesting
    boolean isProfileLoadedForTest() {
        return loadState.get() == LoadState.LOADED;
    }

    // Reset to the initial pristine state so a test can re-drive a cold load. Also clears the
    // backoff bookkeeping so a prior failed-load test does not suppress the next attempt.
    @VisibleForTesting
    void resetLoadStateForTest() {
        loadState.set(LoadState.NOT_LOADED);
        consecutiveLoadFailures.set(0);
        nextLoadRetryTimeMs.set(0);
        nextFailedRewarnTimeMs.set(0);
    }

    @VisibleForTesting
    void forceLoadedForTest() {
        loadState.set(LoadState.LOADED);
    }

    // Clear only the backoff timer so a test can drive the next retry immediately, without resetting
    // the consecutive-failure count or the load state -- lets a test exercise the retry/terminate
    // path deterministically instead of sleeping through real backoff windows.
    @VisibleForTesting
    void resetBackoffWindowForTest() {
        nextLoadRetryTimeMs.set(0);
    }

    // Returns the number of profiles that failed to read (0 means a fully indexed, clean load).
    private int loadProfilesFromStorage() throws Exception {
        List<String> profileDirAbsPaths = getOnStorageProfileInfos();
        LOG.info("Reading {} profiles from {}", profileDirAbsPaths.size(), PROFILE_STORAGE_PATH);
        // Newest profile first
        profileDirAbsPaths.sort(Collections.reverseOrder());

        int readFailures = 0;
        // Process profiles in batches
        for (int i = 0; i < profileDirAbsPaths.size(); i += BATCH_SIZE) {
            // Thread safe list
            List<Profile> profiles = Collections.synchronizedList(new ArrayList<>());
            int end = Math.min(i + BATCH_SIZE, profileDirAbsPaths.size());
            List<String> batch = profileDirAbsPaths.subList(i, end);

            // List of profile io futures for current batch
            List<Future<?>> profileIOFutures = Lists.newArrayList();

            // Create and add tasks for current batch to executor
            for (String profileDirAbsPath : batch) {
                profileIOFutures.add(profileIOExecutor.submit(() -> {
                    // NOTE: Profile.read() returns null for BOTH a genuinely malformed file (safe to
                    // treat as absent) and a transient IO error (must not be treated as absent). It
                    // swallows every exception internally, so a transient read failure is invisible
                    // here and does not increment readFailures below. This gap means the state can
                    // still become LOADED on a load that silently skipped a valid-but-unreadable
                    // file; distinguishing the two requires Profile.read() to throw on IO-class
                    // errors, which is out of scope for this change (tracked as a follow-up).
                    Profile profile = Profile.read(profileDirAbsPath);
                    if (profile != null) {
                        profiles.add(profile);
                    }
                }));
            }

            // Wait for all futures in current batch to complete
            for (Future<?> future : profileIOFutures) {
                try {
                    future.get();
                } catch (Exception e) {
                    readFailures++;
                    LOG.warn("Failed to read profile from storage", e);
                }
            }

            for (Profile profile : profiles) {
                pushProfile(profile);
            }

            LOG.debug("Processed batch {} - {} of {} profiles", i, end, profileDirAbsPaths.size());
        }
        return readFailures;
    }

    private void waitForProfileLoadFinish(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (loadState.get() == LoadState.LOADING) {
            if (System.currentTimeMillis() >= deadline) {
                LOG.warn("Timed out waiting for in-flight profile load to finish, timeoutMs={}", timeoutMs);
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while waiting for profile loading to finish", e);
                return;
            }
        }
    }

    protected void createProfileStorageDirIfNecessary() {
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

    protected List<ProfileElement> getProfilesNeedStore() {
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
    protected void writeProfileToStorage() {
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
                profileWriteFutures.add(profileIOExecutor.submit(
                        () -> profileElement.writeToStorage(PROFILE_STORAGE_PATH)));
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
                    profileElement.profile.releaseMemory();
                }
            } finally {
                writeLock.unlock();
            }
        } catch (Exception e) {
            LOG.error("Failed to remove query profile", e);
        }
    }

    protected List<ProfileElement> getProfilesToBeRemoved() {
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
    protected void deleteOutdatedProfilesFromStorage() {
        if (!checkIfProfileLoaded()) {
            return;
        }

        try {
            List<ProfileElement> queryIdToBeRemoved = Lists.newArrayList();
            readLock.lock();
            try {
                queryIdToBeRemoved = getProfilesToBeRemoved();
            } finally {
                readLock.unlock();
            }

            if (queryIdToBeRemoved.isEmpty()) {
                return;
            }

            // Archive or delete profiles based on configuration
            if (Config.enable_profile_archive) {
                // Move profiles to pending directory for archiving
                moveProfilesToArchivePending(queryIdToBeRemoved);
            } else {
                // Directly delete profiles if archiving is disabled
                deleteProfilesFromStorage(queryIdToBeRemoved);
            }

            // Remove profile references from memory
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

    protected List<String> getBrokenProfiles() {
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

    protected void deleteBrokenProfiles() {
        if (!checkIfProfileLoaded()) {
            return;
        }

        List<String> brokenProfiles = getBrokenProfiles();
        List<Future<?>> profileDeleteFutures = Lists.newArrayList();

        for (String brokenProfile : brokenProfiles) {
            profileDeleteFutures.add(profileIOExecutor.submit(() -> {
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
            }));
        }

        for (Future<?> future : profileDeleteFutures) {
            try {
                future.get();
            } catch (Exception e) {
                LOG.error("Failed to remove broken profile", e);
            }
        }
    }

    // The init value of query finish time of profile is MAX_VALUE,
    // So a more recent query will be on the top of the heap.
    protected PriorityQueue<ProfileElement> getProfileOrderByQueryFinishTimeDesc() {
        readLock.lock();
        try {
            PriorityQueue<ProfileElement> queryIdDeque = new PriorityQueue<>(Comparator.comparingLong(
                    (ProfileElement profileElement) -> profileElement.profile.getQueryFinishTimestamp()).reversed());

            queryIdToProfileMap.forEach((queryId, profileElement) -> {
                queryIdDeque.add(profileElement);
            });

            return queryIdDeque;
        } finally {
            readLock.unlock();
        }
    }

    // The init value of query finish time of profile is MAX_VALUE
    // So query finished earlier will be on the top of heap
    protected PriorityQueue<ProfileElement> getProfileOrderByQueryFinishTime() {
        readLock.lock();
        try {
            PriorityQueue<ProfileElement> queryIdDeque = new PriorityQueue<>(Comparator.comparingLong(
                    (ProfileElement profileElement) -> profileElement.profile.getQueryFinishTimestamp()));

            queryIdToProfileMap.forEach((queryId, profileElement) -> {
                queryIdDeque.add(profileElement);
            });

            return queryIdDeque;
        } finally {
            readLock.unlock();
        }
    }

    // Older query will be on the top of heap
    protected PriorityQueue<ProfileElement> getProfileOrderByQueryStartTime() {
        readLock.lock();
        try {
            PriorityQueue<ProfileElement> queryIdDeque = new PriorityQueue<>(Comparator.comparingLong(
                    (ProfileElement profileElement) -> profileElement.profile.getSummaryProfile().getQueryBeginTime()));

            queryIdToProfileMap.forEach((queryId, profileElement) -> {
                queryIdDeque.add(profileElement);
            });

            return queryIdDeque;
        } finally {
            readLock.unlock();
        }
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
        PriorityQueue<ProfileElement> queueIdDeque = getProfileOrderByQueryFinishTimeDesc();
        ProfileElement profileElement = queueIdDeque.poll();
        return profileElement.profile.getSummaryProfile().getProfileId();
    }

    private void preventExecutionProfileLeakage() {
        StringBuilder stringBuilder = new StringBuilder();
        int executionProfileNum = 0;
        writeLock.lock();
        try {
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

                for (ExecutionProfile tmp : finishOrExpireExecutionProfiles) {
                    stringBuilder.append(DebugUtil.printId(tmp.getQueryId())).append(",");
                    queryIdToExecutionProfiles.remove(tmp.getQueryId());
                }

                executionProfileNum = queryIdToExecutionProfiles.size();
            }
        } finally {
            writeLock.unlock();
            if (stringBuilder.length() != 0) {
                LOG.warn("Remove expired execution profiles {}, current execution profile map size {},"
                        + "Config.max_query_profile_num {}, Config.profile_async_collect_expire_time_secs {}",
                        stringBuilder.toString(), executionProfileNum,
                        Config.max_query_profile_num, Config.profile_async_collect_expire_time_secs);
            }
        }
    }

    protected void deleteOutdatedProfilesFromMemory(int numOfNewProfiles) {
        StringBuilder stringBuilder = new StringBuilder();
        writeLock.lock();

        try {
            if (this.queryIdToProfileMap.size() + numOfNewProfiles <= Config.max_query_profile_num) {
                return;
            }

            // profile is ordered by query finish time
            // query finished earlier will be on the top of heap
            // query finished time of unfinished query is INT_MAX, so they will be on the bottom of the heap.
            PriorityQueue<ProfileElement> queueIdDeque = getProfileOrderByQueryFinishTime();

            while (queueIdDeque.size() + numOfNewProfiles > Config.max_query_profile_num && !queueIdDeque.isEmpty()) {
                ProfileElement profileElement = queueIdDeque.poll();
                String profileId = profileElement.profile.getSummaryProfile().getProfileId();
                stringBuilder.append(profileId).append(",");
                queryIdToProfileMap.remove(profileId);
                for (ExecutionProfile executionProfile : profileElement.profile.getExecutionProfiles()) {
                    queryIdToExecutionProfiles.remove(executionProfile.getQueryId());
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Remove profile {} since ttl from memory, info {}", profileId,
                                        profileElement.profile.debugInfo());
                }
            }
        } finally {
            int profileNum = queryIdToProfileMap.size();
            writeLock.unlock();

            if (stringBuilder.length() != 0) {
                LOG.info("Outdated profiles {}, they are removed from memory, current profile map size {}",
                        stringBuilder.toString(), profileNum);
            }
        }
    }

    protected String getDebugInfo() {
        StringBuilder stringBuilder = new StringBuilder();
        readLock.lock();
        try {
            for (ProfileElement profileElement : queryIdToProfileMap.values()) {
                stringBuilder.append(profileElement.profile.debugInfo()).append("\n");
            }
        } finally {
            readLock.unlock();
        }
        return stringBuilder.toString();
    }

    public List<List<String>> getProfileMetaWithType(ProfileType profileType, long limit) {
        List<List<String>> result = Lists.newArrayList();
        readLock.lock();

        try {
            PriorityQueue<ProfileElement> queueIdDeque = getProfileOrderByQueryFinishTimeDesc();
            while (!queueIdDeque.isEmpty() && limit > 0) {
                ProfileElement profileElement = queueIdDeque.poll();
                Map<String, String> infoStrings = profileElement.infoStrings;
                if (infoStrings.get(SummaryProfile.TASK_TYPE).equals(profileType.toString())) {
                    List<String> row = Lists.newArrayList();
                    for (String str : SummaryProfile.SUMMARY_KEYS) {
                        row.add(getProfileInfoString(profileElement, str));
                    }
                    result.add(row);
                    limit--;
                }
            }
        } finally {
            readLock.unlock();
        }

        return result;
    }

    private boolean checkIfProfileLoaded() {
        return loadState.get() == LoadState.LOADED;
    }

    public void removeProfile(String profileId) {
        writeLock.lock();
        try {
            ProfileElement profileToRemove = this.queryIdToProfileMap.remove(profileId);
            if (profileToRemove != null) {
                for (ExecutionProfile executionProfile : profileToRemove.profile.getExecutionProfiles()) {
                    queryIdToExecutionProfiles.remove(executionProfile.getQueryId());
                }
            }
        } finally {
            writeLock.unlock();
        }
    }


    /**
     * Moves profiles to the archive pending directory.
     * Files in pending will be archived when batch size is reached or timeout occurs.
     *
     * @param profileElements list of profile elements to move to pending
     */
    private void moveProfilesToArchivePending(List<ProfileElement> profileElements) {
        try {
            ProfileArchiveManager archiveManager = new ProfileArchiveManager(
                    PROFILE_STORAGE_PATH, Config.profile_archive_batch_size);

            int movedCount = 0;
            for (ProfileElement element : profileElements) {
                String profilePath = element.profile.getProfileStoragePath();
                if (profilePath != null) {
                    File profileFile = new File(profilePath);
                    if (profileFile.exists()) {
                        if (archiveManager.moveToArchivePending(profileFile)) {
                            movedCount++;
                        } else {
                            // If move fails, fall back to direct deletion
                            LOG.warn("Failed to move profile to pending, deleting: {}", profilePath);
                            element.deleteFromStorage();
                        }
                    }
                }
            }

            LOG.info("Moved {} profiles to archive pending", movedCount);

            // Immediately check if archiving should be triggered (e.g., batch size reached)
            int archived = archiveManager.checkAndArchivePendingProfiles();
            if (archived > 0) {
                LOG.info("Immediately archived {} profiles from pending", archived);
            }

        } catch (Exception e) {
            LOG.error("Failed to move profiles to pending, falling back to direct deletion", e);
            // Fall back to direct deletion if archiving fails
            deleteProfilesFromStorage(profileElements);
        }
    }

    /**
     * Directly deletes profiles from storage (used when archiving is disabled).
     *
     * @param profileElements list of profile elements to delete
     */
    private void deleteProfilesFromStorage(List<ProfileElement> profileElements) {
        List<Thread> iothreads = Lists.newArrayList();

        for (ProfileElement profileElement : profileElements) {
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
            LOG.error("Failed to delete profiles from storage", e);
        }
    }

    /**
     * Periodically checks the pending directory and archives profiles if conditions are met.
     * This is a fast operation that runs every time runAfterCatalogReady() is called.
     */
    private void checkAndArchivePendingProfilesPeriodically() {
        try {
            ProfileArchiveManager archiveManager = new ProfileArchiveManager(
                    PROFILE_STORAGE_PATH, Config.profile_archive_batch_size);

            int archived = archiveManager.checkAndArchivePendingProfiles();
            if (archived > 0) {
                LOG.info("Periodically archived {} profiles from pending", archived);
            }
        } catch (Exception e) {
            LOG.error("Failed to check and archive pending profiles", e);
        }
    }

    /**
     * Cleans up old archived profiles that exceed the retention period.
     * This is a slow operation that runs once per day.
     */
    private void cleanOldArchivedProfiles() {
        try {
            ProfileArchiveManager archiveManager = new ProfileArchiveManager(
                    PROFILE_STORAGE_PATH, Config.profile_archive_batch_size);

            int deleted = archiveManager.cleanOldArchives();
            if (deleted > 0) {
                LOG.info("Cleaned {} old archived profiles", deleted);
            }
        } catch (Exception e) {
            LOG.error("Failed to clean old archived profiles", e);
        }
    }
}
