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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.profile.MultiProfileTreeBuilder;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.profile.ProfileTreeBuilder;
import org.apache.doris.common.profile.ProfileTreeNode;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.nereids.stats.StatsErrorEstimator;
import org.apache.doris.qe.CoordInterface;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
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
    private static final String PROFILE_STORAGE_PATH = Config.audit_log_dir + File.separator + "profile";

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
        public void store(String profileStoragePath) {
            profile.store(profileStoragePath);
        }

        // Remove profile from disk
        public void remove() {
            profile.remove();
        }
    }

    // only protect queryIdDeque; queryIdToProfileMap is concurrent, no need to protect
    private ReentrantReadWriteLock lock;
    private ReadLock readLock;
    private WriteLock writeLock;

    // this variable is assgiened to true the first time the profile is loaded from disk
    // no futher write operaiton, so no data race
    boolean isProfileLoaded = false;
    private Map<String, ProfileElement> queryIdToProfileMap; // from QueryId to RuntimeProfile
    // Sometimes one Profile is related with multiple execution profiles(Brokerload), so that
    // execution profile's query id is not related with Profile's query id.
    private Map<TUniqueId, ExecutionProfile> queryIdToExecutionProfiles;

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

    private ProfileManager() {
        lock = new ReentrantReadWriteLock(true);
        readLock = lock.readLock();
        writeLock = lock.writeLock();
        queryIdToProfileMap = Maps.newHashMap();
        queryIdToExecutionProfiles = Maps.newHashMap();
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
        writeLock.unlock();
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

    // The init value of query finish time of profile is MIN_VALUE
    // So more recent query will be on the top of heap
    private PriorityQueue<ProfileElement> getQueryOrderByQueryFinishTime() {
        PriorityQueue<ProfileElement> queryIdDeque = new PriorityQueue<>(Comparator.comparingLong(
                (ProfileElement profileElement) -> profileElement.profile.getQueryFinishTimestamp()));

        queryIdToProfileMap.forEach((queryId, profileElement) -> {
            queryIdDeque.add(profileElement);
        });

        return queryIdDeque;
    }

    public List<List<String>> getQueryWithType(ProfileType type) {
        List<List<String>> result = Lists.newArrayList();
        readLock.lock();
        try {
            PriorityQueue<ProfileElement> queueIdDeque = getQueryOrderByQueryFinishTime();
            while (!queueIdDeque.isEmpty()) {
                ProfileElement profileElement = queueIdDeque.poll();
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

    public String getProfile(String queryID) {
        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(queryID);
            if (element == null) {
                return null;
            }

            TUniqueId thriftQueryId = Util.parseTUniqueIdFromString(queryID);
            if (thriftQueryId != null) {
                CoordInterface coor = QeProcessorImpl.INSTANCE.getCoordinator(thriftQueryId);
                if (coor != null) {
                    coor.refreshExecStatus();
                }
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
            queryIdToExecutionProfiles.clear();
        } finally {
            writeLock.unlock();
        }
    }

    // When the query is finished, the profile should be marked as finished
    public void markQueryFinished(TUniqueId queryId) {
        // just to make sure no exception will be thrown
        try {
            readLock.lock();

            ProfileElement element = queryIdToProfileMap.get(DebugUtil.printId(queryId));
            if (element == null) {
                LOG.warn("Query {} does not exist or has already finished", DebugUtil.printId(queryId));
                return;
            }

            element.profile.markisFinished(System.currentTimeMillis());
        } catch (Exception e) {
            LOG.error("Failed to mark query {} finished", DebugUtil.printId(queryId), e);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        loadProfilesFromDiskIfFirstTime();
        persistQueryProfiles();
        removeBrokenProfiles();
        removeOutdatedProfilesFromDisk();
    }

    // List PROFILE_STORAGE_PATH and return all dir names
    // string will contain profile id and its storage timestamp
    private List<String> getOnDiskProfileInfos() {
        List<String> res = Lists.newArrayList();
        try {
            File profileDir = new File(PROFILE_STORAGE_PATH);
            if (!profileDir.exists()) {
                LOG.warn("Profile storage directory {} does not exist", PROFILE_STORAGE_PATH);
                return res;
            }

            File[] files = profileDir.listFiles();
            for (File file : files) {
                res.add(file.getAbsolutePath());
            }
        } catch (Exception e) {
            LOG.error("Failed to get profile meta from disk", e);
        }

        return res;
    }

    // read profile file on disk
    // deserialize to an object Profile
    // push them to memory structure of ProfileManager for index
    private void loadProfilesFromDiskIfFirstTime() {
        if (this.isProfileLoaded) {
            return;
        }

        try {
            LOG.info("Reading profile from {}", PROFILE_STORAGE_PATH);
            List<String> profileDirAbsPaths = getOnDiskProfileInfos();
            // Thread safe list
            List<Profile> profiles = Lists.newCopyOnWriteArrayList();

            List<Thread> profileReadThreads = Lists.newArrayList();

            for (String profileDirAbsPath : profileDirAbsPaths) {
                Thread thread = new Thread(() -> {
                    Profile profile = Profile.read(profileDirAbsPath);
                    if (profile != null) {
                        profiles.add(profile);
                    }
                });
                thread.start();
                profileReadThreads.add(thread);
            }

            for (Thread thread : profileReadThreads) {
                thread.join();
            }

            LOG.info("There are {} profiles loaded into memory", profiles.size());

            // there may already has some queries running before this thread running
            // so we should not clear current memory structure

            for (Profile profile : profiles) {
                this.pushProfile(profile);
            }

            this.isProfileLoaded = true;
        } catch (Exception e) {
            LOG.error("Failed to load query profile from disk", e);
        }
    }

    // Collect profiles that need to be stored to disk
    // Store them to disk
    // Release the memory
    private void persistQueryProfiles() {
        try {
            if (Strings.isNullOrEmpty(PROFILE_STORAGE_PATH)) {
                LOG.error("Logical error, PROFILE_STORAGE_PATH is empty");
                return;
            }

            File profileDir = new File(PROFILE_STORAGE_PATH);
            if (!profileDir.exists()) {
                // create query_id directory
                if (!profileDir.mkdir()) {
                    LOG.warn("create profile directory {} failed", profileDir.getAbsolutePath());
                    return;
                } else {
                    LOG.info("Create profile storage {} succeed", PROFILE_STORAGE_PATH);
                }
            }

            rwlock.readLock().lock();

            List<ProfileElement> profilesToBeStored = Lists.newArrayList();

            queryIdToProfileMap.forEach((queryId, profileElement) -> {
                if (profileElement.profile.shouldStoreToDisk()) {
                    profilesToBeStored.add(profileElement);
                }
            });

            rwlock.readLock().unlock();


            for (ProfileElement profileElement : profilesToBeStored) {
                profileElement.store(PROFILE_STORAGE_PATH);
            }

            // After profile is stored to disk, the data must be ejected from memory
            // or the memory will be exhausted
            rwlock.writeLock().lock();

            for (ProfileElement profileElement : profilesToBeStored) {
                for (ExecutionProfile executionProfile : profileElement.profile.getExecutionProfiles()) {
                    this.queryIdToExecutionProfiles.remove(executionProfile.getQueryId());
                }
                profileElement.profile.releaseExecutionProfile();
            }

            rwlock.writeLock().unlock();
        } catch (Exception e) {
            LOG.error("Failed to remove query profile", e);
        }
    }

    // We can not store all profiles on disk, because the disk space is limited
    // So we need to remove the outdated profiles
    private void removeOutdatedProfilesFromDisk() {
        try {
            final int maxProfilesOnDisk = 5;
            readLock.lock();

            // By order of profile storage timestamp
            PriorityQueue<ProfileElement> profileDeque = new PriorityQueue<>(Comparator.comparingLong(
                    (ProfileElement profileElement) -> profileElement.profile.getProfileStoreTimestamp()));

            // Collect all profiles that has been stored to disk
            queryIdToProfileMap.forEach((queryId, profileElement) -> {
                if (profileElement.profile.profileHasBeenStoredToDisk()) {
                    profileDeque.add(profileElement);
                }
            });

            readLock.unlock();

            List<ProfileElement> queryIdToBeRemoved = Lists.newArrayList();

            while (profileDeque.size() > maxProfilesOnDisk) {
                queryIdToBeRemoved.add(profileDeque.poll());
            }

            List<Thread> iothreads = Lists.newArrayList();

            for (ProfileElement profileElement : queryIdToBeRemoved) {
                Thread thread = new Thread(() -> {
                    profileElement.remove();
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
                    TUniqueId thriftQueryId = Util.parseTUniqueIdFromString(
                            profileElement.profile.getSummaryProfile().getProfileId());
                    queryIdToExecutionProfiles.remove(thriftQueryId);
                }
            } finally {
                writeLock.unlock();
            }

            if (queryIdToBeRemoved.size() != 0) {
                StringBuilder builder = new StringBuilder();
                for (ProfileElement profileElement : queryIdToBeRemoved) {
                    builder.append(profileElement.profile.getSummaryProfile().getProfileId()).append(",");
                }
                LOG.info("Remove outdated profile: {}", builder.toString());
            }
        } catch (Exception e) {
            LOG.error("Failed to remove outdated query profile", e);
        }
    }

    private void removeBrokenProfiles() {
        List<String> profilesOnDisk = getOnDiskProfileInfos();

        List<String> brokenProfiles = Lists.newArrayList();

        for (String profileDirAbsPath : profilesOnDisk) {
            int separatorIdx = profileDirAbsPath.lastIndexOf(File.separator);
            if (separatorIdx == -1) {
                LOG.error("Invalid profile directory path: {}", profileDirAbsPath);
                brokenProfiles.add(profileDirAbsPath);
                continue;
            }

            String profileId = "";
            try {
                String profileIdAndTimestamp = profileDirAbsPath.substring(separatorIdx + 1);
                String[] parsed = Profile.parseProfileDirName(profileIdAndTimestamp);
                if (parsed == null) {
                    LOG.error("Invalid profile directory path: {}", profileDirAbsPath);
                    brokenProfiles.add(profileDirAbsPath);
                    continue;
                } else {
                    profileId = parsed[0];
                }
            } catch (Exception e) {
                LOG.error("Failed to get profile id from path: {}", profileDirAbsPath, e);
                brokenProfiles.add(profileDirAbsPath);
                continue;
            }

            readLock.lock();

            if (!queryIdToProfileMap.containsKey(profileId)) {
                LOG.info("Profile {} is broken, need to be deleted from disk", profileDirAbsPath);
                brokenProfiles.add(profileDirAbsPath);
            }

            readLock.unlock();
        }

        for (String brokenProfile : brokenProfiles) {
            try {
                File profileDir = new File(brokenProfile);
                if (!profileDir.isDirectory()) {
                    LOG.warn("Profile path {} is not a directory: {}", brokenProfile);
                    continue;
                }

                FileUtils.deleteDirectory(profileDir);
                LOG.info("Delete broken profile: {}", brokenProfile);
            } catch (Exception e) {
                LOG.error("Failed to delete broken profile: {}", brokenProfile, e);
            }
        }
    }
}
