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
import org.apache.doris.common.Pair;
import org.apache.doris.common.profile.MultiProfileTreeBuilder;
import org.apache.doris.common.profile.ProfileTreeBuilder;
import org.apache.doris.common.profile.ProfileTreeNode;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.nereids.stats.StatsErrorEstimator;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
        public ProfileElement(RuntimeProfile profile) {
            this.profile = profile;
        }

        private final RuntimeProfile profile;
        // cache the result of getProfileContent method
        private String profileContent = null;
        public Map<String, String> infoStrings = Maps.newHashMap();
        public MultiProfileTreeBuilder builder = null;
        public String errMsg = "";

        public StatsErrorEstimator statsErrorEstimator;

        /**
         * lazy load profileContent because sometimes profileContent is very large
         * guarded by {@link org.apache.doris.common.util.ProfileManager#readLock}
         */
        public String getProfileContent() {
            if (profileContent != null) {
                return profileContent;
            }

            boolean isFinished = profile.getIsCancel() || profile.getIsDone();
            String tmpProfileContent = null;
            // Simple profile will change the structure of the profile.
            try {
                tmpProfileContent = profile.getProfileByLevel();
                if (isFinished && tmpProfileContent != null) {
                    profileContent = tmpProfileContent;
                }
            } catch (Exception e) {
                LOG.warn("profile get error : " + e.toString());
            }
            return tmpProfileContent;
        }

        public String getProfileBrief() {
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            return gson.toJson(profile.toBrief());
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
    }

    public ProfileElement createElement(RuntimeProfile profile) {
        ProfileElement element = new ProfileElement(profile);
        RuntimeProfile summaryProfile = profile.getChildList().get(0).first;
        for (String header : SummaryProfile.SUMMARY_KEYS) {
            element.infoStrings.put(header, summaryProfile.getInfoString(header));
        }
        List<Pair<RuntimeProfile, Boolean>> childList = summaryProfile.getChildList();
        if (!childList.isEmpty()) {
            RuntimeProfile executionProfile = childList.get(0).first;
            for (String header : SummaryProfile.EXECUTION_SUMMARY_KEYS) {
                element.infoStrings.put(header, executionProfile.getInfoString(header));
            }
        }

        MultiProfileTreeBuilder builder = new MultiProfileTreeBuilder(profile);
        try {
            builder.build();
        } catch (Exception e) {
            element.errMsg = e.getMessage();
            LOG.debug("failed to build profile tree", e);
            return element;
        }
        element.builder = builder;
        return element;
    }

    public void pushProfile(RuntimeProfile profile) {
        if (profile == null) {
            return;
        }

        ProfileElement element = createElement(profile);
        // 'insert into' does have job_id, put all profiles key with query_id
        String key = element.infoStrings.get(SummaryProfile.PROFILE_ID);
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
                    queryIdToProfileMap.remove(queryIdDeque.getFirst());
                    queryIdDeque.removeFirst();
                }
                queryIdDeque.addLast(key);
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

    public String getProfile(String queryID) {
        readLock.lock();
        try {
            ProfileElement element = queryIdToProfileMap.get(queryID);
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
