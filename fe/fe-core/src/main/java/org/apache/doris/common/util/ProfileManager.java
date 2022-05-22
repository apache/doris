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
import org.apache.doris.common.profile.InMemoryProfileStorage;
import org.apache.doris.common.profile.MultiProfileTreeBuilder;
import org.apache.doris.common.profile.ProfileStorage;
import org.apache.doris.common.profile.ProfileTreeBuilder;
import org.apache.doris.common.profile.ProfileTreeNode;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
    public static final String QUERY_ID = "Query ID";
    public static final String START_TIME = "Start Time";
    public static final String END_TIME = "End Time";
    public static final String TOTAL_TIME = "Total";
    public static final String QUERY_TYPE = "Query Type";
    public static final String QUERY_STATE = "Query State";
    public static final String DORIS_VERSION = "Doris Version";
    public static final String USER = "User";
    public static final String DEFAULT_DB = "Default Db";
    public static final String SQL_STATEMENT = "Sql Statement";
    public static final String IS_CACHED = "Is Cached";

    public enum ProfileType {
        QUERY,
        LOAD,
    }

    public static final List<String> PROFILE_HEADERS = Arrays.asList(
            QUERY_ID, USER, DEFAULT_DB, SQL_STATEMENT, QUERY_TYPE,
            START_TIME, END_TIME, TOTAL_TIME, QUERY_STATE);

    /*
     * Store the base element of each query.
     */
    public static class ProfileElement {
        public Map<String, String> infoStrings = Maps.newHashMap();
        public String profileContent = "";
        public MultiProfileTreeBuilder builder = null;
        public String errMsg = "";
    }

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

    private ProfileStorage storage;

    private ProfileManager() {
        storage = new InMemoryProfileStorage();
    }


    public void pushProfile(RuntimeProfile profile) {
        storage.pushProfile(profile);
    }

    public List<List<String>> getAllQueries() {
        return storage.getAllProfiles(null);
    }

    public List<List<String>> getQueryWithType(ProfileType type) {
        return storage.getAllProfiles(type);
    }

    public String getProfile(String queryID) {
        ProfileElement element = storage.getProfile(queryID);
        if (element == null) {
            return null;
        }

        return element.profileContent;
    }

    public ProfileTreeNode getFragmentProfileTree(String queryID, String executionId) throws AnalysisException {
        ProfileElement element = storage.getProfile(queryID);
        if (element == null || element.builder == null) {
            throw new AnalysisException("failed to get fragment profile tree. err: "
                    + (element == null ? "not found" : element.errMsg));
        }

        return element.builder.getFragmentTreeRoot(executionId);
    }

    public List<Triple<String, String, Long>> getFragmentInstanceList(String queryID, String executionId, String fragmentId)
            throws AnalysisException {

        ProfileElement element = storage.getProfile(queryID);
        if (element == null || element.builder == null) {
            throw new AnalysisException("failed to get instance list. err: "
                    + (element == null ? "not found" : element.errMsg));
        }
        return element.builder.getInstanceList(executionId, fragmentId);
    }

    public ProfileTreeNode getInstanceProfileTree(String queryID, String executionId, String fragmentId, String instanceId)
            throws AnalysisException {
        ProfileElement element = storage.getProfile(queryID);
        if (element == null || element.builder == null) {
            throw new AnalysisException("failed to get instance profile tree. err: "
                    + (element == null ? "not found" : element.errMsg));
        }
        return element.builder.getInstanceTreeRoot(executionId, fragmentId, instanceId);
    }

    // Return the tasks info of the specified load job
    // Columns: TaskId, ActiveTime
    public List<List<String>> getLoadJobTaskList(String jobId) throws AnalysisException {
        MultiProfileTreeBuilder builder = getMultiProfileTreeBuilder(jobId);
        return builder.getSubTaskInfo();
    }

    public List<ProfileTreeBuilder.FragmentInstances> getFragmentsAndInstances(String queryId) throws AnalysisException{
        return getMultiProfileTreeBuilder(queryId).getFragmentInstances(queryId);
    }

    private MultiProfileTreeBuilder getMultiProfileTreeBuilder(String jobId) throws AnalysisException{
        ProfileElement element = storage.getProfile(jobId);
        if (element == null || element.builder == null) {
            throw new AnalysisException("failed to get task ids. err: "
                    + (element == null ? "not found" : element.errMsg));
        }
        return element.builder;
    }
}
