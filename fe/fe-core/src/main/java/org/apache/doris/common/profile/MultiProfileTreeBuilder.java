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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Counter;
import org.apache.doris.common.util.RuntimeProfile;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.tuple.Triple;
import org.glassfish.jersey.internal.guava.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// MultiProfileTreeBuilder saves a set of ProfileTreeBuilder.
// For a query profile, there is usually only one ExecutionProfile node.
// For a load job profile, it may produce multiple subtasks, so there may be multiple ExecutionProfile nodes.
//
// Each ExecutionProfile node corresponds to a ProfileTreeBuilder
public class MultiProfileTreeBuilder {
    private static final Set<String> PROFILE_ROOT_NAMES;
    public static final String PROFILE_NAME_EXECUTION = "Execution Profile";

    private static final String EXECUTION_ID_PATTERN_STR = "^Execution Profile (.*)";
    private static final Pattern EXECUTION_ID_PATTERN;

    private RuntimeProfile profileRoot;
    private Map<String, RuntimeProfile> idToSingleProfile = Maps.newHashMap();
    private Map<String, ProfileTreeBuilder> idToSingleTreeBuilder = Maps.newHashMap();

    static {
        PROFILE_ROOT_NAMES = Sets.newHashSet();
        PROFILE_ROOT_NAMES.add("Query");
        PROFILE_ROOT_NAMES.add("BrokerLoadJob");
        EXECUTION_ID_PATTERN = Pattern.compile(EXECUTION_ID_PATTERN_STR);
    }

    public MultiProfileTreeBuilder(RuntimeProfile root) {
        this.profileRoot = root;
    }

    public void build() throws UserException {
        unwrapProfile();
        buildTrees();
    }

    private void unwrapProfile() throws UserException {
        if (PROFILE_ROOT_NAMES.stream().anyMatch(n -> profileRoot.getName().startsWith(n))) {
            List<Pair<RuntimeProfile, Boolean>> children = profileRoot.getChildList();
            boolean find = false;
            for (Pair<RuntimeProfile, Boolean> pair : children) {
                if (pair.first.getName().startsWith(PROFILE_NAME_EXECUTION)) {
                    String executionProfileId = getExecutionProfileId(pair.first.getName());
                    idToSingleProfile.put(executionProfileId, pair.first);
                    find = true;
                }
            }
            if (!find) {
                throw new UserException("Invalid profile. Expected " + PROFILE_NAME_EXECUTION);
            }
        }
    }

    private String getExecutionProfileId(String executionName) throws UserException {
        Matcher m = EXECUTION_ID_PATTERN.matcher(executionName);
        if (!m.find() || m.groupCount() != 1) {
            throw new UserException("Invalid execution profile name: " + executionName);
        }
        return m.group(1);
    }

    private void buildTrees() throws UserException {
        for (Map.Entry<String, RuntimeProfile> entry : idToSingleProfile.entrySet()) {
            ProfileTreeBuilder builder = new ProfileTreeBuilder(entry.getValue());
            builder.build();
            idToSingleTreeBuilder.put(entry.getKey(), builder);
        }
    }

    public List<List<String>> getSubTaskInfo() {
        List<List<String>> rows = Lists.newArrayList();
        for (Map.Entry<String, RuntimeProfile> entry : idToSingleProfile.entrySet()) {
            List<String> row = Lists.newArrayList();
            Counter activeCounter = entry.getValue().getCounterTotalTime();
            row.add(entry.getKey());
            row.add(RuntimeProfile.printCounter(activeCounter.getValue(), activeCounter.getType()));
            rows.add(row);
        }
        return rows;
    }

    public List<Triple<String, String, Long>> getInstanceList(String executionId, String fragmentId)
         throws AnalysisException {
        ProfileTreeBuilder singleBuilder = getExecutionProfileTreeBuilder(executionId);
        return singleBuilder.getInstanceList(fragmentId);
    }

    public ProfileTreeNode getInstanceTreeRoot(String executionId, String fragmentId, String instanceId)
            throws AnalysisException {
        ProfileTreeBuilder singleBuilder = getExecutionProfileTreeBuilder(executionId);
        return singleBuilder.getInstanceTreeRoot(fragmentId, instanceId);
    }

    public ProfileTreeNode getFragmentTreeRoot(String executionId) throws AnalysisException {
        ProfileTreeBuilder singleBuilder = getExecutionProfileTreeBuilder(executionId);
        return singleBuilder.getFragmentTreeRoot();
    }

    private ProfileTreeBuilder getExecutionProfileTreeBuilder(String executionId) throws AnalysisException {
        ProfileTreeBuilder singleBuilder = idToSingleTreeBuilder.get(executionId);
        if (singleBuilder == null) {
            throw new AnalysisException("Can not find execution profile: " + executionId);
        }
        return singleBuilder;
    }
}
