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

import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.RuntimeProfile;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * Profile is a class to record the execution time of a query.
 * It has following structure:
 * root profile:
 *     [summary profile]
 *     [execution profile 1]
 *     [execution profile 2]
 *
 * summary profile:
 *     Summary:
 *         Execution Summary:
 *
 * execution profile:
 *     Fragment 0:
 *     Fragment 1:
 */
public class Profile {
    private long startTimeNs = 0;
    private RuntimeProfile rootProfile;
    private SummaryProfile summaryProfile;
    private List<ExecutionProfile> executionProfiles = Lists.newArrayList();
    private boolean isFinished;

    public Profile(String name, boolean isEnable) {
        this.rootProfile = new RuntimeProfile(name);
        this.summaryProfile = new SummaryProfile(rootProfile);
        this.isFinished = !isEnable;
    }

    // Must be called before update()
    public void addExecutionProfile(ExecutionProfile executionProfile) {
        this.executionProfiles.add(executionProfile);
        executionProfile.addToProfileAsChild(rootProfile);
    }

    public synchronized void update(long startTimeNs, Map<String, String> summaryInfo, boolean isFinished) {
        if (this.isFinished) {
            return;
        }
        this.startTimeNs = (this.startTimeNs == 0 && startTimeNs > 0 ? startTimeNs : this.startTimeNs);
        summaryProfile.update(summaryInfo);
        for (ExecutionProfile executionProfile : executionProfiles) {
            executionProfile.update(startTimeNs, isFinished);
        }
        rootProfile.computeTimeInProfile();
        ProfileManager.getInstance().pushProfile(rootProfile);
        this.isFinished = isFinished;
    }

    public SummaryProfile getSummaryProfile() {
        return summaryProfile;
    }
}
