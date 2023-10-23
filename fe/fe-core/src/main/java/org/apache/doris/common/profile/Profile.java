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
import org.apache.doris.planner.Planner;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.List;
import java.util.Map;

/**
 * Profile is a class to record the execution time of a query.
 * It has the following structure:
 * root profile:
 *     // summary of this profile, such as start time, end time, query id, etc.
 *     [SummaryProfile]
 *     // each execution profile is a complete execution of a query, a job may contain multiple queries.
 *     [List<ExecutionProfile>]
 *
 * SummaryProfile:
 *     Summary:
 *         Execution Summary:
 *
 *
 * ExecutionProfile:
 *     Fragment 0:
 *     Fragment 1:
 *     ...
 */
public class Profile {
    private RuntimeProfile rootProfile;
    private SummaryProfile summaryProfile;
    private List<ExecutionProfile> executionProfiles = Lists.newArrayList();
    private boolean isFinished;

    public Profile(String name, boolean isEnable) {
        this.rootProfile = new RuntimeProfile(name);
        this.summaryProfile = new SummaryProfile(rootProfile);
        // if disabled, just set isFinished to true, so that update() will do nothing
        this.isFinished = !isEnable;
    }

    public void addExecutionProfile(ExecutionProfile executionProfile) {
        this.executionProfiles.add(executionProfile);
        executionProfile.addToProfileAsChild(rootProfile);
    }

    public synchronized void update(long startTime, Map<String, String> summaryInfo, boolean isFinished,
            int profileLevel, Planner planner, boolean isPipelineX) {
        if (this.isFinished) {
            return;
        }
        summaryProfile.update(summaryInfo);
        for (ExecutionProfile executionProfile : executionProfiles) {
            executionProfile.update(startTime, isFinished);
        }
        rootProfile.computeTimeInProfile();
        rootProfile.setPlaner(planner);
        rootProfile.setProfileLevel(profileLevel);
        rootProfile.setIsPipelineX(isPipelineX);
        ProfileManager.getInstance().pushProfile(this);
        this.isFinished = isFinished;
    }

    public RuntimeProfile getRootProfile() {
        return this.rootProfile;
    }

    public SummaryProfile getSummaryProfile() {
        return summaryProfile;
    }

    public String getProfileByLevel(int level) {
        StringBuilder builder = new StringBuilder();
        // add summary to builder
        summaryProfile.prettyPrint(builder);
        builder.append("\n \n");
        if (level >= 1) {
            this.rootProfile.prettyPrintSimplifiedProfile(builder);
        }
        if (level >= 3) {
            for (ExecutionProfile executionProfile : executionProfiles) {
                executionProfile.getExecutionProfile().prettyPrint(builder, "abcd");
            }
        }
        return builder.toString();
    }

    public String getProfileBrief() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(rootProfile.toBrief());
    }
}
