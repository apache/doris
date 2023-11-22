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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Profile is a class to record the execution time of a query. It has the
 * following structure: root profile: // summary of this profile, such as start
 * time, end time, query id, etc. [SummaryProfile] // each execution profile is
 * a complete execution of a query, a job may contain multiple queries.
 * [List<ExecutionProfile>]
 *
 * SummaryProfile: Summary: Execution Summary:
 *
 *
 * ExecutionProfile: Fragment 0: Fragment 1: ...
 */
public class Profile {
    private static final Logger LOG = LogManager.getLogger(Profile.class);
    private RuntimeProfile rootProfile;
    private SummaryProfile summaryProfile;
    private AggregatedProfile aggregatedProfile;
    private ExecutionProfile executionProfile;
    private boolean isFinished;
    private Map<Integer, String> planNodeMap;

    public Profile(String name, boolean isEnable) {
        this.rootProfile = new RuntimeProfile(name);
        this.summaryProfile = new SummaryProfile(rootProfile);
        // if disabled, just set isFinished to true, so that update() will do nothing
        this.isFinished = !isEnable;
    }

    public void setExecutionProfile(ExecutionProfile executionProfile) {
        if (executionProfile == null) {
            LOG.warn("try to set a null excecution profile, it is abnormal", new Exception());
            return;
        }
        this.executionProfile = executionProfile;
        this.executionProfile.addToProfileAsChild(rootProfile);
        this.aggregatedProfile = new AggregatedProfile(rootProfile, executionProfile);
    }

    public synchronized void update(long startTime, Map<String, String> summaryInfo, boolean isFinished,
            int profileLevel, Planner planner, boolean isPipelineX) {
        try {
            if (this.isFinished) {
                return;
            }
            if (executionProfile == null) {
                // Sometimes execution profile is not set
                return;
            }
            summaryProfile.update(summaryInfo);
            executionProfile.update(startTime, isFinished);
            rootProfile.computeTimeInProfile();
            // Nerids native insert not set planner, so it is null
            if (planner != null) {
                this.planNodeMap = planner.getExplainStringMap();
            }
            rootProfile.setIsPipelineX(isPipelineX);
            ProfileManager.getInstance().pushProfile(this);
            this.isFinished = isFinished;
        } catch (Throwable t) {
            LOG.warn("update profile failed", t);
            throw t;
        }
    }

    public RuntimeProfile getRootProfile() {
        return this.rootProfile;
    }

    public SummaryProfile getSummaryProfile() {
        return summaryProfile;
    }

    public String getProfileByLevel() {
        StringBuilder builder = new StringBuilder();
        // add summary to builder
        summaryProfile.prettyPrint(builder);
        LOG.info(builder.toString());
        builder.append("\n MergedProfile \n");
        aggregatedProfile.getAggregatedFragmentsProfile(planNodeMap).prettyPrint(builder, "     ");
        try {
            builder.append("\n");
            executionProfile.getExecutionProfile().prettyPrint(builder, "");
            LOG.info(builder.toString());
        } catch (Throwable aggProfileException) {
            LOG.warn("build merged simple profile failed", aggProfileException);
            builder.append("build merged simple profile failed");
        }
        return builder.toString();
    }

    public String getProfileBrief() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(rootProfile.toBrief());
    }
}
