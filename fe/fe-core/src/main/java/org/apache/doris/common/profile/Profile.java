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
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.planner.Planner;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Profile is a class to record the execution time of a query. It has the
 * following structure: root profile: // summary of this profile, such as start
 * time, end time, query id, etc. [SummaryProfile] // each execution profile is
 * a complete execution of a query, a job may contain multiple queries.
 * [List<ExecutionProfile>].
 * There maybe multi execution profiles for one job, for example broker load job.
 * It will create one execution profile for every single load task.
 *
 * SummaryProfile: Summary: Execution Summary:
 *
 *
 * ExecutionProfile1: Fragment 0: Fragment 1: ...
 * ExecutionProfile2: Fragment 0: Fragment 1: ...
 *
 * ExecutionProfile: Fragment 0: Fragment 1: ...
 * And also summary profile contains plan information, but execution profile is for
 * be execution time.
 * StmtExecutor(Profile) ---> Coordinator(ExecutionProfile)
 */
public class Profile {
    private static final Logger LOG = LogManager.getLogger(Profile.class);
    private static final int MergedProfileLevel = 1;
    private final String name;
    private SummaryProfile summaryProfile;
    private List<ExecutionProfile> executionProfiles = Lists.newArrayList();
    private boolean isFinished;
    private Map<Integer, String> planNodeMap;

    private int profileLevel = 3;

    public Profile(String name, boolean isEnable, int profileLevel) {
        this.name = name;
        this.summaryProfile = new SummaryProfile();
        // if disabled, just set isFinished to true, so that update() will do nothing
        this.isFinished = !isEnable;
        this.profileLevel = profileLevel;
    }

    // For load task, the profile contains many execution profiles
    public void addExecutionProfile(ExecutionProfile executionProfile) {
        if (executionProfile == null) {
            LOG.warn("try to set a null excecution profile, it is abnormal", new Exception());
            return;
        }
        executionProfile.setSummaryProfile(summaryProfile);
        this.executionProfiles.add(executionProfile);
    }

    public List<ExecutionProfile> getExecutionProfiles() {
        return this.executionProfiles;
    }

    // This API will also add the profile to ProfileManager, so that we could get the profile from ProfileManager.
    // isFinished ONLY means the coordinator or stmtexecutor is finished.
    public synchronized void updateSummary(long startTime, Map<String, String> summaryInfo, boolean isFinished,
            Planner planner) {
        try {
            if (this.isFinished) {
                return;
            }
            if (planner instanceof NereidsPlanner) {
                NereidsPlanner nereidsPlanner = ((NereidsPlanner) planner);
                StringBuilder builder = new StringBuilder();
                builder.append("\n");
                builder.append(nereidsPlanner.getPhysicalPlan()
                        .treeString());
                builder.append("\n");
                for (PhysicalRelation relation : nereidsPlanner.getPhysicalRelations()) {
                    if (relation.getStats() != null) {
                        builder.append(relation).append("\n")
                                .append(relation.getStats().printColumnStats());
                    }
                }
                summaryInfo.put(SummaryProfile.PHYSICAL_PLAN,
                        builder.toString().replace("\n", "\n     "));

                FragmentIdMapping<DistributedPlan> distributedPlans = nereidsPlanner.getDistributedPlans();
                if (distributedPlans != null) {
                    summaryInfo.put(SummaryProfile.DISTRIBUTED_PLAN,
                            DistributedPlan.toString(Lists.newArrayList(distributedPlans.values()))
                                    .replace("\n", "\n     ")
                    );
                }
            }
            summaryProfile.update(summaryInfo);
            for (ExecutionProfile executionProfile : executionProfiles) {
                // Tell execution profile the start time
                executionProfile.update(startTime, isFinished);
            }

            // Nerids native insert not set planner, so it is null
            if (planner != null) {
                this.planNodeMap = planner.getExplainStringMap();
            }
            ProfileManager.getInstance().pushProfile(this);
            this.isFinished = isFinished;
        } catch (Throwable t) {
            LOG.warn("update profile failed", t);
            throw t;
        }
    }

    public SummaryProfile getSummaryProfile() {
        return summaryProfile;
    }

    public String getProfileByLevel() {
        StringBuilder builder = new StringBuilder();
        // add summary to builder
        summaryProfile.prettyPrint(builder);
        waitProfileCompleteIfNeeded();
        // Only generate merged profile for select, insert into select.
        // Not support broker load now.
        if (this.profileLevel == MergedProfileLevel && this.executionProfiles.size() == 1) {
            try {
                builder.append("\n MergedProfile \n");
                this.executionProfiles.get(0).getAggregatedFragmentsProfile(planNodeMap).prettyPrint(builder, "     ");
            } catch (Throwable aggProfileException) {
                LOG.warn("build merged simple profile failed", aggProfileException);
                builder.append("build merged simple profile failed");
            }
        }
        try {
            // For load task, they will have multiple execution_profiles.
            for (ExecutionProfile executionProfile : executionProfiles) {
                builder.append("\n");
                executionProfile.getRoot().prettyPrint(builder, "");
            }
        } catch (Throwable aggProfileException) {
            LOG.warn("build profile failed", aggProfileException);
            builder.append("build  profile failed");
        }
        return builder.toString();
    }

    // If the query is already finished, and user wants to get the profile, we should check
    // if BE has reported all profiles, if not, sleep 2s.
    private void waitProfileCompleteIfNeeded() {
        if (!this.isFinished) {
            return;
        }
        boolean allCompleted = true;
        for (ExecutionProfile executionProfile : executionProfiles) {
            if (!executionProfile.isCompleted()) {
                allCompleted = false;
                break;
            }
        }
        if (!allCompleted) {
            try {
                Thread.currentThread().sleep(2000);
            } catch (InterruptedException e) {
                // Do nothing
            }
        }
    }

    private RuntimeProfile composeRootProfile() {
        RuntimeProfile rootProfile = new RuntimeProfile(name);
        rootProfile.addChild(summaryProfile.getSummary());
        rootProfile.addChild(summaryProfile.getExecutionSummary());
        for (ExecutionProfile executionProfile : executionProfiles) {
            rootProfile.addChild(executionProfile.getRoot());
        }
        rootProfile.computeTimeInProfile();
        return rootProfile;
    }

    public String getProfileBrief() {
        waitProfileCompleteIfNeeded();
        RuntimeProfile rootProfile = composeRootProfile();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(rootProfile.toBrief());
    }
}
