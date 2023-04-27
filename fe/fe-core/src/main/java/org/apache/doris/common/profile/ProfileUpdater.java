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

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.QueryPlannerProfile;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.TimeUtils;

import java.util.Map;

public class ProfileUpdater {

    private final String name;
    private final boolean enabled;

    private boolean isInit = false;

    private RuntimeProfile rootProfile;
    private RuntimeProfile summaryProfile;
    private RuntimeProfile executionSummaryProfile;

    private volatile boolean isFinishedProfile = false;

    private final Object writeProfileLock = new Object();

    public ProfileUpdater(String profileName, boolean enabled, String profileId) {
        this.name = profileName;
        this.enabled = enabled;
        init();
    }

    private void init() {
        // rootProfile:
        //   summaryProfile:
        //     executionSummaryProfile:
        executionSummaryProfile = new RuntimeProfile("Execution Summary");

        summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addChild(executionSummaryProfile);

        rootProfile = new RuntimeProfile(name);
        rootProfile.addChild(summaryProfile);
    }

    private void updateSummaryProfile(boolean waiteBeReport) {

    }

    public void writeProfile(boolean isLastWriteProfile, QueryPlannerProfile plannerProfile) {
        if (!enabled) {
            return;
        }
        synchronized (writeProfileLock) {
            if (isFinishedProfile) {
                return;
            }
            initProfile(plannerProfile, isLastWriteProfile);
            profile.computeTimeInChildProfile();
            ProfileManager.getInstance().pushProfile(profile);
            isFinishedProfile = isLastWriteProfile;
        }
    }

    // At the end of query execution, we begin to add up profile
    private void initProfile(QueryPlannerProfile plannerProfile, boolean waiteBeReport) {
        RuntimeProfile queryProfile;
        // when a query hits the sql cache, `coord` is null.
        if (coord == null) {
            queryProfile = new RuntimeProfile("Execution Profile " + DebugUtil.printId(context.queryId()));
        } else {
            queryProfile = coord.getQueryProfile();
        }
        if (profile == null) {
            profile = new RuntimeProfile("Query");
            summaryProfile = new RuntimeProfile("Summary");
            profile.addChild(summaryProfile);
            summaryProfile.addInfoString(ProfileManager.START_TIME, TimeUtils.longToTimeString(context.getStartTime()));
            updateSummaryProfile(waiteBeReport);
            for (Map.Entry<String, String> entry : getSummaryInfo().entrySet()) {
                summaryProfile.addInfoString(entry.getKey(), entry.getValue());
            }
            summaryProfile.addInfoString(ProfileManager.TRACE_ID, context.getSessionVariable().getTraceId());
            plannerRuntimeProfile = new RuntimeProfile("Execution Summary");
            summaryProfile.addChild(plannerRuntimeProfile);
            profile.addChild(queryProfile);
        } else {
            updateSummaryProfile(waiteBeReport);
        }
        plannerProfile.initRuntimeProfile(plannerRuntimeProfile);

        queryProfile.getCounterTotalTime().setValue(TimeUtils.getEstimatedTime(plannerProfile.getQueryBeginTime()));
        endProfile(waiteBeReport);
    }

}
