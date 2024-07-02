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

import org.apache.doris.common.Config;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.profile.SummaryProfile.SummaryBuilder;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.wildfly.common.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;


public class ProfileManagerEjectOutdatedProfileTest {
    private static final Logger LOG = LogManager.getLogger(ProfileManagerEjectOutdatedProfileTest.class);

    private static SummaryProfile constructRandomSummaryProfile(String name, boolean finished) {

        // Construct a summary profile
        SummaryBuilder builder = new SummaryBuilder();
        builder.profileId(name);
        builder.taskType(System.currentTimeMillis() % 2 == 0 ? "QUERY" : "LOAD");
        long currentTimestampSeconds = System.currentTimeMillis() / 1000;
        builder.startTime(TimeUtils.longToTimeString(currentTimestampSeconds));
        builder.endTime(TimeUtils.longToTimeString(currentTimestampSeconds + 10));
        builder.totalTime(DebugUtil.getPrettyStringMs(10));
        builder.taskState(finished ? "FINISHED" : "RUNNING");

        SummaryProfile summaryProfile = new SummaryProfile();
        summaryProfile.fuzzyInit();
        summaryProfile.update(builder.build());

        return summaryProfile;
    }


    private static Profile constructRandomProfile(String name, boolean finished, int executionProfileNum,
            int framgnetNum) {
        Profile profile = new Profile(name);
        SummaryProfile summaryProfile = constructRandomSummaryProfile(name, finished);
        profile.setSummaryProfile(summaryProfile);

        for (int i = 0; i < executionProfileNum; i++) {
            TUniqueId qUniqueId = new TUniqueId();
            UUID uuid = UUID.randomUUID();
            qUniqueId.setHi(uuid.getMostSignificantBits());
            qUniqueId.setLo(uuid.getLeastSignificantBits());

            List<PlanFragment> fragments = new ArrayList<>();
            ExecutionProfile executionProfile = new ExecutionProfile(qUniqueId, fragments);
            Map<Integer, RuntimeProfile> fragmentProfiles = Maps.newHashMap();
            Map<Integer, Integer> fragmentIdBeNum = Maps.newHashMap();

            for (int j = 0; j < framgnetNum; j++) {
                RuntimeProfile fragmentRuntimeProfile = new RuntimeProfile("Fragment " + j);
                RuntimeProfile pipelineRuntimeProfile = null;
                pipelineRuntimeProfile = new RuntimeProfile("PipelineProfile");
                if (j == 0) {
                    pipelineRuntimeProfile.setIsDone(true);
                } else {
                    pipelineRuntimeProfile.setIsDone(finished);
                }

                fragmentRuntimeProfile.addChild(pipelineRuntimeProfile);

                fragmentIdBeNum.put(j, 1);
                fragmentProfiles.put(j, fragmentRuntimeProfile);
            }

            executionProfile.setFragmentProfiles(fragmentProfiles);
            executionProfile.setFragmentIdBeNum(fragmentIdBeNum);
            profile.addExecutionProfile(executionProfile);
        }
        return profile;
    }

    @Test
    public void normalTest() {
        // Insert N profiles into ProfileManager
        // Half of them is finished
        ProfileManager profileManager = ProfileManager.getInstance();
        profileManager.cleanProfile();

        for (int i = 0; i < Config.max_query_profile_num; i++) {
            int executionProfileNum = i;
            Profile profile = constructRandomProfile("Profile_For_Eject_" + String.valueOf(i),
                    i % 2 == 0 ? true : false,
                    executionProfileNum, 2);
            profileManager.pushProfile(profile);
        }

        // Insert 50 profiles into ProfileManager
        for (int i = 0; i < 50; i++) {
            int executionProfileNum = i;
            Profile profile = constructRandomProfile("Profile_New_" + String.valueOf(i), true,
                    executionProfileNum, 2);
            profileManager.pushProfile(profile);
        }

        // Make sure profiles are ejected by design.
        // Ejected profile must be reported finished.
        for (int i = 0; i < 50; i++) {
            String profileName = "Profile_For_Eject_" + String.valueOf(i);
            String profileString = profileManager.getProfile(profileName);

            if (i % 2 == 0) {
                Assert.assertTrue(profileString == null);
            } else {
                Assert.assertTrue(profileString != null);
            }
        }
    }

    @Test
    public void allPorfilesAreNotFinishedTest() {
        // Insert N profiles into ProfileManager
        // All of them are not finished
        Config.max_query_profile_num = 10;
        ProfileManager profileManager = ProfileManager.getInstance();
        profileManager.cleanProfile();

        for (int i = 0; i < Config.max_query_profile_num; i++) {
            int executionProfileNum = i;
            Profile profile = constructRandomProfile("Profile_For_Eject_" + String.valueOf(i), false,
                    executionProfileNum, 2);
            LOG.info("Profile {} is complete: {}", profile.getName(), profile.isComplete());
            profileManager.pushProfile(profile);
        }

        // Insert 50 profiles into ProfileManager
        // All of them are finished
        for (int i = 0; i < 10; i++) {
            int executionProfileNum = i;
            Profile profile = constructRandomProfile("Profile_New_" + String.valueOf(i), true,
                    executionProfileNum, 2);
            profileManager.pushProfile(profile);
        }

        // Make sure profiles are ejected by design.
        // Ejected profile must be reported finished.
        for (int i = 0; i < Config.max_query_profile_num; i++) {
            String profileName = "Profile_For_Eject_" + String.valueOf(i);
            String profileString = profileManager.getProfile(profileName);
            if (i == 0) {
                // Although the first profile is not finished, it will still be ejected when first finished profile is inserted.
                Assert.assertTrue(profileString == null);
            } else {
                // The rest profiles are not finished, and already have new finished profile, so they will not be ejected.
                Assert.assertTrue(profileString != null);
            }
        }

        for (int i = 0; i < 10; i++) {
            String profileName = "Profile_New_" + String.valueOf(i);

            String profileString2 = profileManager.getProfile(profileName);
            if (i == 9) {
                // The last finished profile will not be ejected
                Assert.assertTrue(profileString2 != null);
            } else {
                // The rest finished profiles will not eject any outdated profiles.
                Assert.assertTrue(profileString2 == null);
            }
        }
    }
}
