package org.apache.doris.common.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.doris.common.Config;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.profile.SummaryProfile.SummaryBuilder;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import org.checkerframework.com.google.common.collect.Maps;
import org.junit.Test;
import org.wildfly.common.Assert;

import java.util.Map;
import java.util.UUID;


public class ProfileManagerEjectOutdatedProfileTest {
    private static final Logger LOG = LogManager.getLogger(ProfileManagerEjectOutdatedProfileTest.class);

    private static SummaryProfile constructRandomSummaryProfile(boolean finished) {
        TUniqueId qUniqueId = new TUniqueId();
        UUID uuid = UUID.randomUUID();
        qUniqueId.setHi(uuid.getMostSignificantBits());
        qUniqueId.setLo(uuid.getLeastSignificantBits());
        // Construct a summary profile
        SummaryBuilder builder = new SummaryBuilder();
        builder.profileId(DebugUtil.printId(qUniqueId));
        builder.taskType(System.currentTimeMillis() % 2 == 0 ? "QUERY" : "LOAD");
        long currentTimestampSeconds = System.currentTimeMillis() / 1000;
        builder.startTime(TimeUtils.longToTimeString(currentTimestampSeconds));
        builder.endTime(TimeUtils.longToTimeString(currentTimestampSeconds + 10));
        builder.totalTime(DebugUtil.getPrettyStringMs(10));
        builder.taskState(finished ? "FINISHED" : "RUNNING");
        builder.user(DebugUtil.printId(qUniqueId) + "-user");
        builder.defaultDb(DebugUtil.printId(qUniqueId) + "-db");

        SummaryProfile summaryProfile = new SummaryProfile();
        summaryProfile.fuzzyInit();
        summaryProfile.update(builder.build());

        return summaryProfile;
    }


    private static Profile constructRandomProfile(String name, boolean finished, int executionProfileNum, int framgnetNum) {
        Profile profile = new Profile(name);
        SummaryProfile summaryProfile = constructRandomSummaryProfile(finished);
        String stringUniqueId = summaryProfile.getProfileId();
        TUniqueId thriftUniqueId = DebugUtil.parseTUniqueIdFromString(stringUniqueId);
        profile.setSummaryProfile(summaryProfile);

        for (int i = 0; i < executionProfileNum; i++) {
            List<PlanFragment> fragments = new ArrayList<>();
            ExecutionProfile executionProfile = new ExecutionProfile(thriftUniqueId, fragments);
            Map<Integer, RuntimeProfile> fragmentProfiles = Maps.newHashMap();
            Map<Integer, Integer> fragmentIdBeNum = Maps.newHashMap();

            for (int j = 0; j < framgnetNum; j++) {
                RuntimeProfile fragmentRuntimeProfile = new RuntimeProfile("Fragment " + j);
                fragmentProfiles.put(j, fragmentRuntimeProfile);

                RuntimeProfile pipelineRuntimeProfile1 = new RuntimeProfile("PipelineProfile1");
                pipelineRuntimeProfile1.setIsDone(true);

                RuntimeProfile pipelineRuntimeProfile2 = new RuntimeProfile("PipelineProfile2");
                pipelineRuntimeProfile2.setIsDone(finished);

                fragmentRuntimeProfile.addChild(pipelineRuntimeProfile1);
                fragmentRuntimeProfile.addChild(pipelineRuntimeProfile1);

                fragmentIdBeNum.put(j, 1);
            }

            executionProfile.setFragmentProfiles(fragmentProfiles);
            executionProfile.setFragmentIdBeNum(fragmentIdBeNum);
            profile.addExecutionProfile(executionProfile);
        }
        return profile;
    }
    
    @Test
    public void test1() {
        Config.max_query_profile_num = 10;
        ProfileManager profileManager = ProfileManager.getInstance();

        for (int i = 0; i < Config.max_query_profile_num; i++) {
            int executionProfileNum = i;
            Profile profile = constructRandomProfile("Profile_For_Eject_" + String.valueOf(i), i % 2 == 0 ? true : false,
                                                    executionProfileNum, 2);
            profileManager.pushProfile(profile);
        }

        for (int i = 0; i < 10; i++) {
            int executionProfileNum = i;
            Profile profile = constructRandomProfile("Profile_New_" + String.valueOf(i), true,
                                                    executionProfileNum, 2);
            profileManager.pushProfile(profile);
        }

        LOG.info("Debug profile manager {}", profileManager.debugAllProfile());

        for (int i = 0; i < 10; i++) {
            String profileName = "Profile_For_Eject_" + String.valueOf(i);
            String profileString = profileManager.getProfile(profileName);

            if (i % 2 == 0) {
                Assert.assertTrue(profileString == null);
            } else {
                Assert.assertTrue(profileString != null);
            }
        }

    }

}
