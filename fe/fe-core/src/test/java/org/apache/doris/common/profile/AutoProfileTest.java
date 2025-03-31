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
import org.apache.doris.thrift.TUniqueId;

import mockit.Expectations;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@ResourceLock("global")
public class AutoProfileTest {
    @BeforeAll
    static void setUp() {
        ProfileManager.getInstance().cleanProfile();
    }

    private Profile createProfile() {
        UUID taskId = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
        List<Integer> fragments = new ArrayList<>();
        ExecutionProfile executionProfile = new ExecutionProfile(queryId, fragments);

        Profile profile = ProfileManagerTest.constructProfile(DebugUtil.printId(queryId));
        profile.addExecutionProfile(executionProfile);
        return profile;
    }

    @Test
    public void testAutoProfile() throws InterruptedException {
        Profile profile = createProfile();
        SummaryProfile summaryProfile = new SummaryProfile();
        profile.setSummaryProfile(summaryProfile);
        Map<String, String> summaryInfo = new HashMap<>();

        new Expectations(summaryProfile) {
            {
                summaryProfile.update(summaryInfo);
                summaryProfile.getQueryBeginTime();
                result = System.currentTimeMillis();
            }
        };
        profile.autoProfileDurationMs = 1000;
        Thread.sleep(899);
        profile.updateSummary(summaryInfo, true, null);
        Assertions.assertNull(ProfileManager.getInstance().queryIdToProfileMap.get(profile.getId()));

        profile = createProfile();
        profile.setSummaryProfile(summaryProfile);
        profile.autoProfileDurationMs = 500;
        Thread.sleep(899);
        profile.updateSummary(summaryInfo, true, null);
        Assertions.assertNotNull(ProfileManager.getInstance().queryIdToProfileMap.get(profile.getId()));
    }
}
