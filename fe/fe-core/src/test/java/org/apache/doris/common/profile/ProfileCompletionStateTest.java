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
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.thrift.TDetailedReportParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryProfile;
import org.apache.doris.thrift.TRuntimeProfileNode;
import org.apache.doris.thrift.TRuntimeProfileTree;
import org.apache.doris.thrift.TUniqueId;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

public class ProfileCompletionStateTest {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    private Profile profile() {
        UUID id = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        Profile profile = new Profile();
        profile.getSummaryProfile().getSummary().addInfoString(SummaryProfile.PROFILE_ID, DebugUtil.printId(queryId));
        profile.addExecutionProfile(new ExecutionProfile(queryId, Collections.singletonList(0)));
        return profile;
    }

    private void report(ExecutionProfile execution, String host, boolean done) {
        TRuntimeProfileNode node = new TRuntimeProfileNode();
        node.setName("PipelineProfile");
        node.setNumChildren(0);
        node.setCounters(new ArrayList<>());
        node.setMetadata(0);
        node.setIndent(false);
        node.setInfoStrings(new HashMap<>());
        node.setInfoStringsDisplayOrder(new ArrayList<>());
        node.setChildCountersMap(new HashMap<>());
        node.setTimestamp(0);
        TDetailedReportParams params = new TDetailedReportParams();
        params.setProfile(new TRuntimeProfileTree(Collections.singletonList(node)));
        params.setIsFragmentLevel(false);
        TQueryProfile report = new TQueryProfile();
        report.setQueryId(execution.getQueryId());
        // Multiple pipeline nodes from one BE must not count as multiple BE reports.
        report.putToFragmentIdToProfile(0, Arrays.asList(params, params));
        execution.updateProfile(report, new TNetworkAddress(host, 9050), done);
    }

    @Test
    public void waitsForEveryBackendAndRejectsStaleReports() {
        Profile profile = profile();
        ExecutionProfile execution = profile.getExecutionProfiles().get(0);
        execution.addFragmentBackend(new PlanFragmentId(0), 1L);
        execution.addFragmentBackend(new PlanFragmentId(0), 2L);
        Assert.assertEquals("RUNNING", profile.getProfileCompletionState());
        profile.markQueryFinished();
        Assert.assertEquals("COLLECTING", profile.getProfileCompletionState());
        report(execution, "127.0.0.1", true);
        Assert.assertEquals("COLLECTING", profile.getProfileCompletionState());
        report(execution, "127.0.0.2", true);
        Assert.assertEquals("COMPLETE", profile.getProfileCompletionState());
        report(execution, "127.0.0.1", false);
        Assert.assertEquals("COMPLETE", profile.getProfileCompletionState());
        Assert.assertTrue(profile.getProfileByLevel().contains("Profile Completion State: COMPLETE"));
    }

    @Test
    public void persistsTerminalStateBeforeReleasingReports() throws Exception {
        for (boolean complete : new boolean[] {false, true}) {
            Profile profile = profile();
            ExecutionProfile execution = profile.getExecutionProfiles().get(0);
            execution.addFragmentBackend(new PlanFragmentId(0), 1L);
            profile.markQueryFinished();
            if (complete) {
                report(execution, "127.0.0.1", true);
            }
            profile.writeToStorage(temporary.newFolder().getAbsolutePath());
            Assert.assertTrue(profile.profileHasBeenStored());
            profile.releaseMemory();
            String expected = complete ? "COMPLETE" : "INCOMPLETE";
            Assert.assertEquals(expected, profile.getProfileCompletionState());
            Profile restored = Profile.read(profile.getProfileStoragePath());
            Assert.assertNotNull(restored);
            Assert.assertEquals(expected, restored.getProfileCompletionState());
            Assert.assertTrue(restored.getProfileByLevel().contains("Profile Completion State: " + expected));
        }
    }
}
