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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class ProfileCompletionStateTest {
    @TempDir
    Path temporary;

    private String newFolder() throws IOException {
        return Files.createTempDirectory(temporary, "profile_").toString();
    }

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
    void waitsForEveryBackendAndRejectsStaleReports() {
        Profile profile = profile();
        ExecutionProfile execution = profile.getExecutionProfiles().get(0);
        execution.addFragmentBackend(new PlanFragmentId(0), 1L);
        execution.addFragmentBackend(new PlanFragmentId(0), 2L);
        Assertions.assertEquals("RUNNING", profile.getProfileCompletionState());
        profile.markQueryFinished();
        Assertions.assertEquals("COLLECTING", profile.getProfileCompletionState());
        report(execution, "127.0.0.1", true);
        Assertions.assertEquals("COLLECTING", profile.getProfileCompletionState());
        report(execution, "127.0.0.2", true);
        Assertions.assertEquals("COMPLETE", profile.getProfileCompletionState());
        report(execution, "127.0.0.1", false);
        Assertions.assertEquals("COMPLETE", profile.getProfileCompletionState());
        Assertions.assertTrue(profile.getProfileByLevel().contains("Profile Completion State: COMPLETE"));
    }

    private static class PausingSummaryProfile extends SummaryProfile {
        private final transient boolean beforeSerialization;
        private final transient CountDownLatch storagePaused;
        private final transient CountDownLatch resumeStorage;

        private PausingSummaryProfile(boolean beforeSerialization, CountDownLatch storagePaused,
                CountDownLatch resumeStorage) {
            this.beforeSerialization = beforeSerialization;
            this.storagePaused = storagePaused;
            this.resumeStorage = resumeStorage;
        }

        @Override
        public void write(DataOutput output) throws IOException {
            if (!beforeSerialization) {
                super.write(output);
            }
            storagePaused.countDown();
            try {
                if (!resumeStorage.await(10, TimeUnit.SECONDS)) {
                    throw new IOException("Timed out waiting for concurrent profile rendering");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            if (beforeSerialization) {
                super.write(output);
            }
        }
    }

    private static class FailingSummaryProfile extends SummaryProfile {
        private transient boolean fail = true;

        @Override
        public void write(DataOutput output) throws IOException {
            if (fail) {
                fail = false;
                throw new IOException("Injected storage failure");
            }
            super.write(output);
        }
    }

    @Test
    void failedStorageDoesNotFreezeCompletionState() throws Exception {
        Profile profile = profile();
        SummaryProfile summary = new FailingSummaryProfile();
        summary.getSummary().addInfoString(SummaryProfile.PROFILE_ID, profile.getId());
        profile.setSummaryProfile(summary);
        ExecutionProfile execution = profile.getExecutionProfiles().get(0);
        execution.addFragmentBackend(new PlanFragmentId(0), 1L);
        profile.markQueryFinished();
        String directory = newFolder();
        profile.writeToStorage(directory);
        Assertions.assertFalse(profile.profileHasBeenStored());
        Assertions.assertEquals("COLLECTING", profile.getProfileCompletionState());
        report(execution, "127.0.0.1", true);
        profile.writeToStorage(directory);
        Assertions.assertTrue(profile.profileHasBeenStored());
        Assertions.assertEquals("COMPLETE", Profile.read(profile.getProfileStoragePath()).getProfileCompletionState());
    }

    @Test
    void renderingDuringStoragePreservesTerminalState() throws Exception {
        for (boolean beforeSerialization : new boolean[] {true, false}) {
            Profile profile = profile();
            CountDownLatch storagePaused = new CountDownLatch(1);
            CountDownLatch resumeStorage = new CountDownLatch(1);
            SummaryProfile summary = new PausingSummaryProfile(beforeSerialization, storagePaused, resumeStorage);
            summary.getSummary().addInfoString(SummaryProfile.PROFILE_ID, profile.getId());
            profile.setSummaryProfile(summary);
            profile.getExecutionProfiles().get(0).addFragmentBackend(new PlanFragmentId(0), 1L);
            profile.markQueryFinished();
            String directory = newFolder();
            ExecutorService writer = Executors.newSingleThreadExecutor();
            try {
                Future<?> stored = writer.submit(() -> profile.writeToStorage(directory));
                Assertions.assertTrue(storagePaused.await(10, TimeUnit.SECONDS));
                // Render both before serialization and before path publication, while storage is paused.
                profile.getProfileByLevel();
                resumeStorage.countDown();
                stored.get(10, TimeUnit.SECONDS);
                Assertions.assertTrue(profile.profileHasBeenStored());
                Assertions.assertEquals("INCOMPLETE", profile.getProfileCompletionState());
                Profile restored = Profile.read(profile.getProfileStoragePath());
                Assertions.assertNotNull(restored);
                Assertions.assertEquals("INCOMPLETE", restored.getProfileCompletionState());
            } finally {
                resumeStorage.countDown();
                writer.shutdownNow();
                Assertions.assertTrue(writer.awaitTermination(10, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void persistsTerminalStateBeforeReleasingReports() throws Exception {
        for (boolean complete : new boolean[] {false, true}) {
            Profile profile = profile();
            ExecutionProfile execution = profile.getExecutionProfiles().get(0);
            execution.addFragmentBackend(new PlanFragmentId(0), 1L);
            profile.markQueryFinished();
            if (complete) {
                report(execution, "127.0.0.1", true);
            }
            profile.writeToStorage(newFolder());
            Assertions.assertTrue(profile.profileHasBeenStored());
            profile.releaseMemory();
            String expected = complete ? "COMPLETE" : "INCOMPLETE";
            Assertions.assertEquals(expected, profile.getProfileCompletionState());
            Profile restored = Profile.read(profile.getProfileStoragePath());
            Assertions.assertNotNull(restored);
            Assertions.assertEquals(expected, restored.getProfileCompletionState());
            Assertions.assertTrue(restored.getProfileByLevel().contains("Profile Completion State: " + expected));
        }
    }
}
