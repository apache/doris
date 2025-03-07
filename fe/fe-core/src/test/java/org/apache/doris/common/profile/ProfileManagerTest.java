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

import org.apache.doris.common.Config;
import org.apache.doris.common.profile.ProfileManager.ProfileElement;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import mockit.Expectations;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

@ResourceLock("global")
class ProfileManagerTest {
    private static final Logger LOG = LogManager.getLogger(ProfilePersistentTest.class);

    private static ProfileManager profileManager;
    private File tempDir;
    private String originalPath;
    private int originMaxProfiles;

    @BeforeAll
    static void setUp() throws Exception {
        profileManager = new ProfileManager();
    }

    @BeforeEach
    void init() throws IOException {
        tempDir = Files.createTempDirectory("profile_test_").toFile();
        originalPath = ProfileManager.PROFILE_STORAGE_PATH;
        ProfileManager.PROFILE_STORAGE_PATH = tempDir.getAbsolutePath();
        profileManager.cleanProfile();
        profileManager.isProfileLoaded = false;
        originMaxProfiles = Config.max_query_profile_num;
    }

    @AfterEach
    void cleanup() {
        ProfileManager.PROFILE_STORAGE_PATH = originalPath;
        FileUtils.deleteQuietly(tempDir);
        Config.max_query_profile_num = originMaxProfiles;
    }

    @Test
    void returnsEmptyQueueWhenNoProfiles() {
        PriorityQueue<ProfileManager.ProfileElement> result = profileManager.getProfileOrderByQueryFinishTimeDesc();
        Assertions.assertTrue(result.isEmpty());
        result = profileManager.getProfileOrderByQueryFinishTime();
        Assertions.assertTrue(result.isEmpty());
        result = profileManager.getProfileOrderByQueryStartTime();
        Assertions.assertTrue(result.isEmpty());
    }

    static Profile constructProfile(String id) {
        Profile profile = new Profile();
        SummaryProfile summaryProfile = new SummaryProfile();
        summaryProfile.getSummary().getInfoStrings().put(SummaryProfile.PROFILE_ID, id);
        profile.setSummaryProfile(summaryProfile);
        return profile;
    }

    @Test
    void getProfileByOrder() {
        final int normalProfiles = 100;
        for (int i = 0; i < normalProfiles; i++) {
            Profile profile = constructProfile(String.valueOf(i));
            Random random = new Random();
            profile.setQueryFinishTimestamp(random.nextInt(200 - 101) + 101);
            // set query start time in range of [0, 1000)
            profile.getSummaryProfile().setQueryBeginTime(random.nextInt(100));
            profileManager.pushProfile(profile);

            if (i == 10) {
                LOG.info("Profile manager debug info: {}", profileManager.getDebugInfo());
            }
        }

        // Insert two profiles with default value.
        Profile profile1 = constructProfile("Default 1");
        profileManager.pushProfile(profile1);
        Profile profile2 = constructProfile("Default 2");
        profileManager.pushProfile(profile2);

        profile1 = constructProfile("Default 3");
        profile1.setQueryFinishTimestamp(1000L);
        profileManager.pushProfile(profile1);
        profile1 = constructProfile("Default 4");
        profile1.setQueryFinishTimestamp(1000L);
        profileManager.pushProfile(profile1);

        profile1 = constructProfile("Default 5");
        profile1.getSummaryProfile().setQueryBeginTime(1000L);
        profileManager.pushProfile(profile1);
        profile1 = constructProfile("Default 6");
        profile1.getSummaryProfile().setQueryBeginTime(1000L);
        profileManager.pushProfile(profile1);


        Set<String> profileThatHasQueryFinishTime1000 = new HashSet<>();
        profileThatHasQueryFinishTime1000.add("Default 3");
        profileThatHasQueryFinishTime1000.add("Default 4");
        Set<String> profileThatHasQueryStartTime1000 = new HashSet<>();
        profileThatHasQueryStartTime1000.add("Default 5");
        profileThatHasQueryStartTime1000.add("Default 6");
        Set<String> profileThatHasDefaultQueryFinishTime = new HashSet<>();
        profileThatHasDefaultQueryFinishTime.add("Default 1");
        profileThatHasDefaultQueryFinishTime.add("Default 2");
        profileThatHasDefaultQueryFinishTime.add("Default 5");
        profileThatHasDefaultQueryFinishTime.add("Default 6");
        Set<String> profileThatHasDefaultQueryStartTime = new HashSet<>();
        profileThatHasDefaultQueryStartTime.add("Default 1");
        profileThatHasDefaultQueryStartTime.add("Default 2");
        profileThatHasDefaultQueryStartTime.add("Default 3");
        profileThatHasDefaultQueryStartTime.add("Default 4");


        // Profile should be ordered by query finish time in descending order.
        // Meas that the profile with the latest query finish time should be at the top of the queue.
        PriorityQueue<ProfileManager.ProfileElement> orderedResults = profileManager.getProfileOrderByQueryFinishTimeDesc();
        assert orderedResults != null;
        assert !orderedResults.isEmpty();
        Assertions.assertEquals(106, orderedResults.size(), profileManager.getDebugInfo());

        for (int i = 0; i < profileThatHasDefaultQueryFinishTime.size(); i++) {
            ProfileManager.ProfileElement result = orderedResults.poll();
            Assertions.assertNotEquals(result, null);
            Assertions.assertTrue(profileThatHasDefaultQueryFinishTime.contains(result.profile.getId()));
        }
        for (int i = 0; i < profileThatHasQueryFinishTime1000.size(); i++) {
            ProfileManager.ProfileElement result = orderedResults.poll();
            Assertions.assertNotEquals(result, null);
            Assertions.assertTrue(profileThatHasQueryFinishTime1000.contains(result.profile.getId()));
        }

        long prevQueryFinishTime = 1000L;
        for (int i = 0; i < normalProfiles; i++) {
            ProfileManager.ProfileElement result = orderedResults.poll();
            Assertions.assertNotEquals(result, null);
            Assertions.assertTrue(result.profile.getQueryFinishTimestamp() <= prevQueryFinishTime);
            prevQueryFinishTime = result.profile.getQueryFinishTimestamp();
        }

        orderedResults = profileManager.getProfileOrderByQueryFinishTime();
        Assertions.assertEquals(orderedResults.size(), 106);
        // Profile should be ordered by query finish time in ascending order.
        prevQueryFinishTime = Long.MIN_VALUE;
        for (int i = 0; i < normalProfiles; i++) {
            ProfileManager.ProfileElement result = orderedResults.poll();
            Assertions.assertNotEquals(result, null);
            Assertions.assertTrue(result.profile.getQueryFinishTimestamp() >= prevQueryFinishTime);
            prevQueryFinishTime = result.profile.getQueryFinishTimestamp();
        }
        for (int i = 0; i < profileThatHasQueryFinishTime1000.size(); i++) {
            ProfileManager.ProfileElement result = orderedResults.poll();
            Assertions.assertNotEquals(result, null);
            Assertions.assertTrue(profileThatHasQueryFinishTime1000.contains(result.profile.getId()));
        }
        for (int i = 0; i < profileThatHasDefaultQueryFinishTime.size(); i++) {
            ProfileManager.ProfileElement result = orderedResults.poll();
            Assertions.assertNotEquals(result, null);
            Assertions.assertTrue(profileThatHasDefaultQueryFinishTime.contains(result.profile.getId()));
        }

        orderedResults = profileManager.getProfileOrderByQueryStartTime();
        Assertions.assertEquals(orderedResults.size(), 106);
        // Profile should be ordered by query start time in ascending order.
        long prevQueryStartTime = -1;
        for (int i = 0; i < profileThatHasDefaultQueryStartTime.size(); i++) {
            ProfileManager.ProfileElement result = orderedResults.poll();
            Assertions.assertNotEquals(result, null);
            Assertions.assertTrue(profileThatHasDefaultQueryStartTime.contains(result.profile.getId()),
                                result.profile.getId() + " " + result.profile.getSummaryProfile().getQueryBeginTime());
        }

        for (int i = 0; i < normalProfiles; i++) {
            ProfileManager.ProfileElement result = orderedResults.poll();
            Assertions.assertNotEquals(result, null);
            Assertions.assertTrue(result.profile.getSummaryProfile().getQueryBeginTime() >= prevQueryStartTime);
            prevQueryStartTime = result.profile.getSummaryProfile().getQueryBeginTime();
        }

        for (int i = 0; i < profileThatHasQueryStartTime1000.size(); i++) {
            ProfileManager.ProfileElement result = orderedResults.poll();
            Assertions.assertNotEquals(result, null);
            Assertions.assertTrue(profileThatHasQueryStartTime1000.contains(result.profile.getId()),
                                result.profile.getId() + " " + result.profile.getSummaryProfile().getQueryBeginTime());
        }
    }

    @Test
    void getProfileByOrderParallel() throws InterruptedException {
        // Test the parallel case.
        // Create a thread pool with 3 threads.
        final int threadNum = 3;
        List<Thread> threads = new ArrayList<>();
        AtomicBoolean stopFlag = new AtomicBoolean(false);

        // These threads keep adding profiles to the profile manager.
        // The profile they create has random name, random query finish time and random query start time.
        for (int i = 0; i < threadNum; i++) {
            threads.add(new Thread(() -> {
                Random random = new Random();
                for (int j = 0; j < 100; j++) {
                    Profile profile = constructProfile(String.valueOf(random.nextInt(1000)));
                    profile.getSummaryProfile().setQueryBeginTime(random.nextInt(1000));
                    profile.setQueryFinishTimestamp(random.nextInt(2000) + 1000);
                    profileManager.pushProfile(profile);
                }
            }));
        }
        // Create another thread to get the profile by different order.
        for (int i = 0; i < threadNum; i++) {
            threads.add(new Thread(() -> {
                while (!stopFlag.get()) {
                    PriorityQueue<ProfileManager.ProfileElement> orderedResults = profileManager.getProfileOrderByQueryFinishTimeDesc();
                    long prevQueryFinishTime = Long.MAX_VALUE;
                    while (!orderedResults.isEmpty()) {
                        ProfileManager.ProfileElement result = orderedResults.poll();
                        Assertions.assertTrue(result.profile.getQueryFinishTimestamp() <= prevQueryFinishTime);
                        prevQueryFinishTime = result.profile.getQueryFinishTimestamp();
                    }
                }
            }));
        }

        for (int i = 0; i < threadNum; i++) {
            threads.add(new Thread(() -> {
                while (!stopFlag.get()) {
                    PriorityQueue<ProfileManager.ProfileElement> orderedResults = profileManager.getProfileOrderByQueryStartTime();
                    long prevQueryStartTime = -1;
                    while (!orderedResults.isEmpty()) {
                        ProfileManager.ProfileElement result = orderedResults.poll();
                        Assertions.assertTrue(result.profile.getSummaryProfile().getQueryBeginTime() >= prevQueryStartTime);
                        prevQueryStartTime = result.profile.getSummaryProfile().getQueryBeginTime();
                    }
                }
            }));
        }

        for (int i = 0; i < threadNum; i++) {
            threads.add(new Thread(() -> {
                while (!stopFlag.get()) {
                    PriorityQueue<ProfileManager.ProfileElement> orderedResults = profileManager.getProfileOrderByQueryFinishTime();
                    long prevQueryFinishTime = Long.MIN_VALUE;
                    while (!orderedResults.isEmpty()) {
                        ProfileManager.ProfileElement result = orderedResults.poll();
                        Assertions.assertTrue(result.profile.getQueryFinishTimestamp() >= prevQueryFinishTime);
                        prevQueryFinishTime = result.profile.getQueryFinishTimestamp();
                    }
                }
            }));
        }

        for (Thread thread : threads) {
            thread.start();
        }

        Thread.sleep(5000);

        stopFlag.set(true);

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    void cleanProfileTest() {
        // Create and push profile
        final int normalProfiles = 100;
        for (int i = 0; i < normalProfiles; i++) {
            Profile profile = constructProfile(String.valueOf(i));
            Random random = new Random();
            profile.setQueryFinishTimestamp(random.nextInt(200 - 101) + 101);
            // set query start time in range of [0, 1000)
            profile.getSummaryProfile().setQueryBeginTime(random.nextInt(100));
            profileManager.pushProfile(profile);
        }
        // Clean profile
        profileManager.cleanProfile();
        // Make sure map is cleaned.
        Assertions.assertTrue(profileManager.queryIdToProfileMap.isEmpty());
        Assertions.assertTrue(profileManager.queryIdToExecutionProfiles.isEmpty());
    }

    @Test
    void addExecutionProfileTest() {
        final int normalProfiles = 100;
        for (int i = 0; i < normalProfiles; i++) {
            Profile profile = constructProfile(String.valueOf(i));
            Random random = new Random();
            profile.setQueryFinishTimestamp(random.nextInt(200 - 101) + 101);
            profile.getSummaryProfile().setQueryBeginTime(random.nextInt(100));
            UUID taskId = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            List<Integer> fragments = new ArrayList<>();
            ExecutionProfile executionProfile = new ExecutionProfile(queryId, fragments);
            profile.addExecutionProfile(executionProfile);
            if (i == normalProfiles - 1) {
                profileManager.addExecutionProfile(null);
            } else {
                for (ExecutionProfile executionProfileTemp : profile.getExecutionProfiles()) {
                    profileManager.addExecutionProfile(executionProfileTemp);
                }
            }
        }

        Assertions.assertEquals(normalProfiles - 1, profileManager.queryIdToExecutionProfiles.size());
    }

    @Test
    void getOnStorageProfileInfosTest() throws Exception {
        // Create some test profile files
        for (int i = 0; i < 3; i++) {
            UUID taskId = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            File profileFile = new File(tempDir, System.currentTimeMillis() + '_' + DebugUtil.printId(queryId));
            profileFile.createNewFile();
        }

        // Get profiles from storage
        List<String> profiles = profileManager.getOnStorageProfileInfos();

        // Verify result
        Assertions.assertEquals(3, profiles.size());
        for (String profile : profiles) {
            Assertions.assertTrue(profile.startsWith(tempDir.getAbsolutePath()));
        }
    }

    @Test
    void testLoadProfile() throws IOException {
        profileManager.isProfileLoaded = false;

        try {
            // Create some test profile files
            for (int i = 0; i < 30; i++) {
                Thread.sleep(200);
                Profile profile = ProfilePersistentTest.constructRandomProfile(1);
                profile.writeToStorage(ProfileManager.PROFILE_STORAGE_PATH);
            }

            profileManager.loadProfilesFromStorageIfFirstTime(true);
            Assertions.assertTrue(profileManager.isProfileLoaded);
            Assertions.assertEquals(30, profileManager.queryIdToProfileMap.size());
            Assertions.assertEquals(0, profileManager.queryIdToExecutionProfiles.size());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testGetProfilesNeedStore() throws InterruptedException {
        // Create some test profile files
        for (int i = 0; i < 30; i++) {
            // Sleep 200 ms, so that query finish time is different.
            Thread.sleep(100);
            Profile profile = ProfilePersistentTest.constructRandomProfile(1);
            profile.isQueryFinished = true;
            profile.setQueryFinishTimestamp(System.currentTimeMillis());
            UUID taskId = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            List<Integer> fragments = new ArrayList<>();
            profile.addExecutionProfile(new ExecutionProfile(queryId, fragments));
            if (i % 2 == 0) {
                new Expectations(profile) {
                    {
                        profile.shouldStoreToStorage();
                        result = true;
                    }
                };
            } else {
                new Expectations(profile) {
                    {
                        profile.shouldStoreToStorage();
                        result = false;
                    }
                };
            }
            profileManager.pushProfile(profile);
        }

        List<ProfileElement> profiles = profileManager.getProfilesNeedStore();

        // Verify result
        Assertions.assertEquals(30 / 2, profiles.size());
    }

    @Test
    void testWriteProfileToStorage() throws InterruptedException {
        // Create some test profile files
        for (int i = 0; i < 30; i++) {
            // Sleep 200 ms, so that query finish time is different.
            Thread.sleep(100);
            Profile profile = ProfilePersistentTest.constructRandomProfile(1);
            profile.isQueryFinished = true;
            profile.setQueryFinishTimestamp(System.currentTimeMillis());
            UUID taskId = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            List<Integer> fragments = new ArrayList<>();
            profile.addExecutionProfile(new ExecutionProfile(queryId, fragments));
            for (ExecutionProfile executionProfile : profile.getExecutionProfiles()) {
                profileManager.addExecutionProfile(executionProfile);
            }

            // Make sure all profile is released
            new Expectations(profile) {
                {
                    profile.shouldStoreToStorage();
                    result = true;
                    profile.releaseMemory();
                    times = 1;
                }
            };

            profileManager.pushProfile(profile);
        }

        profileManager.writeProfileToStorage();

        // Verify result
        File[] files = tempDir.listFiles();
        assert files != null;
        Assertions.assertEquals(30, files.length);
        Assertions.assertEquals(30, profileManager.queryIdToProfileMap.size());
        Assertions.assertEquals(0, profileManager.queryIdToExecutionProfiles.size());
    }

    @Test
    void testGetProfilesToBeRemoved() throws IOException {
        int originMaxSpilledProfileNum = Config.max_spilled_profile_num;

        try {
            Config.max_spilled_profile_num = 10;

            // Create some test profile files
            for (int i = 0; i < 30; i++) {
                // Sleep 200 ms, so that query finish time is different.
                Thread.sleep(100);
                Profile profile = ProfilePersistentTest.constructRandomProfile(1);
                profile.isQueryFinished = true;
                profile.setQueryFinishTimestamp(System.currentTimeMillis());
                UUID taskId = UUID.randomUUID();
                TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
                List<Integer> fragments = new ArrayList<>();
                profile.addExecutionProfile(new ExecutionProfile(queryId, fragments));
                for (ExecutionProfile executionProfile : profile.getExecutionProfiles()) {
                    profileManager.addExecutionProfile(executionProfile);
                }
                new Expectations(profile) {
                    {
                        profile.profileHasBeenStored();
                        result = true;
                    }
                };

                profileManager.pushProfile(profile);
            }

            List<ProfileElement> remove = profileManager.getProfilesToBeRemoved();

            // Verify result
            Assertions.assertEquals(remove.size(), 30 - Config.max_spilled_profile_num);
            PriorityQueue<ProfileElement> notRemove = profileManager.getProfileOrderByQueryFinishTimeDesc();
            List<ProfileElement> notRemove2 = Lists.newArrayList();
            for (int i = 0; i < Config.max_spilled_profile_num; i++) {
                notRemove2.add(notRemove.poll());
            }

            for (ProfileElement profileElement : notRemove2) {
                long timestamp = profileElement.profile.getQueryFinishTimestamp();
                for (ProfileElement removeProfile : remove) {
                    // Make sure timestamp is larger than all removed profile.
                    Assertions.assertTrue(timestamp > removeProfile.profile.getQueryFinishTimestamp());
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            Config.max_spilled_profile_num = originMaxSpilledProfileNum;
        }
    }

    @Test
    void testDeleteOutdatedProfilesFromStorage() throws IOException {
        int originMaxSpilledProfileNum = Config.max_spilled_profile_num;

        try {
            Config.max_spilled_profile_num = 10;

            // Create test profiles
            for (int i = 0; i < 30; i++) {
                Thread.sleep(100);
                Profile profile = ProfilePersistentTest.constructRandomProfile(1);
                profile.isQueryFinished = true;
                profile.setQueryFinishTimestamp(System.currentTimeMillis());
                int finalI = i;
                new Expectations(profile) {
                    {
                        profile.profileHasBeenStored();
                        result = true;
                        profile.deleteFromStorage();
                        times = finalI < 20 ? 1 : 0; // First 20 should be deleted
                    }
                };

                profileManager.pushProfile(profile);
            }

            // Execute deletion
            profileManager.isProfileLoaded = true;
            profileManager.deleteOutdatedProfilesFromStorage();

            // Verify correct profiles were deleted
            Assertions.assertEquals(10, profileManager.queryIdToProfileMap.size());

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            Config.max_spilled_profile_num = originMaxSpilledProfileNum;
        }
    }

    @Test
    void testGetBrokenProfiles() throws IOException {
        // Create normal profiles
        for (int i = 0; i < 3; i++) {
            UUID taskId = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            String profileId = DebugUtil.printId(queryId);

            // Create profile in memory
            Profile profile = constructProfile(profileId);
            profileManager.pushProfile(profile);

            // Create profile file
            File profileFile = new File(tempDir, System.currentTimeMillis() + "_" + profileId + ".zip");
            profileFile.createNewFile();
        }

        // Create broken profiles (no corresponding memory entry)
        for (int i = 0; i < 2; i++) {
            UUID taskId = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            File brokenFile = new File(tempDir, System.currentTimeMillis() + "_" + DebugUtil.printId(queryId));
            brokenFile.createNewFile();
        }

        // Get broken profiles
        List<String> brokenProfiles = profileManager.getBrokenProfiles();

        // Verify result - should find 2 broken profiles
        Assertions.assertEquals(2, brokenProfiles.size());
        for (String profile : brokenProfiles) {
            Assertions.assertTrue(profile.startsWith(tempDir.getAbsolutePath()));
        }
    }

    @Test
    void testDeleteBrokenProfiles() throws IOException {
        // Create normal and broken profile files
        List<File> normalFiles = new ArrayList<>();
        List<File> brokenFiles = new ArrayList<>();

        // Create normal profiles with memory entries
        for (int i = 0; i < 3; i++) {
            UUID taskId = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            String profileId = DebugUtil.printId(queryId);

            Profile profile = constructProfile(profileId);
            profileManager.pushProfile(profile);

            File normalFile = new File(tempDir, System.currentTimeMillis() + "_" + profileId + ".zip");
            normalFile.createNewFile();
            normalFiles.add(normalFile);
        }

        // Create broken profiles (no memory entries)
        for (int i = 0; i < 2; i++) {
            UUID taskId = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            File brokenFile = new File(tempDir, System.currentTimeMillis() + "_" + DebugUtil.printId(queryId));
            brokenFile.createNewFile();
            brokenFiles.add(brokenFile);
        }

        // Delete broken profiles
        profileManager.isProfileLoaded = true;
        profileManager.deleteBrokenProfiles();

        // Verify normal files still exist
        for (File file : normalFiles) {
            Assertions.assertTrue(file.exists());
        }

        // Verify broken files were deleted
        for (File file : brokenFiles) {
            Assertions.assertFalse(file.exists());
        }
    }

    @Test
    public void testConcurrentLoadFromStorage() throws Exception {
        // Create multiple test profiles and save them to storage
        int numProfiles = 50;
        for (int i = 0; i < numProfiles; i++) {
            UUID taskId = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            String profileId = DebugUtil.printId(queryId);

            // Create profile in memory
            Profile profile = constructProfile(profileId);
            profile.writeToStorage(ProfileManager.PROFILE_STORAGE_PATH);
        }

        // Test concurrent loading
        profileManager.loadProfilesFromStorageIfFirstTime(true);

        // Verify all profiles are loaded
        Assertions.assertEquals(numProfiles, profileManager.queryIdToProfileMap.size());
    }

    @Test
    public void testProfileStorageLimit() throws Exception {
        // Set small storage limit
        Config.spilled_profile_storage_limit_bytes = 1000;
        Config.max_spilled_profile_num = 5;

        // Create and store more profiles than the limit
        for (int i = 0; i < 10; i++) {
            UUID taskId = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            String profileId = DebugUtil.printId(queryId);

            // Create profile in memory
            Profile profile = constructProfile(profileId);
            profileManager.pushProfile(profile);
            profile.writeToStorage(ProfileManager.PROFILE_STORAGE_PATH);
            Thread.sleep(100); // Ensure different timestamps
        }

        // Trigger cleanup
        profileManager.isProfileLoaded = true;
        profileManager.deleteOutdatedProfilesFromStorage();

        // Verify number of profiles is within limits
        File storageDir = new File(ProfileManager.PROFILE_STORAGE_PATH);
        Assertions.assertTrue(storageDir.list().length <= Config.max_spilled_profile_num);
    }

    @Test
    public void testBrokenProfileCleanup() throws Exception {
        // Create a valid profile
        UUID taskId = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
        String profileId = DebugUtil.printId(queryId);

        // Create profile in memory
        Profile validProfile = constructProfile(profileId);
        profileManager.pushProfile(validProfile);
        validProfile.writeToStorage(ProfileManager.PROFILE_STORAGE_PATH);

        // Create a broken profile file
        File brokenFile = new File(ProfileManager.PROFILE_STORAGE_PATH, "broken_profile");
        brokenFile.createNewFile();

        // Trigger cleanup
        profileManager.isProfileLoaded = true;
        profileManager.deleteBrokenProfiles();

        // Verify broken profile is removed but valid one remains
        File[] remainingFiles = new File(ProfileManager.PROFILE_STORAGE_PATH).listFiles();
        Assertions.assertEquals(1, remainingFiles.length);
        Assertions.assertTrue(remainingFiles[0].getName().contains(profileId));
    }

    @Test
    public void testMemoryProfileLimit() {
        Config.max_query_profile_num = 3;
        List<String> profilesToKeep = new ArrayList<>();

        // Add more profiles than the limit
        for (int i = 0; i < 5; i++) {
            UUID taskId = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            String profileId = DebugUtil.printId(queryId);

            // Create profile in memory
            Profile profile = constructProfile(profileId);
            profile.setQueryFinishTimestamp(System.currentTimeMillis() + i * 1000); // Different finish times
            if (i >= Config.max_query_profile_num) {
                profilesToKeep.add(profileId);
            }
            profileManager.pushProfile(profile);
        }

        // Trigger cleanup
        profileManager.deleteOutdatedProfilesFromMemory(0);

        // Verify memory profile count is within limit
        Assertions.assertEquals(Config.max_query_profile_num, profileManager.queryIdToProfileMap.size());

        // Verify newest profiles are kept
        for (String profileId : profilesToKeep) {
            Assertions.assertTrue(profileManager.queryIdToProfileMap.containsKey(profileId));
        }
    }

    @Test
    public void testDeleteOutdatedProfilesWhenExceedLimit() {
        List<Profile> profiles = new ArrayList<>();

        Config.max_query_profile_num = 3;
        for (int i = 0; i < 5; i++) {
            UUID taskId = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            String profileId = DebugUtil.printId(queryId);
            Profile profile = constructProfile(profileId);
            profile.setQueryFinishTimestamp(System.currentTimeMillis() + i * 1000); // Different finish times
            profiles.add(profile);
        }
        for (int i = 0; i < 3; i++) {
            Profile profile = profiles.get(i);
            profileManager.pushProfile(profile);
        }
        // Verify profile count
        Assertions.assertEquals(3, profileManager.queryIdToProfileMap.size());

        // Try to delete outdated profiles
        profileManager.deleteOutdatedProfilesFromMemory(0);

        // Verify no profiles were deleted
        Assertions.assertEquals(3, profileManager.queryIdToProfileMap.size());

        for (int i = 3; i < 5; i++) {
            Profile profile = profiles.get(i);
            profileManager.pushProfile(profile);
        }

        for (int i = 0; i < 5; i++) {
            Profile profile = profiles.get(i);
            if (i <= 1) {
                Assertions.assertFalse(profileManager.queryIdToProfileMap.containsKey(profile.getId()));
            } else {
                Assertions.assertTrue(profileManager.queryIdToProfileMap.containsKey(profile.getId()));
            }
        }

        profiles.clear();

        Assertions.assertEquals(3, profileManager.queryIdToProfileMap.size());
        for (Profile profile : profiles) {
            Assertions.assertTrue(profileManager.queryIdToProfileMap.containsKey(profile.getId()));
        }
    }

    @Test
    public void testUnfinishedProfilesNotDeleted() {
        // Create an unfinished profile (finish time = Long.MAX_VALUE)
        List<Profile> profileUnfinished = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            UUID taskId = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            String profileId = DebugUtil.printId(queryId);
            Profile profile = constructProfile(profileId);
            profileUnfinished.add(profile);
            profileManager.pushProfile(profile);
        }

        List<Profile> profileFinished = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            UUID taskId = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            String profileId = DebugUtil.printId(queryId);
            Profile profile = constructProfile(profileId);
            profile.setQueryFinishTimestamp(System.currentTimeMillis() + i * 1000); // Different finish times
            profileFinished.add(profile);
            profileManager.pushProfile(profile);
        }

        // Try to delete outdated profiles
        profileManager.deleteOutdatedProfilesFromMemory(0);

        // Verify unfinished profile was not deleted
        for (Profile profile : profileUnfinished) {
            Assertions.assertTrue(profileManager.queryIdToProfileMap.containsKey(profile.getId()));
        }

        Assertions.assertEquals(5, profileManager.queryIdToProfileMap.size());

        profileFinished.clear();
        for (int i = 0; i < 5; i++) {
            UUID taskId = UUID.randomUUID();
            TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
            String profileId = DebugUtil.printId(queryId);
            Profile profile = constructProfile(profileId);
            profile.setQueryFinishTimestamp(System.currentTimeMillis() + i * 1000); // Different finish times
            profileFinished.add(profile);
            profileManager.pushProfile(profile);
        }

        // Verify unfinished profile was not deleted
        for (Profile profile : profileUnfinished) {
            Assertions.assertTrue(profileManager.queryIdToProfileMap.containsKey(profile.getId()));
        }
    }
}
