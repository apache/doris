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

import org.apache.doris.common.util.ProfilePersistentTest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

class ProfileManagerTest {
    // We need a logger.
    private static final Logger LOG = LogManager.getLogger(ProfilePersistentTest.class);

    private static ProfileManager profileManager;

    @BeforeAll
    static void setUp() {
        profileManager = new ProfileManager();
    }

    @BeforeEach
    void cleanProfile() {
        profileManager.cleanProfile();
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
}
