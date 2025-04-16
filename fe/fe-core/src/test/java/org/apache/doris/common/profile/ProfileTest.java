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
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TUniqueId;

import mockit.Expectations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ProfileTest {
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private Profile profile;
    private File tempDir;
    private ExecutionProfile executionProfile;
    private String testProfileStoragePath;

    @BeforeEach
    public void setUp() throws IOException {
        profile = ProfilePersistentTest.constructRandomProfile(1);
        // Setup a temporary directory for profile storage
        tempDir = Files.createTempDirectory("profile_test_").toFile();
        testProfileStoragePath = tempDir.getAbsolutePath();
        executionProfile = profile.getExecutionProfiles().get(0);
    }

    @AfterEach
    public void tearDown() {
        ProfileManager.getInstance().removeProfile(profile.getId());
    }

    @Test
    public void testBasicProfileCreation() {
        Assertions.assertNotNull(profile);
        Assertions.assertFalse(profile.isQueryFinished);
        Assertions.assertEquals(1, profile.getExecutionProfiles().size());
    }

    @Test
    public void testUpdateSummary() {
        Map<String, String> summaryInfo = new HashMap<>();
        summaryInfo.put("TestKey", "TestValue");

        profile.updateSummary(summaryInfo, false, null);

        Assertions.assertFalse(profile.isQueryFinished);

        profile.updateSummary(summaryInfo, true, null);
        Assertions.assertTrue(profile.isQueryFinished);
        Assertions.assertTrue(Long.MAX_VALUE != profile.getQueryFinishTimestamp());
    }

    @Test
    public void testShouldStoreToStorage() {
        // Initially not finished, should not store
        Assertions.assertFalse(profile.shouldStoreToStorage());

        // Mark as finished
        profile.markQueryFinished();

        // Execution profile is not completed yet
        Assertions.assertFalse(executionProfile.isCompleted());

        // Should still not store because execution profile isn't complete
        // and time hasn't passed the threshold
        Assertions.assertFalse(profile.shouldStoreToStorage());


        new Expectations(executionProfile) {
            {
                executionProfile.isCompleted();
                result = true;
            }
        };
        // Now it should be ready to store
        Assertions.assertTrue(profile.shouldStoreToStorage());
    }

    @Test
    public void testWriteToStorage() {
        // Prepare for storage
        profile.markQueryFinished();
        profile.setQueryFinishTimestamp(System.currentTimeMillis());
        new Expectations(executionProfile) {
            {
                executionProfile.isCompleted();
                result = true;
            }
        };

        // Should be true before we write
        Assertions.assertTrue(profile.shouldStoreToStorage());
        Assertions.assertFalse(profile.profileHasBeenStored());

        // Write to storage
        profile.writeToStorage(testProfileStoragePath);

        // Verify it's stored
        Assertions.assertTrue(profile.profileHasBeenStored());
        Assertions.assertNotNull(profile.getProfileStoragePath());
        Assertions.assertTrue(new File(profile.getProfileStoragePath()).exists());
        Assertions.assertTrue(profile.getProfileSize() > 0);
    }

    @Test
    public void testWriteToStorageWithIncompletedExecution() {
        // Prepare for storage
        profile.markQueryFinished();
        profile.setQueryFinishTimestamp(System.currentTimeMillis());

        // Mock that execution profile is not completed
        new Expectations(executionProfile) {
            {
                executionProfile.isCompleted();
                result = false;
            }
        };

        // Should be false before we write because execution profile isn't complete
        Assertions.assertFalse(profile.shouldStoreToStorage());
        Assertions.assertFalse(profile.profileHasBeenStored());

        // Sleep to simulate time passing
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        int orig = Config.profile_waiting_time_for_spill_seconds;
        Config.profile_waiting_time_for_spill_seconds = 1;
        Assertions.assertTrue(profile.shouldStoreToStorage());
        Config.profile_waiting_time_for_spill_seconds = orig;
    }

    @Test
    public void testReadFromStorage() throws IOException {
        profile.markQueryFinished();
        // First write to storage
        profile.setQueryFinishTimestamp(System.currentTimeMillis());
        profile.writeToStorage(testProfileStoragePath);

        // Now read it back
        Profile readProfile = Profile.read(profile.getProfileStoragePath());

        // Verify read profile
        Assertions.assertNotNull(readProfile);
        Assertions.assertEquals(profile.getId(), readProfile.getId());
        Assertions.assertTrue(readProfile.isQueryFinished);
        Assertions.assertTrue(readProfile.profileHasBeenStored());
    }

    @Test
    public void testDeleteFromStorage() throws IOException {
        // First write to storage
        profile.markQueryFinished();
        profile.setQueryFinishTimestamp(System.currentTimeMillis());
        profile.writeToStorage(testProfileStoragePath);

        String storagePath = profile.getProfileStoragePath();
        Assertions.assertTrue(new File(storagePath).exists());

        // Now delete it
        profile.deleteFromStorage();

        // Verify it's gone
        Assertions.assertFalse(new File(storagePath).exists());
    }

    @Test
    public void testCreateProfileFileInputStream() throws IOException {
        // First write to storage
        profile.markQueryFinished();
        profile.setQueryFinishTimestamp(System.currentTimeMillis());
        profile.writeToStorage(testProfileStoragePath);

        // Test with valid path
        FileInputStream fis = Profile.createPorfileFileInputStream(profile.getProfileStoragePath());
        Assertions.assertNotNull(fis);
        fis.close();

        // Test with invalid path
        FileInputStream invalidFis = Profile.createPorfileFileInputStream("/invalid/path/to/profile.zip");
        Assertions.assertNull(invalidFis);
    }

    @Test
    public void testParseProfileFileName() {
        // Valid profile name
        long timestamp = System.currentTimeMillis();
        UUID taskId = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
        String id = DebugUtil.printId(queryId);
        String validName = timestamp + "_" + id + ".zip";

        String[] parts = Profile.parseProfileFileName(validName);
        Assertions.assertNotNull(parts);
        Assertions.assertEquals(2, parts.length);
        Assertions.assertEquals(String.valueOf(timestamp), parts[0]);
        Assertions.assertEquals(id, parts[1]);

        // Invalid profile name
        String invalidName = "not_a_valid_profile_name";
        Assertions.assertNull(Profile.parseProfileFileName(invalidName));

        // Wrong extension
        String wrongExtension = timestamp + "_" + id + ".txt";
        Assertions.assertNull(Profile.parseProfileFileName(wrongExtension));
    }

    @Test
    public void testGetOnStorageProfile() throws IOException {
        // First write to storage
        profile.markQueryFinished();
        profile.setQueryFinishTimestamp(System.currentTimeMillis());
        profile.writeToStorage(testProfileStoragePath);
        profile.releaseMemory();
        StringBuilder builder = new StringBuilder();
        profile.getOnStorageProfile(builder);

        // Verify we got content
        Assertions.assertTrue(builder.length() > 0);
    }

    @Test
    public void testReleaseMemory() {
        Assertions.assertEquals(1, profile.getExecutionProfiles().size());
        profile.setChangedSessionVar("test=1");

        profile.releaseMemory();

        Assertions.assertEquals(0, profile.getExecutionProfiles().size());
    }
}
