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

import org.apache.doris.common.profile.SummaryProfile.SummaryBuilder;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.thrift.QueryState;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUnit;

import com.google.common.base.Strings;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@ResourceLock("global")

public class ProfilePersistentTest {
    private static final Logger LOG = LogManager.getLogger(ProfilePersistentTest.class);

    public static SummaryProfile constructRandomSummaryProfile() {
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
        builder.taskState(QueryState.RUNNING.toString());
        builder.user(DebugUtil.printId(qUniqueId) + "-user");
        builder.defaultDb(DebugUtil.printId(qUniqueId) + "-db");

        SummaryProfile summaryProfile = new SummaryProfile();
        summaryProfile.fuzzyInit();
        summaryProfile.update(builder.build());

        return summaryProfile;
    }

    public static Profile constructRandomProfile(int executionProfileNum) {
        Profile profile = new Profile();
        SummaryProfile summaryProfile = constructRandomSummaryProfile();
        String stringUniqueId = summaryProfile.getProfileId();
        TUniqueId thriftUniqueId = DebugUtil.parseTUniqueIdFromString(stringUniqueId);
        profile.setSummaryProfile(summaryProfile);

        for (int i = 0; i < executionProfileNum; i++) {
            RuntimeProfile runtimeProfile = new RuntimeProfile("profile-" + i);
            runtimeProfile.addCounter(String.valueOf(0), TUnit.BYTES, RuntimeProfile.ROOT_COUNTER);
            runtimeProfile.addCounter(String.valueOf(1), TUnit.BYTES, String.valueOf(0));
            runtimeProfile.addCounter(String.valueOf(2), TUnit.BYTES, String.valueOf(1));
            runtimeProfile.addCounter(String.valueOf(3), TUnit.BYTES, String.valueOf(2));
            List<Integer> fragmentIds = new ArrayList<>();
            fragmentIds.add(i);

            ExecutionProfile executionProfile = new ExecutionProfile(thriftUniqueId, fragmentIds);
            profile.addExecutionProfile(executionProfile);
        }

        return profile;
    }

    @Test
    public void summaryProfileBasicTest() {
        SummaryProfile summaryProfile = new SummaryProfile();
        summaryProfile.fuzzyInit();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(baos);
        boolean writeFailed = false;

        try {
            summaryProfile.write(output);
        } catch (Exception e) {
            writeFailed = true;
        }

        Assert.assertFalse(writeFailed);

        byte[] data = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInput input = new DataInputStream(bais);

        boolean readFailed = false;
        SummaryProfile deserializedSummaryProfile = null;
        try {
            deserializedSummaryProfile = SummaryProfile.read(input);
        } catch (Exception e) {
            LOG.info("read failed: {}", e.getMessage(), e);
            readFailed = true;
        }
        Assert.assertFalse(readFailed);

        StringBuilder builder1 = new StringBuilder();
        summaryProfile.prettyPrint(builder1);
        StringBuilder builder2 = new StringBuilder();
        deserializedSummaryProfile.prettyPrint(builder2);

        Assert.assertNotEquals("", builder1.toString());
        Assert.assertEquals(builder1.toString(), builder2.toString());

        for (Entry<String, String> entry : summaryProfile.getAsInfoStings().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            String deserializedValue = deserializedSummaryProfile.getAsInfoStings().get(key);
            Assert.assertEquals(value, deserializedValue);
        }
    }

    @Test
    public void profileBasicTest() throws IOException {
        final int executionProfileNum = 5;
        Profile profile = constructRandomProfile(executionProfileNum);

        Path tempDir = Files.createTempDirectory("profile-persistent-test");
        try {
            profile.writeToStorage(tempDir.toString());

            // after profile is stored to disk, futher read will be from disk
            // so we store the original answer to a string
            String profileContentString = profile.getProfileByLevel();
            String profileStoragePathTmp = profile.getProfileStoragePath();
            Assert.assertFalse(Strings.isNullOrEmpty(profileStoragePathTmp));

            LOG.info("Profile storage path: {}", profileStoragePathTmp);

            Profile deserializedProfile = Profile.read(profileStoragePathTmp);
            Assert.assertNotNull(deserializedProfile);
            Assert.assertEquals(profileContentString, profile.getProfileByLevel());
            Assert.assertEquals(profile.getProfileByLevel(), deserializedProfile.getProfileByLevel());

            // make sure file is removed
            profile.deleteFromStorage();
            File tmpFile = new File(profileStoragePathTmp);
            Assert.assertFalse(tmpFile.exists());
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    @Test
    public void testWriteAndReadStorage() throws IOException {
        final int executionProfileNum = 3;
        Profile profile = constructRandomProfile(executionProfileNum);

        Path tempDir = Files.createTempDirectory("profile-persistent-test");
        try {
            // Test writeToStorage
            profile.writeToStorage(tempDir.toString());
            Assert.assertFalse(Strings.isNullOrEmpty(profile.getProfileStoragePath()));
            Assert.assertTrue(new File(profile.getProfileStoragePath()).exists());

            // Test read
            Profile readProfile = Profile.read(profile.getProfileStoragePath());
            Assert.assertNotNull(readProfile);
            Assert.assertEquals(profile.getId(), readProfile.getId());
            Assert.assertEquals(profile.getQueryFinishTimestamp(), readProfile.getQueryFinishTimestamp());

            // Verify content is readable
            StringBuilder builder = new StringBuilder();
            readProfile.getOnStorageProfile(builder);
            Assert.assertFalse(Strings.isNullOrEmpty(builder.toString()));

            // Clean up
            profile.deleteFromStorage();
            Assert.assertFalse(new File(profile.getProfileStoragePath()).exists());
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    @Test
    public void testCreateProfileFileInputStream() throws IOException {
        final int executionProfileNum = 1;
        Profile profile = constructRandomProfile(executionProfileNum);

        Path tempDir = Files.createTempDirectory("profile-persistent-test");
        try {
            // Write profile first
            profile.writeToStorage(tempDir.toString());
            String path = profile.getProfileStoragePath();

            // Test createPorfileFileInputStream
            FileInputStream fis = Profile.createPorfileFileInputStream(path);
            Assert.assertNotNull(fis);
            fis.close();

            // Test with invalid path
            Assert.assertNull(Profile.createPorfileFileInputStream("/invalid/path"));

            // Test with directory
            Assert.assertNull(Profile.createPorfileFileInputStream(tempDir.toString()));

            // Clean up
            profile.deleteFromStorage();
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    @Test
    public void testGetOnStorageProfile() throws IOException {
        final int executionProfileNum = 2;
        Profile profile = constructRandomProfile(executionProfileNum);

        Path tempDir = Files.createTempDirectory("profile-persistent-test");
        try {
            // First get profile content before storage
            StringBuilder beforeStorage = new StringBuilder();
            beforeStorage.append(profile.getProfileByLevel());
            Assert.assertFalse(Strings.isNullOrEmpty(beforeStorage.toString()));

            // Write to storage
            profile.writeToStorage(tempDir.toString());

            // Test getOnStorageProfile
            StringBuilder afterStorage = new StringBuilder();
            afterStorage.append(profile.getProfileByLevel());
            Assert.assertFalse(Strings.isNullOrEmpty(afterStorage.toString()));

            // Content should be same
            Assert.assertEquals(beforeStorage.toString().trim(), afterStorage.toString().trim());

            // Test with corrupted file
            File profileFile = new File(profile.getProfileStoragePath());
            FileUtils.writeStringToFile(profileFile, "corrupted content", StandardCharsets.UTF_8);
            StringBuilder corruptedContent = new StringBuilder();
            profile.getOnStorageProfile(corruptedContent);
            Assert.assertTrue(corruptedContent.toString().contains("Failed to read profile"));

            // Clean up
            profile.deleteFromStorage();
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    @Test
    public void testProfileRead() throws IOException {
        final int executionProfileNum = 1;
        Profile profile = constructRandomProfile(executionProfileNum);

        Path tempDir = Files.createTempDirectory("profile-persistent-test");
        try {
            // Write profile
            profile.writeToStorage(tempDir.toString());

            // Test read with valid path
            Profile readProfile = Profile.read(profile.getProfileStoragePath());
            Assert.assertNotNull(readProfile);
            Assert.assertEquals(profile.getId(), readProfile.getId());

            // Test read with invalid path
            Assert.assertNull(Profile.read("/invalid/path"));

            // Test read with directory
            Assert.assertNull(Profile.read(tempDir.toString()));

            // Test read with corrupted file
            File profileFile = new File(profile.getProfileStoragePath());
            FileUtils.writeStringToFile(profileFile, "corrupted", StandardCharsets.UTF_8);
            Assert.assertNull(Profile.read(profile.getProfileStoragePath()));

            // Clean up
            profile.deleteFromStorage();
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    @Test
    public void testwriteToStorage() throws IOException {
        final int executionProfileNum = 3;
        Profile profile = constructRandomProfile(executionProfileNum);

        Path tempDir = Files.createTempDirectory("profile-persistent-test");
        try {
            // Test writeToStorage
            profile.writeToStorage(tempDir.toString());
            Assert.assertFalse(Strings.isNullOrEmpty(profile.getProfileStoragePath()));
            Assert.assertTrue(new File(profile.getProfileStoragePath()).exists());
            Assert.assertTrue(profile.getProfileStoragePath().endsWith(".zip"));

            // Test write with empty id
            Profile emptyProfile = new Profile();
            emptyProfile.writeToStorage(tempDir.toString());
            Assert.assertTrue(Strings.isNullOrEmpty(emptyProfile.getProfileStoragePath()));

            // Test write already stored profile
            profile.writeToStorage(tempDir.toString());
            Assert.assertTrue(profile.getProfileStoragePath().endsWith(".zip"));

            // Clean up
            profile.deleteFromStorage();
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    @Test
    public void testCreateProfileFileInputStreamWithCorruptedFiles() throws IOException {
        final int executionProfileNum = 1;
        Profile profile = constructRandomProfile(executionProfileNum);

        Path tempDir = Files.createTempDirectory("profile-persistent-test");
        try {
            // Write profile first
            profile.writeToStorage(tempDir.toString());

            // Test with empty file
            File emptyFile = new File(tempDir.toString(), "empty_1234567_abcdef.zip");
            emptyFile.createNewFile();
            Assert.assertNull(Profile.createPorfileFileInputStream(emptyFile.getAbsolutePath()));

            // Test with invalid filename format
            File invalidFile = new File(tempDir.toString(), "invalid_name.zip");
            invalidFile.createNewFile();
            Assert.assertNull(Profile.createPorfileFileInputStream(invalidFile.getAbsolutePath()));

            // Test with non-existing file
            Assert.assertNull(Profile.createPorfileFileInputStream(tempDir + "/non_existing.zip"));

            // Clean up
            profile.deleteFromStorage();
            emptyFile.delete();
            invalidFile.delete();
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    @Test
    public void testGetOnStorageProfileComprehensive() throws IOException {
        final int executionProfileNum = 2;
        Profile profile = constructRandomProfile(executionProfileNum);

        Path tempDir = Files.createTempDirectory("profile-persistent-test");
        try {
            // First get profile content before storage
            StringBuilder beforeStorage = new StringBuilder();
            profile.getExecutionProfileContent(beforeStorage);

            // Write to storage
            profile.writeToStorage(tempDir.toString());

            // Test with non-stored profile
            Profile nonStoredProfile = constructRandomProfile(1);
            StringBuilder nonStoredBuilder = new StringBuilder();
            nonStoredProfile.getOnStorageProfile(nonStoredBuilder);
            Assert.assertEquals("", nonStoredBuilder.toString());

            // Test with invalid zip entry
            File profileFile = new File(profile.getProfileStoragePath());
            FileOutputStream fos = new FileOutputStream(profileFile);
            ZipOutputStream zos = new ZipOutputStream(fos);
            zos.putNextEntry(new ZipEntry("wrong_entry_name"));
            zos.write("test data".getBytes());
            zos.closeEntry();
            zos.close();
            fos.close();

            StringBuilder invalidBuilder = new StringBuilder();
            profile.getOnStorageProfile(invalidBuilder);
            Assert.assertTrue(invalidBuilder.toString().contains("Failed to read profile"));

            // Clean up
            profile.deleteFromStorage();
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    @Test
    public void testProfileReadComprehensive() throws IOException {
        final int executionProfileNum = 1;
        Profile profile = constructRandomProfile(executionProfileNum);

        Path tempDir = Files.createTempDirectory("profile-persistent-test");
        try {
            // Write profile
            profile.writeToStorage(tempDir.toString());

            // Test read with missing entry in zip
            File profileFile = new File(profile.getProfileStoragePath());
            FileOutputStream fos = new FileOutputStream(profileFile);
            ZipOutputStream zos = new ZipOutputStream(fos);
            zos.close();
            Assert.assertNull(Profile.read(profileFile.getAbsolutePath()));

            // Test read with corrupted zip
            FileUtils.writeStringToFile(profileFile, "not a zip file", StandardCharsets.UTF_8);
            Assert.assertNull(Profile.read(profileFile.getAbsolutePath()));

            // Test read with empty file
            FileUtils.writeStringToFile(profileFile, "", StandardCharsets.UTF_8);
            Assert.assertNull(Profile.read(profileFile.getAbsolutePath()));

            // Clean up
            profile.deleteFromStorage();
        } finally {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }
}
