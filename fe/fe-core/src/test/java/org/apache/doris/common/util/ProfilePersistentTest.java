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

import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.profile.SummaryProfile.SummaryBuilder;
import org.apache.doris.thrift.QueryState;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUnit;

import com.google.common.base.Strings;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

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
        profile.setId(stringUniqueId);
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
    public void compressBasicTest() {
        // Initialize StringBuilder for faster string construction
        StringBuilder executionProfileTextBuilder = new StringBuilder(1024 * 1024);
        // Populate the StringBuilder with random characters
        for (int i = 0; i < 1024 * 1024; i++) {
            executionProfileTextBuilder.append((char) (Math.random() * 26 + 'a'));
        }
        // Convert StringBuilder to String
        String executionProfileText = executionProfileTextBuilder.toString();

        byte[] compressed = null;
        try {
            compressed = Profile.compressExecutionProfile(executionProfileText);
        } catch (IOException e) {
            LOG.error("Failed to compress execution profile: {}", e.getMessage(), e);
            Assert.fail();
        }
        String executionProfileTextDecompressed = null;
        try {
            executionProfileTextDecompressed = Profile.decompressExecutionProfile(compressed);
        } catch (IOException e) {
            LOG.error("Failed to decompress execution profile: {}", e.getMessage(), e);
            Assert.fail();
        }
        Assert.assertEquals(executionProfileText, executionProfileTextDecompressed);
    }

    @Test
    public void counterBasicTest() {
        TUnit thriftType = TUnit.TIME_NS;
        long value = 1000;
        Counter counter = new Counter(thriftType, value);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(baos);
        boolean writeFailed = false;

        try {
            counter.write(output);
        } catch (Exception e) {
            writeFailed = true;
        }

        Assert.assertFalse(writeFailed);

        byte[] data = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInput input = new DataInputStream(bais);

        boolean readFailed = false;
        Counter deserializedCounter = null;
        try {
            deserializedCounter = Counter.read(input);
        } catch (Exception e) {
            readFailed = true;
        }

        Assert.assertFalse(readFailed);
        Assert.assertEquals(deserializedCounter.getValue(), counter.getValue());
        Assert.assertEquals(deserializedCounter.getType(), counter.getType());
        Assert.assertEquals(deserializedCounter.toString(), counter.toString());
    }

    @Test
    public void runtimeProfileBasicTest() {
        RuntimeProfile profile = new RuntimeProfile("profile");
        for (int i = 0; i < 5; i++) {
            if (i == 0) {
                profile.addCounter(String.valueOf(i), TUnit.BYTES, RuntimeProfile.ROOT_COUNTER);
            } else {
                profile.addCounter(String.valueOf(i), TUnit.BYTES, String.valueOf(i - 1));
            }
        }

        // 1 second
        profile.getCounterTotalTime().setValue(1000 * 1000 * 1000);
        profile.computeTimeInProfile();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(baos);
        boolean writeFailed = false;

        try {
            profile.write(output);
        } catch (Exception e) {
            writeFailed = true;
        }

        Assert.assertFalse(writeFailed);

        byte[] data = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInput input = new DataInputStream(bais);

        boolean readFailed = false;
        RuntimeProfile deserializedProfile = null;
        try {
            deserializedProfile = RuntimeProfile.read(input);
        } catch (Exception e) {
            readFailed = true;
        }

        Assert.assertFalse(readFailed);
        Assert.assertEquals(profile.getName(), deserializedProfile.getName());
        Assert.assertEquals(profile.getCounterTotalTime(), deserializedProfile.getCounterTotalTime());

        for (Entry<String, Counter> entry : profile.getCounterMap().entrySet()) {
            String key = entry.getKey();
            Counter counter = entry.getValue();
            Counter deserializedCounter = deserializedProfile.getCounterMap().get(key);
            Assert.assertEquals(counter, deserializedCounter);
        }

        StringBuilder builder1 = new StringBuilder();
        profile.prettyPrint(builder1, "");
        StringBuilder builder2 = new StringBuilder();
        deserializedProfile.prettyPrint(builder2, "");
        Assert.assertEquals(builder1.toString(), builder2.toString());
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
    public void profileBasicTest() {
        final int executionProfileNum = 5;
        Profile profile = constructRandomProfile(executionProfileNum);

        // after profile is stored to disk, futher read will be from disk
        // so we store the original answer to a string
        String profileContentString = profile.getProfileByLevel();
        String currentBinaryWorkingDir = System.getProperty("user.dir");
        String profileStoragePath = currentBinaryWorkingDir + File.separator + "doris-feut-profile";
        File profileDir = new File(profileStoragePath);
        if (!profileDir.exists()) {
            // create query_id directory
            if (!profileDir.mkdir()) {
                LOG.warn("create profile directory {} failed", profileDir.getAbsolutePath());
                Assert.fail();
                return;
            }
        }

        try {
            profile.writeToStorage(profileStoragePath);

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
            FileUtils.deleteQuietly(profileDir);
        } finally {
            try {
                FileUtils.deleteDirectory(profileDir);
            } catch (Exception e) {
                LOG.warn("delete profile directory {} failed", profileDir.getAbsolutePath());
                Assert.fail();
            }
        }
    }
}
