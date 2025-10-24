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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class ProfileArchiveTest {
    private TemporaryFolder tempFolder;
    private String archivePath;
    private ProfileArchiveManager archiveManager;
    private ArchiveStorage archiveStorage;
    private ArchiveIndex archiveIndex;

    // Helper to generate valid queryId in hex-hi-hex-lo format
    private static String genQueryId(int hiSeed, int loSeed) {
        TUniqueId id = new TUniqueId(hiSeed, loSeed);
        return DebugUtil.printId(id);
    }

    private static String genQueryIdForIndex(int i) {
        return genQueryId(0x1000 + i, 0x2000 + i);
    }

    @Before
    public void setUp() throws IOException {
        tempFolder = new TemporaryFolder();
        tempFolder.create();
        archivePath = tempFolder.getRoot().getAbsolutePath();

        // Configure archiving
        Config.enable_profile_archive = true;
        Config.profile_archive_cleanup_interval_seconds = 0; // perform cleanup immediately in tests
        Config.profile_archive_retention_seconds = 7 * 24 * 60 * 60; // default 7 days
        Config.profile_archive_max_size_bytes = 1024 * 1024; // 1MB default for tests
        Config.max_archived_profile_num = 100000; // large default for tests

        archiveStorage = new ArchiveStorage(archivePath);
        archiveIndex = new ArchiveIndex();
        archiveManager = ProfileArchiveManager.getInstance(archivePath);
        archiveManager.loadArchiveIfNeeded();
    }

    @After
    public void tearDown() {
        if (archiveManager != null) {
            archiveManager.shutdown();
        }
        if (tempFolder != null) {
            tempFolder.delete();
        }
    }

    private Profile buildAndStoreProfile(String queryId, String user, String database, long finishTime) {
        Profile profile = new Profile();
        SummaryProfile.SummaryBuilder builder = new SummaryProfile.SummaryBuilder()
                .profileId(queryId)
                .user(user)
                .defaultDb(database)
                .startTime(String.valueOf(finishTime))
                .endTime(String.valueOf(finishTime));
        Map<String, String> info = builder.build();
        profile.updateSummary(info, true, null);
        profile.setQueryFinishTimestamp(finishTime);
        profile.writeToStorage(archivePath);
        return profile;
    }

    @Test
    public void testArchiveCountLimit() throws Exception {
        // Set very small count limit for testing
        Config.max_archived_profile_num = 3; // Only keep 3 profiles

        String userPrefix = "user";
        String dbPrefix = "db";

        // Archive 5 profiles to exceed the count limit
        for (int i = 0; i < 5; i++) {
            String queryId = genQueryIdForIndex(i);
            long finishTime = System.currentTimeMillis() - i * 1000;
            Profile profile = buildAndStoreProfile(queryId, userPrefix + i, dbPrefix + i, finishTime);
            archiveManager.archiveProfile(profile);
        }

        // Trigger cleanup to enforce count limit
        archiveManager.performArchiveCleanup();

        // Check that only the configured number of profiles remain
        Map<String, Object> stats = archiveManager.getArchiveStatistics();
        long totalCount = (Long) stats.get("total_count");
        Assert.assertTrue("Total archive count should be within limit", totalCount <= Config.max_archived_profile_num);

        // Verify that the newest profiles are kept (should be query_0, query_1, query_2)
        for (int i = 0; i < 3; i++) {
            String queryId = genQueryIdForIndex(i);
            Profile profile = archiveManager.getArchivedProfile(queryId);
            Assert.assertNotNull("Recent profile " + queryId + " should still exist", profile);
        }

        // Verify that the oldest profiles are removed (should be query_3, query_4)
        for (int i = 3; i < 5; i++) {
            String queryId = genQueryIdForIndex(i);
            Profile profile = archiveManager.getArchivedProfile(queryId);
            Assert.assertNull("Old profile " + queryId + " should be removed", profile);
        }

        // Reset count limit for other tests
        Config.max_archived_profile_num = 100000;
    }

    @Test
    public void testArchiveStorage() throws Exception {
        String queryId = genQueryIdForIndex(123);
        String user = "test_user";
        String database = "test_db";
        long startTime = System.currentTimeMillis();

        // Create a mock profile for testing
        Profile profile = buildAndStoreProfile(queryId, user, database, startTime);

        // Test writing profile
        ArchiveIndex.ArchiveEntry entry = archiveStorage.writeArchive(profile);
        Assert.assertNotNull("Archive entry should not be null", entry);
        Assert.assertEquals("Query ID should match", queryId, entry.getQueryId());

        // Test reading profile using ArchiveEntry
        Profile readProfile = archiveStorage.readArchive(entry);
        Assert.assertNotNull("Read profile should not be null", readProfile);
        Assert.assertEquals("Query ID should match", queryId, readProfile.getSummaryProfile().getProfileId());

        // Test listing profiles
        List<Path> archivedFiles = archiveStorage.getAllArchivedFiles();
        Assert.assertTrue("Archive list should contain the written profile",
                archivedFiles.stream().anyMatch(path -> path.toString().contains(queryId)));

        // Test deleting profile using ArchiveEntry
        archiveStorage.deleteArchive(entry);
        Assert.assertFalse("Profile should not exist after deletion", Files.exists(Paths.get(entry.getArchivePath())));
    }

    @Test
    public void testArchiveIndex() throws Exception {
        String queryId = genQueryIdForIndex(456);
        long fileSize = 12345L;
        long finishTime = System.currentTimeMillis();
        String archiveFilePath = Paths.get(archivePath, "archive", "2024", "10", "21",
                finishTime + "_" + queryId + ".zip").toString();
        ArchiveIndex.ArchiveEntry entry = new ArchiveIndex.ArchiveEntry(queryId, archiveFilePath, finishTime, fileSize);

        // Test adding entry
        archiveIndex.addEntry(entry);
        Assert.assertTrue("Index should contain the entry", archiveIndex.containsEntry(queryId));

        // Test getting entry
        ArchiveIndex.ArchiveEntry retrievedEntry = archiveIndex.getEntry(queryId);
        Assert.assertNotNull("Retrieved entry should not be null", retrievedEntry);
        Assert.assertEquals("Retrieved entry should match", entry, retrievedEntry);

        // Test entry count and total size
        Assert.assertEquals("Entry count should be 1", 1, archiveIndex.getEntryCount());
        Assert.assertEquals("Total size should match file size", fileSize, archiveIndex.getTotalSize());

        // Test removing entry
        archiveIndex.removeEntry(queryId);
        Assert.assertFalse("Index should not contain the entry after removal", archiveIndex.containsEntry(queryId));
        Assert.assertEquals("Entry count should be 0 after removal", 0, archiveIndex.getEntryCount());
    }

    @Test
    public void testProfileArchiveManager() throws Exception {
        String queryId = genQueryIdForIndex(789);
        String user = "test_user";
        String database = "test_db";
        long startTime = System.currentTimeMillis();

        // Create a mock profile for testing
        Profile profile = buildAndStoreProfile(queryId, user, database, startTime);

        // Test archiving profile
        archiveManager.archiveProfile(profile);

        // Test retrieving archived profile
        Profile retrievedProfile = archiveManager.getArchivedProfile(queryId);
        Assert.assertNotNull("Retrieved profile should not be null", retrievedProfile);
        Assert.assertEquals("Query ID should match", queryId, retrievedProfile.getSummaryProfile().getProfileId());

        // Trigger cleanup old profiles (should not affect recent profile)
        archiveManager.performArchiveCleanup();
        Profile stillExists = archiveManager.getArchivedProfile(queryId);
        Assert.assertNotNull("Recent profile should still exist after cleanup", stillExists);
    }

    @Test
    public void testArchiveCleanup() throws Exception {
        // Set very short retention for testing
        Config.profile_archive_retention_seconds = 0; // Immediate cleanup

        String queryId = genQueryIdForIndex(9000);
        String user = "test_user";
        String database = "test_db";
        long startTime = System.currentTimeMillis() - 86400000; // 1 day ago

        // Archive a profile
        Profile profile = buildAndStoreProfile(queryId, user, database, startTime);
        archiveManager.archiveProfile(profile);

        // Verify it exists
        Assert.assertNotNull("Profile should exist before cleanup", archiveManager.getArchivedProfile(queryId));

        // Trigger cleanup
        archiveManager.performArchiveCleanup();

        // Verify it's cleaned up
        Assert.assertNull("Profile should be cleaned up", archiveManager.getArchivedProfile(queryId));

        // Reset retention for other tests
        Config.profile_archive_retention_seconds = 7 * 24 * 60 * 60; // 7 days in seconds
    }

    @Test
    public void testArchiveSizeLimit() throws Exception {
        // Set very small size limit for testing, but ensure count limit doesn't interfere
        Config.profile_archive_max_size_bytes = 100; // 100 bytes
        Config.max_archived_profile_num = 1000; // Set high count limit to avoid interference

        // Try to archive multiple profiles that exceed the size limit
        for (int i = 0; i < 5; i++) {
            String queryId = genQueryIdForIndex(10000 + i);
            long finishTime = System.currentTimeMillis() - i * 1000;
            Profile profile = buildAndStoreProfile(queryId, "user" + i, "db" + i, finishTime);
            archiveManager.archiveProfile(profile);
        }

        // Trigger cleanup to enforce size limit
        archiveManager.performArchiveCleanup();

        // Check that total size is within limit
        Map<String, Object> stats = archiveManager.getArchiveStatistics();
        long totalSize = (Long) stats.get("total_size");
        Assert.assertTrue("Total archive size should be within limit", totalSize <= Config.profile_archive_max_size_bytes);

        // Reset size limit for other tests
        Config.profile_archive_max_size_bytes = 1024 * 1024;
        Config.max_archived_profile_num = 100000;
    }

    @Test
    public void testConcurrentArchiving() throws Exception {
        int numThreads = 5;
        int profilesPerThread = 10;
        Thread[] threads = new Thread[numThreads];

        // Create multiple threads to archive profiles concurrently
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                for (int i = 0; i < profilesPerThread; i++) {
                    String queryId = genQueryIdForIndex(threadId * 1000 + i);
                    String user = "user_" + threadId;
                    String database = "db_" + threadId;
                    long startTime = System.currentTimeMillis() - i * 1000;

                    try {
                        Profile profile = buildAndStoreProfile(queryId, user, database, startTime);
                        archiveManager.archiveProfile(profile);
                    } catch (Exception e) {
                        Assert.fail("Concurrent archiving failed: " + e.getMessage());
                    }
                }
            });
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Verify all profiles were archived
        Map<String, Object> stats = archiveManager.getArchiveStatistics();
        long totalCount = (Long) stats.get("total_count");
        Assert.assertEquals("All profiles should be archived", numThreads * profilesPerThread, totalCount);
    }
}
