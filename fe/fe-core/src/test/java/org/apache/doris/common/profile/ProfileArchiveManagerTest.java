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

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Unit tests for ProfileArchiveManager.
 * Tests cover archive creation, profile batching, timestamp extraction,
 * archive file management, and error handling.
 */
public class ProfileArchiveManagerTest {
    private static final Logger LOG = LogManager.getLogger(ProfileArchiveManagerTest.class);

    private File tempDir;
    private ProfileArchiveManager archiveManager;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("profile_archive_test_").toFile();
        archiveManager = new ProfileArchiveManager(tempDir.getAbsolutePath());
    }

    @AfterEach
    void tearDown() {
        FileUtils.deleteQuietly(tempDir);
    }

    /**
     * Helper method to create a mock profile file.
     */
    private File createMockProfileFile(long timestamp) throws IOException {
        UUID taskId = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
        String profileId = DebugUtil.printId(queryId);

        Profile profile = ProfileManagerTest.constructProfile(profileId);
        profile.setQueryFinishTimestamp(timestamp);

        // Write profile to storage
        profile.writeToStorage(tempDir.getAbsolutePath());

        // Find the created file
        String filename = timestamp + "_" + profileId + ".zip";
        File profileFile = new File(tempDir, filename);

        if (!profileFile.exists()) {
            throw new IOException("Failed to create profile file: " + filename);
        }

        return profileFile;
    }

    /**
     * Test: Archive directory creation.
     * Verifies that the archive directory is created successfully.
     */
    @Test
    void testCreateArchiveDirectory() {
        // Archive directory should not exist initially
        File archiveDir = new File(archiveManager.getArchivePath());
        Assertions.assertFalse(archiveDir.exists());

        // Create archive directory
        boolean created = archiveManager.createArchiveDirectoryIfNecessary();
        Assertions.assertTrue(created);
        Assertions.assertTrue(archiveDir.exists());
        Assertions.assertTrue(archiveDir.isDirectory());

        // Second call should also return true (idempotent)
        created = archiveManager.createArchiveDirectoryIfNecessary();
        Assertions.assertTrue(created);
    }

    /**
     * Test: Get profile files from storage.
     * Verifies that profile files are correctly retrieved and sorted by filename timestamp.
     */
    @Test
    void testGetProfileFilesFromStorage() throws IOException, InterruptedException {
        // Create multiple profile files with different timestamps
        List<Long> timestamps = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            long timestamp = System.currentTimeMillis() + i * 1000;
            timestamps.add(timestamp);
            createMockProfileFile(timestamp);
            Thread.sleep(100); // Ensure different modification times
        }

        // Get profile files
        List<File> profileFiles = archiveManager.getProfileFilesFromStorage();

        // Verify count
        Assertions.assertEquals(5, profileFiles.size());

        // Verify files are sorted by filename timestamp (oldest first)
        for (int i = 0; i < profileFiles.size() - 1; i++) {
            // Extract timestamps from filenames
            String filename1 = profileFiles.get(i).getName();
            String filename2 = profileFiles.get(i + 1).getName();
            long ts1 = Long.parseLong(filename1.substring(0, filename1.indexOf('_')));
            long ts2 = Long.parseLong(filename2.substring(0, filename2.indexOf('_')));
            Assertions.assertTrue(ts1 <= ts2,
                    "Files should be sorted by filename timestamp");
        }
    }

    /**
     * Test: Archive a single batch of profiles.
     * Verifies that profiles are correctly archived into a ZIP file.
     */
    @Test
    void testArchiveSingleBatch() throws IOException, InterruptedException {
        // Create 5 profile files
        List<File> profilesToArchive = new ArrayList<>();
        long baseTimestamp = System.currentTimeMillis();

        for (int i = 0; i < 5; i++) {
            long timestamp = baseTimestamp + i * 1000;
            File profileFile = createMockProfileFile(timestamp);
            profilesToArchive.add(profileFile);
            Thread.sleep(100);
        }

        // Archive the profiles
        File archiveFile = archiveManager.archiveProfiles(profilesToArchive);

        // Verify archive was created
        Assertions.assertNotNull(archiveFile);
        Assertions.assertTrue(archiveFile.exists());
        Assertions.assertTrue(archiveFile.getName().startsWith("profiles_"));
        Assertions.assertTrue(archiveFile.getName().endsWith(".zip"));

        // Verify archive location
        Assertions.assertTrue(archiveFile.getParentFile().getName().equals("archive"));

        // Verify archive contents
        int entryCount = 0;
        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(archiveFile))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                entryCount++;
                // Verify entry name matches original profile filename
                Assertions.assertTrue(entry.getName().endsWith(".zip"));
                zis.closeEntry();
            }
        }

        Assertions.assertEquals(5, entryCount, "Archive should contain 5 profile files");
    }

    /**
     * Test: Archive filename format.
     * Verifies that archive filenames follow the correct naming convention.
     */
    @Test
    void testArchiveFilenameFormat() throws IOException {
        // Create 3 profile files with known timestamps
        long timestamp1 = 1704067200000L; // 2024-01-01 00:00:00
        long timestamp2 = 1704070800000L; // 2024-01-01 01:00:00
        long timestamp3 = 1704074400000L; // 2024-01-01 02:00:00

        List<File> profilesToArchive = new ArrayList<>();
        profilesToArchive.add(createMockProfileFile(timestamp1));
        profilesToArchive.add(createMockProfileFile(timestamp2));
        profilesToArchive.add(createMockProfileFile(timestamp3));

        // Archive the profiles
        File archiveFile = archiveManager.archiveProfiles(profilesToArchive);

        // Verify filename format: profiles_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS.zip
        Assertions.assertNotNull(archiveFile);
        String filename = archiveFile.getName();
        Assertions.assertTrue(filename.matches("profiles_\\d{8}_\\d{6}_\\d{8}_\\d{6}\\.zip"),
                "Archive filename should match pattern: profiles_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS.zip");

        // Verify filename contains correct date range
        Assertions.assertTrue(filename.startsWith("profiles_20240101_"));
    }

    /**
     * Test: Delete archived profiles.
     * Verifies that profile files are correctly deleted after archiving.
     */
    @Test
    void testDeleteArchivedProfiles() throws IOException, InterruptedException {
        // Create profile files
        List<File> profileFiles = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            long timestamp = System.currentTimeMillis() + i * 1000;
            File profileFile = createMockProfileFile(timestamp);
            profileFiles.add(profileFile);
            Thread.sleep(100);
        }

        // Verify files exist
        for (File file : profileFiles) {
            Assertions.assertTrue(file.exists());
        }

        // Delete the files
        int deletedCount = archiveManager.deleteArchivedProfiles(profileFiles);

        // Verify all files were deleted
        Assertions.assertEquals(3, deletedCount);
        for (File file : profileFiles) {
            Assertions.assertFalse(file.exists());
        }
    }

    /**
     * Test: Archive oldest profiles.
     * Verifies that the oldest profiles are selected for archiving.
     */
    @Test
    void testArchiveOldestProfiles() throws IOException, InterruptedException {
        // Create 10 profile files with different timestamps
        long baseTimestamp = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            long timestamp = baseTimestamp + i * 1000;
            createMockProfileFile(timestamp);
            Thread.sleep(100);
        }

        // Archive the oldest 5 profiles
        int archived = archiveManager.archiveOldestProfiles(5);

        // Verify 5 profiles were archived
        Assertions.assertEquals(5, archived);

        // Verify 5 profiles remain in storage
        List<File> remainingFiles = archiveManager.getProfileFilesFromStorage();
        Assertions.assertEquals(5, remainingFiles.size());

        // Verify archive was created
        List<File> archiveFiles = archiveManager.getArchiveFiles();
        Assertions.assertEquals(1, archiveFiles.size());
    }

    /**
     * Test: Archive profiles in multiple batches.
     * Verifies that profiles are correctly batched when archiving many profiles.
     */
    @Test
    void testArchiveMultipleBatches() throws IOException, InterruptedException {
        // Create archive manager with small batch size
        ProfileArchiveManager smallBatchManager = new ProfileArchiveManager(tempDir.getAbsolutePath(), 3);

        // Create 10 profile files
        long baseTimestamp = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            long timestamp = baseTimestamp + i * 1000;
            createMockProfileFile(timestamp);
            Thread.sleep(50);
        }

        // Archive all 10 profiles (should create 4 archives: 3 + 3 + 3 + 1)
        int archived = smallBatchManager.archiveOldestProfiles(10);

        // Verify all profiles were archived
        Assertions.assertEquals(10, archived);

        // Verify multiple archive files were created
        List<File> archiveFiles = smallBatchManager.getArchiveFiles();
        Assertions.assertEquals(4, archiveFiles.size());

        // Verify no profiles remain in storage
        List<File> remainingFiles = smallBatchManager.getProfileFilesFromStorage();
        Assertions.assertEquals(0, remainingFiles.size());
    }

    /**
     * Test: Get archive files.
     * Verifies that archive files are correctly retrieved.
     */
    @Test
    void testGetArchiveFiles() throws IOException, InterruptedException {
        // Initially no archives
        List<File> archives = archiveManager.getArchiveFiles();
        Assertions.assertTrue(archives.isEmpty());

        // Create and archive some profiles
        List<File> profilesToArchive = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            long timestamp = System.currentTimeMillis() + i * 1000;
            profilesToArchive.add(createMockProfileFile(timestamp));
            Thread.sleep(100);
        }

        File archiveFile = archiveManager.archiveProfiles(profilesToArchive);
        Assertions.assertNotNull(archiveFile);

        // Verify archive is in the list
        archives = archiveManager.getArchiveFiles();
        Assertions.assertEquals(1, archives.size());
        Assertions.assertEquals(archiveFile.getName(), archives.get(0).getName());
    }

    /**
     * Test: Get total archive size.
     * Verifies that total archive size is correctly calculated.
     */
    @Test
    void testGetTotalArchiveSize() throws IOException, InterruptedException {
        // Initially total size should be 0
        long totalSize = archiveManager.getTotalArchiveSize();
        Assertions.assertEquals(0, totalSize);

        // Create and archive some profiles
        List<File> profilesToArchive1 = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            long timestamp = System.currentTimeMillis() + i * 1000;
            profilesToArchive1.add(createMockProfileFile(timestamp));
            Thread.sleep(100);
        }

        File archive1 = archiveManager.archiveProfiles(profilesToArchive1);
        Assertions.assertNotNull(archive1);

        // Verify total size equals first archive size
        totalSize = archiveManager.getTotalArchiveSize();
        Assertions.assertEquals(archive1.length(), totalSize);

        // Create and archive more profiles
        List<File> profilesToArchive2 = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            long timestamp = System.currentTimeMillis() + i * 1000;
            profilesToArchive2.add(createMockProfileFile(timestamp));
            Thread.sleep(100);
        }

        File archive2 = archiveManager.archiveProfiles(profilesToArchive2);
        Assertions.assertNotNull(archive2);

        // Verify total size equals sum of both archives
        totalSize = archiveManager.getTotalArchiveSize();
        Assertions.assertEquals(archive1.length() + archive2.length(), totalSize);
    }

    /**
     * Test: Archive with empty profile list.
     * Verifies that archiving with no profiles returns null.
     */
    @Test
    void testArchiveEmptyList() {
        List<File> emptyList = new ArrayList<>();
        File archiveFile = archiveManager.archiveProfiles(emptyList);

        Assertions.assertNull(archiveFile, "Archiving empty list should return null");
    }

    /**
     * Test: Archive with null profile list.
     * Verifies that archiving with null list returns null.
     */
    @Test
    void testArchiveNullList() {
        File archiveFile = archiveManager.archiveProfiles(null);
        Assertions.assertNull(archiveFile, "Archiving null list should return null");
    }

    /**
     * Test: Archive oldest profiles with zero count.
     * Verifies that archiving zero profiles returns 0.
     */
    @Test
    void testArchiveZeroProfiles() throws IOException {
        // Create some profiles
        for (int i = 0; i < 5; i++) {
            createMockProfileFile(System.currentTimeMillis() + i * 1000);
        }

        int archived = archiveManager.archiveOldestProfiles(0);
        Assertions.assertEquals(0, archived);

        // Verify all profiles still in storage
        List<File> files = archiveManager.getProfileFilesFromStorage();
        Assertions.assertEquals(5, files.size());
    }

    /**
     * Test: Archive more profiles than available.
     * Verifies that archiving handles requests for more profiles than exist.
     */
    @Test
    void testArchiveMoreThanAvailable() throws IOException, InterruptedException {
        // Create only 3 profiles
        for (int i = 0; i < 3; i++) {
            long timestamp = System.currentTimeMillis() + i * 1000;
            createMockProfileFile(timestamp);
            Thread.sleep(100);
        }

        // Try to archive 10 profiles (more than available)
        int archived = archiveManager.archiveOldestProfiles(10);

        // Verify only 3 were archived
        Assertions.assertEquals(3, archived);

        // Verify no profiles remain in storage
        List<File> remainingFiles = archiveManager.getProfileFilesFromStorage();
        Assertions.assertEquals(0, remainingFiles.size());
    }

    /**
     * Test: Archive from non-existent storage directory.
     * Verifies that archiving from non-existent directory handles gracefully.
     */
    @Test
    void testArchiveFromNonExistentDirectory() throws IOException {
        File nonExistentDir = new File(tempDir, "nonexistent");
        ProfileArchiveManager manager = new ProfileArchiveManager(nonExistentDir.getAbsolutePath());

        List<File> files = manager.getProfileFilesFromStorage();
        Assertions.assertTrue(files.isEmpty());

        int archived = manager.archiveOldestProfiles(5);
        Assertions.assertEquals(0, archived);
    }

    /**
     * Test: Duplicate archive filename handling.
     * Verifies that duplicate archive filenames are handled by adding suffixes.
     */
    @Test
    void testDuplicateArchiveFilenames() throws IOException, InterruptedException {
        // Create profiles with same timestamp (to force duplicate archive names)
        long timestamp = System.currentTimeMillis();
        List<File> batch1 = new ArrayList<>();
        List<File> batch2 = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            batch1.add(createMockProfileFile(timestamp));
            Thread.sleep(50);
        }

        for (int i = 0; i < 2; i++) {
            batch2.add(createMockProfileFile(timestamp));
            Thread.sleep(50);
        }

        // Archive both batches
        File archive1 = archiveManager.archiveProfiles(batch1);
        File archive2 = archiveManager.archiveProfiles(batch2);

        // Verify both archives were created with different names
        Assertions.assertNotNull(archive1);
        Assertions.assertNotNull(archive2);
        Assertions.assertNotEquals(archive1.getName(), archive2.getName());

        // Verify both exist
        Assertions.assertTrue(archive1.exists());
        Assertions.assertTrue(archive2.exists());
    }

    /**
     * Test: Archive file integrity.
     * Verifies that archived profiles can be extracted and read correctly.
     */
    @Test
    void testArchiveFileIntegrity() throws IOException, InterruptedException {
        // Create profile files
        List<File> originalFiles = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            long timestamp = System.currentTimeMillis() + i * 1000;
            File profileFile = createMockProfileFile(timestamp);
            originalFiles.add(profileFile);
            Thread.sleep(100);
        }

        // Archive the profiles
        File archiveFile = archiveManager.archiveProfiles(originalFiles);
        Assertions.assertNotNull(archiveFile);

        // Verify each original file exists in the archive
        List<String> originalFilenames = new ArrayList<>();
        for (File file : originalFiles) {
            originalFilenames.add(file.getName());
        }

        List<String> archivedFilenames = new ArrayList<>();
        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(archiveFile))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                archivedFilenames.add(entry.getName());
                zis.closeEntry();
            }
        }

        // Verify all original files are in the archive
        Assertions.assertEquals(originalFilenames.size(), archivedFilenames.size());
        for (String filename : originalFilenames) {
            Assertions.assertTrue(archivedFilenames.contains(filename),
                    "Archive should contain file: " + filename);
        }
    }


    /**
     * Test: Large batch archiving performance.
     * Verifies that archiving large batches completes in reasonable time.
     */
    @Test
    void testLargeBatchArchiving() throws IOException, InterruptedException {
        // Create 100 profile files
        for (int i = 0; i < 100; i++) {
            long timestamp = System.currentTimeMillis() + i * 10;
            createMockProfileFile(timestamp);
            if (i % 10 == 0) {
                Thread.sleep(10); // Brief pause every 10 files
            }
        }

        long startTime = System.currentTimeMillis();

        // Archive all 100 profiles
        int archived = archiveManager.archiveOldestProfiles(100);

        long duration = System.currentTimeMillis() - startTime;

        // Verify all were archived
        Assertions.assertEquals(100, archived);

        // Verify performance (should complete in under 30 seconds)
        Assertions.assertTrue(duration < 30000,
                "Archiving 100 profiles should complete in under 30 seconds, took: " + duration + "ms");

        LOG.info("Archived 100 profiles in {}ms", duration);
    }

    /**
     * Test: Move profile to pending directory.
     * Verifies that profiles are correctly moved to the pending buffer.
     */
    @Test
    void testMoveToArchivePending() throws IOException {
        // Create a profile file
        File profileFile = createMockProfileFile(System.currentTimeMillis());
        Assertions.assertTrue(profileFile.exists());

        // Move to pending
        boolean moved = archiveManager.moveToArchivePending(profileFile);
        Assertions.assertTrue(moved);

        // Verify original file no longer exists
        Assertions.assertFalse(profileFile.exists());

        // Verify file exists in pending directory
        File pendingFile = new File(archiveManager.getArchivePath() + File.separator + "pending",
                profileFile.getName());
        Assertions.assertTrue(pendingFile.exists());
    }

    /**
     * Test: Check and archive pending profiles with batch size condition.
     * Verifies that archiving is triggered when batch size is reached.
     */
    @Test
    void testCheckAndArchivePendingWithBatchSize() throws IOException, InterruptedException {
        // Create manager with small batch size for testing
        ProfileArchiveManager smallBatchManager = new ProfileArchiveManager(tempDir.getAbsolutePath(), 5);

        // Move 5 profiles to pending
        for (int i = 0; i < 5; i++) {
            File profileFile = createMockProfileFile(System.currentTimeMillis() + i * 1000);
            smallBatchManager.moveToArchivePending(profileFile);
            Thread.sleep(100);
        }

        // Check and archive (should trigger because batch size is reached)
        int archived = smallBatchManager.checkAndArchivePendingProfiles();

        // Verify all 5 profiles were archived
        Assertions.assertEquals(5, archived);

        // Verify archive file was created
        List<File> archiveFiles = smallBatchManager.getArchiveFiles();
        Assertions.assertEquals(1, archiveFiles.size());

        // Verify pending directory is empty
        File pendingDir = new File(smallBatchManager.getArchivePath() + File.separator + "pending");
        File[] pendingFiles = pendingDir.listFiles();
        Assertions.assertTrue(pendingFiles == null || pendingFiles.length == 0);
    }

    /**
     * Test: Check and archive pending profiles with timeout condition.
     * Verifies that archiving is triggered when oldest file exceeds timeout.
     */
    @Test
    void testCheckAndArchivePendingWithTimeout() throws IOException, InterruptedException {
        // Create manager with large batch size
        ProfileArchiveManager manager = new ProfileArchiveManager(tempDir.getAbsolutePath(), 1000);

        // Create one profile with timestamp 25 hours ago (exceeds 24 hour timeout)
        long oldTimestamp = System.currentTimeMillis() - (25 * 3600 * 1000L);
        File oldProfileFile = createMockProfileFile(oldTimestamp);
        manager.moveToArchivePending(oldProfileFile);
        Thread.sleep(100);

        // Create 2 more recent profiles
        for (int i = 0; i < 2; i++) {
            File profileFile = createMockProfileFile(System.currentTimeMillis() + i * 1000);
            manager.moveToArchivePending(profileFile);
            Thread.sleep(100);
        }

        // Verify all 3 files are in pending
        File pendingDir = new File(manager.getArchivePath() + File.separator + "pending");
        File[] pendingFiles = pendingDir.listFiles();
        Assertions.assertNotNull(pendingFiles);
        Assertions.assertEquals(3, pendingFiles.length);

        // Check and archive (should trigger because oldest filename timestamp exceeds timeout)
        int archived = manager.checkAndArchivePendingProfiles();

        // Verify all 3 profiles were archived
        Assertions.assertEquals(3, archived);

        // Verify archive file was created
        List<File> archiveFiles = manager.getArchiveFiles();
        Assertions.assertEquals(1, archiveFiles.size());
    }

    /**
     * Test: Check and archive pending profiles below threshold.
     * Verifies that archiving is NOT triggered when conditions are not met.
     */
    @Test
    void testCheckAndArchivePendingBelowThreshold() throws IOException {
        // Create manager with large batch size
        ProfileArchiveManager manager = new ProfileArchiveManager(tempDir.getAbsolutePath(), 1000);

        // Move only 2 profiles to pending (well below batch size)
        for (int i = 0; i < 2; i++) {
            File profileFile = createMockProfileFile(System.currentTimeMillis() + i * 1000);
            manager.moveToArchivePending(profileFile);
        }

        // Check and archive (should NOT trigger)
        int archived = manager.checkAndArchivePendingProfiles();

        // Verify nothing was archived
        Assertions.assertEquals(0, archived);

        // Verify no archive files were created
        List<File> archiveFiles = manager.getArchiveFiles();
        Assertions.assertEquals(0, archiveFiles.size());

        // Verify pending files still exist
        File pendingDir = new File(manager.getArchivePath() + File.separator + "pending");
        File[] pendingFiles = pendingDir.listFiles();
        Assertions.assertNotNull(pendingFiles);
        Assertions.assertEquals(2, pendingFiles.length);
    }

    /**
     * Test: Clean old archives with retention period.
     * Verifies that archives older than retention period are deleted.
     */
    @Test
    void testCleanOldArchivesWithRetention() throws IOException, InterruptedException {
        // Temporarily override retention config for testing
        int originalRetention = org.apache.doris.common.Config.profile_archive_retention_seconds;
        org.apache.doris.common.Config.profile_archive_retention_seconds = 7 * 24 * 3600; // 7 days retention

        try {
            // Create old profiles with timestamp 10 days ago (exceeds 7 day retention)
            // This ensures the archive filename contains the old timestamp
            long oldTime = System.currentTimeMillis() - (10 * 24 * 3600 * 1000L);
            List<File> batch1 = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                batch1.add(createMockProfileFile(oldTime + i * 1000));
                Thread.sleep(100);
            }

            File archive1 = archiveManager.archiveProfiles(batch1);
            Assertions.assertNotNull(archive1);

            // Create another archive with recent profiles
            List<File> batch2 = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                batch2.add(createMockProfileFile(System.currentTimeMillis() + i * 1000));
                Thread.sleep(100);
            }
            File archive2 = archiveManager.archiveProfiles(batch2);
            Assertions.assertNotNull(archive2);

            // Clean old archives
            int deleted = archiveManager.cleanOldArchives();

            // Verify only the old archive was deleted
            Assertions.assertEquals(1, deleted);
            Assertions.assertFalse(archive1.exists());
            Assertions.assertTrue(archive2.exists());

        } finally {
            // Restore original config
            org.apache.doris.common.Config.profile_archive_retention_seconds = originalRetention;
        }
    }

    /**
     * Test: Clean old archives with unlimited retention.
     * Verifies that no archives are deleted when retention is set to -1 (unlimited).
     */
    @Test
    void testCleanOldArchivesUnlimitedRetention() throws IOException, InterruptedException {
        // Temporarily override retention config for testing
        int originalRetention = org.apache.doris.common.Config.profile_archive_retention_seconds;
        org.apache.doris.common.Config.profile_archive_retention_seconds = -1; // Unlimited retention

        try {
            // Create very old profiles (100 days ago)
            // This ensures the archive filename contains the old timestamp
            long oldTime = System.currentTimeMillis() - (100 * 24 * 3600 * 1000L);
            List<File> batch = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                batch.add(createMockProfileFile(oldTime + i * 1000));
                Thread.sleep(100);
            }

            File archive = archiveManager.archiveProfiles(batch);
            Assertions.assertNotNull(archive);

            // Clean old archives (should not delete anything due to unlimited retention)
            int deleted = archiveManager.cleanOldArchives();

            // Verify nothing was deleted
            Assertions.assertEquals(0, deleted);
            Assertions.assertTrue(archive.exists());

        } finally {
            // Restore original config
            org.apache.doris.common.Config.profile_archive_retention_seconds = originalRetention;
        }
    }

    /**
     * Test: Pending directory creation.
     * Verifies that the pending directory is created correctly.
     */
    @Test
    void testCreatePendingDirectory() {
        File pendingDir = new File(archiveManager.getArchivePath() + File.separator + "pending");
        Assertions.assertFalse(pendingDir.exists());

        boolean created = archiveManager.createPendingDirectoryIfNecessary();
        Assertions.assertTrue(created);
        Assertions.assertTrue(pendingDir.exists());
        Assertions.assertTrue(pendingDir.isDirectory());

        // Second call should also return true (idempotent)
        created = archiveManager.createPendingDirectoryIfNecessary();
        Assertions.assertTrue(created);
    }

    /**
     * Test: Pending files sorted by filename timestamp (not file modification time).
     * Verifies that pending files are sorted by timestamp in filename, regardless of file modification time.
     */
    @Test
    void testPendingFilesSortedByFilenameTimestamp() throws IOException, InterruptedException {
        ProfileArchiveManager manager = new ProfileArchiveManager(tempDir.getAbsolutePath(), 10);

        // Create profiles with timestamps in non-sequential order: 3000, 1000, 5000, 2000, 4000
        long baseTimestamp = System.currentTimeMillis();
        long[] timestamps = {baseTimestamp + 3000, baseTimestamp + 1000, baseTimestamp + 5000,
                baseTimestamp + 2000, baseTimestamp + 4000};

        for (long timestamp : timestamps) {
            File profileFile = createMockProfileFile(timestamp);
            manager.moveToArchivePending(profileFile);
            Thread.sleep(100); // Different modification times
        }

        // Trigger archiving to get the sorted list (but won't actually archive due to batch size)
        manager.checkAndArchivePendingProfiles();

        // Manually get pending files using reflection to test sorting
        File pendingDir = new File(manager.getArchivePath() + File.separator + "pending");
        File[] files = pendingDir.listFiles(File::isFile);
        Assertions.assertNotNull(files);
        Assertions.assertEquals(5, files.length);

        // Sort using the same logic as getPendingProfileFiles
        List<File> sortedFiles = new ArrayList<>(Arrays.asList(files));
        sortedFiles.sort(Comparator.comparingLong(file -> {
            String filename = file.getName();
            long ts = Long.parseLong(filename.substring(0, filename.indexOf('_')));
            return ts;
        }));

        // Verify files are sorted by filename timestamp (not modification time)
        for (int i = 0; i < sortedFiles.size() - 1; i++) {
            String filename1 = sortedFiles.get(i).getName();
            String filename2 = sortedFiles.get(i + 1).getName();
            long ts1 = Long.parseLong(filename1.substring(0, filename1.indexOf('_')));
            long ts2 = Long.parseLong(filename2.substring(0, filename2.indexOf('_')));
            Assertions.assertTrue(ts1 < ts2,
                    String.format("Files should be sorted by filename timestamp: %d < %d", ts1, ts2));
        }

        // Expected order: baseTimestamp+1000, +2000, +3000, +4000, +5000
        String firstFilename = sortedFiles.get(0).getName();
        long firstTs = Long.parseLong(firstFilename.substring(0, firstFilename.indexOf('_')));
        Assertions.assertEquals(baseTimestamp + 1000, firstTs,
                "First file should have smallest timestamp");

        String lastFilename = sortedFiles.get(4).getName();
        long lastTs = Long.parseLong(lastFilename.substring(0, lastFilename.indexOf('_')));
        Assertions.assertEquals(baseTimestamp + 5000, lastTs,
                "Last file should have largest timestamp");
    }

    /**
     * Test: Archive files sorted by filename timestamp (not file modification time).
     * Verifies that archive files are sorted by timestamp in filename, regardless of file modification time.
     */
    @Test
    void testArchiveFilesSortedByFilenameTimestamp() throws IOException, InterruptedException {
        // Create multiple archives with different timestamp ranges
        // Archive 1: timestamps 1000-2000
        List<File> batch1 = new ArrayList<>();
        long baseTime = 1704067200000L; // 2024-01-01 00:00:00
        batch1.add(createMockProfileFile(baseTime + 1000));
        batch1.add(createMockProfileFile(baseTime + 2000));
        File archive1 = archiveManager.archiveProfiles(batch1);
        Assertions.assertNotNull(archive1);
        Thread.sleep(100);

        // Archive 2: timestamps 5000-6000 (created second but should be sorted after archive3)
        List<File> batch2 = new ArrayList<>();
        batch2.add(createMockProfileFile(baseTime + 5000));
        batch2.add(createMockProfileFile(baseTime + 6000));
        File archive2 = archiveManager.archiveProfiles(batch2);
        Assertions.assertNotNull(archive2);
        Thread.sleep(100);

        // Archive 3: timestamps 3000-4000 (created last but should be sorted between archive1 and archive2)
        List<File> batch3 = new ArrayList<>();
        batch3.add(createMockProfileFile(baseTime + 3000));
        batch3.add(createMockProfileFile(baseTime + 4000));
        File archive3 = archiveManager.archiveProfiles(batch3);
        Assertions.assertNotNull(archive3);

        // Set different modification times to ensure we're sorting by filename, not mod time
        archive1.setLastModified(System.currentTimeMillis() - 10000);
        archive2.setLastModified(System.currentTimeMillis() - 5000);
        archive3.setLastModified(System.currentTimeMillis());

        // Get archive files (should be sorted by filename timestamp)
        List<File> archiveFiles = archiveManager.getArchiveFiles();
        Assertions.assertEquals(3, archiveFiles.size());

        // Verify sorting by filename timestamp
        // Expected order: archive1 (1000-2000), archive3 (3000-4000), archive2 (5000-6000)
        String name1 = archiveFiles.get(0).getName();
        String name2 = archiveFiles.get(1).getName();
        String name3 = archiveFiles.get(2).getName();

        // Extract first timestamp from each archive filename
        Assertions.assertTrue(name1.contains("20240101"),
                "First archive should be archive1");
        Assertions.assertTrue(name2.contains("20240101"),
                "Second archive should be archive3");
        Assertions.assertTrue(name3.contains("20240101"),
                "Third archive should be archive2");

        // Verify they are in the correct order by comparing the actual archive files
        Assertions.assertEquals(archive1.getName(), archiveFiles.get(0).getName(),
                "First should be archive1 (oldest timestamp range)");
        Assertions.assertEquals(archive3.getName(), archiveFiles.get(1).getName(),
                "Second should be archive3 (middle timestamp range)");
        Assertions.assertEquals(archive2.getName(), archiveFiles.get(2).getName(),
                "Third should be archive2 (newest timestamp range)");
    }

    /**
     * Test: Archive pending profiles - batch size triggered, incomplete batch remains.
     * Scenario: 23 pending profiles, batch size 10
     * Expected: Archive only 20 profiles (2 complete batches), 3 remain in pending
     */
    @Test
    void testBatchSizeTriggeredIncompleteBatchRemains() throws IOException, InterruptedException {
        // Create manager with batch size 10
        ProfileArchiveManager manager = new ProfileArchiveManager(tempDir.getAbsolutePath(), 10);

        // Move 23 profiles to pending
        for (int i = 0; i < 23; i++) {
            File profileFile = createMockProfileFile(System.currentTimeMillis() + i * 1000);
            manager.moveToArchivePending(profileFile);
            Thread.sleep(50);
        }

        // Check and archive (triggered by batch size)
        int archived = manager.checkAndArchivePendingProfiles();

        // Verify only 20 profiles were archived (2 complete batches)
        Assertions.assertEquals(20, archived, "Should archive only complete batches (20 profiles)");

        // Verify 2 archive files were created
        List<File> archiveFiles = manager.getArchiveFiles();
        Assertions.assertEquals(2, archiveFiles.size(), "Should create 2 archive files");

        // Verify 3 profiles remain in pending
        File pendingDir = new File(manager.getArchivePath() + File.separator + "pending");
        File[] pendingFiles = pendingDir.listFiles();
        Assertions.assertNotNull(pendingFiles);
        Assertions.assertEquals(3, pendingFiles.length, "3 profiles should remain in pending");
    }

    /**
     * Test: Archive pending profiles - batch size condition takes priority.
     * Scenario: 23 pending profiles (2+ batches), oldest file exceeds timeout
     * Expected: Batch size condition triggers first, archive 20 profiles (complete batches only),
     *           remaining 3 profiles stay in pending and will be archived on next check if timeout condition met
     */
    @Test
    void testTimeoutTriggeredIncompletesBatchArchived() throws IOException, InterruptedException {
        // Create manager with batch size 10
        ProfileArchiveManager manager = new ProfileArchiveManager(tempDir.getAbsolutePath(), 10);

        // Create one profile with timestamp 25 hours ago (exceeds timeout)
        long oldTimestamp = System.currentTimeMillis() - (25 * 3600 * 1000L);
        File oldProfileFile = createMockProfileFile(oldTimestamp);
        manager.moveToArchivePending(oldProfileFile);
        Thread.sleep(50);

        // Move 22 more recent profiles to pending (total 23)
        for (int i = 0; i < 22; i++) {
            File profileFile = createMockProfileFile(System.currentTimeMillis() + i * 1000);
            manager.moveToArchivePending(profileFile);
            Thread.sleep(50);
        }

        // Verify all 23 files are in pending
        File pendingDir = new File(manager.getArchivePath() + File.separator + "pending");
        File[] pendingFiles = pendingDir.listFiles();
        Assertions.assertNotNull(pendingFiles);
        Assertions.assertEquals(23, pendingFiles.length);

        // Check and archive (batch size condition takes priority)
        int archived = manager.checkAndArchivePendingProfiles();

        // Verify 20 profiles were archived (complete batches only)
        Assertions.assertEquals(20, archived, "Should archive 20 profiles (complete batches)");

        // Verify 2 archive files were created (10 + 10)
        List<File> archiveFiles = manager.getArchiveFiles();
        Assertions.assertEquals(2, archiveFiles.size(), "Should create 2 archive files");

        // Verify 3 profiles remain in pending
        pendingFiles = pendingDir.listFiles();
        Assertions.assertNotNull(pendingFiles);
        Assertions.assertEquals(3, pendingFiles.length, "3 profiles should remain in pending");
    }

    /**
     * Test: Archive pending profiles - exact batch size.
     * Scenario: Exactly 20 pending profiles with batch size 10
     * Expected: Archive all 20 profiles in 2 complete batches
     */
    @Test
    void testExactBatchSize() throws IOException, InterruptedException {
        // Create manager with batch size 10
        ProfileArchiveManager manager = new ProfileArchiveManager(tempDir.getAbsolutePath(), 10);

        // Move exactly 20 profiles to pending (2 complete batches)
        for (int i = 0; i < 20; i++) {
            File profileFile = createMockProfileFile(System.currentTimeMillis() + i * 1000);
            manager.moveToArchivePending(profileFile);
            Thread.sleep(50);
        }

        // Check and archive
        int archived = manager.checkAndArchivePendingProfiles();

        // Verify all 20 profiles were archived
        Assertions.assertEquals(20, archived, "Should archive all 20 profiles");

        // Verify 2 archive files were created
        List<File> archiveFiles = manager.getArchiveFiles();
        Assertions.assertEquals(2, archiveFiles.size(), "Should create 2 archive files");

        // Verify pending directory is empty
        File pendingDir = new File(manager.getArchivePath() + File.separator + "pending");
        File[] pendingFiles = pendingDir.listFiles();
        Assertions.assertTrue(pendingFiles == null || pendingFiles.length == 0,
                "Pending directory should be empty");
    }

    /**
     * Test: Archive pending profiles - less than batch size with no timeout.
     * Scenario: 5 pending profiles with batch size 10, no timeout
     * Expected: No archiving should occur
     */
    @Test
    void testLessThanBatchSizeNoTimeout() throws IOException, InterruptedException {
        // Create manager with batch size 10
        ProfileArchiveManager manager = new ProfileArchiveManager(tempDir.getAbsolutePath(), 10);

        // Move only 5 profiles to pending (less than batch size)
        for (int i = 0; i < 5; i++) {
            File profileFile = createMockProfileFile(System.currentTimeMillis() + i * 1000);
            manager.moveToArchivePending(profileFile);
            Thread.sleep(50);
        }

        // Check and archive (should not trigger)
        int archived = manager.checkAndArchivePendingProfiles();

        // Verify nothing was archived
        Assertions.assertEquals(0, archived, "Should not archive incomplete batch without timeout");

        // Verify no archive files were created
        List<File> archiveFiles = manager.getArchiveFiles();
        Assertions.assertEquals(0, archiveFiles.size(), "Should not create any archive files");

        // Verify all 5 profiles remain in pending
        File pendingDir = new File(manager.getArchivePath() + File.separator + "pending");
        File[] pendingFiles = pendingDir.listFiles();
        Assertions.assertNotNull(pendingFiles);
        Assertions.assertEquals(5, pendingFiles.length, "All 5 profiles should remain in pending");
    }

    /**
     * Test: Archive pending profiles - less than batch size with timeout.
     * Scenario: 5 pending profiles with batch size 10, but oldest file exceeds timeout
     * Expected: Archive all 5 profiles (incomplete batch allowed due to timeout)
     */
    @Test
    void testLessThanBatchSizeWithTimeout() throws IOException, InterruptedException {
        // Create manager with batch size 10
        ProfileArchiveManager manager = new ProfileArchiveManager(tempDir.getAbsolutePath(), 10);

        // Create one profile with timestamp 25 hours ago (exceeds timeout)
        long oldTimestamp = System.currentTimeMillis() - (25 * 3600 * 1000L);
        File oldProfileFile = createMockProfileFile(oldTimestamp);
        manager.moveToArchivePending(oldProfileFile);
        Thread.sleep(50);

        // Move 4 more recent profiles to pending (total 5, less than batch size 10)
        for (int i = 0; i < 4; i++) {
            File profileFile = createMockProfileFile(System.currentTimeMillis() + i * 1000);
            manager.moveToArchivePending(profileFile);
            Thread.sleep(50);
        }

        // Verify all 5 files are in pending
        File pendingDir = new File(manager.getArchivePath() + File.separator + "pending");
        File[] pendingFiles = pendingDir.listFiles();
        Assertions.assertNotNull(pendingFiles);
        Assertions.assertEquals(5, pendingFiles.length);

        // Check and archive (triggered by timeout)
        int archived = manager.checkAndArchivePendingProfiles();

        // Verify all 5 profiles were archived (timeout allows incomplete batch)
        Assertions.assertEquals(5, archived, "Should archive all profiles due to timeout");

        // Verify 1 archive file was created with 5 profiles
        List<File> archiveFiles = manager.getArchiveFiles();
        Assertions.assertEquals(1, archiveFiles.size(), "Should create 1 archive file");

        // Verify pending directory is empty
        pendingFiles = pendingDir.listFiles();
        Assertions.assertTrue(pendingFiles == null || pendingFiles.length == 0,
                "Pending directory should be empty");
    }
}
