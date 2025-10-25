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
import org.apache.doris.common.util.DebugPointUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ProfileArchiveManager is the main controller class for the profile archiving feature.
 * It coordinates all archiving components and provides a unified interface for profile archiving operations.
 */
public class ProfileArchiveManager {
    private static final Logger LOG = LogManager.getLogger(ProfileArchiveManager.class);

    private static volatile ProfileArchiveManager instance;

    private final ArchiveStorage archiveStorage;
    private final ArchiveIndex archiveIndex;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicLong totalArchivedProfiles = new AtomicLong(0);
    private final AtomicLong totalArchivedSize = new AtomicLong(0);
    // Cleanup related fields
    private final AtomicLong lastCleanupTime = new AtomicLong(0);
    private final AtomicLong totalCleanedFiles = new AtomicLong(0);
    private final AtomicLong totalCleanedSize = new AtomicLong(0);
    private final String baseStoragePath;

    private ProfileArchiveManager(String baseStoragePath) {
        this.baseStoragePath = baseStoragePath;
        this.archiveStorage = new ArchiveStorage(baseStoragePath);
        this.archiveIndex = new ArchiveIndex();
    }

    /**
     * Get the singleton instance of ProfileArchiveManager.
     *
     * @param baseStoragePath the base storage path for profiles
     * @return the ProfileArchiveManager instance
     */
    public static ProfileArchiveManager getInstance(String baseStoragePath) {
        if (instance == null) {
            synchronized (ProfileArchiveManager.class) {
                if (instance == null) {
                    instance = new ProfileArchiveManager(baseStoragePath);
                }
            }
        }
        return instance;
    }

    /**
     * Initialize the archive manager synchronously.
     */
    public List<String> loadArchiveIfNeeded() {
        List<String> brokenFiles = null;
        if (initialized.compareAndSet(false, true)) {
            try {
                // Load existing archive index from storage
                brokenFiles = loadExistingArchives();

                LOG.info("ProfileArchiveManager initialized successfully. Loaded {} archived profiles",
                        archiveIndex.getEntryCount());
            } catch (Exception e) {
                LOG.error("Failed to initialize ProfileArchiveManager", e);
                initialized.set(false);
                throw new RuntimeException("Failed to initialize ProfileArchiveManager", e);
            }
        }
        return brokenFiles == null ? new ArrayList<>() : brokenFiles;
    }

    /**
     * Shutdown the archive manager.
     */
    public void shutdown() {
        if (initialized.compareAndSet(true, false)) {
            try {
                LOG.info("ProfileArchiveManager shutdown completed");
            } catch (Exception e) {
                LOG.error("Error during ProfileArchiveManager shutdown", e);
            }
        }
    }



    /**
     * Archive a profile synchronously.
     *
     * @param profile the profile to archive
     * @return true if archiving was successful
     */
    public boolean archiveProfile(Profile profile) {
        if (!Config.enable_profile_archive) {
            LOG.debug("Profile archive is disabled");
            return false;
        }

        if (!initialized.get()) {
            LOG.warn("ProfileArchiveManager is not initialized");
            return false;
        }

        try {
            String queryId = profile.getId();

            LOG.debug("Starting to archive profile: {}", queryId);

            // Check if already archived
            if (archiveIndex.containsEntry(queryId)) {
                LOG.warn("Profile {} is already archived", queryId);
                return false;
            }
            // In Debug Mode We Write profile to storage if needed
            // otherWise, we skip some profiles that has not been written to storage
            if (DebugPointUtil.isEnable("FE.ProfileArchiveManager.archiveProfile.writeBeforeArchive")) {
                if (profile.shouldStoreToStorage()) {
                    profile.writeToStorage(baseStoragePath);
                }
            }
            // Archive the profile
            ArchiveIndex.ArchiveEntry entry = archiveStorage.writeArchive(profile);
            if (entry == null) {
                LOG.error("Failed to write archive for profile: {}", queryId);
                return false;
            }

            // Add to index
            archiveIndex.addEntry(entry);

            // Update statistics
            totalArchivedProfiles.incrementAndGet();
            totalArchivedSize.addAndGet(entry.getFileSize());

            LOG.info("Successfully archived profile: {} (size: {} bytes)", queryId, entry.getFileSize());
            return true;

        } catch (Exception e) {
            LOG.error("Failed to archive profile: {}", profile.getId(), e);
            return false;
        }
    }

    /**
     * Retrieve an archived profile.
     *
     * @param queryId the query ID
     * @return the archived profile, or null if not found
     */
    public Profile getArchivedProfile(String queryId) {
        if (!Config.enable_profile_archive) {
            LOG.debug("Profile archive is disabled");
            return null;
        }

        if (!initialized.get()) {
            LOG.warn("ProfileArchiveManager is not initialized");
            return null;
        }

        ArchiveIndex.ArchiveEntry entry = archiveIndex.getEntry(queryId);
        if (entry == null) {
            LOG.debug("No archived profile found for query: {}", queryId);
            return null;
        }

        return archiveStorage.readArchive(entry);
    }

    /**
     * Check if a profile is archived.
     *
     * @param queryId the query ID
     * @return true if the profile is archived
     */
    public boolean isProfileArchived(String queryId) {
        if (!Config.enable_profile_archive) {
            return false;
        }

        if (!initialized.get()) {
            LOG.warn("ProfileArchiveManager is not initialized");
            return false;
        }

        if (queryId == null || queryId.isEmpty()) {
            return false;
        }

        return archiveIndex.containsEntry(queryId);
    }

    /**
     * Get archive statistics.
     *
     * @return a map containing archive statistics
     */
    public java.util.Map<String, Object> getArchiveStatistics() {
        java.util.Map<String, Object> stats = new java.util.HashMap<>();

        if (!initialized.get()) {
            stats.put("initialized", false);
            stats.put("total_count", 0L);
            stats.put("total_size", 0L);
            return stats;
        }

        stats.put("initialized", true);
        stats.put("total_count", (long) archiveIndex.getEntryCount());
        stats.put("total_size", archiveIndex.getTotalSize());
        stats.put("last_cleanup_time", lastCleanupTime.get());
        stats.put("total_cleaned_files", totalCleanedFiles.get());
        stats.put("total_cleaned_size", totalCleanedSize.get());

        return stats;
    }

    /**
     * Perform archive cleanup.
     * This method is called by ProfileManager's periodic maintenance task.
     */
    public void performArchiveCleanup() {
        if (!initialized.get()) {
            LOG.debug("ProfileArchiveManager is not initialized, skipping cleanup");
            return;
        }

        long startTime = System.currentTimeMillis();
        long deletedFiles = 0;
        long deletedSize = 0;

        try {
            LOG.info("Starting archive cleanup process");

            // Check if archive feature is enabled
            if (!Config.enable_profile_archive) {
                LOG.debug("Profile archive is disabled, skipping cleanup");
                return;
            }

            // Check if enough time has passed since last cleanup (6 hours)
            long currentTime = System.currentTimeMillis();
            long lastCleanup = lastCleanupTime.get();
            long cleanupIntervalMs = Config.profile_archive_cleanup_interval_seconds * 1000L;

            if (lastCleanup > 0 && (currentTime - lastCleanup) < cleanupIntervalMs) {
                LOG.debug("Skipping cleanup, not enough time passed since last cleanup");
                return;
            }

            // Cleanup by retention time
            long[] retentionStats = cleanupByRetentionTime();
            deletedFiles += retentionStats[0];
            deletedSize += retentionStats[1];

            // Cleanup by count and size limits
            long[] countAndSizeStats = cleanupByCountAndSize();
            deletedFiles += countAndSizeStats[0];
            deletedSize += countAndSizeStats[1];

            // Clean up empty directories after all file deletions are complete
            if (deletedFiles > 0) {
                LOG.debug("Cleaning up empty directories after deleting {} files", deletedFiles);
                archiveStorage.cleanupEmptyDirectories();
            }

            // Update statistics
            totalCleanedFiles.addAndGet(deletedFiles);
            totalCleanedSize.addAndGet(deletedSize);
            long endTime = System.currentTimeMillis();
            lastCleanupTime.set(endTime);

            long duration = endTime - startTime;
            LOG.info("Archive cleanup completed in {}ms. Deleted {} files, freed {} bytes",
                    duration, deletedFiles, deletedSize);

        } catch (Exception e) {
            LOG.error("Archive cleanup failed", e);
        }
    }

    /**
     * Cleanup archives based on retention time.
     *
     * @return array with [deletedFiles, deletedSize]
     */
    private long[] cleanupByRetentionTime() {
        long deletedFiles = 0;
        long deletedSize = 0;

        if (Config.profile_archive_retention_seconds < 0) {
            LOG.debug("Archive retention is disabled (retention_seconds <= 0)");
            return new long[]{deletedFiles, deletedSize};
        }

        long cutoffTimestamp = System.currentTimeMillis() - (Config.profile_archive_retention_seconds * 1000L);
        List<ArchiveIndex.ArchiveEntry> expiredEntries = archiveIndex.getEntriesOlderThan(cutoffTimestamp);

        LOG.info("Found {} expired archive entries (older than {} seconds)",
                expiredEntries.size(), Config.profile_archive_retention_seconds);

        for (ArchiveIndex.ArchiveEntry entry : expiredEntries) {
            try {
                // Delete the physical file
                if (archiveStorage.deleteArchive(entry)) {
                    // Remove from index
                    archiveIndex.removeEntry(entry.getQueryId());

                    deletedFiles++;
                    deletedSize += entry.getFileSize();

                    LOG.debug("Deleted expired archive: {}", entry.getQueryId());
                } else {
                    LOG.warn("Failed to delete expired archive file: {}", entry.getArchivePath());
                }
            } catch (Exception e) {
                LOG.error("Error deleting expired archive: {}", entry.getQueryId(), e);
            }
        }

        return new long[]{deletedFiles, deletedSize};
    }

    /**
     * Cleanup archives based on both count and size limits.
     * This method will delete the oldest archives first until both limits are satisfied.
     *
     * @return array with [deletedFiles, deletedSize]
     */
    private long[] cleanupByCountAndSize() {
        // Check if any cleanup is needed
        boolean sizeCleanupNeeded = Config.profile_archive_max_size_bytes > 0;
        boolean countCleanupNeeded = Config.max_archived_profile_num > 0;

        if (!sizeCleanupNeeded && !countCleanupNeeded) {
            LOG.debug("Both archive size and count limits are disabled");
            return new long[]{0, 0};
        }

        // Calculate current state
        long currentSize = archiveIndex.getTotalSize();
        int currentCount = archiveIndex.getEntryCount();

        // Determine what needs to be cleaned up
        long sizeToDelete = 0;
        int countToDelete = 0;
        boolean needSizeCleanup = false;
        boolean needCountCleanup = false;

        if (sizeCleanupNeeded && currentSize > Config.profile_archive_max_size_bytes) {
            sizeToDelete = currentSize - Config.profile_archive_max_size_bytes;
            needSizeCleanup = true;
            LOG.info("Archive size {} exceeds limit {}, need to delete {} bytes",
                    currentSize, Config.profile_archive_max_size_bytes, sizeToDelete);
        }

        if (countCleanupNeeded && currentCount > Config.max_archived_profile_num) {
            countToDelete = currentCount - Config.max_archived_profile_num;
            needCountCleanup = true;
            LOG.info("Archive count {} exceeds limit {}, need to delete {} files",
                    currentCount, Config.max_archived_profile_num, countToDelete);
        }

        if (!needSizeCleanup && !needCountCleanup) {
            LOG.debug("Current archive size {} and count {} are within limits", currentSize, currentCount);
            return new long[]{0, 0};
        }

        // Perform cleanup - inline implementation
        long deletedFiles = 0;
        long deletedSize = 0;

        // Get all entries ordered by time (oldest first)
        List<ArchiveIndex.ArchiveEntry> allEntries = archiveIndex.getAllEntries();
        // Sort by archive time (oldest first)
        allEntries.sort((e1, e2) -> Long.compare(e1.getQueryFinishTime(), e2.getQueryFinishTime()));

        int deletedCount = 0;
        for (ArchiveIndex.ArchiveEntry entry : allEntries) {
            // Check if we should stop deletion
            boolean sizeConditionMet = !needSizeCleanup || deletedSize >= sizeToDelete;
            boolean countConditionMet = !needCountCleanup || deletedCount >= countToDelete;
            if (sizeConditionMet && countConditionMet) {
                break;
            }

            try {
                // Delete the physical file
                if (archiveStorage.deleteArchive(entry)) {
                    // Remove from index
                    archiveIndex.removeEntry(entry.getQueryId());

                    deletedCount++;
                    deletedFiles++;
                    deletedSize += entry.getFileSize();

                    LOG.debug("Deleted archive for count and size limits: {} (size: {})",
                            entry.getQueryId(), entry.getFileSize());
                } else {
                    LOG.warn("Failed to delete archive file for count and size limits: {}", entry.getArchivePath());
                }
            } catch (Exception e) {
                LOG.error("Error deleting archive for count and size limits: {}", entry.getQueryId(), e);
            }
        }

        // Log summary
        if (needSizeCleanup && needCountCleanup) {
            LOG.info("Deleted {} files ({} bytes) to comply with count and size limits", deletedCount, deletedSize);
        } else if (needSizeCleanup) {
            LOG.info("Deleted {} bytes to comply with size limit", deletedSize);
        } else if (needCountCleanup) {
            LOG.info("Deleted {} files to comply with count limit", deletedCount);
        }

        return new long[]{deletedFiles, deletedSize};
    }


    /**
     * Load existing archives from storage into the index.
     */
    private List<String> loadExistingArchives() {
        LOG.info("Loading existing archived profiles from storage");

        List<Path> archivedFiles = archiveStorage.getAllArchivedFiles();
        List<String> brokenFiles = new ArrayList<>();
        int loadedCount = 0;
        long totalSize = 0;

        for (Path archivePath : archivedFiles) {
            try {
                String fileName = archivePath.getFileName().toString();
                String[] parsed = Profile.parseProfileFileName(fileName);
                if (parsed == null) {
                    LOG.warn("Invalid archived profile filename: {}", fileName);
                    brokenFiles.add(fileName);
                    continue;
                }

                String queryId = parsed[1];
                long queryFinishTime = 0;
                try {
                    queryFinishTime = Long.parseLong(parsed[0]);
                } catch (Exception e) {
                    LOG.warn("Invalid archived profile filename: {}", fileName);
                    brokenFiles.add(fileName);
                    continue;
                }

                long fileSize = Files.size(archivePath);
                ArchiveIndex.ArchiveEntry entry = new ArchiveIndex.ArchiveEntry(
                        queryId, archivePath.toString(), queryFinishTime, fileSize
                );
                archiveIndex.addEntry(entry);
                loadedCount++;
                totalSize += fileSize;
            } catch (Exception e) {
                LOG.warn("Failed to load archived profile: {}", archivePath, e);
            }
        }

        totalArchivedProfiles.set(loadedCount);
        totalArchivedSize.set(totalSize);

        LOG.info("Loaded {} archived profiles with total size {} bytes", loadedCount, totalSize);
        return brokenFiles;
    }

    /**
     * Extract archive time from file path.
     *
     * @param archivePath the archive file path
     * @return the archive time
     */
    private LocalDateTime extractArchiveTimeFromPath(Path archivePath) {
        try {
            // Try to extract time from filename (format: yyyyMMdd_HHmmss_queryId.zip)
            String fileName = archivePath.getFileName().toString();
            if (fileName.matches("\\d{8}_\\d{6}_.*\\.zip")) {
                String timeStr = fileName.substring(0, 15); // yyyyMMdd_HHmmss
                return LocalDateTime.parse(timeStr, java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            }
        } catch (Exception e) {
            LOG.debug("Failed to extract time from filename: {}", archivePath.getFileName(), e);
        }

        // Fallback to file modification time
        try {
            return LocalDateTime.ofInstant(
                    Files.getLastModifiedTime(archivePath).toInstant(),
                    java.time.ZoneId.systemDefault()
            );
        } catch (Exception e) {
            LOG.warn("Failed to get file modification time for: {}", archivePath, e);
            return LocalDateTime.now();
        }
    }

}
