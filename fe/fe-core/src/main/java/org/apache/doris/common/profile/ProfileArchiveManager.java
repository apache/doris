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

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * ProfileArchiveManager handles archiving of profile files from the spilled profile storage
 * to compressed ZIP archives. This is triggered when the number of profiles or total storage
 * size exceeds configured limits.
 *
 * <p>Archive Strategy:
 * <ul>
 *   <li>Profiles are moved to pending directory first</li>
 *   <li>When pending has >= batch_size files OR oldest file > timeout hours: archive is triggered</li>
 *   <li>Batch size: configurable via Config.profile_archive_batch_size (default 1000)</li>
 *   <li>Naming pattern: profiles_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS.zip</li>
 *   <li>Archive location: ${LOG_DIR}/profile/archive (or Config.profile_archive_path)</li>
 *   <li>Archives oldest profiles first (by finish timestamp)</li>
 * </ul>
 *
 * <p>Directory Structure:
 * <pre>
 * ${spilled_profile_storage_path}/
 * ├── {timestamp}_{queryid}.zip          (active spilled profiles)
 * └── archive/
 *     ├── pending/                       (pending buffer for archiving)
 *     │   └── {timestamp}_{queryid}.zip
 *     └── profiles_20240101_000000_20240101_235959.zip  (archived ZIPs)
 * </pre>
 */
public class ProfileArchiveManager {
    private static final Logger LOG = LogManager.getLogger(ProfileArchiveManager.class);

    // Default batch size for archiving profiles
    private static final int DEFAULT_ARCHIVE_BATCH_SIZE = 100;

    // Date format for archive file names (thread-safe)
    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(ZoneId.systemDefault());

    // Archive directory name
    private static final String ARCHIVE_DIR_NAME = "archive";

    // Pending directory name (buffer for files waiting to be archived)
    private static final String PENDING_DIR_NAME = "pending";

    private final String spilledProfileStoragePath;
    private final String archivePath;
    private final String pendingPath;
    private final int archiveBatchSize;

    /**
     * Constructs a ProfileArchiveManager with default batch size.
     *
     * @param spilledProfileStoragePath the path where spilled profiles are stored
     */
    public ProfileArchiveManager(String spilledProfileStoragePath) {
        this(spilledProfileStoragePath, DEFAULT_ARCHIVE_BATCH_SIZE);
    }

    /**
     * Constructs a ProfileArchiveManager with custom batch size.
     *
     * @param spilledProfileStoragePath the path where spilled profiles are stored
     * @param archiveBatchSize number of profiles to include in each archive
     */
    public ProfileArchiveManager(String spilledProfileStoragePath, int archiveBatchSize) {
        this.spilledProfileStoragePath = spilledProfileStoragePath;
        // Use custom archive path from config if specified, otherwise use default
        this.archivePath = Config.profile_archive_path.isEmpty()
                ? spilledProfileStoragePath + File.separator + ARCHIVE_DIR_NAME
                : Config.profile_archive_path;
        this.pendingPath = archivePath + File.separator + PENDING_DIR_NAME;
        this.archiveBatchSize = archiveBatchSize;
    }

    /**
     * Creates the archive directory if it doesn't exist.
     *
     * @return true if directory exists or was created successfully
     */
    public boolean createArchiveDirectoryIfNecessary() {
        File archiveDir = new File(archivePath);
        if (archiveDir.exists()) {
            return true;
        }

        if (archiveDir.mkdirs()) {
            LOG.info("Created profile archive directory: {}", archivePath);
            return true;
        } else {
            LOG.error("Failed to create profile archive directory: {}", archivePath);
            return false;
        }
    }

    /**
     * Creates the pending directory if it doesn't exist.
     *
     * @return true if directory exists or was created successfully
     */
    public boolean createPendingDirectoryIfNecessary() {
        File pendingDir = new File(pendingPath);
        if (pendingDir.exists()) {
            return true;
        }

        if (pendingDir.mkdirs()) {
            LOG.info("Created profile pending directory: {}", pendingPath);
            return true;
        } else {
            LOG.error("Failed to create profile pending directory: {}", pendingPath);
            return false;
        }
    }

    /**
     * Gets the list of profile files from the spilled storage directory,
     * excluding the archive subdirectory.
     *
     * @return list of profile files sorted by filename timestamp (oldest first)
     */
    public List<File> getProfileFilesFromStorage() {
        File storageDir = new File(spilledProfileStoragePath);
        if (!storageDir.exists() || !storageDir.isDirectory()) {
            LOG.warn("Profile storage directory does not exist: {}", spilledProfileStoragePath);
            return new ArrayList<>();
        }

        File[] files = storageDir.listFiles(file -> {
            // Only include files (not directories), exclude hidden files and archive directory
            return file.isFile() && !file.getName().startsWith(".");
        });

        if (files == null || files.length == 0) {
            return new ArrayList<>();
        }

        // Sort by timestamp extracted from filename (oldest first)
        List<File> profileFiles = new ArrayList<>(Arrays.asList(files));
        profileFiles.sort(Comparator.comparingLong(file -> {
            long timestamp = extractTimestampFromProfileFile(file);
            // Files with invalid timestamps go to the end
            return timestamp == -1 ? Long.MAX_VALUE : timestamp;
        }));

        return profileFiles;
    }

    /**
     * Gets the list of profile files from the pending directory.
     *
     * @return list of profile files sorted by filename timestamp (oldest first)
     */
    private List<File> getPendingProfileFiles() {
        File pendingDir = new File(pendingPath);
        if (!pendingDir.exists() || !pendingDir.isDirectory()) {
            return new ArrayList<>();
        }

        File[] files = pendingDir.listFiles(File::isFile);
        if (files == null || files.length == 0) {
            return new ArrayList<>();
        }

        List<File> profileFiles = new ArrayList<>(Arrays.asList(files));
        // Sort by timestamp extracted from filename (oldest first)
        profileFiles.sort(Comparator.comparingLong(file -> {
            long timestamp = extractTimestampFromProfileFile(file);
            // Files with invalid timestamps go to the end
            return timestamp == -1 ? Long.MAX_VALUE : timestamp;
        }));

        return profileFiles;
    }

    /**
     * Extracts timestamp from profile filename.
     * Profile filename format: timestamp_queryid.zip
     *
     * @param profileFile the profile file
     * @return timestamp in milliseconds, or -1 if parsing fails
     */
    private long extractTimestampFromProfileFile(File profileFile) {
        String filename = profileFile.getName();
        if (!filename.endsWith(".zip")) {
            return -1;
        }

        // Remove .zip extension
        filename = filename.substring(0, filename.length() - 4);

        // Extract timestamp (format: timestamp_queryid)
        String[] parts = filename.split("_", 2);
        if (parts.length < 1) {
            return -1;
        }

        try {
            return Long.parseLong(parts[0]);
        } catch (NumberFormatException e) {
            LOG.warn("Failed to parse timestamp from profile filename: {}", profileFile.getName());
            return -1;
        }
    }

    /**
     * Extracts timestamp from archive filename.
     * Archive filename format: profiles_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS.zip
     * or profiles_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS_N.zip (with suffix)
     *
     * @param archiveFile the archive file
     * @return timestamp in milliseconds (parsed from first date), or -1 if parsing fails
     */
    private long extractTimestampFromArchiveFile(File archiveFile) {
        String filename = archiveFile.getName();
        if (!filename.startsWith("profiles_") || !filename.endsWith(".zip")) {
            return -1;
        }

        // Remove "profiles_" prefix and ".zip" extension
        // Format: YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS[_N]
        String dateStr = filename.substring(9, filename.length() - 4);

        // Extract first timestamp: YYYYMMDD_HHMMSS
        String[] parts = dateStr.split("_");
        if (parts.length < 2) {
            return -1;
        }

        try {
            // Parse YYYYMMDD_HHMMSS format
            String firstTimestamp = parts[0] + "_" + parts[1];
            Instant instant = Instant.from(DATE_FORMAT.parse(firstTimestamp));
            return instant.toEpochMilli();
        } catch (Exception e) {
            LOG.warn("Failed to parse timestamp from archive filename: {}", archiveFile.getName(), e);
            return -1;
        }
    }

    /**
     * Archives a batch of profile files into a single ZIP archive.
     *
     * @param profilesToArchive list of profile files to archive
     * @return the created archive file, or null if archiving failed
     */
    public File archiveProfiles(List<File> profilesToArchive) {
        if (profilesToArchive == null || profilesToArchive.isEmpty()) {
            LOG.warn("No profiles to archive");
            return null;
        }

        if (!createArchiveDirectoryIfNecessary()) {
            LOG.error("Failed to create archive directory");
            return null;
        }

        // Extract timestamps to determine archive filename
        List<Long> timestamps = profilesToArchive.stream()
                .map(this::extractTimestampFromProfileFile)
                .filter(ts -> ts > 0)
                .sorted()
                .collect(Collectors.toList());

        if (timestamps.isEmpty()) {
            LOG.error("Failed to extract timestamps from profile files");
            return null;
        }

        long minTimestamp = timestamps.get(0);
        long maxTimestamp = timestamps.get(timestamps.size() - 1);

        // Generate archive filename: profiles_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS.zip
        String minDateStr = DATE_FORMAT.format(Instant.ofEpochMilli(minTimestamp));
        String maxDateStr = DATE_FORMAT.format(Instant.ofEpochMilli(maxTimestamp));
        String archiveFilename = String.format("profiles_%s_%s.zip", minDateStr, maxDateStr);

        File archiveFile = new File(archivePath, archiveFilename);

        // If archive file already exists, append a suffix
        int suffix = 1;
        while (archiveFile.exists()) {
            archiveFilename = String.format("profiles_%s_%s_%d.zip", minDateStr, maxDateStr, suffix);
            archiveFile = new File(archivePath, archiveFilename);
            suffix++;
        }

        // Create the archive
        try (FileOutputStream fos = new FileOutputStream(archiveFile);
                ZipOutputStream zos = new ZipOutputStream(fos)) {

            for (File profileFile : profilesToArchive) {
                try (FileInputStream fis = new FileInputStream(profileFile)) {
                    // Add profile file to archive with its original name
                    ZipEntry zipEntry = new ZipEntry(profileFile.getName());
                    zos.putNextEntry(zipEntry);

                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = fis.read(buffer)) != -1) {
                        zos.write(buffer, 0, bytesRead);
                    }

                    zos.closeEntry();
                } catch (IOException e) {
                    LOG.error("Failed to add profile {} to archive", profileFile.getName(), e);
                    // Continue with other files
                }
            }

            LOG.info("Created archive {} with {} profiles", archiveFilename, profilesToArchive.size());
            return archiveFile;

        } catch (IOException e) {
            LOG.error("Failed to create archive file: {}", archiveFilename, e);
            // Clean up partial archive file
            FileUtils.deleteQuietly(archiveFile);
            return null;
        }
    }

    /**
     * Deletes profile files from the spilled storage after they have been archived.
     *
     * @param profileFiles list of profile files to delete
     * @return number of files successfully deleted
     */
    public int deleteArchivedProfiles(List<File> profileFiles) {
        int deletedCount = 0;

        for (File profileFile : profileFiles) {
            if (FileUtils.deleteQuietly(profileFile)) {
                deletedCount++;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Deleted archived profile: {}", profileFile.getName());
                }
            } else {
                LOG.warn("Failed to delete archived profile: {}", profileFile.getName());
            }
        }

        LOG.info("Deleted {} out of {} archived profiles", deletedCount, profileFiles.size());
        return deletedCount;
    }

    /**
     * Moves a profile file to the pending archive directory.
     * The file will be archived later when batch size is reached or timeout occurs.
     *
     * @param profileFile the profile file to move
     * @return true if the file was moved successfully, false otherwise
     */
    public boolean moveToArchivePending(File profileFile) {
        if (profileFile == null || !profileFile.exists()) {
            LOG.warn("Profile file does not exist: {}", profileFile);
            return false;
        }

        // Create pending directory if necessary
        if (!createPendingDirectoryIfNecessary()) {
            LOG.error("Failed to create pending directory");
            return false;
        }

        // Move file to pending directory
        File targetFile = new File(pendingPath, profileFile.getName());
        try {
            Files.move(profileFile.toPath(), targetFile.toPath(),
                    StandardCopyOption.REPLACE_EXISTING);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Moved profile to pending: {}", profileFile.getName());
            }
            return true;
        } catch (IOException e) {
            LOG.error("Failed to move profile to pending: {}", profileFile.getName(), e);
            return false;
        }
    }

    /**
     * Checks the pending directory and archives profiles if conditions are met.
     * Archiving is triggered when:
     * 1. Number of pending files >= batch size, OR
     * 2. Oldest file in pending has exceeded timeout hours
     *
     * Archiving strategy:
     * - If triggered by batch size: only archive complete batches, leave incomplete batch in pending
     * - If triggered by timeout: archive all files including incomplete batch
     *
     * @return number of profiles successfully archived
     */
    public int checkAndArchivePendingProfiles() {
        File pendingDir = new File(pendingPath);
        if (!pendingDir.exists()) {
            return 0;
        }

        // Get all pending files
        List<File> pendingFiles = getPendingProfileFiles();
        if (pendingFiles.isEmpty()) {
            return 0;
        }

        // Check if archiving should be triggered
        boolean shouldArchive = false;
        boolean isTimeoutTriggered = false;
        String reason = "";

        // Condition 1: Number of files >= batch size
        if (pendingFiles.size() >= archiveBatchSize) {
            shouldArchive = true;
            isTimeoutTriggered = false;
            reason = String.format("batch size reached (%d files >= %d)",
                    pendingFiles.size(), archiveBatchSize);
        }

        // Condition 2: Oldest file exceeds timeout
        // Use filename timestamp (query finish time) instead of file modification time
        if (!shouldArchive) {
            long oldestFileTimestamp = extractTimestampFromProfileFile(pendingFiles.get(0));
            if (oldestFileTimestamp > 0) {
                long currentTime = System.currentTimeMillis();
                long ageSeconds = (currentTime - oldestFileTimestamp) / 1000;

                if (ageSeconds >= Config.profile_archive_pending_timeout_seconds) {
                    shouldArchive = true;
                    isTimeoutTriggered = true;
                    reason = String.format("oldest file age %d seconds exceeds timeout %d seconds",
                            ageSeconds, Config.profile_archive_pending_timeout_seconds);
                }
            }
        }

        if (!shouldArchive) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Pending profiles: {} files, not ready to archive", pendingFiles.size());
            }
            return 0;
        }

        // Determine how many files to archive based on trigger condition
        int filesToArchiveCount;
        if (isTimeoutTriggered) {
            // Timeout triggered: archive all files including incomplete batch
            filesToArchiveCount = pendingFiles.size();
            LOG.info("Archiving all {} pending profiles due to timeout, reason: {}",
                    filesToArchiveCount, reason);
        } else {
            // Batch size triggered: only archive complete batches
            filesToArchiveCount = (pendingFiles.size() / archiveBatchSize) * archiveBatchSize;
            int remainingFiles = pendingFiles.size() - filesToArchiveCount;
            LOG.info("Archiving {} pending profiles (complete batches only), {} files remain in pending, reason: {}",
                    filesToArchiveCount, remainingFiles, reason);
        }

        int totalArchived = 0;
        // Archive in batches
        List<File> filesToArchive = pendingFiles.subList(0, filesToArchiveCount);
        for (int i = 0; i < filesToArchive.size(); i += archiveBatchSize) {
            int endIdx = Math.min(i + archiveBatchSize, filesToArchive.size());
            List<File> batch = filesToArchive.subList(i, endIdx);

            File archiveFile = archiveProfiles(batch);
            if (archiveFile != null) {
                // Delete original files after successful archiving
                int deleted = deleteArchivedProfiles(batch);
                totalArchived += deleted;
            } else {
                LOG.error("Failed to create archive for batch starting at index {}", i);
                // Continue with next batch
            }
        }

        LOG.info("Successfully archived {} profiles from pending, {} files remain",
                totalArchived, pendingFiles.size() - totalArchived);
        return totalArchived;
    }

    /**
     * Archives profiles in batches from the spilled storage.
     * This should be called when storage limits are exceeded.
     *
     * @param numProfilesToArchive number of profiles to archive
     * @return number of profiles successfully archived
     */
    public int archiveOldestProfiles(int numProfilesToArchive) {
        if (numProfilesToArchive <= 0) {
            return 0;
        }

        List<File> allProfileFiles = getProfileFilesFromStorage();
        if (allProfileFiles.isEmpty()) {
            LOG.info("No profiles available to archive");
            return 0;
        }

        // Limit to available profiles
        int profilesToArchive = Math.min(numProfilesToArchive, allProfileFiles.size());
        List<File> filesToArchive = allProfileFiles.subList(0, profilesToArchive);

        int totalArchived = 0;

        // Archive in batches
        for (int i = 0; i < filesToArchive.size(); i += archiveBatchSize) {
            int endIdx = Math.min(i + archiveBatchSize, filesToArchive.size());
            List<File> batch = filesToArchive.subList(i, endIdx);

            File archiveFile = archiveProfiles(batch);
            if (archiveFile != null) {
                // Delete original files after successful archiving
                int deleted = deleteArchivedProfiles(batch);
                totalArchived += deleted;
            } else {
                LOG.error("Failed to create archive for batch starting at index {}", i);
                // Continue with next batch
            }
        }

        LOG.info("Successfully archived {} profiles", totalArchived);
        return totalArchived;
    }

    /**
     * Gets the list of archive files sorted by archive timestamp (from filename).
     *
     * @return list of archive files sorted by archive timestamp (oldest first)
     */
    public List<File> getArchiveFiles() {
        File archiveDir = new File(archivePath);
        if (!archiveDir.exists() || !archiveDir.isDirectory()) {
            return new ArrayList<>();
        }

        File[] files = archiveDir.listFiles(file ->
            file.isFile() && file.getName().startsWith("profiles_") && file.getName().endsWith(".zip")
        );

        if (files == null || files.length == 0) {
            return new ArrayList<>();
        }

        List<File> archiveFiles = new ArrayList<>();
        for (File file : files) {
            archiveFiles.add(file);
        }

        // Sort by timestamp extracted from archive filename (oldest first)
        archiveFiles.sort(Comparator.comparingLong(file -> {
            long timestamp = extractTimestampFromArchiveFile(file);
            // Files with invalid timestamps go to the end
            return timestamp == -1 ? Long.MAX_VALUE : timestamp;
        }));

        return archiveFiles;
    }

    /**
     * Gets the total size of all archive files in bytes.
     *
     * @return total archive size in bytes
     */
    public long getTotalArchiveSize() {
        List<File> archiveFiles = getArchiveFiles();
        long totalSize = 0;

        for (File file : archiveFiles) {
            totalSize += file.length();
        }

        return totalSize;
    }

    /**
     * Gets the archive directory path.
     *
     * @return archive directory path
     */
    public String getArchivePath() {
        return archivePath;
    }

    /**
     * Cleans up old archive files that exceed the retention period.
     * Retention period is configured via Config.profile_archive_retention_seconds.
     * -1 means keep forever, 0 means don't retain (disable archiving), >0 means delete after N seconds.
     *
     * The age of an archive file is determined by the timestamp in its filename
     * (profiles_YYYYMMDD_HHMMSS_...), not by the file system modification time.
     *
     * @return number of archive files deleted
     */
    public int cleanOldArchives() {
        if (Config.profile_archive_retention_seconds < 0) {
            // -1 means keep forever
            if (LOG.isDebugEnabled()) {
                LOG.debug("Archive retention is set to unlimited, no cleanup needed");
            }
            return 0;
        }

        if (Config.profile_archive_retention_seconds == 0) {
            // 0 means don't retain, but this should be handled by disabling archive feature
            LOG.warn("profile_archive_retention_seconds is 0, consider disabling archive feature");
            return 0;
        }

        List<File> archiveFiles = getArchiveFiles();
        if (archiveFiles.isEmpty()) {
            return 0;
        }

        long retentionMillis = Config.profile_archive_retention_seconds * 1000L;
        long currentTime = System.currentTimeMillis();

        int deletedCount = 0;
        for (File archiveFile : archiveFiles) {
            // Use timestamp from filename instead of file modification time
            long archiveTimestamp = extractTimestampFromArchiveFile(archiveFile);
            if (archiveTimestamp <= 0) {
                // Skip files with invalid timestamps
                LOG.warn("Skipping archive file with invalid timestamp: {}", archiveFile.getName());
                continue;
            }

            long fileAge = currentTime - archiveTimestamp;
            if (fileAge >= retentionMillis) {
                if (FileUtils.deleteQuietly(archiveFile)) {
                    deletedCount++;
                    LOG.info("Deleted old archive file: {}, age: {} seconds",
                            archiveFile.getName(), fileAge / 1000);
                } else {
                    LOG.warn("Failed to delete old archive file: {}", archiveFile.getName());
                }
            }
        }

        if (deletedCount > 0) {
            LOG.info("Cleaned {} old archive files, retention: {} seconds",
                    deletedCount, Config.profile_archive_retention_seconds);
        }

        return deletedCount;
    }
}
