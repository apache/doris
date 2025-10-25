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

import org.apache.doris.common.profile.ArchiveIndex.ArchiveEntry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * ArchiveStorage handles the physical storage management of archived profile files.
 * It organizes archived profiles in a hierarchical directory structure by date (year/month/day).
 */
public class ArchiveStorage {
    private static final Logger LOG = LogManager.getLogger(ArchiveStorage.class);

    private static final String ARCHIVE_DIR_NAME = "archive";
    private static final DateTimeFormatter YEAR_FORMATTER = DateTimeFormatter.ofPattern("yyyy");
    private static final DateTimeFormatter MONTH_FORMATTER = DateTimeFormatter.ofPattern("MM");
    private static final DateTimeFormatter DAY_FORMATTER = DateTimeFormatter.ofPattern("dd");

    private final String archiveBasePath;

    public ArchiveStorage(String baseStoragePath) {
        this.archiveBasePath = Paths.get(baseStoragePath, ARCHIVE_DIR_NAME).toString();
        createArchiveDirectoryIfNecessary();
    }

    /**
     * Write a profile to archive storage.
     *
     * @param profile the profile to archive
     * @return the ArchiveEntry containing metadata about the archived profile, or null if failed
     */
    public ArchiveEntry writeArchive(Profile profile) {
        String queryId = profile.getId();
        if (queryId == null || queryId.isEmpty()) {
            LOG.warn("Profile has no id, cannot archive");
            return null;
        }
        try {
            // Get the original profile file path
            String originalFilePath = profile.getProfileStoragePath();
            if (originalFilePath == null || originalFilePath.isEmpty()) {
                LOG.warn("Profile {} has no file path, cannot archive", queryId);
                return null;
            }

            Path originalPath = Paths.get(originalFilePath);
            if (!Files.exists(originalPath)) {
                LOG.warn("Profile file {} does not exist, cannot archive", originalFilePath);
                return null;
            }

            Path archivePath = getArchivePath(originalFilePath);
            createDirectoryIfNecessary(archivePath.getParent());

            // Move the file to archive location
            Files.move(originalPath, archivePath, StandardCopyOption.REPLACE_EXISTING);

            // Get file size and query finish time
            long fileSize = Files.size(archivePath);
            long queryFinishTime = profile.getQueryFinishTimestamp();

            LOG.info("Successfully archived profile {} to {}", queryId, archivePath);

            // Return ArchiveEntry with metadata
            return new ArchiveEntry(queryId, archivePath.toString(), queryFinishTime, fileSize);
        } catch (IOException e) {
            LOG.error("Failed to archive profile {}", queryId, e);
            return null;
        }
    }

    /**
     * Read a profile from archive storage using ArchiveEntry.
     *
     * @param entry the archive entry containing metadata
     * @return the profile object, or null if failed to read
     */
    public Profile readArchive(ArchiveEntry entry) {
        if (entry == null) {
            LOG.warn("ArchiveEntry is null, cannot read archive");
            return null;
        }
        return readArchive(Paths.get(entry.getArchivePath()));
    }

    /**
     * Read a profile from archive storage.
     *
     * @param archivePath the path to the archived profile file
     * @return the profile object, or null if failed to read
     */
    private Profile readArchive(Path archivePath) {
        try {
            if (!Files.exists(archivePath)) {
                LOG.warn("Archive file {} does not exist", archivePath);
                return null;
            }

            Profile profile = Profile.read(archivePath.toString());
            if (profile != null) {
                LOG.debug("Successfully read archived profile from {}", archivePath);
            } else {
                LOG.warn("Failed to read archived profile from {}", archivePath);
            }

            return profile;
        } catch (Exception e) {
            LOG.error("Failed to read archived profile from {}", archivePath, e);
            return null;
        }
    }

    /**
     * Check if an archived profile exists using ArchiveEntry.
     *
     * @param entry the archive entry containing metadata
     * @return true if the archived profile exists
     */
    public boolean existsArchive(ArchiveEntry entry) {
        if (entry == null) {
            LOG.warn("ArchiveEntry is null, cannot check existence");
            return false;
        }
        return Files.exists(Paths.get(entry.getArchivePath()));
    }

    /**
     * Check if an archived profile exists.
     *
     * @param originalFilePath the original profile file path
     * @return true if the archived profile exists
     */
    public boolean existsArchive(String originalFilePath) {
        Path archivePath = getArchivePath(originalFilePath);
        return Files.exists(archivePath);
    }

    /**
     * Delete an archived profile file using ArchiveEntry.
     *
     * @param entry the archive entry containing metadata
     * @return true if successfully deleted
     */
    public boolean deleteArchive(ArchiveEntry entry) {
        if (entry == null) {
            LOG.warn("ArchiveEntry is null, cannot delete archive");
            return false;
        }
        return deleteArchive(Paths.get(entry.getArchivePath()));
    }

    /**
     * Delete an archived profile file.
     *
     * @param archivePath the path to the archived profile file
     * @return true if successfully deleted
     */
    private boolean deleteArchive(Path archivePath) {
        try {
            if (Files.exists(archivePath)) {
                Files.delete(archivePath);
                LOG.debug("Successfully deleted archived profile {}", archivePath);
                return true;
            } else {
                LOG.warn("Archive file {} does not exist, cannot delete", archivePath);
                return false;
            }
        } catch (IOException e) {
            LOG.error("Failed to delete archived profile {}", archivePath, e);
            return false;
        }
    }


    /**
     * Clean up all empty directories in the archive directory tree.
     * This method performs a batch cleanup of empty directories after bulk delete operations.
     * It's more efficient than cleaning up directories after each individual file deletion.
     */
    public void cleanupEmptyDirectories() {
        Path archiveBasePath = Paths.get(this.archiveBasePath);
        if (!Files.exists(archiveBasePath)) {
            return;
        }

        LOG.debug("Starting batch cleanup of empty directories in archive");
        try {
            cleanupEmptyDirectoriesRecursively(archiveBasePath);
            LOG.debug("Completed batch cleanup of empty directories");
        } catch (Exception e) {
            LOG.warn("Error during batch cleanup of empty directories: {}", e.getMessage());
        }
    }

    /**
     * Recursively clean up empty directories in the given directory tree.
     *
     * @param rootPath the root directory to start cleaning from
     */
    private void cleanupEmptyDirectoriesRecursively(Path rootPath) {
        if (!Files.exists(rootPath) || !Files.isDirectory(rootPath)) {
            return;
        }

        try {
            // First, recursively clean up subdirectories
            Files.list(rootPath)
                    .filter(Files::isDirectory)
                    .forEach(this::cleanupEmptyDirectoriesRecursively);

            // Then check if current directory is empty (but don't delete the archive base directory)
            Path archiveBasePath = Paths.get(this.archiveBasePath);
            if (!rootPath.equals(archiveBasePath) && isDirectoryEmpty(rootPath)) {
                Files.delete(rootPath);
                LOG.debug("Deleted empty directory: {}", rootPath);
            }
        } catch (IOException e) {
            LOG.warn("Failed to clean up directory {}: {}", rootPath, e.getMessage());
        }
    }

    /**
     * Check if a directory is empty.
     *
     * @param dirPath the directory path to check
     * @return true if the directory is empty, false otherwise
     */
    private boolean isDirectoryEmpty(Path dirPath) {
        try (java.util.stream.Stream<Path> stream = Files.list(dirPath)) {
            return !stream.findAny().isPresent();
        } catch (IOException e) {
            LOG.warn("Failed to check if directory {} is empty: {}", dirPath, e.getMessage());
            return false;
        }
    }

    /**
     * Get all archived profile files.
     *
     * @return list of archived profile file paths
     */
    public List<Path> getAllArchivedFiles() {
        List<Path> archivedFiles = new ArrayList<>();
        try {
            Path archiveDir = Paths.get(archiveBasePath);
            if (!Files.exists(archiveDir)) {
                return archivedFiles;
            }

            Files.walk(archiveDir)
                    .filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".zip"))
                    .forEach(archivedFiles::add);

        } catch (IOException e) {
            LOG.error("Failed to list archived files", e);
        }

        return archivedFiles;
    }

    /**
     * Get the archive path for a given profile file.
     *
     * @param originalFilePath the original profile file path
     * @return the archive file path
     */
    private Path getArchivePath(String originalFilePath) {
        // Extract the original filename from the original file path
        Path originalPath = Paths.get(originalFilePath);
        String originalFileName = originalPath.getFileName().toString();

        // Prefer to reuse Profile.parseProfileFileName to obtain timestamp and id
        String[] timeAndId = Profile.parseProfileFileName(originalFileName);
        LocalDateTime queryFinishTime = null;
        if (timeAndId != null) {
            String timestampStr = timeAndId[0];
            long millis = Long.parseLong(timestampStr);
            queryFinishTime = LocalDateTime.ofInstant(
                    java.time.Instant.ofEpochMilli(millis),
                    java.time.ZoneId.systemDefault()
                );
        }

        String year = queryFinishTime.format(YEAR_FORMATTER);
        String month = queryFinishTime.format(MONTH_FORMATTER);
        String day = queryFinishTime.format(DAY_FORMATTER);

        return Paths.get(archiveBasePath, year, month, day, originalFileName);
    }

    /**
     * Create the archive base directory if it doesn't exist.
     */
    private void createArchiveDirectoryIfNecessary() {
        createDirectoryIfNecessary(Paths.get(archiveBasePath));
    }

    /**
     * Create a directory if it doesn't exist.
     *
     * @param dirPath the directory path to create
     */
    private void createDirectoryIfNecessary(Path dirPath) {
        try {
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
                LOG.debug("Created directory {}", dirPath);
            }
        } catch (IOException e) {
            LOG.error("Failed to create directory {}", dirPath, e);
        }
    }

    /**
     * Get the archive base path.
     *
     * @return the archive base path
     */
    public String getArchiveBasePath() {
        return archiveBasePath;
    }

    /**
     * Get the total size of all archived files.
     *
     * @return total size in bytes
     */
    public long getTotalArchivedSize() {
        long totalSize = 0;
        try {
            Path archiveDir = Paths.get(archiveBasePath);
            if (Files.exists(archiveDir)) {
                totalSize = Files.walk(archiveDir)
                        .filter(Files::isRegularFile)
                        .mapToLong(path -> {
                            try {
                                return Files.size(path);
                            } catch (IOException e) {
                                LOG.warn("Failed to get size of file {}", path, e);
                                return 0;
                            }
                        })
                        .sum();
            }
        } catch (IOException e) {
            LOG.error("Failed to calculate total archived size", e);
        }

        return totalSize;
    }
}
