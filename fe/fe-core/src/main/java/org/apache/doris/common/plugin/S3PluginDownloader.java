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

package org.apache.doris.common.plugin;

import org.apache.doris.backup.Status;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.fs.obj.S3ObjStorage;
import org.apache.doris.fs.remote.RemoteFile;

import com.google.common.base.Strings;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * S3PluginDownloader is an independent, generic S3 downloader.
 * <p>
 * Design principles:
 * 1. Single responsibility: Only downloads files from S3, no business logic
 * 2. Complete decoupling: No dependency on cloud mode or Doris-specific configuration
 * 3. Reusable: Supports both cloud mode auto-configuration and manual S3 parameters
 * 4. Features: Single file download, batch directory download, MD5 verification, retry mechanism
 */
public class S3PluginDownloader implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(S3PluginDownloader.class);
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 1000;

    private final S3ObjStorage s3Storage;

    /**
     * S3 configuration info (completely independent, no dependency on any Doris internal types)
     */
    public static class S3Config {
        public final String endpoint;
        public final String region;
        public final String bucket;
        public final String accessKey;
        public final String secretKey;

        public S3Config(String endpoint, String region, String bucket,
                String accessKey, String secretKey) {
            this.endpoint = endpoint;
            this.region = region;
            this.bucket = bucket;
            this.accessKey = accessKey;
            this.secretKey = secretKey;
        }

        @Override
        public String toString() {
            return String.format("S3Config{endpoint='%s', region='%s', bucket='%s', accessKey='%s'}",
                    endpoint, region, bucket, accessKey != null ? "***" : "null");
        }
    }

    /**
     * Constructor - pass in S3 configuration
     */
    public S3PluginDownloader(S3Config config) throws Exception {
        this.s3Storage = createS3Storage(config);
    }

    // ======================== Core Download Methods ========================

    /**
     * Download single file (supports MD5 verification and retry)
     *
     * @param remoteS3Path complete S3 path like "s3://bucket/path/to/file.jar"
     * @param localPath local target file path
     * @param expectedMd5 optional MD5 verification value, null to skip verification
     * @return returns local file path on success, empty string on failure
     */
    public String downloadFile(String remoteS3Path, String localPath, String expectedMd5) {
        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                return doDownloadFile(remoteS3Path, localPath, expectedMd5);
            } catch (Exception e) {
                LOG.warn("Download attempt {}/{} failed for {}: {}",
                        attempt, MAX_RETRY_ATTEMPTS, remoteS3Path, e.getMessage());

                if (attempt == MAX_RETRY_ATTEMPTS) {
                    LOG.error("Download failed after {} attempts: {}", MAX_RETRY_ATTEMPTS, e.getMessage());
                    return "";
                }

                try {
                    Thread.sleep(RETRY_DELAY_MS * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return "";
                }
            }
        }
        return "";
    }

    /**
     * Download and extract tar.gz file directory (for connectors and other batch resources)
     *
     * @param remoteS3Directory S3 directory path like "s3://bucket/path/to/connectors/"
     * @param localDirectory local target directory path
     * @return returns local directory path on success, empty string on failure
     */
    public String downloadAndExtractDirectory(String remoteS3Directory, String localDirectory) {
        try {
            // Ensure directory path ends with /
            if (!remoteS3Directory.endsWith("/")) {
                remoteS3Directory += "/";
            }

            // Create local directory
            createParentDirectory(localDirectory + "/dummy");

            // List remote tar.gz files
            List<RemoteFile> remoteFiles = new ArrayList<>();
            Status status = s3Storage.listFiles(remoteS3Directory, false, remoteFiles);

            if (status != Status.OK) {
                LOG.warn("Failed to list files from {}: {}", remoteS3Directory, status.getErrMsg());
                return "";
            }

            boolean hasDownload = false;
            for (RemoteFile remoteFile : remoteFiles) {
                if (remoteFile.isFile() && remoteFile.getName().endsWith(".tar.gz")) {
                    String fileName = Paths.get(remoteFile.getName()).getFileName().toString();
                    String tempFilePath = localDirectory + "/" + fileName;

                    // Build complete S3 path
                    String fullS3Path = remoteS3Directory + fileName;

                    // Download tar.gz file
                    String downloadedFile = downloadFile(fullS3Path, tempFilePath, null);
                    if (!downloadedFile.isEmpty()) {
                        // Extract to target directory
                        if (extractTarGz(tempFilePath, localDirectory)) {
                            // Delete temporary tar.gz file
                            new File(tempFilePath).delete();
                            LOG.info("Extracted and cleaned up: {}", fileName);
                            hasDownload = true;
                        } else {
                            LOG.warn("Failed to extract: {}", fileName);
                        }
                    }
                }
            }

            return hasDownload ? localDirectory : "";

        } catch (Exception e) {
            LOG.warn("Failed to download and extract directory {}: {}", remoteS3Directory, e.getMessage());
            return "";
        }
    }

    // ======================== Internal Implementation Methods ========================

    private String doDownloadFile(String remoteS3Path, String localPath, String expectedMd5) throws Exception {
        // Check if local file exists and is valid
        if (isLocalFileValid(localPath, expectedMd5)) {
            LOG.info("Local file {} is up to date, skipping download", localPath);
            return localPath;
        }

        // Create parent directory
        createParentDirectory(localPath);

        // Execute download
        File localFile = new File(localPath);
        Status status = s3Storage.getObject(remoteS3Path, localFile);

        if (status != Status.OK) {
            throw new RuntimeException("Download failed: " + status.getErrMsg());
        }

        // MD5 verification (if expected value is provided)
        if (!Strings.isNullOrEmpty(expectedMd5)) {
            String actualMd5 = calculateFileMD5(localFile);
            if (!expectedMd5.equalsIgnoreCase(actualMd5)) {
                localFile.delete(); // Delete invalid file
                throw new RuntimeException(String.format(
                        "MD5 mismatch: expected=%s, actual=%s", expectedMd5, actualMd5));
            }
        }

        LOG.info("Successfully downloaded {} to {}", remoteS3Path, localPath);
        return localPath;
    }

    private boolean isLocalFileValid(String localPath, String expectedMd5) {
        try {
            File localFile = new File(localPath);
            if (!localFile.exists() || localFile.length() == 0) {
                return false;
            }

            // If no MD5 provided, only check file existence and size
            if (Strings.isNullOrEmpty(expectedMd5)) {
                LOG.debug("Local file {} exists with size {}, assuming valid",
                        localPath, localFile.length());
                return true;
            }

            // MD5 verification
            String actualMd5 = calculateFileMD5(localFile);
            boolean isValid = expectedMd5.equalsIgnoreCase(actualMd5);

            if (!isValid) {
                LOG.info("Local file {} MD5 mismatch: expected={}, actual={}",
                        localPath, expectedMd5, actualMd5);
            }

            return isValid;
        } catch (Exception e) {
            LOG.warn("Error checking local file validity for {}: {}", localPath, e.getMessage());
            return false;
        }
    }

    private void createParentDirectory(String filePath) throws Exception {
        Path parentDir = Paths.get(filePath).getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
            Files.createDirectories(parentDir);
        }
    }

    private boolean extractTarGz(String tarGzFilePath, String targetDir) {
        try {
            createParentDirectory(targetDir + "/dummy");

            try (FileInputStream fis = new FileInputStream(tarGzFilePath);
                    GZIPInputStream gis = new GZIPInputStream(fis);
                    TarArchiveInputStream tis = new TarArchiveInputStream(gis)) {

                TarArchiveEntry entry;
                while ((entry = tis.getNextTarEntry()) != null) {
                    if (!tis.canReadEntryData(entry)) {
                        continue;
                    }

                    File destFile = new File(targetDir, entry.getName());
                    if (entry.isDirectory()) {
                        destFile.mkdirs();
                    } else {
                        destFile.getParentFile().mkdirs();
                        try (FileOutputStream fos = new FileOutputStream(destFile)) {
                            byte[] buffer = new byte[8192];
                            int bytesRead;
                            while ((bytesRead = tis.read(buffer)) != -1) {
                                fos.write(buffer, 0, bytesRead);
                            }
                        }
                    }
                }
            }

            LOG.info("Successfully extracted {} to {}", tarGzFilePath, targetDir);
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to extract tar.gz file {}: {}", tarGzFilePath, e.getMessage());
            return false;
        }
    }

    private String calculateFileMD5(File file) {
        try (FileInputStream fis = new FileInputStream(file)) {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[8192];
            int bytesRead;

            while ((bytesRead = fis.read(buffer)) != -1) {
                md.update(buffer, 0, bytesRead);
            }

            byte[] hashBytes = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            LOG.warn("Failed to calculate MD5 for file {}: {}", file.getAbsolutePath(), e.getMessage());
            return null;
        }
    }

    private S3ObjStorage createS3Storage(S3Config config) {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.endpoint", config.endpoint);
        properties.put("s3.region", config.region);
        properties.put("s3.access_key", config.accessKey);
        properties.put("s3.secret_key", config.secretKey);

        S3Properties s3Properties = S3Properties.of(properties);
        return new S3ObjStorage(s3Properties);
    }

    @Override
    public void close() throws Exception {
        if (s3Storage != null) {
            s3Storage.close();
        }
    }
}
