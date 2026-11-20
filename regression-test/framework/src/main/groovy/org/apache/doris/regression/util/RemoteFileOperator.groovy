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

package org.apache.doris.regression.util

import com.amazonaws.services.s3.model.GetObjectRequest
import com.azure.storage.blob.BlobContainerClient
import com.azure.storage.blob.BlobContainerClientBuilder
import com.azure.storage.blob.models.ListBlobsOptions
import com.azure.storage.common.StorageSharedKeyCredential
import org.apache.doris.regression.suite.Suite
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.security.SecureRandom

/**
 * A utility class for remote file operations via SSH protocol, supporting batch operations on multiple hosts.
 * Core capabilities include remote directory creation, file downloading (via SCP), and directory deletion.
 * <p>Key Features:
 * <ul>
 *   <li>Batch operation support for multiple remote hosts (shared SSH credentials and port)</li>
 *   <li>Built-in timeout control for all operations to prevent long-term blocking</li>
 *   <li>Safety check for deletion: only paths containing {@link #SAFETY_PREFIX} can be deleted (avoids accidental data loss)</li>
 *   <li>Pre-download file count check: skips SCP if remote directory is empty (prevents "No such file" errors)</li>
 * </ul>
 *
 * <p>Usage Example:
 * <pre>
 * // 1. Initialize operator with 2 remote hosts, SSH user "root", port 22, 15s timeout
 * List<String> hosts = Arrays.asList("172.20.56.7", "172.20.56.14");
 * RemoteFileOperator operator = new RemoteFileOperator(hosts, "root", 22, 15000);
 *
 * // 2. Create directory on all remote hosts
 * operator.createRemoteDirectories("/tmp/doristest_data");
 *
 * // 3. Download files from remote to local (skips if remote is empty)
 * operator.scpToLocal("/tmp/doristest_data", "/local/data");
 *
 * // 4. Delete remote directory (only allowed if path contains "doristest")
 * operator.deleteRemoteDirectories("/tmp/doristest_data");
 * </pre>
 */
class RemoteFileOperator {
    private static final Logger logger = LoggerFactory.getLogger(RemoteFileOperator.class)
    public static final String SAFETY_PREFIX = "doristest"
    
    private List<String> hosts
    private String username
    private int port
    private int timeout

    /**
     * Constructor for RemoteFileOperator
     * 
     * @param hosts Single host string or list of host strings
     * @param username SSH username (shared across all hosts)
     * @param port SSH port (shared across all hosts), default is 22
     * @param timeout Operation timeout in milliseconds, default is 10000
     */
    RemoteFileOperator(def hosts, String username, int port = 22, int timeout = 10000) {
        if (hosts instanceof List) {
            this.hosts = hosts
        } else if (hosts instanceof String) {
            this.hosts = [hosts]
        } else {
            throw new IllegalArgumentException("Hosts must be a string or list of strings")
        }
        
        this.username = username
        this.port = port
        this.timeout = timeout
    }

    /**
     * Create directories on all remote hosts
     * Throws exception if any host fails
     * 
     * @param dirPath Path of the directory to create
     * @throws Exception if directory creation fails on any host
     */
    void createRemoteDirectories(String dirPath) throws Exception {
        hosts.each { host ->
            logger.info("Processing host: ${host}")
            def command = "mkdir -p ${escapePath(dirPath)}"
            def execResult = executeSshCommand(host, command)
            
            if (execResult.timedOut) {
                def errorMsg = "Timeout creating directory on ${host} (${timeout}ms)"
                logger.error(errorMsg)
                throw new Exception(errorMsg)
            }
            
            if (execResult.exitCode != 0) {
                def errorMsg = "Failed to create directory on ${host} (exit code: ${execResult.exitCode}): ${execResult.stderr}"
                logger.error(errorMsg)
                throw new Exception(errorMsg)
            }
        }
        
        logger.info("Successfully created directory '${dirPath}' on all hosts")
    }

    /**
     * Download files from all remote hosts directly to local base directory
     * without host-specific subfolders
     * Throws exception if any host fails
     * 
     * @param remoteDir Remote directory path to download
     * @param localBaseDir Local directory where all files will be copied
     * @throws Exception if SCP fails on any host
     */
    void scpToLocal(String remoteDir, String localBaseDir) throws Exception {
        logger.info("Starting SCP download process from ${hosts.size()} hosts to ${localBaseDir}")
        remoteDir = remoteDir.trim()
        localBaseDir = localBaseDir.trim()
        if (remoteDir == null || remoteDir.trim().isEmpty()) {
            throw new IllegalArgumentException("Remote directory cannot be null or empty")
        }
        if (localBaseDir == null || localBaseDir.trim().isEmpty()) {
            throw new IllegalArgumentException("Local base directory cannot be null or empty")
        }
        
        // Create base directory if it doesn't exist
        def baseDir = new File(localBaseDir)
        if (!baseDir.exists() && !baseDir.mkdirs()) {
            def errorMsg = "Failed to create local base directory: ${localBaseDir}"
            logger.error(errorMsg)
            throw new Exception(errorMsg)
        }
        
        hosts.each { host ->
            // Step 1: Check if remote directory has files (count regular files only)
            // scp user@host:/tmp/doristest/* will fail if doristest has no files.
            String countCommand = isLocalHost(host)
                    ? "find ${escapePath(remoteDir)} -maxdepth 1 -type f | wc -l"
                    : """ssh -p ${port} ${username}@${host} "find ${escapePath(remoteDir)} -maxdepth 1 -type f | wc -l" """
            def countResult = executeLocalCommand(countCommand)
            
            if (countResult.timedOut) {
                def errorMsg = "Timeout checking file count on ${host} (${timeout}ms)"
                logger.error(errorMsg)
                throw new Exception(errorMsg)
            }
            
            if (countResult.exitCode != 0) {
                def errorMsg = "Failed to check file count on ${host} (exit code: ${countResult.exitCode}): ${countResult.stderr}"
                logger.error(errorMsg)
                throw new Exception(errorMsg)
            }

            // Parse file count (handle possible whitespace in output)
            int fileCount = countResult.stdout.trim().toInteger()  // wc -l output is in error stream due to redirect
            logger.info("Found ${fileCount} files in ${host}:${remoteDir}")

            // Step 2: Only execute SCP if there are files to copy
            if (fileCount > 0) {
                logger.info("Downloading ${fileCount} files from ${host}:${remoteDir} to ${localBaseDir}")

                def copyCommand
                if (isLocalHost(host)) {
                    copyCommand = "find ${escapePath(remoteDir)} -maxdepth 1 -type f " +
                            "-exec cp -f '{}' ${escapePath(localBaseDir)} ';'"
                } else {
                    def normalizedRemoteDir = (remoteDir.endsWith('/') ? remoteDir : "${remoteDir}/") + "*"
                    copyCommand = "scp -P ${port} ${username}@${host}:${escapePath(normalizedRemoteDir)} " +
                            escapePath(localBaseDir)
                }
                def execResult = executeLocalCommand(copyCommand)

                if (execResult.timedOut) {
                    def errorMsg = "Timeout downloading from ${host} (${timeout}ms)"
                    logger.error(errorMsg)
                    throw new Exception(errorMsg)
                }

                if (execResult.exitCode != 0) {
                    def errorMsg = "Failed to download from ${host} (exit code: ${execResult.exitCode}): ${execResult.stderr}"
                    logger.error(errorMsg)
                    throw new Exception(errorMsg)
                }
                logger.info("Successfully downloaded ${fileCount} files from ${host}")
            } else {
                logger.info("No files found in ${host}:${remoteDir}, skipping SCP")
            }
        }

        logger.info("SCP download process completed for all hosts")
    }

    /**
     * Delete directories on all remote hosts
     * Throws exception if any host fails
     * 
     * @param dirPath Path of the directory to delete
     * @throws Exception if directory deletion fails on any host
     */
    void deleteRemoteDirectories(String dirPath) throws Exception {
        logger.info("Deleting directory '${dirPath}' on ${hosts.size()} hosts")
        if (!dirPath.contains(SAFETY_PREFIX)) {
            def errorMsg = "Deletion forbidden: Path '${dirPath}' does not contain safety prefix '${SAFETY_PREFIX}'"
            logger.error(errorMsg)
            throw new SecurityException(errorMsg);
        }

        hosts.each { host ->
            logger.info("Processing host: ${host}")
            def command = "rm -rf ${escapePath(dirPath)}"
            def execResult = executeSshCommand(host, command)
            
            if (execResult.timedOut) {
                def errorMsg = "Timeout deleting directory on ${host} (${timeout}ms)"
                logger.error(errorMsg)
                throw new Exception(errorMsg)
            }
            
            if (execResult.exitCode != 0) {
                def errorMsg = "Failed to delete directory on ${host} (exit code: ${execResult.exitCode}): ${execResult.stderr}"
                logger.error(errorMsg)
                throw new Exception(errorMsg)
            }
        }
        
        logger.info("Successfully deleted directory '${dirPath}' on all hosts")
    }

    private Map executeSshCommand(String host, String command) {
        if (isLocalHost(host)) {
            return executeLocalCommand(command)
        }
        def sshCommand = "ssh -p ${port} ${username}@${host} '${command}'"
        return executeLocalCommand(sshCommand)
    }

    private boolean isLocalHost(String host) {
        return host in ["127.0.0.1", "localhost", "::1"]
    }

    private Map executeLocalCommand(String command) {
        def result = [
            exitCode: -1,
            stdout: '',
            stderr: '',
            timedOut: false
        ]

        try {
            logger.debug("Executing command: ${command}")
            Process process = new ProcessBuilder('/bin/sh', '-c', command)
                .redirectErrorStream(false)
                .start()

            boolean completed = process.waitFor(timeout, TimeUnit.MILLISECONDS)

            if (!completed) {
                process.destroyForcibly()
                result.timedOut = true
                logger.warn("Command timed out after ${timeout}ms: ${command}")
                return result
            }

            result.exitCode = process.exitValue()
            result.stdout = process.inputStream.text.trim()
            result.stderr = process.errorStream.text.trim()

        } catch (Exception e) {
            result.stderr = e.message
            logger.error("Error executing command: ${e.message}", e)
        }
        
        return result
    }

    private String escapePath(String path) {
        return "'${path.replace("'", "'\"'\"'")}'"
    }
}

class UniquePathGenerator {
    private static final SecureRandom random = new SecureRandom()
    private static final String RANDOM_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    private static final int RANDOM_LENGTH = 6  // Adjust length for more/less uniqueness

    /**
     * Generates a short, unique local path with "doristest_" prefix
     * Format: doristest_&lt;timestamp&gt;_&lt;random_chars&gt;
     * 
     * @param baseDir (Optional) Base directory to prepend, null for current working directory
     * @return Unique path string
     */
    static String generateUniqueLocalPath(String baseDir, String prefix) {
        // Generate random characters
        String randomStr = (1..RANDOM_LENGTH).collect { 
            RANDOM_CHARS[random.nextInt(RANDOM_CHARS.length())] 
        }.join()
        
        // Create base filename
        String fileName = "${prefix}_${randomStr}"
        
        // Combine with base directory if provided
        if (baseDir) {
            return new File(baseDir, fileName).absolutePath
        }
        
        return new File(fileName).absolutePath
    }
}

class ExportTestHelper {
    private final Suite suite
    private final String bucket
    private final String endpoint
    private final BlobContainerClient azure

    ExportTestHelper(Suite suite) {
        this.suite = suite
        bucket = suite.getS3BucketName()
        String configuredEndpoint = suite.getS3Endpoint()
        if ("azure".equalsIgnoreCase(suite.getS3Provider())) {
            if (configuredEndpoint == "azure-endpoint") {
                configuredEndpoint = "${suite.getS3AK()}.blob.core.windows.net"
            }
            if (!configuredEndpoint.contains("://")) {
                configuredEndpoint = "https://${configuredEndpoint}"
            }
            azure = new BlobContainerClientBuilder()
                    .endpoint(configuredEndpoint)
                    .containerName(bucket)
                    .credential(new StorageSharedKeyCredential(suite.getS3AK(), suite.getS3SK()))
                    .buildClient()
        }
        endpoint = configuredEndpoint
    }

    String storageProperties() {
        return """
            "s3.endpoint"="${endpoint}", "s3.region"="${suite.getS3Region()}",
            "s3.access_key"="${suite.getS3AK()}", "s3.secret_key"="${suite.getS3SK()}",
            "provider"="${suite.getS3Provider()}"
        """
    }

    String uri(String prefix) {
        return "s3://${bucket}/${prefix}"
    }

    List<String> keys(String prefix) {
        if (azure != null) {
            return azure.listBlobs(new ListBlobsOptions().setPrefix(prefix), Duration.ofMinutes(1)).collect { it.name }
        }
        def client = suite.getS3Client()
        def result = []
        def page = client.listObjects(bucket, prefix)
        while (true) {
            result.addAll(page.objectSummaries.collect { it.key })
            if (!page.truncated) {
                return result
            }
            page = client.listNextBatchOfObjects(page)
        }
    }

    List<File> downloadFiles(String prefix, File directory) {
        java.nio.file.Files.createDirectories(directory.toPath())
        return keys(prefix).findAll { !it.endsWith("/") }.collect { key ->
            File file = new File(directory, key.tokenize("/").last())
            download(key, file)
            file
        }
    }

    private void download(String key, File file) {
        if (azure != null) {
            azure.getBlobClient(key).downloadToFile(file.absolutePath, true)
        } else {
            suite.getS3Client().getObject(new GetObjectRequest(bucket, key), file)
        }
    }

    void deleteFiles(String prefix) {
        keys(prefix).each { key ->
            if (azure != null) {
                azure.getBlobClient(key).delete()
            } else {
                suite.getS3Client().deleteObject(bucket, key)
            }
        }
    }

    static void withParquetOutfile(Suite suite, String query, Closure consumeFile) {
        def helper = new ExportTestHelper(suite)
        String prefix = "regression/outfile/${UUID.randomUUID()}/"
        try {
            suite.sql """${query} INTO OUTFILE 's3://${helper.bucket}/${prefix}part_' FORMAT AS PARQUET
                PROPERTIES (${helper.storageProperties()})"""
            def files = helper.keys(prefix)
            assert !files.isEmpty()
            for (String key : files) {
                File file = File.createTempFile("doris-outfile-", ".parquet")
                try {
                    helper.download(key, file)
                    consumeFile(file.absolutePath)
                } finally {
                    file.delete()
                }
            }
        } finally {
            helper.deleteFiles(prefix)
        }
    }
}
