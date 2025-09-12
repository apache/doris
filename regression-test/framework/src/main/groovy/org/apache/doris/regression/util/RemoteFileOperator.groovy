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

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.time.Instant

class RemoteFileOperator {
    private static final Logger logger = LoggerFactory.getLogger(RemoteFileOperator.class)
    
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
        logger.info("Initialized with ${this.hosts.size()} hosts")
    }

    /**
     * Create directories on all remote hosts
     * Throws exception if any host fails
     * 
     * @param dirPath Path of the directory to create
     * @throws Exception if directory creation fails on any host
     */
    void createRemoteDirectories(String dirPath) throws Exception {
        logger.info("Creating directory '${dirPath}' on all ${hosts.size()} hosts")
        
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
                def errorMsg = "Failed to create directory on ${host} (exit code: ${execResult.exitCode}): ${execResult.error}"
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
        logger.info("Starting SCP download from ${hosts.size()} hosts to ${localBaseDir}")
        
        // Create base directory if it doesn't exist
        def baseDir = new File(localBaseDir)
        if (!baseDir.exists() && !baseDir.mkdirs()) {
            def errorMsg = "Failed to create local base directory: ${localBaseDir}"
            logger.error(errorMsg)
            throw new Exception(errorMsg)
        }
        
        hosts.each { host ->
            logger.info("Downloading from ${host}:${remoteDir} to ${localBaseDir}")
            
            // SCP command to copy directly to base directory
            // Adding trailing slash to remoteDir ensures contents are copied, not the directory itself
            def normalizedRemoteDir = remoteDir.endsWith('/') ? remoteDir : "${remoteDir}/"
            def scpCommand = "scp -r -P ${port} ${username}@${host}:${escapePath(normalizedRemoteDir)}* ${escapePath(localBaseDir)}"
            def execResult = executeLocalCommand(scpCommand)
            
            if (execResult.timedOut) {
                def errorMsg = "Timeout downloading from ${host} (${timeout}ms)"
                logger.error(errorMsg)
                throw new Exception(errorMsg)
            }
            
            if (execResult.exitCode != 0) {
                def errorMsg = "Failed to download from ${host} (exit code: ${execResult.exitCode}): ${execResult.error}"
                logger.error(errorMsg)
                throw new Exception(errorMsg)
            }
        }
        
        logger.info("Successfully downloaded all files from remote hosts to ${localBaseDir}")
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
                def errorMsg = "Failed to delete directory on ${host} (exit code: ${execResult.exitCode}): ${execResult.error}"
                logger.error(errorMsg)
                throw new Exception(errorMsg)
            }
        }
        
        logger.info("Successfully deleted directory '${dirPath}' on all hosts")
    }

    private Map executeSshCommand(String host, String command) {
        def sshCommand = "ssh -p ${port} ${username}@${host} '${command}'"
        return executeLocalCommand(sshCommand)
    }

    private Map executeLocalCommand(String command) {
        def result = [
            exitCode: -1,
            error: '',
            timedOut: false
        ]
        
        try {
            logger.debug("Executing command: ${command}")
            Process process = new ProcessBuilder('/bin/sh', '-c', command)
                .redirectErrorStream(false)
                .start()
            
            def errorStream = new StringBuilder()
            def errorThread = new Thread({ process.errorStream.eachLine { errorStream.append(it).append('\n') } })
            errorThread.start()
            
            def completed = process.waitFor(timeout, TimeUnit.MILLISECONDS)
            
            if (!completed) {
                process.destroyForcibly()
                result.timedOut = true
            } else {
                result.exitCode = process.exitValue()
            }
            
            errorThread.join(1000)
            result.error = errorStream.toString().trim()
            
        } catch (Exception e) {
            result.error = e.message
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
        // Get timestamp (last 8 digits of epoch seconds for brevity)
        String timestamp = Instant.now().epochSecond.toString().take(-8) ?: 
                          Instant.now().epochSecond.toString()
        
        // Generate random characters
        String randomStr = (1..RANDOM_LENGTH).collect { 
            RANDOM_CHARS[random.nextInt(RANDOM_CHARS.length())] 
        }.join()
        
        // Create base filename
        String fileName = "${prefix}_${timestamp}_${randomStr}"
        
        // Combine with base directory if provided
        if (baseDir) {
            return new File(baseDir, fileName).absolutePath
        }
        
        return new File(fileName).absolutePath
    }
}

class ExportTestHelper implements AutoCloseable {
    String localDir 
    String remoteDir
    RemoteFileOperator operator
    boolean deleteTmpFile = false

    public ExportTestHelper() {
        localDir = UniquePathGenerator.generateUniqueLocalPath("/tmp", "doristest_l_")
        remoteDir = UniquePathGenerator.generateUniqueLocalPath("/tmp", "doristest_")

        def ip =[:]
        def portList = [:]
        getBackendIpHeartbeatPort(hosts, portList)
        operator = new RemoteFileOperator(ip, "root")
        operator.createRemoteDirectories(remoteDir)
        deleteTmpFile = true
    }

    public void collect() {
        operator.scpToLocal(remoteDir, localDir)
    }

    @Override
    protected void close() throws Throwable {
        super.finalize()
        if (deleteTmpFile && operator) {
            operator.deleteRemoteDirectories()
            executeLocalCommand("rm -rf localDir")
        }
    }
}
    