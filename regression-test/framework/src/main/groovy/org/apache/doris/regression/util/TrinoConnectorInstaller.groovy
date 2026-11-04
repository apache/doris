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

import groovy.util.logging.Slf4j

import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.util.zip.ZipFile

@Slf4j
class TrinoConnectorInstaller {
    static final String MONGODB_URL = "https://repo.maven.apache.org/maven2/io/trino/trino-mongodb/435/trino-mongodb-435.zip"
    static final String MONGODB_SHA256 = "d607a9499cce26bc4a4fcc179c572018663a6a4bf482ca4650aad95eee7ddd70"

    private boolean downloaded
    private boolean mongoDbInstalled

    synchronized void dispatch(List<String> hosts, File downloadDir, String bundleUrl, boolean includeMongoDb) {
        if (downloaded) {
            if (includeMongoDb && !mongoDbInstalled) {
                throw new IllegalStateException("Enable MongoDB before the first Trino fixture distribution")
            }
            return
        }
        File stagingDir = new File(downloadDir, "connectors_tmp")
        if (stagingDir.exists() && !stagingDir.deleteDir()) {
            throw new IOException("Cannot remove Trino fixture staging directory: ${stagingDir}")
        }
        Files.createDirectories(stagingDir.toPath())
        prepareBundle(stagingDir, bundleUrl)
        // The shared dispatcher replaces the remote directory, and Trino loads plugins only once.
        // Assemble MongoDB with the other fixtures before any suite can initialize a connector.
        if (includeMongoDb) {
            prepareMongoDbPlugin(stagingDir)
        }
        hosts.unique().each { host ->
            deploy(host, new File(stagingDir, "connectors"), new File(downloadDir, "connectors"))
        }
        mongoDbInstalled = includeMongoDb
        downloaded = true
        log.info("Distributed Trino fixtures to {} (MongoDB: {})", hosts.unique(), includeMongoDb)
    }

    protected void prepareBundle(File stagingDir, String url) {
        File archive = new File(stagingDir, "trino-connectors.tar.gz")
        download(url, archive)
        runCommand(["tar", "-zxf", archive.path, "-C", stagingDir.path])
    }

    protected void prepareMongoDbPlugin(File stagingDir) {
        File archive = new File(stagingDir, "trino-mongodb-435.zip")
        download(MONGODB_URL, archive)
        extractMongoDbPlugin(archive, new File(stagingDir, "connectors"), MONGODB_SHA256)
    }

    static void extractMongoDbPlugin(File archive, File connectorsDir, String expectedChecksum) {
        MessageDigest digest = MessageDigest.getInstance("SHA-256")
        archive.withInputStream { input ->
            byte[] buffer = new byte[8192]
            int length
            while ((length = input.read(buffer)) != -1) {
                digest.update(buffer, 0, length)
            }
        }
        if (digest.digest().encodeHex().toString() != expectedChecksum) {
            throw new IllegalStateException("MongoDB Trino plugin checksum mismatch: ${archive.name}")
        }
        def root = connectorsDir.toPath().toAbsolutePath().normalize()
        new ZipFile(archive).withCloseable { zip ->
            zip.entries().each { entry ->
                def target = root.resolve(entry.name).normalize()
                if (!entry.name.startsWith("trino-mongodb-435/") || !target.startsWith(root)) {
                    throw new IllegalStateException("Unexpected MongoDB plugin ZIP entry: ${entry.name}")
                }
                if (entry.directory) {
                    Files.createDirectories(target)
                } else {
                    Files.createDirectories(target.parent)
                    zip.getInputStream(entry).withCloseable { input ->
                        Files.copy(input, target, StandardCopyOption.REPLACE_EXISTING)
                    }
                }
            }
        }
    }

    protected void download(String url, File destination) {
        runCommand(["curl", "--fail", "--location", "--retry", "3", "--max-time", "600",
                "--silent", "--show-error", url, "--output", destination.path])
    }

    protected void deploy(String host, File source, File destination) {
        String parent = shellQuote(destination.parent)
        String target = shellQuote(destination.path)
        runCommand(["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                "root@${host}".toString(),
                "mkdir -p ${parent} && rm -rf ${target}".toString()])
        runCommand(["scp", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                "-r", source.path, "root@${host}:${destination.path}".toString()])
    }

    private static String shellQuote(String value) {
        return "'" + value.replace("'", "'\"'\"'") + "'"
    }

    private static void runCommand(List<String> command) {
        log.info("Execute: {}", command)
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start()
        // Drain output before waiting: large archive/SSH diagnostics must not block a full pipe.
        String output = process.inputStream.getText("UTF-8")
        int exitCode = process.waitFor()
        if (exitCode != 0) {
            throw new IOException("Trino fixture command failed (${exitCode}): ${command}\n${output}")
        }
    }
}
