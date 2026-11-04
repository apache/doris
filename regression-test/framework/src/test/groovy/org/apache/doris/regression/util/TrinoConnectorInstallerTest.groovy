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

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.security.MessageDigest
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

import static org.junit.jupiter.api.Assertions.*

class TrinoConnectorInstallerTest {
    @TempDir
    File temporaryDir

    @Test
    void fullPluginIsReadyOnEveryHostBeforeDispatchReturns() {
        def installer = fixtureInstaller()
        installer.dispatch(["fe", "be", "fe"], new File(temporaryDir, "download"), "fixture", true)
        assertEquals(["fe", "be"], installer.deployedHosts)
        assertEquals(1, installer.mongoDownloads)
        assertEquals(1, installer.bundleDownloads)
    }

    @Test
    void concurrentSuitesShareOneCompleteDistribution() {
        def installer = fixtureInstaller()
        def start = new CountDownLatch(1)
        def pool = Executors.newFixedThreadPool(8)
        try {
            def jobs = (1..8).collect {
                pool.submit({
                    assertTrue(start.await(10, TimeUnit.SECONDS))
                    installer.dispatch(["fe", "be"], new File(temporaryDir, "download"), "fixture", true)
                } as Runnable)
            }
            start.countDown()
            jobs.each { it.get(10, TimeUnit.SECONDS) }
            assertEquals(["fe", "be"], installer.deployedHosts)
            assertEquals(1, installer.mongoDownloads)
        } finally {
            pool.shutdownNow()
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS))
        }
    }

    @Test
    void disabledMongoDoesNotDownloadOrReplaceItsPlugin() {
        def installer = fixtureInstaller()
        installer.includeMongo = false
        installer.dispatch(["fe"], new File(temporaryDir, "download"), "fixture", false)
        assertEquals(0, installer.mongoDownloads)
        assertEquals(["fe"], installer.deployedHosts)
    }

    @Test
    void checksumFailureDoesNotDeployOrMarkDistributionComplete() {
        def installer = fixtureInstaller()
        installer.checksum = "0" * 64
        assertThrows(IllegalStateException) {
            installer.dispatch(["fe"], new File(temporaryDir, "download"), "fixture", true)
        }
        assertTrue(installer.deployedHosts.isEmpty())
        installer.checksum = checksum(installer.archive)
        installer.dispatch(["fe"], new File(temporaryDir, "download"), "fixture", true)
        assertEquals(["fe"], installer.deployedHosts)
        assertEquals(2, installer.bundleDownloads)
    }

    @Test
    void deploymentFailureDoesNotMarkDistributionComplete() {
        def installer = fixtureInstaller()
        installer.failingHost = "be"
        assertThrows(IOException) {
            installer.dispatch(["fe", "be"], new File(temporaryDir, "download"), "fixture", true)
        }
        installer.failingHost = null
        installer.dispatch(["fe", "be"], new File(temporaryDir, "download"), "fixture", true)
        assertEquals(["fe", "fe", "be"], installer.deployedHosts)
        assertEquals(2, installer.bundleDownloads)
    }

    @Test
    void lateMongoEnableCannotReplacePluginsAlreadyUsedByOtherSuites() {
        def installer = fixtureInstaller()
        installer.includeMongo = false
        installer.dispatch(["fe"], new File(temporaryDir, "download"), "fixture", false)
        assertThrows(IllegalStateException) {
            installer.dispatch(["fe"], new File(temporaryDir, "download"), "fixture", true)
        }
        assertEquals(1, installer.bundleDownloads)
        assertEquals(["fe"], installer.deployedHosts)
    }

    @Test
    void archiveCannotEscapeTheStagingDirectory() {
        File archive = archive(["trino-mongodb-435/../../escaped.jar": "invalid"])
        assertThrows(IllegalStateException) {
            TrinoConnectorInstaller.extractMongoDbPlugin(archive, new File(temporaryDir, "connectors"),
                    checksum(archive))
        }
        assertFalse(new File(temporaryDir, "escaped.jar").exists())
    }

    private FixtureInstaller fixtureInstaller() {
        File archive = archive([
                "trino-mongodb-435/trino-mongodb-435.jar": "connector",
                "trino-mongodb-435/trino-mongodb-435-services.jar": "service metadata",
                "trino-mongodb-435/mongodb-driver-sync-4.11.1.jar": "dependency"])
        return new FixtureInstaller(archive: archive, checksum: checksum(archive))
    }

    private File archive(Map<String, String> entries) {
        File archive = new File(temporaryDir, "mongodb.zip")
        new ZipOutputStream(new FileOutputStream(archive)).withCloseable { zip ->
            entries.each { name, content ->
                zip.putNextEntry(new ZipEntry(name))
                zip.write(content.getBytes("UTF-8"))
                zip.closeEntry()
            }
        }
        return archive
    }

    private static String checksum(File archive) {
        return MessageDigest.getInstance("SHA-256").digest(archive.bytes).encodeHex().toString()
    }

    private static class FixtureInstaller extends TrinoConnectorInstaller {
        File archive
        String checksum
        boolean includeMongo = true
        String failingHost
        int mongoDownloads
        int bundleDownloads
        List<String> deployedHosts = []

        @Override
        protected void prepareBundle(File dir, String url) {
            bundleDownloads++
            new File(dir, "connectors/trino-hive-435").mkdirs()
            new File(dir, "connectors/trino-hive-435/fixture.jar").text = "hive"
        }

        @Override
        protected void prepareMongoDbPlugin(File dir) {
            mongoDownloads++
            extractMongoDbPlugin(archive, new File(dir, "connectors"), checksum)
        }

        @Override
        protected void deploy(String host, File source, File destination) {
            if (host == failingHost) {
                throw new IOException("injected deployment failure")
            }
            assertEquals("hive", new File(source, "trino-hive-435/fixture.jar").text)
            if (includeMongo) {
                assertEquals("connector", new File(source, "trino-mongodb-435/trino-mongodb-435.jar").text)
                assertEquals("service metadata",
                        new File(source, "trino-mongodb-435/trino-mongodb-435-services.jar").text)
                assertEquals("dependency",
                        new File(source, "trino-mongodb-435/mongodb-driver-sync-4.11.1.jar").text)
            } else {
                assertFalse(new File(source, "trino-mongodb-435").exists())
            }
            deployedHosts.add(host)
        }
    }
}
