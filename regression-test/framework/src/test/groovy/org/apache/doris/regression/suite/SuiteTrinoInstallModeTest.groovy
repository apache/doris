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

package org.apache.doris.regression.suite

import org.junit.jupiter.api.Test

import java.util.concurrent.Executors

import static org.junit.jupiter.api.Assertions.*

class SuiteTrinoInstallModeTest {
    private static class TestSuite extends Suite {
        final Map<String, String> settings

        TestSuite(Map<String, String> settings) {
            super("trino_install_mode", "external", null, null)
            this.settings = settings
        }

        @Override
        String getConf(String key, String defaultValue) {
            return settings.getOrDefault(key, defaultValue)
        }

        @Override
        String getS3Url() {
            throw new AssertionError("Installer requested S3")
        }
    }

    @Test
    void preinstalledLeavesTestsEnabledAndContinuesAfterDispatch() {
        def settings = [trinoConnectorInstallMode: "preinstalled",
                        enableTrinoConnectorTest: "true", enableMongoDBTest: "true"]
        def suite = new TestSuite(settings)
        // No context, download path, S3 or SSH configuration: installation must not be entered.
        suite.dispatchTrinoConnectors(["unreachable.example"] as ArrayList)
        assertEquals("true", settings.enableTrinoConnectorTest)
        assertEquals("true", settings.enableMongoDBTest)
    }

    @Test
    void concurrentPreinstalledSuitesAllContinue() {
        def executor = Executors.newFixedThreadPool(4)
        try {
            def tasks = (1..12).collect {
                executor.submit({
                    new TestSuite([trinoConnectorInstallMode: "preinstalled"])
                            .dispatchTrinoConnectors([] as ArrayList)
                    return "query assertions continue"
                } as java.util.concurrent.Callable<String>)
            }
            tasks.each { assertEquals("query assertions continue", it.get()) }
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    void defaultAndExplicitFrameworkModesStillEnterInstaller() {
        [[:], [trinoConnectorInstallMode: "framework"]].each { settings ->
            def error = assertThrows(AssertionError) {
                new TestSuite(settings).dispatchTrinoConnectors([] as ArrayList)
            }
            assertEquals("Installer requested S3", error.message)
        }
    }

    @Test
    void invalidModeFailsBeforeAccessingInstallationConfiguration() {
        def error = assertThrows(IllegalArgumentException) {
            new TestSuite([trinoConnectorInstallMode: "preinstaled"])
                    .dispatchTrinoConnectors([] as ArrayList)
        }
        assertTrue(error.message.contains("preinstaled"))
    }
}
