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

import org.apache.doris.regression.Config
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import static org.junit.jupiter.api.Assertions.*

class SuiteTrinoFixtureDispatchTest {
    @TempDir
    File temporaryDir

    @Test
    void standaloneMongoDoesNotRequireSharedFixtureConfiguration() {
        [null, "false"].each { enabled ->
            def suite = suite(enabled)
            suite.dispatchTrinoConnectors()
            assertTrue(suite.queries.isEmpty())
            assertNull(suite.dispatchedHosts)
        }
    }

    @Test
    void sharedFixtureModeFindsEveryUniqueFrontendAndBackendHost() {
        def suite = suite("true")
        suite.dispatchTrinoConnectors()
        assertEquals(["SHOW BACKENDS", "SHOW FRONTENDS"], suite.queries)
        assertEquals(["shared-host", "be-host", "fe-host"], suite.dispatchedHosts)
    }

    private RecordingSuite suite(String enabled) {
        def config = new Config(suitePath: temporaryDir.path, dataPath: temporaryDir.path,
                realDataPath: temporaryDir.path, defaultDb: "regression_test")
        config.otherConfigs.put("enableMongoDBTest", "true")
        if (enabled != null) {
            config.otherConfigs.put("enableTrinoConnectorTest", enabled)
        }
        File file = new File(temporaryDir, "fixture.groovy")
        def script = new ScriptContext(file, null, null, config, [], { true })
        def context = new SuiteContext(file, "fixture", "external", script, null, null, null, config)
        return new RecordingSuite(context)
    }

    private static class RecordingSuite extends Suite {
        List<String> queries = []
        ArrayList dispatchedHosts

        RecordingSuite(SuiteContext context) {
            super("fixture", "external", context, null)
        }

        @Override
        List<List<Object>> sql(String query, boolean isOrder = false) {
            queries.add(query)
            return query == "SHOW BACKENDS" ? [[1, "shared-host"], [2, "be-host"]] :
                    [[1, "shared-host"], [2, "fe-host"]]
        }

        @Override
        void dispatchTrinoConnectors(ArrayList hosts) {
            dispatchedHosts = hosts
        }
    }
}
