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

suite('test_show_python_versions_command') {
    def result = sql 'SHOW PYTHON VERSIONS'

    if (result.size() > 0) {
        // Verify column structure: [Version, EnvName, EnvType, BasePath, ExecutablePath]
        for (row in result) {
            // Version (column 0) should be a non-empty version string like "3.9.16"
            def version = row[0]
            assertNotNull(version)
            assertTrue(version.length() > 0, 'Version should not be empty')
            assertTrue(version ==~ /\d+\.\d+(\.\d+)?/,
                    "Version '${version}' should match pattern x.y or x.y.z")

            // EnvName (column 1) should be non-empty
            def envName = row[1]
            assertNotNull(envName)
            assertTrue(envName.length() > 0, 'EnvName should not be empty')

            // EnvType (column 2) must be either "conda" or "venv"
            def envType = row[2]
            assertTrue(envType == 'conda' || envType == 'venv',
                    "EnvType '${envType}' should be 'conda' or 'venv'")

            // BasePath (column 3) should be a non-empty path
            def basePath = row[3]
            assertNotNull(basePath)
            assertTrue(basePath.length() > 0, 'BasePath should not be empty')
            assertTrue(basePath.startsWith('/'),
                    "BasePath '${basePath}' should be an absolute path")

            // ExecutablePath (column 4) should be a non-empty path
            def execPath = row[4]
            assertNotNull(execPath)
            assertTrue(execPath.length() > 0, 'ExecutablePath should not be empty')
            assertTrue(execPath.startsWith('/'),
                    "ExecutablePath '${execPath}' should be an absolute path")
        }

        // Verify uniqueness of versions in the result
        result.collect { it[0] } as Set
    }
}
