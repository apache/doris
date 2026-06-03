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

suite('test_show_python_packages_command') {
    // get available Python versions so we can pick one for the packages test
    def versions = sql 'SHOW PYTHON VERSIONS'
    if (versions.size() == 0) {
        return
    }
    def testVersion = versions[0][0]

    // Execute SHOW PYTHON PACKAGES IN '<version>'
    def result = sql "SHOW PYTHON PACKAGES IN '${testVersion}'"

    // There should be at least some packages installed
    assertTrue(result.size() > 0,
            "Expected at least some packages for Python ${testVersion}")

    // Collect all package names for later assertions
    def packageNames = [] as Set
    for (row in result) {
        // Package name (column 0) should be non-empty
        def pkgName = row[0]
        assertNotNull(pkgName)
        assertTrue(pkgName.length() > 0, 'Package name should not be empty')

        // Package version (column 1) should be non-empty
        def pkgVersion = row[1]
        assertNotNull(pkgVersion)
        assertTrue(pkgVersion.length() > 0,
                "Package version for '${pkgName}' should not be empty")

        packageNames.add(pkgName.toLowerCase())
    }

    // Verify that essential packages (pyarrow and pandas) are present
    assertTrue(packageNames.contains('pyarrow'),
            "pyarrow should be installed, found packages: ${packageNames}")
    assertTrue(packageNames.contains('pandas'),
            "pandas should be installed, found packages: ${packageNames}")
}
