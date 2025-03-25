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

suite("test_create_catalog_command", "nereids_p0") {
    def catalogName = "test_create_catalog"
    def resourceName = "test_resource"
    def catalogProperties = "\"type\"=\"es\", \"hosts\"=\"http://127.0.0.1:9200\""

    try {
        // Drop catalog if it already exists
        sql("DROP CATALOG IF EXISTS ${catalogName}")

        // Create a new catalog
        checkNereidsExecute(
            """
            CREATE CATALOG ${catalogName} 
            COMMENT 'Test catalog for regression'
            PROPERTIES (${catalogProperties})
            """
        )

        // Verify the catalog was created
        checkNereidsExecute("""show create catalog ${catalogName}""")
        qt_cmd("""show create catalog ${catalogName}""")
    } finally {
        // Clean up
        sql("DROP CATALOG IF EXISTS ${catalogName}")
    }
}
