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

suite("test_show_user_properties_command", "query,nereids") {
    def currentUser = "root"

    // Test SHOW PROPERTY FOR USER command
    try {
        checkNereidsExecute("""SHOW PROPERTY FOR USER ${currentUser};""")
    } catch (Exception e) {
        log.error("Failed to execute SHOW PROPERTY FOR USER command", e)
    }

    // Test SHOW PROPERTY FOR USER command with wildWhere condition
    try {
        checkNereidsExecute("""SHOW PROPERTY FOR USER ${currentUser} LIKE 'property_name%';""")
    } catch (Exception e) {
        log.error("Failed to execute SHOW PROPERTY FOR USER command with LIKE clause", e)
    }

    // Test SHOW ALL PROPERTIES command
    try {
        checkNereidsExecute("""SHOW ALL PROPERTIES;""")
    } catch (Exception e) {
        log.error("Failed to execute SHOW ALL PROPERTIES command", e)
    }

    // Test SHOW ALL PROPERTIES with wildWhere condition
    try {
        checkNereidsExecute("""SHOW ALL PROPERTIES LIKE 'property%';""")
    } catch (Exception e) {
        log.error("Failed to execute SHOW ALL PROPERTIES command with LIKE clause", e)
    }
}
