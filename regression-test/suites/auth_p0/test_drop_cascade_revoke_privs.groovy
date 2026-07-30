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

suite("test_drop_cascade_revoke_privs", "auth") {
    def user1 = 'test_drop_cascade_revoke_user1'
    def role1 = 'test_drop_cascade_revoke_role1'
    def pwd = '123456'
    def dbName = 'test_drop_cascade_revoke_db'
    def tableName = 'test_drop_cascade_revoke_tbl'
    def resourceName = 'test_drop_cascade_revoke_resource'
    def workloadGroupName = 'test_drop_cascade_revoke_wg'
    def tokens = context.config.jdbcUrl.split('/')
    def url = tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?"

    def forComputeGroupStr = ""
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        forComputeGroupStr = " for  $validCluster "
    }

    sql """drop user if exists ${user1}"""
    sql """drop role if exists ${role1}"""
    sql """DROP DATABASE IF EXISTS ${dbName}"""
    sql """CREATE USER '${user1}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE ROLE ${role1}"""
    sql """GRANT '${role1}' TO ${user1}"""

    // ========== Test 1: drop database should cascade revoke db and table privs ==========
    sql """CREATE DATABASE ${dbName}"""
    sql """GRANT SELECT_PRIV ON ${dbName}.* TO ROLE '${role1}'"""
    sql """GRANT ALTER_PRIV ON ${dbName}.* TO ROLE '${role1}'"""

    def result = sql """SHOW GRANTS FOR ${user1}"""
    assertTrue(result.any { row -> row.any { it != null && it.contains("${dbName}") } })

    sql """DROP DATABASE ${dbName}"""

    result = sql """SHOW GRANTS FOR ${user1}"""
    assertFalse(result.any { row -> row.any { it != null && it.contains("${dbName}") } })

    // ========== Test 2: drop table should cascade revoke table privs ==========
    sql """CREATE DATABASE ${dbName}"""
    sql """USE ${dbName}"""
    sql """CREATE TABLE ${tableName} (k INT) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num" = "1")"""
    sql """GRANT SELECT_PRIV ON ${dbName}.${tableName} TO ROLE '${role1}'"""
    sql """GRANT ALTER_PRIV ON ${dbName}.${tableName} TO ROLE '${role1}'"""

    result = sql """SHOW GRANTS FOR ${user1}"""
    assertTrue(result.any { row -> row.any { it != null && it.contains("${tableName}") } })

    sql """DROP TABLE ${dbName}.${tableName}"""

    result = sql """SHOW GRANTS FOR ${user1}"""
    assertFalse(result.any { row -> row.any { it != null && it.contains("${tableName}") } })

    // db-level privs should still exist
    sql """GRANT SELECT_PRIV ON ${dbName}.* TO ROLE '${role1}'"""
    result = sql """SHOW GRANTS FOR ${user1}"""
    assertTrue(result.any { row -> row.any { it != null && it.contains("${dbName}") } })

    sql """DROP DATABASE ${dbName}"""

    // ========== Test 3: drop resource should cascade revoke resource privs ==========
    sql """CREATE RESOURCE ${resourceName} PROPERTIES("type"="hdfs", "hadoop.fs.defaultFS"="hdfs://localhost:8020")"""
    sql """GRANT USAGE_PRIV ON RESOURCE ${resourceName} TO ROLE '${role1}'"""

    result = sql """SHOW GRANTS FOR ${user1}"""
    assertTrue(result.any { row -> row.any { it != null && it.contains("${resourceName}") } })

    sql """DROP RESOURCE ${resourceName}"""

    result = sql """SHOW GRANTS FOR ${user1}"""
    assertFalse(result.any { row -> row.any { it != null && it.contains("${resourceName}") } })

    // ========== Test 4: drop workload group should cascade revoke workload group privs ==========
    sql """CREATE WORKLOAD GROUP IF NOT EXISTS ${workloadGroupName} ${forComputeGroupStr} PROPERTIES('min_cpu_percent'='0')"""
    sql """GRANT USAGE_PRIV ON WORKLOAD GROUP ${workloadGroupName} TO ROLE '${role1}'"""

    result = sql """SHOW GRANTS FOR ${user1}"""
    assertTrue(result.any { row -> row.any { it != null && it.contains("${workloadGroupName}") } })

    sql """DROP WORKLOAD GROUP ${workloadGroupName} ${forComputeGroupStr}"""

    result = sql """SHOW GRANTS FOR ${user1}"""
    assertFalse(result.any { row -> row.any { it != null && it.contains("${workloadGroupName}") } })

    // cleanup
    sql """DROP DATABASE IF EXISTS ${dbName}"""
    sql """DROP USER IF EXISTS ${user1}"""
    sql """DROP ROLE IF EXISTS ${role1}"""
}
