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

suite("test_show_compute_groups") {
    if (isCloudMode()) {
        return
    }

    // in non cloud mode, a compute group is a resource group, aka the backend location tag
    def groups = sql "SHOW COMPUTE GROUPS"
    assertTrue(!groups.isEmpty())
    groups.each { row ->
        assertEquals(2, row.size())
        assertTrue(Integer.parseInt(row[1].toString()) > 0)
    }
    assertEquals(groups, sql("SHOW CLUSTERS"))

    // a common user only sees the resource groups it is allowed to use
    String user = 'test_show_compute_groups_user'
    String pwd = 'C123_567p'
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    try {
        def boundGroup = groups[0]
        def noDbJdbcUrl = context.config.jdbcUrl.replaceFirst(/(jdbc:mysql:\/\/[^\/]+\/)[^?]*/, '$1')
        sql """SET PROPERTY FOR '${user}' 'resource_tags.location' = '${boundGroup[0]}'"""
        connect(user, "${pwd}", noDbJdbcUrl) {
            assertEquals([boundGroup], sql("SHOW COMPUTE GROUPS"))
        }

        // no such resource group, the user sees nothing instead of an error
        sql """SET PROPERTY FOR '${user}' 'resource_tags.location' = 'no_such_resource_group'"""
        connect(user, "${pwd}", noDbJdbcUrl) {
            assertTrue(sql("SHOW COMPUTE GROUPS").isEmpty())
        }
    } finally {
        try_sql("DROP USER ${user}")
    }
}
