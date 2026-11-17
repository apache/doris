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

suite("test_memo_merge_cycle") {
    def dbName = context.config.getDbNameByFile(context.file)
    sql "USE `${dbName}`"
    sql "DROP TABLE IF EXISTS memo_cycle_parent"
    sql "DROP TABLE IF EXISTS memo_cycle_child"
    sql "DROP TABLE IF EXISTS memo_cycle_acl_cover"
    sql "DROP TABLE IF EXISTS memo_cycle_acl_grant"
    sql "DROP TABLE IF EXISTS memo_cycle_acl_member"
    sql """CREATE TABLE memo_cycle_parent (
      tenant varchar(100) NOT NULL, pk varchar(100) NOT NULL, name varchar(500) NOT NULL, code varchar(255) NULL
    ) UNIQUE KEY(tenant, pk) DISTRIBUTED BY HASH(tenant) BUCKETS 10
    PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")"""

    sql """CREATE TABLE memo_cycle_child (
      tenant varchar(100) NOT NULL, ck varchar(100) NOT NULL, pk varchar(100) NOT NULL, name varchar(500) NOT NULL,
      code varchar(255) NULL, kind varchar(100) NULL
    ) UNIQUE KEY(tenant, ck) DISTRIBUTED BY HASH(tenant) BUCKETS 10
    PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")"""

    sql """CREATE TABLE memo_cycle_acl_cover (
      tenant_hash bigint NOT NULL, resource_hash bigint NOT NULL, resource_type tinyint NOT NULL, side tinyint NOT NULL DEFAULT "0",
      l1 bigint NOT NULL, l2 bigint NOT NULL DEFAULT "0", l3 bigint NOT NULL DEFAULT "0", l4 bigint NOT NULL DEFAULT "0", l5 bigint NOT NULL DEFAULT "0"
    ) UNIQUE KEY(tenant_hash, resource_hash, resource_type, side, l1, l2, l3, l4, l5) DISTRIBUTED BY HASH(tenant_hash, resource_hash) BUCKETS 10
    PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")"""

    sql """CREATE TABLE memo_cycle_acl_grant (
      tenant_hash bigint NOT NULL, l1 bigint NOT NULL, principal bigint NOT NULL, l2 bigint NOT NULL DEFAULT "0", l3 bigint NOT NULL DEFAULT "0",
      l4 bigint NOT NULL DEFAULT "0", l5 bigint NOT NULL DEFAULT "0", resource_type tinyint NOT NULL, channel bigint NOT NULL DEFAULT "0",
      role_level tinyint NOT NULL, reach tinyint NOT NULL DEFAULT "0"
    ) UNIQUE KEY(tenant_hash, l1, principal, l2, l3, l4, l5, resource_type, channel) DISTRIBUTED BY HASH(tenant_hash, l1) BUCKETS 10
    PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")"""

    sql """CREATE TABLE memo_cycle_acl_member (
      tenant_hash bigint NOT NULL, member bigint NOT NULL, principal bigint NOT NULL
    ) UNIQUE KEY(tenant_hash, member, principal) DISTRIBUTED BY HASH(tenant_hash, member) BUCKETS 10
    PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")"""

    sql """INSERT INTO memo_cycle_parent SELECT concat('t', number % 100), concat('p', number), concat('Parent ', number), concat('P-', number) FROM numbers("number" = "500")"""

    sql """INSERT INTO memo_cycle_child SELECT concat('t', number % 100), concat('c', number), concat('p', number), concat('Child ', number), NULL, IF(number % 3 = 0, 'k1', NULL) FROM numbers("number" = "500")"""

    sql """INSERT INTO memo_cycle_acl_cover (tenant_hash, resource_hash, resource_type, side, l1, l2, l3)
    SELECT murmur_hash3_64(concat('t', number % 100)), murmur_hash3_64(concat('t', number % 100), 'P', concat('p', number % 500)),
           IF(number % 4 = 0, 1, 7), number % 2, murmur_hash3_64('g', number % 50), murmur_hash3_64('x', number % 500), IF(number % 3 = 0, number, 0)
    FROM numbers("number" = "4000")"""

    sql """INSERT INTO memo_cycle_acl_grant (tenant_hash, l1, principal, resource_type, channel, role_level, reach)
    SELECT murmur_hash3_64(concat('t', number % 100)), murmur_hash3_64('g', number % 50), number, IF(number % 4 = 0, 1, 7), IF(number % 7 = 0, 5, 0), 10 * (1 + number % 3), number % 2
    FROM numbers("number" = "800")"""

    sql """INSERT INTO memo_cycle_acl_member SELECT murmur_hash3_64(concat('t', number % 100)), number % 3000, number % 800 FROM numbers("number" = "11000")"""

    // SAMPLE analysis must see reported row counts; analyzing too early records zero rows and misses this case.
    awaitUntil(600, 5) {
        ['memo_cycle_parent', 'memo_cycle_child', 'memo_cycle_acl_cover',
         'memo_cycle_acl_grant', 'memo_cycle_acl_member'].every { table ->
            def rows = sql_return_maparray "SHOW DATA FROM `${dbName}`.${table}"
            (rows[0].RowCount as long) > 0
        }
    }
    sql "ANALYZE TABLE memo_cycle_parent WITH SAMPLE PERCENT 100 WITH SYNC"
    sql "ANALYZE TABLE memo_cycle_child WITH SAMPLE PERCENT 100 WITH SYNC"
    sql "ANALYZE TABLE memo_cycle_acl_cover WITH SAMPLE PERCENT 100 WITH SYNC"
    sql "ANALYZE TABLE memo_cycle_acl_grant WITH SAMPLE PERCENT 100 WITH SYNC"
    sql "ANALYZE TABLE memo_cycle_acl_member WITH SAMPLE PERCENT 100 WITH SYNC"

    sql "DROP USER IF EXISTS memo_cycle_reader"
    sql "CREATE USER memo_cycle_reader IDENTIFIED BY 'MemoCycle_12345'"
    sql "GRANT SELECT_PRIV ON `${dbName}`.* TO memo_cycle_reader"
    if (isCloudMode()) {
        def cluster = (sql "SHOW CLUSTERS")[0][0]
        sql "GRANT USAGE_PRIV ON CLUSTER `${cluster}` TO memo_cycle_reader"
    }

    def createPolicies = { boolean withAdmin ->
        sql "DROP ROW POLICY IF EXISTS memo_cycle_parent_policy ON memo_cycle_parent"
        sql """CREATE ROW POLICY memo_cycle_parent_policy ON memo_cycle_parent
            AS RESTRICTIVE TO memo_cycle_reader USING (
                ${withAdmin ? '@is_admin = 1 OR' : ''}
                EXISTS (SELECT 1 FROM memo_cycle_acl_cover c
                    JOIN memo_cycle_acl_grant g ON g.tenant_hash = c.tenant_hash
                        AND g.resource_type = c.resource_type AND g.l1 = c.l1
                    JOIN memo_cycle_acl_member m ON m.principal = g.principal
                        AND m.tenant_hash = g.tenant_hash
                    WHERE c.resource_type = 7 AND c.side = 1 AND m.member = @member
                        AND m.tenant_hash = @tenant_hash AND g.role_level >= 1
                        AND c.resource_hash = murmur_hash3_64(tenant, 'P', pk)
                    ${withAdmin ? 'LIMIT 1' : ''}))"""
        sql "DROP ROW POLICY IF EXISTS memo_cycle_child_policy ON memo_cycle_child"
        sql """CREATE ROW POLICY memo_cycle_child_policy ON memo_cycle_child
            AS RESTRICTIVE TO memo_cycle_reader USING (
                ${withAdmin ? '@is_admin = 1 OR' : ''}
                EXISTS (SELECT 1 FROM memo_cycle_acl_cover c
                    JOIN memo_cycle_acl_grant g ON g.tenant_hash = c.tenant_hash
                        AND g.resource_type = c.resource_type AND g.l1 = c.l1
                    JOIN memo_cycle_acl_member m ON m.principal = g.principal
                        AND m.tenant_hash = g.tenant_hash
                    WHERE c.resource_type = 7 AND c.side = 1 AND m.member = @member
                        AND m.tenant_hash = @tenant_hash AND g.role_level >= 1 AND g.channel != 5
                        AND c.resource_hash = murmur_hash3_64(tenant, 'C', ck)
                    ${withAdmin ? 'LIMIT 1' : ''}))"""
    }
    def singleKey = """SELECT ch.ck, p.name
        FROM memo_cycle_child ch JOIN memo_cycle_parent p ON ch.pk = p.pk AND ch.tenant = p.tenant
        WHERE ch.tenant = 't1' AND p.tenant = 't1' AND ch.pk = 'p1' AND p.pk = 'p1'"""
    def twoKeys = """SELECT ch.ck, p.name
        FROM memo_cycle_child ch JOIN memo_cycle_parent p ON ch.pk = p.pk AND ch.tenant = p.tenant
        WHERE ch.tenant = 't1' AND p.tenant = 't1'
            AND ch.pk IN ('p1', 'p101') AND p.pk IN ('p1', 'p101')"""

    createPolicies(false)
    connect("memo_cycle_reader", "MemoCycle_12345", getCurDbConnectUrl()) {
        sql "USE `${dbName}`"
        sql "SET @is_admin = 0"
        sql "SET @member = 1"
        sql "SET @tenant_hash = 2965303710240291456"
        // This EXPLAIN previously created an indirect Memo cycle and exhausted the FE heap.
        explain {
            sql singleKey
            contains "PLAN FRAGMENT"
        }
        order_qt_plain_single singleKey
        order_qt_plain_two twoKeys
    }
    createPolicies(true)
    connect("memo_cycle_reader", "MemoCycle_12345", getCurDbConnectUrl()) {
        sql "USE `${dbName}`"
        sql "SET @is_admin = 0"
        sql "SET @member = 1"
        sql "SET @tenant_hash = 2965303710240291456"
        explain {
            sql singleKey
            contains "PLAN FRAGMENT"
        }
        order_qt_or_single singleKey
        order_qt_or_two twoKeys
        sql "SET @is_admin = 1"
        order_qt_admin_single singleKey
        order_qt_admin_two twoKeys
    }

    // Give one child row access, then verify both successful and rejected permission filters after planning.
    sql """INSERT INTO memo_cycle_acl_cover (tenant_hash, resource_hash, resource_type, side, l1)
        VALUES (murmur_hash3_64('t1'), murmur_hash3_64('t1', 'C', 'c1'), 7, 1, murmur_hash3_64('g', 1))"""
    connect("memo_cycle_reader", "MemoCycle_12345", getCurDbConnectUrl()) {
        sql "USE `${dbName}`"
        sql "SET @is_admin = 0"
        sql "SET @member = 1"
        sql "SET @tenant_hash = 2965303710240291456"
        order_qt_or_allowed singleKey
        sql "SET @member = 2"
        order_qt_or_denied singleKey
    }
    createPolicies(false)
    connect("memo_cycle_reader", "MemoCycle_12345", getCurDbConnectUrl()) {
        sql "USE `${dbName}`"
        sql "SET @member = 1"
        sql "SET @tenant_hash = 2965303710240291456"
        order_qt_plain_allowed singleKey
        sql "SET @member = 2"
        order_qt_plain_denied singleKey
    }
}
