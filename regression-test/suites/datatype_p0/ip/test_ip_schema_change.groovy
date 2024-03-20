
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

suite("test_ip_schema_change") {
    sql """ DROP TABLE IF EXISTS `test_ip_schema_change_with_varchar` """
    sql """ DROP TABLE IF EXISTS `test_ip_schema_change_with_string` """
    sql """ DROP TABLE IF EXISTS `test_ip_schema_change_with_text` """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    // test schema change between `ip` and `varchar`
    sql """
        CREATE TABLE `test_ip_schema_change_with_varchar` (
            `id` INT,
            `ip_v4` VARCHAR,
            `ip_v6` VARCHAR
        )
        DUPLICATE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        INSERT INTO `test_ip_schema_change_with_varchar` VALUES
        (1, '0.0.0.0', '::'),
        (2, '127.0.0.1', '2001:1b70:a1:610::b102:2'),
        (3, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff');
    """

    qt_sql1 "SELECT * FROM `test_ip_schema_change_with_varchar` ORDER BY id"

    String res = sql """ desc `test_ip_schema_change_with_varchar` """

    assertTrue(res.contains("ip_v4, VARCHAR(65533)") && res.contains("ip_v6, VARCHAR(65533)"))

    sql "ALTER TABLE `test_ip_schema_change_with_varchar` MODIFY COLUMN `ip_v4` IPV4"

    sleep(5000)

    sql "ALTER TABLE `test_ip_schema_change_with_varchar` MODIFY COLUMN `ip_v6` IPV6"

    sleep(5000)

    qt_sql2 "SELECT * FROM `test_ip_schema_change_with_varchar` ORDER BY id"

    res = sql """ desc `test_ip_schema_change_with_varchar` """

    assertTrue(res.contains("ip_v4, IPV4") && res.contains("ip_v6, IPV6"))

    sql "ALTER TABLE `test_ip_schema_change_with_varchar` MODIFY COLUMN `ip_v4` VARCHAR"

    sleep(5000)

    sql "ALTER TABLE `test_ip_schema_change_with_varchar` MODIFY COLUMN `ip_v6` VARCHAR"

    sleep(5000)

    qt_sql3 "SELECT * FROM `test_ip_schema_change_with_varchar` ORDER BY id"

    res = sql """ desc `test_ip_schema_change_with_varchar` """

    assertTrue(res.contains("ip_v4, VARCHAR(65533)") && res.contains("ip_v6, VARCHAR(65533)"))

    // test schema change between `ip` and `string`
    sql """
        CREATE TABLE `test_ip_schema_change_with_string` (
            `id` INT,
            `ip_v4` STRING,
            `ip_v6` STRING
        )
        DUPLICATE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        INSERT INTO `test_ip_schema_change_with_string` VALUES
        (1, '0.0.0.0', '::'),
        (2, '127.0.0.1', '2001:1b70:a1:610::b102:2'),
        (3, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff');
    """

    qt_sql4 "SELECT * FROM `test_ip_schema_change_with_string` ORDER BY id"

    res = sql """ desc `test_ip_schema_change_with_string` """

    assertTrue(res.contains("ip_v4, TEXT") && res.contains("ip_v6, TEXT"))

    sql "ALTER TABLE `test_ip_schema_change_with_string` MODIFY COLUMN `ip_v4` IPV4"

    sleep(5000)

    sql "ALTER TABLE `test_ip_schema_change_with_string` MODIFY COLUMN `ip_v6` IPV6"

    sleep(5000)

    qt_sql5 "SELECT * FROM `test_ip_schema_change_with_string` ORDER BY id"

    res = sql """ desc `test_ip_schema_change_with_string` """

    assertTrue(res.contains("ip_v4, IPV4") && res.contains("ip_v6, IPV6"))

    sql "ALTER TABLE `test_ip_schema_change_with_string` MODIFY COLUMN `ip_v4` STRING"

    sleep(5000)

    sql "ALTER TABLE `test_ip_schema_change_with_string` MODIFY COLUMN `ip_v6` STRING"

    sleep(5000)

    qt_sql6 "SELECT * FROM `test_ip_schema_change_with_string` ORDER BY id"

    res = sql """ desc `test_ip_schema_change_with_string` """

    assertTrue(res.contains("ip_v4, TEXT") && res.contains("ip_v6, TEXT"))

    // test schema change between `ip` and `text`
    sql """
        CREATE TABLE `test_ip_schema_change_with_text` (
            `id` INT,
            `ip_v4` TEXT,
            `ip_v6` TEXT
        )
        DUPLICATE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        INSERT INTO `test_ip_schema_change_with_text` VALUES
        (1, '0.0.0.0', '::'),
        (2, '127.0.0.1', '2001:1b70:a1:610::b102:2'),
        (3, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff');
    """

    qt_sql7 "SELECT * FROM `test_ip_schema_change_with_text` ORDER BY id"

    res = sql """ desc `test_ip_schema_change_with_text` """

    assertTrue(res.contains("ip_v4, TEXT") && res.contains("ip_v6, TEXT"))

    sql "ALTER TABLE `test_ip_schema_change_with_text` MODIFY COLUMN `ip_v4` IPV4"

    sleep(5000)

    sql "ALTER TABLE `test_ip_schema_change_with_text` MODIFY COLUMN `ip_v6` IPV6"

    sleep(5000)

    qt_sql8 "SELECT * FROM `test_ip_schema_change_with_text` ORDER BY id"

    res = sql """ desc `test_ip_schema_change_with_text` """

    assertTrue(res.contains("ip_v4, IPV4") && res.contains("ip_v6, IPV6"))

    sql "ALTER TABLE `test_ip_schema_change_with_text` MODIFY COLUMN `ip_v4` TEXT"

    sleep(5000)

    sql "ALTER TABLE `test_ip_schema_change_with_text` MODIFY COLUMN `ip_v6` TEXT"

    sleep(5000)

    qt_sql9 "SELECT * FROM `test_ip_schema_change_with_text` ORDER BY id"

    res = sql """ desc `test_ip_schema_change_with_text` """

    assertTrue(res.contains("ip_v4, TEXT") && res.contains("ip_v6, TEXT"))

    sql """ DROP TABLE IF EXISTS `test_ip_schema_change_with_varchar` """
    sql """ DROP TABLE IF EXISTS `test_ip_schema_change_with_string` """
    sql """ DROP TABLE IF EXISTS `test_ip_schema_change_with_text` """
}
