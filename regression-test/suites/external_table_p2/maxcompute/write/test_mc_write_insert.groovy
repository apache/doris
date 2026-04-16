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

suite("test_mc_write_insert", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable MaxCompute test.")
        return
    }

    String ak = context.config.otherConfigs.get("ak")
    String sk = context.config.otherConfigs.get("sk")
    String mc_catalog_name = "test_mc_write_insert"
    String defaultProject = "mc_doris_test_write"

    sql """drop catalog if exists ${mc_catalog_name}"""
    sql """
    CREATE CATALOG IF NOT EXISTS ${mc_catalog_name} PROPERTIES (
        "type" = "max_compute",
        "mc.default.project" = "${defaultProject}",
        "mc.access_key" = "${ak}",
        "mc.secret_key" = "${sk}",
        "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api",
        "mc.quota" = "pay-as-you-go",
        "mc.enable.namespace.schema" = "true"
    );
    """

    sql """switch ${mc_catalog_name}"""

    def uuid = UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    String db = "mc_write_insert_${uuid}"
    String internal_db = "mc_write_internal_${uuid}"
    String internal_tb = "insert_src_${uuid}"
    String unsupported_schema = "default"
    String clustered_tb = "mc_write_unsupported_clustered"
    String transactional_tb = "mc_write_unsupported_transactional"
    String delta_tb = "mc_write_unsupported_delta"
    String external_tb = "mc_write_unsupported_external"

    sql """drop database if exists ${db}"""
    sql """create database ${db}"""
    sql """use ${db}"""

    def getProfileTextBySql = { String stmt ->
        def profileAction = new org.apache.doris.regression.action.ProfileAction(context)
        String profileId = ""
        int attempts = 0
        while (attempts < 10 && (profileId == null || profileId.isEmpty())) {
            List profileData = profileAction.getProfileList()
            for (def profileItem : profileData) {
                if (profileItem["Sql Statement"].toString().contains(stmt)) {
                    profileId = profileItem["Profile ID"].toString()
                    break
                }
            }
            if (profileId == null || profileId.isEmpty()) {
                Thread.sleep(300)
            }
            attempts++
        }
        assertTrue(profileId != null && !profileId.isEmpty(),
                "Profile ID of ${stmt} is not found")
        Thread.sleep(500)
        return profileAction.getProfile(profileId).toString()
    }

    try {
        // Test 1: Basic INSERT INTO with VALUES
        String tb1 = "basic_insert_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb1}"""
        sql """
        CREATE TABLE ${tb1} (
            id INT,
            name STRING,
            value DOUBLE
        )
        """
        sql """
        INSERT INTO ${tb1} VALUES
            (1, 'Alice', 10.5),
            (2, 'Bob', 20.3),
            (3, 'Charlie', 30.7)
        """
        order_qt_basic_insert """ SELECT * FROM ${tb1} """

        // Test 2: INSERT INTO with SELECT
        String tb2 = "insert_select_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb2}"""
        sql """
        CREATE TABLE ${tb2} (
            id INT,
            name STRING,
            value DOUBLE
        )
        """
        sql """INSERT INTO ${tb2} SELECT * FROM ${tb1}"""
        order_qt_insert_select """ SELECT * FROM ${tb2} """

        // Test 3: INSERT partial columns (unspecified columns should be NULL)
        String tb3 = "partial_insert_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb3}"""
        sql """
        CREATE TABLE ${tb3} (
            id INT,
            name STRING,
            value DOUBLE,
            extra STRING
        )
        """
        sql """INSERT INTO ${tb3} (id, name) VALUES (1, 'test1'), (2, 'test2')"""
        order_qt_partial_insert """ SELECT * FROM ${tb3} """

        // Test 4: INSERT multiple batches and verify accumulation
        String tb4 = "multi_batch_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb4}"""
        sql """
        CREATE TABLE ${tb4} (
            id INT,
            val STRING
        )
        """
        sql """INSERT INTO ${tb4} VALUES (1, 'batch1')"""
        sql """INSERT INTO ${tb4} VALUES (2, 'batch2')"""
        sql """INSERT INTO ${tb4} VALUES (3, 'batch3')"""
        order_qt_multi_batch """ SELECT * FROM ${tb4} """

        // Test 5: INSERT INTO MC table from internal catalog table
        sql """CREATE DATABASE IF NOT EXISTS internal.${internal_db}"""
        sql """DROP TABLE IF EXISTS internal.${internal_db}.${internal_tb}"""
        sql """
        CREATE TABLE internal.${internal_db}.${internal_tb} (
            id INT,
            name STRING,
            value DOUBLE
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        sql """
        INSERT INTO internal.${internal_db}.${internal_tb} VALUES
            (11, 'internal_alice', 110.5),
            (12, 'internal_bob', 120.3),
            (13, 'internal_charlie', 130.7)
        """

        String tb5 = "cross_catalog_insert_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb5}"""
        sql """
        CREATE TABLE ${tb5} (
            id INT,
            name STRING,
            value DOUBLE
        )
        """
        sql """INSERT INTO ${tb5} SELECT * FROM internal.${internal_db}.${internal_tb}"""
        order_qt_cross_catalog_insert """ SELECT * FROM ${tb5} """

        // Test 5b: INSERT INTO SELECT with LIMIT should keep serial read behavior
        String limit_src = "limit_src_${uuid}"
        sql """DROP TABLE IF EXISTS internal.${internal_db}.${limit_src}"""
        sql """
        CREATE TABLE internal.${internal_db}.${limit_src} (
            id INT,
            name STRING
        )
        DISTRIBUTED BY HASH(id) BUCKETS 5
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        sql """
        INSERT INTO internal.${internal_db}.${limit_src}
        SELECT
            number + 1 AS id,
            concat('limit_', cast(number + 1 AS STRING)) AS name
        FROM numbers("number"="200")
        """

        String tb_limit = "limit_insert_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb_limit}"""
        sql """
        CREATE TABLE ${tb_limit} (
            id INT,
            name STRING
        )
        """
        String limitInsertSql = "INSERT INTO ${tb_limit} SELECT id, name "
                + "FROM internal.${internal_db}.${limit_src} ORDER BY id LIMIT 100"
        sql """set enable_profile=true"""
        sql """set profile_level=2"""
        sql """set parallel_pipeline_task_num=1"""
        sql limitInsertSql
        qt_limit_insert_count """ SELECT count(*) FROM ${tb_limit} """
        order_qt_limit_insert_top10 """ SELECT * FROM ${tb_limit} ORDER BY id LIMIT 10 """
        String limitInsertProfile = getProfileTextBySql(limitInsertSql)
        assertTrue(limitInsertProfile.contains("MaxScanConcurrency: 1"),
                "LIMIT insert should use serial read, profile: ${limitInsertProfile}")
        sql """set enable_profile=false"""

        sql """USE ${mc_catalog_name}.`${unsupported_schema}`"""
        // Test 6: INSERT INTO clustered MaxCompute table should fail
        sql """DESC ${clustered_tb}"""
        test {
            sql """INSERT OVERWRITE TABLE ${clustered_tb} VALUES (21, 'clustered_row', 210.5)"""
            exception "Writing cluster table is not supported yet"
        }
        // Test 7: INSERT INTO transactional MaxCompute table should fail
        sql """DESC ${transactional_tb}"""
        test {
            sql """INSERT INTO ${transactional_tb} VALUES (22, 'transactional_row', 220.5)"""
            exception "not supported by storage api"
        }

        // Test 8: INSERT INTO Delta MaxCompute table should fail
        sql """DESC ${delta_tb}"""
        test {
            sql """INSERT INTO ${delta_tb} VALUES (23, 'delta_row', 230.5)"""
            exception "not supported by storage api"
        }

        // Test 9: INSERT INTO Delta Lake external MaxCompute table should fail
        sql """DESC ${external_tb}"""
        test {
            sql """INSERT INTO ${external_tb} VALUES (24, 'external_row', 240.5)"""
            exception "mc_write_unsupported_external: No oss endpoint provide"
        }
    } finally {
        sql """DROP TABLE IF EXISTS internal.${internal_db}.${internal_tb}"""
        sql """DROP DATABASE IF EXISTS internal.${internal_db}"""
        sql """drop database if exists ${mc_catalog_name}.${db}"""
    }
}
