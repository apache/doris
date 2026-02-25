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

suite("test_mc_write_large_data", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable MaxCompute test.")
        return
    }

    String ak = context.config.otherConfigs.get("ak")
    String sk = context.config.otherConfigs.get("sk")
    String mc_catalog_name = "test_mc_write_large_data"
    String defaultProject = "doris_test_schema"

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

    def uuid = UUID.randomUUID().toString().replace("-", "").substring(0, 8)

    // Step 1: Create internal table and insert 2 million rows
    String internal_db = "mc_large_internal_${uuid}"
    String internal_tb = "large_src_${uuid}"
    sql """CREATE DATABASE IF NOT EXISTS internal.${internal_db}"""
    sql """DROP TABLE IF EXISTS internal.${internal_db}.${internal_tb}"""
    sql """
    CREATE TABLE internal.${internal_db}.${internal_tb} (
        id INT,
        name STRING,
        val DOUBLE,
        ds STRING
    )
    DISTRIBUTED BY HASH(id) BUCKETS 8
    PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // Generate 2 million rows: id 1..2000000, ds in 8 partitions
    sql """
    INSERT INTO internal.${internal_db}.${internal_tb}
    SELECT
        number AS id,
        concat('name_', cast(number AS STRING)) AS name,
        number * 0.01 AS val,
        concat('2025010', cast((number % 8 + 1) AS STRING)) AS ds
    FROM numbers("number"="2000000")
    """

    // Step 2: Create MC tables and write data
    sql """switch ${mc_catalog_name}"""
    String db = "mc_large_data_${uuid}"
    sql """drop database if exists ${db}"""
    sql """create database ${db}"""
    sql """use ${db}"""

    try {
        // Non-partition table
        String tb1 = "large_no_part_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb1}"""
        sql """
        CREATE TABLE ${tb1} (
            id INT,
            name STRING,
            val DOUBLE,
            ds STRING
        )
        """

        // Partition table (8 partitions: 20250101 ~ 20250108)
        String tb2 = "large_part_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb2}"""
        sql """
        CREATE TABLE ${tb2} (
            id INT,
            name STRING,
            val DOUBLE,
            ds STRING
        )
        PARTITION BY (ds)()
        """

        // Step 3: Insert from internal table
        sql """INSERT INTO ${tb1} SELECT * FROM internal.${internal_db}.${internal_tb}"""
        sql """INSERT INTO ${tb2} SELECT * FROM internal.${internal_db}.${internal_tb}"""

        // Step 4: Verify results
        qt_count_no_part """ SELECT count(*) FROM ${tb1} """
        qt_count_part """ SELECT count(*) FROM ${tb2} """
        order_qt_top10_no_part """ SELECT * FROM ${tb1} ORDER BY id LIMIT 10 """
        order_qt_top10_part """ SELECT * FROM ${tb2} ORDER BY id LIMIT 10 """

        // Multi-partition-column table (ds × region = 8 × 3 = 24 partitions, within 10 per column)
        String tb3 = "large_multi_part_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb3}"""
        sql """
        CREATE TABLE ${tb3} (
            id INT,
            name STRING,
            val DOUBLE,
            ds STRING,
            region STRING
        )
        PARTITION BY (ds, region)()
        """

        sql """
        INSERT INTO ${tb3}
        SELECT id, name, val, ds,
            concat('r', cast((id % 3 + 1) AS STRING)) AS region
        FROM internal.${internal_db}.${internal_tb}
        """

        qt_count_multi_part """ SELECT count(*) FROM ${tb3} """
        order_qt_top10_multi_part """ SELECT * FROM ${tb3} ORDER BY id LIMIT 10 """
    } finally {
        sql """drop database if exists ${mc_catalog_name}.${db}"""
        sql """DROP TABLE IF EXISTS internal.${internal_db}.${internal_tb}"""
        sql """DROP DATABASE IF EXISTS internal.${internal_db}"""
    }
}
