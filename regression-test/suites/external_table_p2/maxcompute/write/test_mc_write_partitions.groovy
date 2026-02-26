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

suite("test_mc_write_partitions", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable MaxCompute test.")
        return
    }

    String ak = context.config.otherConfigs.get("ak")
    String sk = context.config.otherConfigs.get("sk")
    String mc_catalog_name = "test_mc_write_partitions"
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

    sql """switch ${mc_catalog_name}"""

    def uuid = UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    String db = "mc_write_part_${uuid}"

    sql """drop database if exists ${db}"""
    sql """create database ${db}"""
    sql """use ${db}"""

    try {
        // Test 1: Single partition column INSERT
        String tb1 = "single_part_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb1}"""
        sql """
        CREATE TABLE ${tb1} (
            id INT,
            name STRING,
            ds STRING
        )
        PARTITION BY (ds)()
        """
        sql """INSERT INTO ${tb1} VALUES (1, 'a', '20250101'), (2, 'b', '20250101'), (3, 'c', '20250102')"""
        order_qt_single_part """ SELECT * FROM ${tb1} """
        order_qt_single_part_filter """ SELECT * FROM ${tb1} WHERE ds = '20250101' """

        // Test 2: Multi-level partition columns
        String tb2 = "multi_part_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb2}"""
        sql """
        CREATE TABLE ${tb2} (
            id INT,
            val STRING,
            ds STRING,
            region STRING
        )
        PARTITION BY (ds, region)()
        """
        sql """
        INSERT INTO ${tb2} VALUES
            (1, 'v1', '20250101', 'bj'),
            (2, 'v2', '20250101', 'sh'),
            (3, 'v3', '20250102', 'bj'),
            (4, 'v4', '20250102', 'sh')
        """
        order_qt_multi_part_all """ SELECT * FROM ${tb2} """
        order_qt_multi_part_ds """ SELECT * FROM ${tb2} WHERE ds = '20250101' """
        order_qt_multi_part_region """ SELECT * FROM ${tb2} WHERE region = 'bj' """
        order_qt_multi_part_both """ SELECT * FROM ${tb2} WHERE ds = '20250101' AND region = 'bj' """

        // Test 3: INSERT into partition table with SELECT
        String tb3 = "part_select_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb3}"""
        sql """
        CREATE TABLE ${tb3} (
            id INT,
            name STRING,
            ds STRING
        )
        PARTITION BY (ds)()
        """
        sql """INSERT INTO ${tb3} SELECT id, name, ds FROM ${tb1}"""
        order_qt_part_select """ SELECT * FROM ${tb3} """
    } finally {
        sql """drop database if exists ${mc_catalog_name}.${db}"""
    }
}
