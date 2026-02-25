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

suite("test_mc_write_static_partitions", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable MaxCompute test.")
        return
    }

    String ak = context.config.otherConfigs.get("ak")
    String sk = context.config.otherConfigs.get("sk")
    String mc_catalog_name = "test_mc_write_static_partitions"
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
    String db = "mc_write_sp_${uuid}"

    sql """drop database if exists ${db}"""
    sql """create database ${db}"""
    sql """use ${db}"""

    try {
        // Test 1: Single partition column static partition INSERT INTO
        String tb1 = "static_single_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb1}"""
        sql """
        CREATE TABLE ${tb1} (
            id INT,
            name STRING,
            ds STRING
        ) PARTITION BY (ds)()
        """
        sql """INSERT INTO ${tb1} PARTITION(ds='20250101') VALUES (1, 'a'), (2, 'b')"""
        sql """INSERT INTO ${tb1} PARTITION(ds='20250102') VALUES (3, 'c')"""
        order_qt_static_single_all """ SELECT * FROM ${tb1} """
        order_qt_static_single_p1 """ SELECT * FROM ${tb1} WHERE ds = '20250101' """
        order_qt_static_single_p2 """ SELECT * FROM ${tb1} WHERE ds = '20250102' """

        // Test 2: Multi-level partition columns static partition INSERT INTO
        String tb2 = "static_multi_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb2}"""
        sql """
        CREATE TABLE ${tb2} (
            id INT,
            val STRING,
            ds STRING,
            region STRING
        ) PARTITION BY (ds, region)()
        """
        sql """INSERT INTO ${tb2} PARTITION(ds='20250101', region='bj') VALUES (1, 'v1'), (2, 'v2')"""
        sql """INSERT INTO ${tb2} PARTITION(ds='20250101', region='sh') VALUES (3, 'v3')"""
        order_qt_static_multi_all """ SELECT * FROM ${tb2} """
        order_qt_static_multi_bj """ SELECT * FROM ${tb2} WHERE region = 'bj' """

        test {
            sql """ INSERT INTO ${tb2} PARTITION(ds='20250101', region='bj', ds='20250102') VALUES (1, 'v1'), (2, 'v2');"""
            exception "Duplicate partition column: ds"
        }

        // Test 3: Static partition INSERT INTO SELECT
        String tb3_src = "src_part_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb3_src}"""
        sql """
        CREATE TABLE ${tb3_src} (
            id INT,
            name STRING,
            ds STRING
        ) PARTITION BY (ds)()
        """
        sql """INSERT INTO ${tb3_src} VALUES (1, 'a', '20250101'), (2, 'b', '20250102')"""

        String tb3_dst = "dst_static_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb3_dst}"""
        sql """
        CREATE TABLE ${tb3_dst} (
            id INT,
            name STRING,
            ds STRING
        ) PARTITION BY (ds)()
        """
        sql """INSERT INTO ${tb3_dst} PARTITION(ds='20250201') SELECT id, name FROM ${tb3_src}"""
        order_qt_static_select """ SELECT * FROM ${tb3_dst} """

        // Test 4: INSERT OVERWRITE static partition
        String tb4 = "overwrite_part_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb4}"""
        sql """
        CREATE TABLE ${tb4} (
            id INT,
            name STRING,
            ds STRING
        ) PARTITION BY (ds)()
        """
        sql """INSERT INTO ${tb4} PARTITION(ds='20250101') VALUES (1, 'old1'), (2, 'old2')"""
        sql """INSERT INTO ${tb4} PARTITION(ds='20250102') VALUES (3, 'keep')"""
        sql """INSERT OVERWRITE TABLE ${tb4} PARTITION(ds='20250101') VALUES (10, 'new1')"""
        order_qt_overwrite_all """ SELECT * FROM ${tb4} """
        order_qt_overwrite_p1 """ SELECT * FROM ${tb4} WHERE ds = '20250101' """
        order_qt_overwrite_p2 """ SELECT * FROM ${tb4} WHERE ds = '20250102' """

        // Test 5: Dynamic partition regression (ensure not broken)
        String tb5 = "dynamic_reg_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb5}"""
        sql """
        CREATE TABLE ${tb5} (
            id INT,
            name STRING,
            ds STRING
        ) PARTITION BY (ds)()
        """
        sql """INSERT INTO ${tb5} VALUES (1, 'a', '20250101'), (2, 'b', '20250102')"""
        order_qt_dynamic_regression """ SELECT * FROM ${tb5} """

        // Test 6: INSERT OVERWRITE non-partitioned table
        String tb6 = "overwrite_nopart_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb6}"""
        sql """
        CREATE TABLE ${tb6} (
            id INT,
            name STRING
        )
        """
        sql """INSERT INTO ${tb6} VALUES (1, 'old')"""
        sql """INSERT OVERWRITE TABLE ${tb6} VALUES (2, 'new')"""
        order_qt_overwrite_no_part """ SELECT * FROM ${tb6} """
    } finally {
        sql """drop database if exists ${mc_catalog_name}.${db}"""
    }
}
