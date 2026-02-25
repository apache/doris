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
    String db = "mc_write_insert_${uuid}"

    sql """drop database if exists ${db}"""
    sql """create database ${db}"""
    sql """use ${db}"""

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
    } finally {
        sql """drop database if exists ${mc_catalog_name}.${db}"""
    }
}
