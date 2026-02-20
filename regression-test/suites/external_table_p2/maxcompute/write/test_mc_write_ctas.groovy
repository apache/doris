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

suite("test_mc_write_ctas", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable MaxCompute test.")
        return
    }

    String ak = context.config.otherConfigs.get("ak")
    String sk = context.config.otherConfigs.get("sk")
    String mc_catalog_name = "test_mc_write_ctas"
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
    String db = "mc_write_ctas_${uuid}"

    sql """drop database if exists ${db}"""
    sql """create database ${db}"""
    sql """use ${db}"""

    try {
        // Prepare source table
        String src = "ctas_src_${uuid}"
        sql """DROP TABLE IF EXISTS ${src}"""
        sql """
        CREATE TABLE ${src} (
            id INT,
            name STRING,
            val DOUBLE,
            dt DATE
        )
        """
        sql """
        INSERT INTO ${src} VALUES
            (1, 'Alice', 10.5, '2025-01-01'),
            (2, 'Bob', 20.3, '2025-01-02'),
            (3, 'Charlie', 30.7, '2025-01-03')
        """

        // Test 1: Basic CTAS
        String tb1 = "ctas_basic_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb1}"""
        sql """CREATE TABLE ${tb1} AS SELECT * FROM ${src}"""
        order_qt_ctas_basic """ SELECT * FROM ${tb1} """

        // Test 2: CTAS with column subset
        String tb2 = "ctas_subset_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb2}"""
        sql """CREATE TABLE ${tb2} AS SELECT id, name FROM ${src}"""
        order_qt_ctas_subset """ SELECT * FROM ${tb2} """

        // Test 3: CTAS with expression
        String tb3 = "ctas_expr_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb3}"""
        sql """CREATE TABLE ${tb3} AS SELECT id, name, val * 2 AS val_double FROM ${src}"""
        order_qt_ctas_expr """ SELECT * FROM ${tb3} """

        // Test 4: CTAS with WHERE filter
        String tb4 = "ctas_filter_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb4}"""
        sql """CREATE TABLE ${tb4} AS SELECT * FROM ${src} WHERE id > 1"""
        order_qt_ctas_filter """ SELECT * FROM ${tb4} """

        // Test 5: CTAS with aggregation
        String tb5 = "ctas_agg_${uuid}"
        sql """DROP TABLE IF EXISTS ${tb5}"""
        sql """CREATE TABLE ${tb5} AS SELECT count(*) AS cnt, sum(val) AS total FROM ${src}"""
        order_qt_ctas_agg """ SELECT * FROM ${tb5} """

        // Test 6: CTAS from internal catalog (cross-catalog)
        String internal_db = "mc_ctas_internal_${uuid}"
        String internal_tb = "ctas_internal_src_${uuid}"
        sql """CREATE DATABASE IF NOT EXISTS internal.${internal_db}"""
        sql """DROP TABLE IF EXISTS internal.${internal_db}.${internal_tb}"""
        sql """
        CREATE TABLE internal.${internal_db}.${internal_tb} (
            id INT,
            name VARCHAR(100),
            val DOUBLE
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
        sql """INSERT INTO internal.${internal_db}.${internal_tb} VALUES (1, 'cross', 99.9)"""

        String tb6 = "ctas_cross_${uuid}"
        sql """DROP TABLE IF EXISTS ${mc_catalog_name}.${db}.${tb6}"""
        sql """CREATE TABLE ${mc_catalog_name}.${db}.${tb6} AS SELECT * FROM internal.${internal_db}.${internal_tb}"""
        order_qt_ctas_cross """ SELECT * FROM ${mc_catalog_name}.${db}.${tb6} """

    } finally {
        sql """DROP TABLE IF EXISTS internal.${internal_db}.${internal_tb}"""
        sql """DROP DATABASE IF EXISTS internal.${internal_db}"""
        sql """drop database if exists ${mc_catalog_name}.${db}"""
    }
}
