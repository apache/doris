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

suite("test_max_compute_create_table_cache_visibility", "p2,external") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("skip maxcompute test.")
        return
    }

    String ak = context.config.otherConfigs.get("ak")
    String sk = context.config.otherConfigs.get("sk")
    String mc_catalog_name = "test_mc_cache_vis"
    String defaultProject = "mc_datalake"

    sql """drop catalog if exists ${mc_catalog_name}"""
    sql """
    CREATE CATALOG IF NOT EXISTS ${mc_catalog_name} PROPERTIES (
        "type" = "max_compute",
        "mc.default.project" = "${defaultProject}",
        "mc.access_key" = "${ak}",
        "mc.secret_key" = "${sk}",
        "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api",
        "mc.quota" = "pay-as-you-go"
    );
    """

    sql """switch ${mc_catalog_name}"""
    sql """use ${defaultProject}"""

    // CREATE TABLE then immediately verify visibility
    String tbl1 = "test_mc_cache_vis_tbl1"
    sql """DROP TABLE IF EXISTS ${tbl1}"""
    sql """
    CREATE TABLE ${tbl1} (
        id INT,
        name STRING
    )
    """

    // The new table should be immediately visible without REFRESH CATALOG
    def show_result = sql """SHOW TABLES LIKE '${tbl1}'"""
    assertTrue(show_result.size() > 0, "Table ${tbl1} should appear in SHOW TABLES after CREATE TABLE")

    def create_result = sql """SHOW CREATE TABLE ${tbl1}"""
    assertTrue(create_result.size() > 0, "SHOW CREATE TABLE should succeed for newly created table")

    // Create a second table
    String tbl2 = "test_mc_cache_vis_tbl2"
    sql """DROP TABLE IF EXISTS ${tbl2}"""
    sql """
    CREATE TABLE ${tbl2} (
        id INT,
        value DOUBLE
    )
    """
    def create_result2 = sql """SHOW CREATE TABLE ${tbl2}"""
    assertTrue(create_result2.size() > 0, "Second table should also be immediately visible")

    // Cleanup
    sql """DROP TABLE IF EXISTS ${tbl1}"""
    sql """DROP TABLE IF EXISTS ${tbl2}"""
    sql """drop catalog if exists ${mc_catalog_name}"""
}
