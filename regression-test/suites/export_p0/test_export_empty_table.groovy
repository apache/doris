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

import org.apache.doris.regression.util.ExportTestHelper

suite("test_export_empty_table", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    def helper = new ExportTestHelper(delegate)

    def table_export_name = "test_export_empty_table"
    def outfile_path_prefix = "regression/test_export_empty_table"

    // create table
    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_export_name} (
        `user_id` INT NOT NULL COMMENT "用户id",
        `date` DATE NOT NULL COMMENT "数据灌入日期时间",
        `datetime` DATETIME NOT NULL COMMENT "数据灌入日期时间",
        `city` VARCHAR(20) COMMENT "用户所在城市",
        `age` SMALLINT COMMENT "用户年龄",
        `sex` TINYINT COMMENT "用户性别",
        `bool_col` boolean COMMENT "",
        `int_col` int COMMENT "",
        `bigint_col` bigint COMMENT "",
        `float_col` float COMMENT "",
        `double_col` double COMMENT "",
        `char_col` CHAR(10) COMMENT "",
        `decimal_col` decimal COMMENT "",
        `ipv4_col` ipv4 COMMENT "",
        `ipv6_col` ipv6 COMMENT ""
        )
        DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
    """
   
    qt_select_export1 """ SELECT * FROM ${table_export_name} t ORDER BY user_id; """

    def waiting_export = { export_label ->
        while (true) {
            def res = sql """ show export where label = "${export_label}" """
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                break;
            } else if (res[0][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[0][10]}""")
            } else {
                sleep(5000)
            }
        }
    }

    // 1. test csv
    def uuid = UUID.randomUUID().toString()
    def outFilePath = "${outfile_path_prefix}/${table_export_name}_${uuid}"
    def label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "${helper.uri(outFilePath)}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv",
                "column_separator"=","
            ) WITH S3 (${helper.storageProperties()});
        """
        waiting_export.call(label)
        
        assertTrue(helper.keys("${outFilePath}/").isEmpty())
    } finally {
        helper.deleteFiles("${outFilePath}/")
    }

    // 2. test parquet
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}/${table_export_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "${helper.uri(outFilePath)}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "parquet"
            ) WITH S3 (${helper.storageProperties()});
        """
        waiting_export.call(label)
        
        assertTrue(helper.keys("${outFilePath}/").isEmpty())
    } finally {
        helper.deleteFiles("${outFilePath}/")
    }

    // 3. test orc
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}/${table_export_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "${helper.uri(outFilePath)}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "orc"
            ) WITH S3 (${helper.storageProperties()});
        """
        waiting_export.call(label)
        
        assertTrue(helper.keys("${outFilePath}/").isEmpty())
    } finally {
        helper.deleteFiles("${outFilePath}/")
    }

    // 4. test csv_with_names
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}/${table_export_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "${helper.uri(outFilePath)}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv_with_names",
                "column_separator"=","
            ) WITH S3 (${helper.storageProperties()});
        """
        waiting_export.call(label)
        
        assertTrue(helper.keys("${outFilePath}/").isEmpty())
    } finally {
        helper.deleteFiles("${outFilePath}/")
    }

    // 5. test csv_with_names_and_types
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}/${table_export_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "${helper.uri(outFilePath)}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv_with_names_and_types",
                "column_separator"=","
            ) WITH S3 (${helper.storageProperties()});
        """
        waiting_export.call(label)
        
        assertTrue(helper.keys("${outFilePath}/").isEmpty())
    } finally {
        helper.deleteFiles("${outFilePath}/")
    }

    try_sql("DROP TABLE IF EXISTS ${table_export_name}")
}
