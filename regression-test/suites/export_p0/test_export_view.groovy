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

suite("test_export_view", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    def helper = new ExportTestHelper(delegate)

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
    def table_export_name = "test_export_base_table"
    def table_export_view_name = "test_export_view_table"
    def outfile_path_prefix = "regression/test_export_view"

    // create table and insert
    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_export_name} (
        `k1` int(11) NULL,
        `k2` string NULL,
        `k3` int(11) NULL,
        `v1` int(11) NULL
        )
        PARTITION BY RANGE(k1)
        (
            PARTITION less_than_20 VALUES LESS THAN ("20"),
            PARTITION between_20_70 VALUES [("20"),("70")),
            PARTITION more_than_70 VALUES LESS THAN ("151")
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES("replication_num" = "1");
    """
    StringBuilder sb = new StringBuilder()
    int i = 1
    for (; i < 150; i ++) {
        if (i < 50) {
            sb.append("""
                    (${i}, 'zhangsan', ${i + 18}, ${i}),
                """)
        } else if (i < 80) {
            sb.append("""
                    (${i}, 'lisi', ${i + 18}, ${i}),
                """)
        } else if (i < 120) {
            sb.append("""
                    (${i}, 'wangwu', ${i + 18}, ${i}),
                """)
        } else {
            sb.append("""
                    (${i}, 'fangfang', ${i + 18}, ${i}),
                """)
        }
    }
    sb.append("""
            (${i}, 'xiexie', NULL, ${i})
        """)
    sql """ INSERT INTO ${table_export_name} VALUES
            ${sb.toString()}
        """
    def insert_res = sql "show last insert;"
    logger.info("insert result: " + insert_res.toString())
    qt_select_export """ SELECT * FROM ${table_export_name} t ORDER BY k1; """

    sql """ DROP VIEW IF EXISTS ${table_export_view_name} """
    sql """
    CREATE VIEW ${table_export_view_name}
        (
            s1 COMMENT "first key",
            k1 COMMENT "second key",
            k2 COMMENT "third key",
            k3 COMMENT "first value"
        )
        COMMENT "my first view"
        AS
        SELECT k2, min(k1), max(k3), SUM(v1) FROM ${table_export_name}
        WHERE k1 > 30 GROUP BY k2;
    """

    // 1. basic test
    def uuid = UUID.randomUUID().toString()
    def outFilePath = "${outfile_path_prefix}/${table_export_view_name}_${uuid}"
    def label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} TO "${helper.uri(outFilePath)}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv",
                "column_separator"=","
            ) WITH S3 (${helper.storageProperties()});
        """
        waiting_export.call(label)
    } finally {
        helper.deleteFiles("${outFilePath}/")
    }

    // 2. test csv_with_names
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}/${table_export_view_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} TO "${helper.uri(outFilePath)}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv_with_names",
                "max_file_size" = "512MB",
                "parallelISM" = "5",
                "column_separator"=","
            ) WITH S3 (${helper.storageProperties()});
        """
        waiting_export.call(label)
    } finally {
        helper.deleteFiles("${outFilePath}/")
    }

    // 3. test where clause
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}/${table_export_view_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} where s1 = 'fangfang' OR k1 = 31
            TO "${helper.uri(outFilePath)}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv_with_names",
                "max_file_size" = "512MB",
                "parallelISM" = "5",
                "column_separator"=","
            ) WITH S3 (${helper.storageProperties()});
        """
        waiting_export.call(label)
    } finally {
        helper.deleteFiles("${outFilePath}/")
    }

    // 4. test where clause and columns property
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}/${table_export_view_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} where s1 = 'fangfang' OR k1 = 31
            TO "${helper.uri(outFilePath)}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv_with_names",
                "max_file_size" = "512MB",
                "parallelISM" = "5",
                "columns" = "k3, s1, k1",
                "column_separator"=","
            ) WITH S3 (${helper.storageProperties()});
        """

        waiting_export.call(label)
        
    } finally {
        helper.deleteFiles("${outFilePath}/")
    }

    // 5. test csv_with_names_and_types
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}/${table_export_view_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} TO "${helper.uri(outFilePath)}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv_with_names_and_types",
                "max_file_size" = "512MB",
                "parallelISM" = "5",
                "column_separator"=","
            ) WITH S3 (${helper.storageProperties()});
        """
        waiting_export.call(label)
        
    } finally {
        helper.deleteFiles("${outFilePath}/")
    }

    // 6. test orc type
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}/${table_export_view_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} TO "${helper.uri(outFilePath)}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "orc",
                "max_file_size" = "512MB"
            ) WITH S3 (${helper.storageProperties()});
        """
        waiting_export.call(label)
        
    } finally {
        helper.deleteFiles("${outFilePath}/")
    }

    // 8. test orc type, where clause and columns property
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}/${table_export_view_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} where s1 = 'fangfang' OR k1 = 31
            TO "${helper.uri(outFilePath)}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "orc",
                "max_file_size" = "512MB",
                "parallelISM" = "5",
                "columns" = "k3, s1, k1"
            ) WITH S3 (${helper.storageProperties()});
        """

        waiting_export.call(label)
    } finally {
        helper.deleteFiles("${outFilePath}/")
    }

    // 7. test parquet type use s3
    uuid = UUID.randomUUID().toString()
    label = "label_${uuid}"
    outFilePath = "${outfile_path_prefix}/${table_export_view_name}_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} TO "${helper.uri(outFilePath)}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "parquet"
            ) WITH S3 (${helper.storageProperties()});
        """

        def outfile_url = ""
        while (true) {
            def res = sql """ show export where label = "${label}" """
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                def json = parseJson(res[0][11])
                assert json instanceof List
                assertEquals("1", json.fileNumber[0][0])
                logger.info("outfile_path: ${json.url[0][0]}")
                outfile_url = json.url[0][0];
                break;
            } else if (res[0][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[0][10]}""")
            } else {
                sleep(5000)
            }
        }

        order_qt_select_load7 """ select * from s3(
                "uri" = "${outfile_url}",
                ${helper.storageProperties()},
                "format" = "parquet"
            );
            """
    } finally {
        helper.deleteFiles("${outFilePath}/")
    }
}
