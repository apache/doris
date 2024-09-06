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

import org.codehaus.groovy.runtime.IOGroovyMethods

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_export_table_with_materialized_view", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def table_export_name = "test_export_table_with_materialized_view"
    def table_load_name = "test_load_table_with_rollup"
    def outfile_path_prefix = """${bucket}/export/p0/export_table_with_materialized_view/exp"""

    // create table and insert
    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_export_name} (
        `k1` INT NOT NULL,
        `k2` DATE NOT NULL COMMENT "数据灌入日期",
        `k3` BIGINT NOT NULL,
        `v1` SMALLINT COMMENT "",
        `v2` TINYINT COMMENT "",
        `v3` int COMMENT "",
        `v4` bigint COMMENT "",
        `v5` largeint COMMENT ""
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 10 PROPERTIES("replication_num" = "1");
    """
    sql """
    CREATE MATERIALIZED VIEW export_table_materialized_view AS SELECT k2, sum(v5) FROM ${table_export_name} GROUP BY k2;
    """
    StringBuilder sb = new StringBuilder()
    int i = 1
    for (; i < 100; i++) {
        sb.append("""
            (${i}, '2017-10-01', ${i} + 1, ${i % 128}, ${i}, ${i}, ${i}, ${i}),
        """)
    }
    sb.append("""
            (${i}, '2018-10-01', 1, 1, 2, 3, 4, 5)
        """)
    sql """ INSERT INTO ${table_export_name} VALUES
            ${sb.toString()}
        """
    def insert_res = sql "show last insert;"
    logger.info("insert result: " + insert_res.toString())
    qt_select_export1 """ SELECT * FROM ${table_export_name} ORDER BY k1; """


    def waiting_export = { export_label ->
        while (true) {
            def res = sql """ show export where label = "${export_label}" """
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                def json = parseJson(res[0][11])
                assert json instanceof List
                assertEquals(1, json[0].size())
                assertEquals("100", json[0][0].totalRows)
                return json.url[0][0];
            } else if (res[0][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[0][10]}""")
            } else {
                sleep(5000)
            }
        }
    }

    def uuid = UUID.randomUUID().toString()
    def outFilePath = """${outfile_path_prefix}_${uuid}"""
    def label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "parquet",
                'columns' = 'k1, k2, k3, v1, v2, v3, v4, v5'
            )
            WITH S3(
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${getS3Provider()}"
            );
        """
        def outfile_url = waiting_export.call(label)

        qt_select_load1 """ select * from s3(
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5+bucket.length(), outfile_url.length() - 1)}0.parquet",
                "s3.access_key"= "${ak}",
                "s3.secret_key" = "${sk}",
                "format" = "parquet",
                "provider" = "${getS3Provider()}",
                "region" = "${region}"
            ) ORDER BY k1;
            """

    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_export_name}")
    }
}