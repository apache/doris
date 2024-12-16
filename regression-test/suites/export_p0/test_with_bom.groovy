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

suite("test_with_bom", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """


    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def this_db = "regression_test_export_p0"
    def table_export_name = "test_with_bom"
    def table_load_name = "test_with_bom_load_back"
    def outfile_path_prefix = """${bucket}/export/test_with_bom/exp_"""
    def file_format = "csv"

    // test csv with bom
    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
        CREATE TABLE IF NOT EXISTS ${table_export_name} (
            `user_id` INT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间"
            )
            DISTRIBUTED BY HASH(user_id)
            PROPERTIES("replication_num" = "1");
        """
    StringBuilder sb = new StringBuilder()
    int i = 1
    for (; i < 11; i ++) {
        sb.append("""
            (${i}, '2017-10-01'),
        """)
    }
    sb.append("""
            (${i}, '2017-10-01')
        """)
    sql """ INSERT INTO ${table_export_name} VALUES
            ${sb.toString()}
        """
    def insert_res = sql "show last insert;"
    logger.info("insert result: " + insert_res.toString())
    order_qt_select_export1 """ SELECT * FROM ${table_export_name} t ORDER BY user_id; """


    def waiting_export = { export_label ->
        while (true) {
            def res = sql """ show export where label = "${export_label}";"""
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                def json = parseJson(res[0][11])
                assert json instanceof List
                assertEquals("1", json.fileNumber[0][0])
                log.info("outfile_path: ${json.url[0][0]}")
                return json.url[0][0];
            } else if (res[0][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[0][10]}""")
            } else {
                sleep(5000)
            }
        }
    }

    def check_bytes = { except_bytes, export_label -> 
        def show_export_res = sql_return_maparray """ show export where label = "${export_label}"; """
        def export_job = show_export_res[0]
        def outfile_json = parseJson(export_job.OutfileInfo)
        assertEquals(except_bytes, outfile_json[0][0].fileSize)
    }

    // 1. exec export without bom
    def uuid = UUID.randomUUID().toString()
    def outFilePath = """${outfile_path_prefix}_${uuid}"""
    def label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "${file_format}"
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
        
        order_qt_select_load1 """ select * from s3(
                                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${file_format}",
                                    "s3.access_key"= "${ak}",
                                    "s3.secret_key" = "${sk}",
                                    "format" = "csv",
                                    "provider" = "${getS3Provider()}",
                                    "region" = "${region}"
                                ) ORDER BY c1;
                                """

        // check outfile bytes
        check_bytes("145bytes", label)
    } finally {
    }


    // 2. exec export with bom
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}_${uuid}"""
    label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "${file_format}",
                "with_bom" = "true"
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
        
        order_qt_select_load1 """ select * from s3(
                                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${file_format}",
                                    "s3.access_key"= "${ak}",
                                    "s3.secret_key" = "${sk}",
                                    "format" = "csv",
                                    "provider" = "${getS3Provider()}",
                                    "region" = "${region}"
                                ) ORDER BY c1;
                                """

        // check outfile bytes
        check_bytes("148bytes", label)
    } finally {
    }


    // 3. test csv_with_names with bom
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}_${uuid}"""
    label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv_with_names",
                "with_bom" = "true"
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
        
        order_qt_select_load1 """ select * from s3(
                                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${file_format}",
                                    "s3.access_key"= "${ak}",
                                    "s3.secret_key" = "${sk}",
                                    "format" = "csv_with_names",
                                    "provider" = "${getS3Provider()}",
                                    "region" = "${region}"
                                ) ORDER BY user_id;
                                """

        // check outfile bytes
        check_bytes("161bytes", label)
    } finally {
    }


    // 4. test csv_with_names_and_types with bom
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}_${uuid}"""
    label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv_with_names_and_types",
                "with_bom" = "true"
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
        
        order_qt_select_load1 """ select * from s3(
                                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${file_format}",
                                    "s3.access_key"= "${ak}",
                                    "s3.secret_key" = "${sk}",
                                    "format" = "csv_with_names_and_types",
                                    "provider" = "${getS3Provider()}",
                                    "region" = "${region}"
                                ) ORDER BY user_id;
                                """

        // check outfile bytes
        check_bytes("172bytes", label)
    } finally {
    }
}
