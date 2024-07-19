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

suite("test_export_data_types", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def table_export_name = "test_export_data_types"
    def outfile_path_prefix = """${bucket}/export/p0/all_data_types/exp"""
    def format = "csv"

    def create_table = {table_name -> 
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            `user_id` INT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `datev2` DATEV2 NOT NULL COMMENT "数据灌入日期时间2",
            `datetime` DATETIME NOT NULL COMMENT "数据灌入日期时间",
            `datetimev2_1` DATETIMEV2 NOT NULL COMMENT "数据灌入日期时间",
            `datetimev2_2` DATETIMEV2(3) NOT NULL COMMENT "数据灌入日期时间",
            `datetimev2_3` DATETIMEV2(6) NOT NULL COMMENT "数据灌入日期时间",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `street` STRING COMMENT "用户所在街道",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",
            `bool_col` boolean COMMENT "",
            `int_col` int COMMENT "",
            `bigint_col` bigint COMMENT "",
            `largeint_col` largeint COMMENT "",
            `float_col` float COMMENT "",
            `double_col` double COMMENT "",
            `char_col` CHAR(10) COMMENT "",
            `decimal_col` decimal COMMENT "",
            `decimalv3_col` decimalv3 COMMENT "",
            `decimalv3_col2` decimalv3(1,0) COMMENT "",
            `decimalv3_col3` decimalv3(1,1) COMMENT "",
            `decimalv3_col4` decimalv3(9,8) COMMENT "",
            `decimalv3_col5` decimalv3(20,10) COMMENT "",
            `decimalv3_col6` decimalv3(38,0) COMMENT "",
            `decimalv3_col7` decimalv3(38,37) COMMENT "",
            `decimalv3_col8` decimalv3(38,38) COMMENT "",
            `ipv4_col` ipv4 COMMENT "",
            `ipv6_col` ipv6 COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
    }

    create_table(table_export_name);

    StringBuilder sb = new StringBuilder()
    int i = 1
    sb.append("""
            (${i}, '2023-04-20', '2023-04-20', '2023-04-20 00:00:00', '2023-04-20 00:00:00', '2023-04-20 00:00:00', '2023-04-20 00:00:00',
            'Beijing', 'Haidian',
            ${i}, ${i % 128}, true, ${i}, ${i}, ${i}, ${i}.${i}, ${i}.${i}, 'char${i}',
            ${i}, ${i}, ${i}, 0.${i}, ${i}, ${i}, ${i}, ${i}, 0.${i}, '0.0.0.${i}', '::${i}'),
        """)

    sb.append("""
            (${++i}, '9999-12-31', '9999-12-31', '9999-12-31 23:59:59', '9999-12-31 23:59:59', '2023-04-20 00:00:00.12', '2023-04-20 00:00:00.3344',
            '', 'Haidian',
            ${Short.MIN_VALUE}, ${Byte.MIN_VALUE}, true, ${Integer.MIN_VALUE}, ${Long.MIN_VALUE}, -170141183460469231731687303715884105728, ${Float.MIN_VALUE}, ${Double.MIN_VALUE}, 'char${i}',
            100000000, 100000000, 4, 0.1, 0.99999999, 9999999999.9999999999, 99999999999999999999999999999999999999, 9.9999999999999999999999999999999999999, 0.99999999999999999999999999999999999999, '0.0.0.0', '::'),
        """)
    
    sb.append("""
            (${++i}, '2023-04-21', '2023-04-21', '2023-04-20 12:34:56', '2023-04-20 00:00:00', '2023-04-20 00:00:00.123', '2023-04-20 00:00:00.123456',
            'Beijing', '', 
            ${Short.MAX_VALUE}, ${Byte.MAX_VALUE}, true, ${Integer.MAX_VALUE}, ${Long.MAX_VALUE}, 170141183460469231731687303715884105727, ${Float.MAX_VALUE}, ${Double.MAX_VALUE}, 'char${i}',
            999999999, 999999999, 9, 0.9, 9.99999999, 1234567890.0123456789, 12345678901234567890123456789012345678, 1.2345678901234567890123456789012345678, 0.12345678901234567890123456789012345678, '255.255.255.255', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),
        """)

    sb.append("""
            (${++i}, '0000-01-01', '0000-01-01', '2023-04-20 00:00:00', '2023-04-20 00:00:00', '2023-04-20 00:00:00', '2023-04-20 00:00:00',
            'Beijing', 'Haidian',
            ${i}, ${i % 128}, true, ${i}, ${i}, ${i}, ${i}.${i}, ${i}.${i}, 'char${i}',
            ${i}, ${i}, ${i}, 0.${i}, ${i}, ${i}, ${i}, ${i}, 0.${i}, '0.0.0.${i}', '::${i}')
        """)

    
    sql """ INSERT INTO ${table_export_name} VALUES
            ${sb.toString()}
        """
   
    def insert_res = sql "show last insert;"
    logger.info("insert result: " + insert_res.toString())
    qt_select_export1 """ SELECT * FROM ${table_export_name} t ORDER BY user_id; """

    def waiting_export = { export_label ->
        while (true) {
            def res = sql """ show export where label = "${export_label}" """
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

    // 1. test csv
    def uuid = UUID.randomUUID().toString()
    def outFilePath = """${outfile_path_prefix}_${uuid}"""
    def label = "label_${uuid}"
    format = "csv"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "${format}"
            ) WITH S3(
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${getS3Provider()}"
            );
        """
        def outfile_url = waiting_export.call(label)
        
        qt_select_load1 """ SELECT * FROM s3(
                                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${format}",
                                    "s3.access_key"= "${ak}",
                                    "s3.secret_key" = "${sk}",
                                    "format" = "${format}",
                                    "provider" = "${getS3Provider()}",
                                    "region" = "${region}"
                                ) ORDER BY c1;
                            """
    } finally {
    }


    // 2. test parquet
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}_${uuid}"""
    label = "label_${uuid}"
    format = "parquet"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "${format}"
            ) WITH S3(
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${getS3Provider()}"
            );
        """
        def outfile_url = waiting_export.call(label)
        
        qt_select_load2 """ SELECT * FROM s3(
                                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${format}",
                                    "s3.access_key"= "${ak}",
                                    "s3.secret_key" = "${sk}",
                                    "format" = "${format}",
                                    "provider" = "${getS3Provider()}",
                                    "region" = "${region}"
                                ) ORDER BY user_id;
                            """
    } finally {
    }

    // 3. test orc
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}_${uuid}"""
    label = "label_${uuid}"
    format = "orc"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "${format}"
            ) WITH S3(
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${getS3Provider()}"
            );
        """
        def outfile_url = waiting_export.call(label)
        
        qt_select_load3 """ SELECT * FROM s3(
                                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${format}",
                                    "s3.access_key"= "${ak}",
                                    "s3.secret_key" = "${sk}",
                                    "format" = "${format}",
                                    "provider" = "${getS3Provider()}",
                                    "region" = "${region}"
                                ) ORDER BY user_id;
                            """
    } finally {
    }

    // 4. test csv_with_names
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}_${uuid}"""
    label = "label_${uuid}"
    format = "csv_with_names"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "${format}"
            ) WITH S3(
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${getS3Provider()}"
            );
        """
        def outfile_url = waiting_export.call(label)
        
        qt_select_load4 """ SELECT * FROM s3(
                                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.csv",
                                    "s3.access_key"= "${ak}",
                                    "s3.secret_key" = "${sk}",
                                    "format" = "${format}",
                                    "provider" = "${getS3Provider()}",
                                    "region" = "${region}"
                                ) ORDER BY user_id;
                            """
    } finally {
    }


    // 5. test csv_with_names_and_types
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}_${uuid}"""
    label = "label_${uuid}"
    format = "csv_with_names_and_types"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "${format}"
            ) WITH S3(
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${getS3Provider()}"
            );
        """
        def outfile_url = waiting_export.call(label)
        
        qt_select_load5 """ SELECT * FROM s3(
                                    "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.csv",
                                    "s3.access_key"= "${ak}",
                                    "s3.secret_key" = "${sk}",
                                    "format" = "${format}",
                                    "provider" = "${getS3Provider()}",
                                    "region" = "${region}"
                                ) ORDER BY user_id;
                            """
    } finally {
    }

    try_sql("DROP TABLE IF EXISTS ${table_export_name}")
}
