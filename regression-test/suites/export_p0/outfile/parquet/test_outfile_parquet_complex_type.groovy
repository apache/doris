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

suite("test_outfile_parquet_complex_type", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def export_table_name = "outfile_parquet_complex_type_export_test"
    def load_table_name = "outfile_parquet_complex_type_load_test"
    def outFilePath = "${bucket}/outfile/parquet/complex_type/exp_"


    def create_table = {table_name, struct_field ->
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `name` STRING COMMENT "用户年龄",
            ${struct_field}
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
    }

    def outfile_to_S3 = {
        // select ... into outfile ...
        def res = sql """
            SELECT * FROM ${export_table_name} t ORDER BY user_id
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS parquet
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """

        return res[0][3]
    }


    // 1. struct NULL type
    try {

        def struct_field_define = "`s_info` STRUCT<s_id:int(11), s_name:string, s_address:string> NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {1, 'sn1', 'sa1'}); """
        sql """ insert into ${export_table_name} values (2, 'doris2', struct(2, 'sn2', 'sa2')); """
        sql """ insert into ${export_table_name} values (3, 'doris3', named_struct('s_id', 3, 's_name', 'sn3', 's_address', 'sa3')); """
        sql """ insert into ${export_table_name} values (4, 'doris4', null); """
        sql """ insert into ${export_table_name} values (5, 'doris5', struct(5, null, 'sa5')); """
        sql """ insert into ${export_table_name} values (6, 'doris6', struct(null, null, null)); """
        sql """ insert into ${export_table_name} values (7, null, struct(null, null, null)); """
        sql """ insert into ${export_table_name} values (8, null, null); """

        // test base data
        qt_select_base """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        // test outfile to s3
        def outfile_url = outfile_to_S3()

        qt_select_load1 """ SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "parquet",
                            "region" = "${region}"
                        );
                        """

    } finally {
    }


    // 2. struct NOT NULL type
    try {
        def struct_field_define = "`s_info` STRUCT<s_id:int(11), s_name:string, s_address:string> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {1, 'sn1', 'sa1'}); """
        sql """ insert into ${export_table_name} values (2, 'doris2', struct(2, 'sn2', 'sa2')); """
        sql """ insert into ${export_table_name} values (3, 'doris3', named_struct('s_id', 3, 's_name', 'sn3', 's_address', 'sa3')); """
        sql """ insert into ${export_table_name} values (5, 'doris5', struct(5, null, 'sa5')); """
        sql """ insert into ${export_table_name} values (6, 'doris6', struct(null, null, null)); """
        sql """ insert into ${export_table_name} values (7, null, struct(null, null, null)); """

        // test base data
        qt_select_base2 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load2 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "parquet",
                "region" = "${region}"
            );
            """
    } finally {
    }

    // 3. test NULL Map
    try {
        def struct_field_define = "`m_info` Map<STRING, LARGEINT> NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'a': 100, 'b': 111}), (2, 'doris2', {'a': 200, 'b': 222}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {'a': null, 'b': 333, 'c':399, 'd':399999999999999}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {'null': null, 'null':null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {'null': 100, 'b': null}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """

        // test base data
        qt_select_base3 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load3 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "parquet",
                "region" = "${region}"
            );
            """
    } finally {
    }

    // 4. test NOT NULL Map
    try {
        def struct_field_define = "`m_info` Map<STRING, LARGEINT> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'a': 100, 'b': 111}), (2, 'doris2', {'a': 200, 'b': 222}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {'a': null, 'b': 333, 'c':399, 'd':399999999999999}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {'null': null, 'null':null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {'null': 100, 'b': null}); """

        // test base data
        qt_select_base4 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load4 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "parquet",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 5. test NULL ARRAY
    try {
        def struct_field_define = "`a_info` ARRAY<int> NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [9, 99, 999]), (2, 'doris2', [8, 88]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (4, 'doris4', null); """
        sql """ insert into ${export_table_name} values (5, 'doris5', [1, null, 2]); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, null, null, 1, 2, 999999, 111111]); """
        sql """ insert into ${export_table_name} values (8, 'doris8', null); """


        // test base data
        qt_select_base5 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load5 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "parquet",
                "region" = "${region}"
            );
            """
    } finally {
    }

    // 6. test NOT NULL ARRAY
    try {
        def struct_field_define = "`a_info` ARRAY<int> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [9, 99, 999]), (2, 'doris2', [8, 88]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (5, 'doris5', [1, null, 2]); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, 'doris7', [null, null, null, 1, 2, 999999, 111111]); """

        // test base data
        qt_select_base6 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load6 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "parquet",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 7. test struct with all type
    try {

        def struct_field_define = "`s_info` STRUCT<user_id:INT, date:DATE, datetime:DATETIME, city:VARCHAR(20), age:SMALLINT, sex:TINYINT, bool_col:BOOLEAN, int_col:INT, bigint_col:BIGINT, largeint_col:LARGEINT, float_col:FLOAT, double_col:DOUBLE, char_col:CHAR(10), decimal_col:DECIMAL> NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)
        // create table to load data
        create_table(load_table_name, struct_field_define)

        // insert data
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10; i ++) {
            sb.append("""
                (${i}, 'doris_${i}', {${i}, '2017-10-01', '2017-10-01 00:00:00', 'Beijing', ${i}, ${i % 128}, true, ${i}, ${i}, ${i}, ${i}.${i}, ${i}.${i}, 'char${i}_1234', ${i}}),
            """)
        }
        sb.append("""
            (${i}, 'doris_${i}', {${i}, '2017-10-01', '2017-10-01 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL})
        """)

        sql """ INSERT INTO ${export_table_name} VALUES ${sb.toString()} """

        // test base data
        qt_select_base7 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        // test outfile to s3
        def outfile_url = outfile_to_S3()

        qt_select_load7 """ SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "parquet",
                            "region" = "${region}"
                        );
                        """

    } finally {
    }

}
