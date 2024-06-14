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

suite("test_outfile_parquet_one_nested_type", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    def export_table_name = "outfile_parquet_one_nested_type_export_test"
    def outFilePath = "${bucket}/outfile/parquet/nested_complex_type/exp_"
    def outfile_format = "parquet"

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
            FORMAT AS ${outfile_format}
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """

        return res[0][3]
    }

    // 1. test NULL STRUCT_STRUCT
    try {
        def struct_field_define = "`ss_info` STRUCT<s_info:STRUCT<s_id:int(11), s_name:string, s_address:string>> NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {{1, 'sn1', 'sa1'}}); """
        sql """ insert into ${export_table_name} values (2, 'doris2', {{2, 'sn2', 'sa2'}}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', null); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {{5, 'sn5', 'sa5'}}); """
        sql """ insert into ${export_table_name} values (6, 'doris6', {{6, 'sn6', 'sa6'}}); """
        sql """ insert into ${export_table_name} values (7, null, {{7, 'sn7', 'sa7'}}); """
        sql """ insert into ${export_table_name} values (8, null, null); """


        // test base data
        qt_select_base1 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load1 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${outfile_format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${outfile_format}",
                "region" = "${region}"
            );
            """
    } finally {
    }

    // 2. test NOT NULL STRUCT_STRUCT
    try {
        def struct_field_define = "`ss_info` STRUCT<s_info:STRUCT<s_id:int(11), s_name:string, s_address:string>> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {{1, 'sn1', 'sa1'}}); """
        sql """ insert into ${export_table_name} values (2, 'doris2', {{2, 'sn2', 'sa2'}}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {{5, 'sn5', 'sa5'}}); """
        sql """ insert into ${export_table_name} values (6, 'doris6', {{6, 'sn6', 'sa6'}}); """
        sql """ insert into ${export_table_name} values (7, null, {{7, 'sn7', 'sa7'}}); """

        // test base data
        qt_select_base2 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load2 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${outfile_format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${outfile_format}",
                "region" = "${region}"
            );
            """
    } finally {
    }

    // 3. test NULL STRUCT_ARRAY
    try {
        def struct_field_define = "`ss_info` STRUCT<l_info:ARRAY<STRING>> NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {['doris1', 'nereids1', 'doris-nereids-1']}); """
        // sql """ insert into ${export_table_name} values (2, 'doris2', {[]}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', null); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {['doris2', null, 'nereids2']}); """
        // sql """ insert into ${export_table_name} values (6, 'doris6', {[null, null, null]}); """
        sql """ insert into ${export_table_name} values (7, null, {[null, 'null', 'doris3']}); """
        sql """ insert into ${export_table_name} values (8, null, null); """
        sql """ insert into ${export_table_name} values (9, null, {['sn7', 'sa7', 'sn8', 'sa8', 'sn9', 'sa9', 'sn10', 'sa10']}); """


        // test base data
        qt_select_base3 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load3 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${outfile_format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${outfile_format}",
                "region" = "${region}"
            );
            """
    } finally {
    }

    // 4. test NULL STRUCT_MAP
    try {
        def struct_field_define = "`ss_info` STRUCT<m_info:MAP<STRING, LARGEINT>> NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {{'a': 100, 'b': 111}}); """
        sql """ insert into ${export_table_name} values (2, 'doris2', {{'a': 200, 'b': 222}}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', null); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {{'a': null, 'b': 333, 'c':399, 'd':399999999999999}}); """
        sql """ insert into ${export_table_name} values (6, 'doris6', {{'null': 100, 'b': null}}); """
        sql """ insert into ${export_table_name} values (7, null, {{'null': null, 'null':null}}); """
        sql """ insert into ${export_table_name} values (8, null, null); """


        // test base data
        qt_select_base4 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load4 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${outfile_format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${outfile_format}",
                "region" = "${region}"
            );
            """
    } finally {
    }

    // 5. test NULL ARRAY_STRUCT
    try {
        def struct_field_define = "`ss_info` ARRAY<STRUCT<i_info:INT, s_info:STRING>> NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [{1, 'doris1'}, {2, 'nereids1'}, {3, 'doris-nereids-1'}]); """ 
        sql """ insert into ${export_table_name} values (2, 'doris2', [{4, 'doris-nereids-4'}]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (4, 'doris4', [null, null , {5, 'doris-nereids-5'}]); """
        sql """ insert into ${export_table_name} values (5, 'doris5', [{6, 'doris7'}, null, null]); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (7, null, null); """
        sql """ insert into ${export_table_name} values (8, null, [{8, 'doris8'}]); """
        sql """ insert into ${export_table_name} values (9, null, [{9, 'doris9'}, {10, 'doris10'}]); """


        // test base data
        qt_select_base5 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load5 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${outfile_format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${outfile_format}",
                "region" = "${region}"
            );
            """
    } finally {
    }
    // 6. test NOT NULL ARRAY_STRUCT
    try {
        def struct_field_define = "`ss_info` ARRAY<STRUCT<i_info:INT, s_info:STRING>> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [{1, 'doris1'}, {2, 'nereids1'}, {3, 'doris-nereids-1'}]); """ 
        sql """ insert into ${export_table_name} values (2, 'doris2', [{4, 'doris-nereids-4'}]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (4, 'doris4', [null, null , {5, 'doris-nereids-5'}]); """
        sql """ insert into ${export_table_name} values (5, 'doris5', [{6, 'doris7'}, null, null]); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, null]); """
        sql """ insert into ${export_table_name} values (8, null, [{8, 'doris8'}]); """
        sql """ insert into ${export_table_name} values (9, null, [{9, 'doris9'}, {10, 'doris10'}]); """


        // test base data
        qt_select_base6 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load6 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${outfile_format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${outfile_format}",
                "region" = "${region}"
            );
            """
    } finally {
    }
    // 7. test NULL ARRAY_ARRAY
    try {
        def struct_field_define = "`ss_info` ARRAY<ARRAY<string>> NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [["doris", "null"], ["basdakljdoaidsjowqd", "asdqqqqsdafdorisdoris"], ["a", "b", "c"]]); """ 
        sql """ insert into ${export_table_name} values (2, 'doris2', [["null"]]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (4, 'doris4', [["xx", "yy", "nereids"], ["doris", "null"]]); """
        sql """ insert into ${export_table_name} values (5, 'doris5', null); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, ["xx", null, "nereids"], null]); """
        sql """ insert into ${export_table_name} values (7, null, null); """
        sql """ insert into ${export_table_name} values (8, null, [[], ["abc"], [], null]); """


        // test base data
        qt_select_base7 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load7 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${outfile_format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${outfile_format}",
                "region" = "${region}"
            );
            """
    } finally {
    }
    // 8. test NULL ARRAY_MAP
    try {
        def struct_field_define = "`ss_info` ARRAY<MAP<STRING, LARGEINT>> NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', [{'a': 100, 'b': 111}, {'a': 200, 'b': 222}, {'a': null, 'b': 333, 'c':399, 'd':399999999999999}]); """ 
        sql """ insert into ${export_table_name} values (2, 'doris2', [{'doris': 200, 'nereids': 222}]); """
        sql """ insert into ${export_table_name} values (3, 'doris3', []); """
        sql """ insert into ${export_table_name} values (4, 'doris4', [null, null, {'a': 100, 'b': 111}]); """
        sql """ insert into ${export_table_name} values (5, 'doris5', null); """
        sql """ insert into ${export_table_name} values (6, 'doris6', [null, null, {'nereids': 222}, null]); """
        sql """ insert into ${export_table_name} values (7, null, null); """
        sql """ insert into ${export_table_name} values (8, null, [null, null]); """
        sql """ insert into ${export_table_name} values (9, null, [{'doris': 200, 'nereids': 222}, null, null]); """


        // test base data
        qt_select_base8 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load8 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${outfile_format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${outfile_format}",
                "region" = "${region}"
            );
            """
    } finally {
    }

    // 9. test NULL MAP_STRUCT
    try {
        def struct_field_define = "`ms_info` MAP<STRING, STRUCT<s_info:string, l_info:LARGEINT>> NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'a': {'doris', 18}, 'b':{'nereids', 20}, 'c':{'nereids', 21}}); """
        sql """ insert into ${export_table_name} values (2, 'doris2', {'xx': null, 'a': {'doris', 18}}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {'dd': {null, null}}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', null); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {'doris-nereids': {'nereids', null}, 'yyzz': {null, 999999}}); """
        sql """ insert into ${export_table_name} values (7, null, {'null': null, 'null':null}); """
        sql """ insert into ${export_table_name} values (8, null, null); """

        // test base data
        qt_select_base9 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load9 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${outfile_format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${outfile_format}",
                "region" = "${region}"
            );
            """
    } finally {
    }
    // 10. test NOT NULL MAP_STRUCT
    try {
        def struct_field_define = "`ms_info` MAP<STRING, STRUCT<s_info:string, l_info:LARGEINT>> NOT NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'a': {'doris', 18}, 'b':{'nereids', 20}, 'c':{'nereids', 21}}); """
        sql """ insert into ${export_table_name} values (2, 'doris2', {'xx': null, 'a': {'doris', 18}}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {'dd': {null, null}}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {'doris-nereids': {'nereids', null}, 'yyzz': {null, 999999}}); """
        sql """ insert into ${export_table_name} values (7, null, {'null': null, 'null':null}); """

        // test base data
        qt_select_base10 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load10 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${outfile_format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${outfile_format}",
                "region" = "${region}"
            );
            """
    } finally {
    }
    // 11. test NULL MAP_MAP
    try {
        def struct_field_define = "`ms_info` MAP<STRING, MAP<STRING, LARGEINT>> NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'a': {'a': 100, 'b': 111}, 'b': {'a': 200, 'b': 222, 'c': 333}, 'c':{'nereids': 99}}); """
        sql """ insert into ${export_table_name} values (2, 'doris2', {'xx': null, 'yy': {'a': null, 'b': 333, 'c':399, 'd':399999999999999}}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {'dd': null, 'qq': null, 'ww': {'null': 100, 'b': null}}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', null); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {'doris-nereids': {'nereids': null}, 'yyzz': {'xx': null, 'doris': 999999}}); """
        sql """ insert into ${export_table_name} values (7, null, {'doris': {'null': null, 'null':null}, 'nereids': {'a': 200, 'b': 222, 'c': 333}}); """
        sql """ insert into ${export_table_name} values (8, null, null); """

        // test base data
        qt_select_base11 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load11 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${outfile_format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${outfile_format}",
                "region" = "${region}"
            );
            """
    } finally {
    }

    // 12. test NULL MAP_LIST
    try {
        def struct_field_define = "`ms_info` MAP<STRING, ARRAY<STRING>> NULL"
        // create table to export data
        create_table(export_table_name, struct_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'a': ['doris', 'nereids', 'zzz'], 'b': ['a', 'b', 'c'], 'c': ['qwe', 'doris-nereids', 'pppppppp']}); """
        sql """ insert into ${export_table_name} values (2, 'doris2', {'a': ['doris', null, 'zzz'], 'b': ['a', 'b', null], 'c': [null, 'doris-nereids', 'pppppppp']}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {'a': [null, null, 'zzz'], 'b': [null, null], 'c': [], 'd': null, 'e': [null]}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', null); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {'a': null, 'b': [], 'null': [null, 'doris-nereids', 'pppppppp']}); """
        sql """ insert into ${export_table_name} values (6, null, null); """

        // test base data
        qt_select_base12 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load12 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.${outfile_format}",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "${outfile_format}",
                "region" = "${region}"
            );
            """
    } finally {
    }
}
