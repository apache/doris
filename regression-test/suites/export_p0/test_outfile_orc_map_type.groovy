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

suite("test_outfile_orc_map_type", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    def export_table_name = "outfile_orc_map_type_export_test"
    def load_table_name = "outfile_orc_map_type_load_test"
    def outFilePath = "${bucket}/outfile/orc/map_type/exp_"


    def create_table = {table_name, map_field ->
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `name` STRING COMMENT "用户年龄",
            ${map_field}
            )
            DISTRIBUTED BY HASH(user_id)
            PROPERTIES("replication_num" = "1");
        """
    }

    def outfile_to_S3 = {
        // select ... into outfile ...
        def res = sql """
            SELECT * FROM ${export_table_name} t ORDER BY user_id
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS ORC
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """

        return res[0][3]
    }


    // 1. test map<STRING, LARGEINT> NULL
    try {
        def map_field_define = "`m_info` Map<STRING, LARGEINT> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'a': 100, 'b': 111}), (2, 'doris2', {'a': 200, 'b': 222}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {'a': null, 'b': 333, 'c':399, 'd':399999999999999}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {null: 100, 'b': null}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """


        // test base data
        qt_select_base1 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load1 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 2. test map<LARGEINT, STRING> NULL
    try {
        def map_field_define = "`m_info` Map<LARGEINT, STRING> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {100: 'null', 111:'b'}), (2, 'doris2', {200:'a', 222:'b'}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null: 'a', 333:'b', 399:'c', 399999999999999:'d'}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {null: '100', null:'b'}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {-170141183460469231731687303715884105728: 'min_largeint', 170141183460469231731687303715884105727: 'max_largeint'}); """
        sql """ insert into ${export_table_name} values (9, 'doris9', {-170141183460469231731687303715884105728: 'min_largeint', 111:'b'}); """
        sql """ insert into ${export_table_name} values (10, 'doris10', {200:'a', 170141183460469231731687303715884105727: 'max_largeint', 111:'b'}); """

        // test base data
        qt_select_base2 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load2 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 3. test map<INT, DECIMAL(15,5)> NULL
    try {
        def map_field_define = "`m_info` Map<INT, DECIMAL(15,5)> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {100: 0.123, 111:1.2345}), (2, 'doris2', {200:8738931.12312, 222:999.999}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null: 1111034.123, 333:7771.1231, 399:0.441241, 39999:0.441241}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {null: 1111034.123, null:8738931.12312}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {${Integer.MIN_VALUE}: 1.2345, ${Integer.MAX_VALUE}: 999.999}); """
        sql """ insert into ${export_table_name} values (9, 'doris9', {${Integer.MIN_VALUE}: 1111034.123}); """
        sql """ insert into ${export_table_name} values (10, 'doris10', {${Integer.MAX_VALUE}: 123456789.12345}); """

        // test base data
        qt_select_base3 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load3 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 4. test map<INT, DOUBLE> NULL
    try {
        def map_field_define = "`m_info` Map<INT, DOUBLE> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {100: ${Double.MIN_VALUE}, 111:${Double.MAX_VALUE}}), (2, 'doris2', {200: 123.123, 222:0.9999999}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null: 187.123, 333:555.6767, 399:129312.113, 3999:123.12314}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {null: 187.123, null:187.123}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {${Integer.MIN_VALUE}: ${Double.MIN_VALUE}, ${Integer.MAX_VALUE}: ${Double.MAX_VALUE}}); """
        sql """ insert into ${export_table_name} values (9, 'doris9', {${Integer.MAX_VALUE}: ${Double.MIN_VALUE}, ${Integer.MIN_VALUE}: ${Double.MAX_VALUE}}); """

        // test base data
        qt_select_base4 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load4 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 5. test map<STRING, DECIMAL(15,5)> NULL
    try {
        def map_field_define = "`m_info` Map<STRING, DECIMAL(15,5)> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'k1': 0.123, '111':1.2345}), (2, 'doris2', {'200':8738931.12312, 'doris':999.999}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null: 1111034.123, '333':7771.1231, '399':0.441241, '3999999999':0.441241}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {'null': 1111034.123, null:8738931.12312}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {'${Integer.MIN_VALUE}': 1.2345, '${Integer.MAX_VALUE}': 999.999}); """
        sql """ insert into ${export_table_name} values (9, 'doris9', {'${Integer.MIN_VALUE}': 1111034.123}); """
        sql """ insert into ${export_table_name} values (10, 'doris10', {'${Integer.MAX_VALUE}': 123456789.12345}); """

        
        // test base data
        qt_select_base5 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load5 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 6. test map<STRING, DOUBLE> NULL
    try {
        def map_field_define = "`m_info` Map<STRING, DOUBLE> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'100': ${Double.MIN_VALUE}, 'doris':${Double.MAX_VALUE}}), (2, 'doris2', {'nereids': 123.123, '222':0.9999999}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null: 187.123, '333':555.6767, '399':129312.113, '39999999999':123.12314}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {null: 187.123, 'null':187.123}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {'${Integer.MIN_VALUE}': ${Double.MIN_VALUE}, '${Integer.MAX_VALUE}': ${Double.MAX_VALUE}}); """
        sql """ insert into ${export_table_name} values (9, 'doris9', {'${Integer.MAX_VALUE}': ${Double.MIN_VALUE}, '${Integer.MIN_VALUE}': ${Double.MAX_VALUE}}); """

        // test base data
        qt_select_base6 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load6 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 7. test map<STRING, BIGINT> NULL
    try {
        def map_field_define = "`m_info` Map<STRING, BIGINT> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'a': 100, 'b': 111}), (2, 'doris2', {'a': 200, 'b': 222}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {'a': null, 'b': 333, 'c':399, 'd':399999999999999}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {null: 100, 'b': null}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {'max_bigint': ${Long.MAX_VALUE}, 'min_bigint': ${Long.MIN_VALUE}}); """

        // test base data
        qt_select_base7 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load7 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 8. test map<STRING, BOOLEAN> NULL
    try {
        def map_field_define = "`m_info` Map<STRING, BOOLEAN> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'a': true, 'b': false}), (2, 'doris2', {'a': false, 'b': false}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {'a': true, 'b': null, 'c':399, 'd':false}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {null: false, 'b': true}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {'true': true, 'false': false}); """

        // test base data
        qt_select_base8 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load8 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 9. test map<INT, BOOLEAN> NULL
    try {
        def map_field_define = "`m_info` Map<INT, BOOLEAN> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {100: true, 111:true}), (2, 'doris2', {200: false, 222:false}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null: true, 333:false, 399:false, 3999:true}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {null: true, null:true}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {${Integer.MIN_VALUE}: false, ${Integer.MAX_VALUE}: false}); """
        sql """ insert into ${export_table_name} values (9, 'doris9', {${Integer.MAX_VALUE}: true, ${Integer.MIN_VALUE}: true}); """

        // test base data
        qt_select_base9 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load9 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 10. test map<DATETIME, STRING> NULL
    try {
        def map_field_define = "`m_info` Map<DATETIME, STRING> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'2023-04-20 01:02:03': 'null', '2018-04-20 10:40:35':'b'}), (2, 'doris2', {'2000-04-20 00:00:00':'a', '1967-12-31 12:24:56':'b'}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null: 'a', '2023-01-01 00:00:00':'b', '2023-02-27 00:01:02':'d'}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {null: '100', null:'b'}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {'2025-12-31 12:01:41': 'min_largeint', '2006-02-19 09:01:02': 'max_largeint'}); """
        sql """ insert into ${export_table_name} values (9, 'doris9', {'209-04-20 00:00:00': 'min_largeint', '102-03-21 00:00:00':'b'}); """
        sql """ insert into ${export_table_name} values (10, 'doris10', {'2003-04-29 01:02:03':'a', '2006-02-22 02:01:04': 'max_largeint', '2020-03-21 19:21:23':'b'}); """

        // test base data
        qt_select_base10 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load10 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 11. test map<DATETIME, INT> NULL
    try {
        def map_field_define = "`m_info` Map<DATETIME, INT> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'2023-04-20 01:02:03': null, '2018-04-20 10:40:35': 123}), (2, 'doris2', {'2000-04-20 00:00:00':${Integer.MIN_VALUE}, '1967-12-31 12:24:56':${Integer.MAX_VALUE}}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null: 4574, '2023-01-01 00:00:00':1246, '2023-02-27 00:01:02':5646}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {null: 87676, null:234}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {'2025-12-31 12:01:41': 524524, '2006-02-19 09:01:02': 2534}); """

        // test base data
        qt_select_base11 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load11 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 12. test map<DATE, INT> NULL
    try {
        def map_field_define = "`m_info` Map<DATE, INT> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'2023-04-20': null, '2018-04-20': 123}), (2, 'doris2', {'2000-04-20':${Integer.MIN_VALUE}, '1967-12-31':${Integer.MAX_VALUE}}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null: 4574, '2023-01-01':1246, '2023-02-27':5646}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {null: 87676, null:234}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {'2025-12-31': 524524, '2006-02-19': 2534}); """

        // test base data
        qt_select_base12 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load12 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 13. test map<DATE, STRING> NULL
    try {
        def map_field_define = "`m_info` Map<DATE, STRING> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'2023-04-20': 'null', '2018-04-20': null}), (2, 'doris2', {'2000-04-20':'${Integer.MIN_VALUE}', '1967-12-31':'${Integer.MAX_VALUE}'}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null: '4574', '2023-01-01':'1246', '2023-02-27':'5646'}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {null: 'doris', null:'nereids'}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {'2025-12-31': 'min_largeint', '2006-02-19': 'max_largeint'}); """

        // test base data
        qt_select_base13 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load13 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 14. test map<DATETIME, STRING> NULL
    try {
        def map_field_define = "`m_info` Map<DATETIME, STRING> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'2023-04-20 12:20:03': 'null', '2018-04-20 12:59:59': null}), (2, 'doris2', {'2000-04-20 23:59:59':'${Integer.MIN_VALUE}', '1967-12-31 00:00:00':'${Integer.MAX_VALUE}'}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null: '4574', '2023-01-01 07:24:54':'1246', '2023-02-27 15:12:13':'5646'}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {null: 'doris', null:'nereids'}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {'2025-12-31 11:22:33': 'min_largeint', '2006-02-19 00:44:55': 'max_largeint'}); """

        // test base data
        qt_select_base14 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load14 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 15. test map<BIGINT, VARCHAR(20)> NULL
    try {
        def map_field_define = "`m_info` Map<BIGINT, VARCHAR(20)> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {100: 'null', 111:'b'}), (2, 'doris2', {200:'a', 222:'b'}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null: 'a', 333:'b', 399:'c', 399999999999999:'d'}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {null: '100', null:'b'}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {${Long.MIN_VALUE}: 'min_bigint', ${Long.MAX_VALUE}: 'max_bigint'}); """
        sql """ insert into ${export_table_name} values (9, 'doris9', {${Long.MAX_VALUE}: 'min_bigint', 111:'b'}); """
        sql """ insert into ${export_table_name} values (10, 'doris10', {200:'a', ${Long.MAX_VALUE}: 'max_bigint', 111:'b'}); """

        // test base data
        qt_select_base15 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load15 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 16. test map<BOOLEAN, VARCHAR(20)> NULL
    try {
        def map_field_define = "`m_info` Map<BOOLEAN, VARCHAR(20)> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, "doris1", {true:"null",false:"b"}), (2, "doris2", {true:"a", true:"b"}); """
        sql """ insert into ${export_table_name} values (3, "doris3", {null: "a", true:"b", false:"c", false:"d"}); """
        sql """ insert into ${export_table_name} values (4, "doris4", {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, "doris5", {null: "100", null:"b"}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, "doris7", null); """
        sql """ insert into ${export_table_name} values (8, "doris8", {false: "min_bigint", false: "max_bigint"}); """
        sql """ insert into ${export_table_name} values (9, "doris9", {true: "min_bigint", false:"b"}); """
        sql """ insert into ${export_table_name} values (10, "doris10", {false:"a", true: "max_bigint", true:"b"}); """

        // test base data
        qt_select_base16 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load16 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }

    // 17. test map<BOOLEAN, STRING> NULL
    try {
        def map_field_define = "`m_info` Map<BOOLEAN, STRING> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)


        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {1: 'xxx', 0:'b'}), (2, 'doris2', {1:'a', 1:'b'}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null: 'a', 1:'b', 0:'c', 0:'d'}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {null: '100', null:'b'}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {0: 'min_bigint', 0: 'max_bigint'}); """
        sql """ insert into ${export_table_name} values (9, 'doris9', {1: 'min_bigint', 0:'b'}); """
        sql """ insert into ${export_table_name} values (10, 'doris10', {0:'a', 1: 'max_bigint', 1:'b'}); """

        // test base data
        qt_select_base17 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load17 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }


    // 18. test map<STRING, STRING> NULL
    try {
        def map_field_define = "`m_info` Map<STRING, STRING> NULL"
        // create table to export data
        create_table(export_table_name, map_field_define)
        // create table to load data
        create_table(load_table_name, map_field_define)

        // insert data
        sql """ insert into ${export_table_name} values (1, 'doris1', {'doris': 'null', 'nereids':'b'}), (2, 'doris2', {'ftw':'a', 'cyx':'b'}); """
        sql """ insert into ${export_table_name} values (3, 'doris3', {null: 'a', '333':'b', '399':'c', '399999999999999':'d'}); """
        sql """ insert into ${export_table_name} values (4, 'doris4', {null: null, null:null}); """
        sql """ insert into ${export_table_name} values (5, 'doris5', {'null': '100', null:'b'}); """
        sql """ insert into ${export_table_name} values (6, null, null); """
        sql """ insert into ${export_table_name} values (7, 'doris7', null); """
        sql """ insert into ${export_table_name} values (8, 'doris8', {'170141183460469231731687303715884105728': 'min_largeint', '170141183460469231731687303715884105727': 'max_largeint'}); """
        sql """ insert into ${export_table_name} values (9, 'doris9', {'170141183460469231731687303715884105728': 'min_largeint', '111':'b'}); """
        sql """ insert into ${export_table_name} values (10, 'doris10', {'200':'a', '170141183460469231731687303715884105727': 'max_largeint', '111':'b'}); """

        // test base data
        qt_select_base18 """ SELECT * FROM ${export_table_name} t ORDER BY user_id; """

        def outfile_url = outfile_to_S3()

        qt_select_load18 """ SELECT * FROM S3 (
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.orc",
                "ACCESS_KEY"= "${ak}",
                "SECRET_KEY" = "${sk}",
                "format" = "orc",
                "region" = "${region}"
            );
            """
    } finally {
    }

}
