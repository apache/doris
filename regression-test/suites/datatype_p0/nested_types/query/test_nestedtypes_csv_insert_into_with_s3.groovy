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


import com.google.common.collect.Lists
import org.apache.commons.lang3.StringUtils
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_nestedtypes_csv_insert_into_with_s3", "p0") {
    sql 'use regression_test_datatype_p0_nested_types'
    sql 'set enable_nereids_planner=false'
    sql 'set max_allowed_packet=4194304'
    sql """ADMIN SET FRONTEND CONFIG ('disable_nested_complex_type' = 'false')"""

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def dataFilePath = "https://"+"${bucket}"+"."+"${s3_endpoint}"+"/regression/datalake"

    ArrayList<String> csv_as_json = ["${dataFilePath}/as_as_json.csv", "${dataFilePath}/arrarr_as_json.csv",
                                     "${dataFilePath}/map_as_json.csv","${dataFilePath}/arrmap_as_json.csv",
                                     "${dataFilePath}/maparr_as_json.csv"]


    // define dup key table with nested table types with one nested scala
    List<String> table_names = new ArrayList<>()
    // table_names should same with json_files order
    def nested_table_dup = "tbl_csv_array_nested_types_s3"
    def nested_table_dup2 = "tbl_csv_array_nested_types_s32"
    def nested_table_map_dup = "tbl_csv_map_types_s3"
    def nested_table_array_map_dup = "tbl_csv_array_map_types_s3"
    def nested_table_map_array_dup = "tbl_csv_map_array_types_s3"
    table_names.add(nested_table_dup)
    table_names.add(nested_table_dup2)
    table_names.add(nested_table_map_dup)
    table_names.add(nested_table_array_map_dup)
    table_names.add(nested_table_map_array_dup)

    sql "DROP TABLE IF EXISTS ${nested_table_dup}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_dup} (
            `k1` bigint(11) NULL,
            `c_bool` array<boolean> NULL,
            `c_tinyint` array<tinyint(4)> NULL,
            `c_smallint` array<smallint(6)> NULL,
            `c_int` array<int(11)> NULL,
            `c_bigint` array<bigint(20)> NULL,
            `c_largeint` array<largeint(40)> NULL,
            `c_float` array<float> NULL,
            `c_double` array<double> NULL,
            `c_decimal` array<decimal(20, 3)> NULL,
            `c_decimalv3` array<decimalv3(20, 3)> NULL,
            `c_date` array<date> NULL,
            `c_datetime` array<datetime> NULL,
            `c_datev2` array<datev2> NULL,
            `c_datetimev2` array<datetimev2(0)> NULL,
            `c_char` array<char(15)> NULL,
            `c_varchar` array<varchar(100)> NULL,
            `c_string` array<text> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """
    // define dup key table with nested table types with two nested scala
    sql "DROP TABLE IF EXISTS ${nested_table_dup2}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_dup2} (
            `k1` bigint(11) NULL,
            `c_bool` array<array<boolean>> NULL,
            `c_tinyint` array<array<tinyint(4)>> NULL,
            `c_smallint` array<array<smallint(6)>> NULL,
            `c_int` array<array<int(11)>> NULL,
            `c_bigint` array<array<bigint(20)>> NULL,
            `c_largeint` array<array<largeint(40)>> NULL,
            `c_float` array<array<float>> NULL,
            `c_double` array<array<double>> NULL,
            `c_decimal` array<array<decimal(20, 3)>> NULL,
            `c_decimalv3` array<array<decimalv3(20, 3)>> NULL,
            `c_date` array<array<date>> NULL,
            `c_datetime` array<array<datetime>> NULL,
            `c_datev2` array<array<datev2>> NULL,
            `c_datetimev2` array<array<datetimev2(0)>> NULL,
            `c_char` array<array<char(15)>> NULL,
            `c_varchar` array<array<varchar(100)>> NULL,
            `c_string` array<array<text>> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """
    
    // define dup key table with array nested map table types with one nested scala
    sql "DROP TABLE IF EXISTS ${nested_table_array_map_dup}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_array_map_dup} (
            `k1` bigint(11) NULL,
            `c_bool` array<map<boolean, boolean>> NULL,
            `c_tinyint` array<map<tinyint(4), tinyint(4)>> NULL,
            `c_smallint` array<map<smallint(6), smallint(6)>> NULL,
            `c_int` array<map<int(11), int(11)>> NULL,
            `c_bigint` array<map<bigint(20), bigint(20)>> NULL,
            `c_largeint` array<map<largeint(40), largeint(40)>> NULL,
            `c_float` array<map<float, float>> NULL,
            `c_double` array<map<double, double>> NULL,
            `c_decimal` array<map<decimal(20, 3), decimal(20, 3)>> NULL,
            `c_decimalv3` array<map<decimalv3(20, 3), decimalv3(20, 3)>> NULL,
            `c_date` array<map<date, date>> NULL,
            `c_datetime` array<map<datetime, datetime>> NULL,
            `c_datev2` array<map<datev2, datev2>> NULL,
            `c_datetimev2` array<map<datetimev2(0), datetimev2(0)>> NULL,
            `c_char` array<map<char(15), char(15)>> NULL,
            `c_varchar` array<map<varchar(100), varchar(100)>> NULL,
            `c_string` array<map<text, text>> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // define dup key table with map types with one nested scala
    sql "DROP TABLE IF EXISTS ${nested_table_map_dup}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_map_dup} (
            `k1` bigint(11) NULL,
            `c_bool` map<boolean, boolean> NULL,
            `c_tinyint` map<tinyint(4), tinyint(4)> NULL,
            `c_smallint` map<smallint(6), smallint(6)> NULL,
            `c_int` map<int(11), int(11)> NULL,
            `c_bigint` map<bigint(20), bigint(20)> NULL,
            `c_largeint` map<largeint(40), largeint(40)> NULL,
            `c_float` map<float, float> NULL,
            `c_double` map<double, double> NULL,
            `c_decimal` map<decimal(20, 3), decimal(20, 3)> NULL,
            `c_decimalv3` map<decimalv3(20, 3), decimalv3(20, 3)> NULL,
            `c_date` map<date, date> NULL,
            `c_datetime` map<datetime, datetime> NULL,
            `c_datev2` map<datev2, datev2> NULL,
            `c_datetimev2` map<datetimev2(0), datetimev2(0)> NULL,
            `c_char` map<char(15), char(15)> NULL,
            `c_varchar` map<varchar(100), varchar(100)> NULL,
            `c_string` map<text, text> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // define dup key table with map nested value array table types with one nested scala
    sql "DROP TABLE IF EXISTS ${nested_table_map_array_dup}"
    sql """
        CREATE TABLE IF NOT EXISTS ${nested_table_map_array_dup} (
            `k1` bigint(11) NULL,
            `c_bool` map<boolean, array<boolean>> NULL,
            `c_tinyint` map<tinyint(4), array<tinyint(4)>> NULL,
            `c_smallint` map<smallint(6), array<smallint(6)>> NULL,
            `c_int` map<int(11), array<int(11)>> NULL,
            `c_bigint` map<bigint(20), array<bigint(20)>> NULL,
            `c_largeint` map<largeint(40), array<largeint(40)>> NULL,
            `c_float` map<float, array<float>> NULL,
            `c_double` map<double, array<double>> NULL,
            `c_decimal` map<decimal(20, 3), array<decimal(20, 3)>> NULL,
            `c_decimalv3` map<decimalv3(20, 3), array<decimalv3(20, 3)>> NULL,
            `c_date` map<date, array<date>> NULL,
            `c_datetime` map<datetime, array<datetime>> NULL,
            `c_datev2` map<datev2, array<datev2>> NULL,
            `c_datetimev2` map<datetimev2(0), array<datetimev2(0)>> NULL,
            `c_char` map<char(15), array<char(15)>> NULL,
            `c_varchar` map<varchar(100), array<varchar(100)>> NULL,
            `c_string` map<text, array<text>> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // step1. select * from s3 with 0-100 items
    // step2. insert into doris table
    // step2. query and check
    for (int i = 0; i < 3; ++i) {
        qt_sql_arr_csv_as_json_s3 """
         select * from s3("uri" = "${csv_as_json[i]}",
                "s3.access_key"= "${ak}",
                "s3.secret_key" = "${sk}",
                "format" = "csv") order by c1 limit 1;
            """

        sql """
        insert into ${table_names[i]} select * from s3("uri" = "${csv_as_json[i]}",
                "s3.access_key"= "${ak}",
                "s3.secret_key" = "${sk}",
                "format" = "csv");
             """

        qt_sql_arr_csv_as_json_doris """ select * from ${table_names[i]} order by k1 limit 1; """
    }


    // now cast for map can not implicit cast type
    // array-map and map-array we only show some column for check otherwise will reach mysql max output bytes limit
    for (int i = 3; i < csv_as_json.size(); ++i) {
        qt_sql_arr_csv_as_json_s3 """
        select c2,c6,c11,c12,c13,c14,c15 from s3 (
            "uri" = "${csv_as_json[i]}",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "format" = "csv") order by c1 limit 1;
            """

        sql """
        insert into ${table_names[i]}
            select * from s3 (
                "uri" = "${csv_as_json[i]}",
                "s3.access_key"= "${ak}",
                "s3.secret_key" = "${sk}",
                "format" = "csv");"""

        qt_sql_arr_csv_as_json_doris """ select c_bool,c_bigint,c_decimalv3,c_date, c_datev2,c_datetime,c_datetimev2 from ${table_names[i]} order by k1 limit 1; """
    }

    qt_sql_arr """ select array_size(c_tinyint) from tbl_csv_array_nested_types_s3 order by k1; """
    qt_sql_arr """ select array_size(c_tinyint) from tbl_csv_array_nested_types_s32 order by k1; """
    qt_sql_arr """ select array_size(c_tinyint) from tbl_csv_array_map_types_s3 order by k1; """
    qt_sql_map_arr """ select array_size(map_values(c_tinyint)) from tbl_csv_map_array_types_s3 order by k1; """

}
