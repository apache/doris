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

suite("test_array_filter") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    // array functions only supported in vectorized engine
    sql """DROP TABLE IF EXISTS table_30_un_pa_ke_pr_di4 """
    sql """ CREATE TABLE IF NOT EXISTS table_30_un_pa_ke_pr_di4 (
              `pk` int NULL,
              `col_int_undef_signed_index_inverted` int NULL,
              `col_int_undef_signed` int NULL,
              `col_int_undef_signed_not_null` int NOT NULL,
              `col_int_undef_signed_not_null_index_inverted` int NOT NULL,
              `col_date_undef_signed` date NULL,
              `col_date_undef_signed_index_inverted` date NULL,
              `col_date_undef_signed_not_null` date NOT NULL,
              `col_date_undef_signed_not_null_index_inverted` date NOT NULL,
              `col_varchar_1024__undef_signed` varchar(1024) NULL,
              `col_varchar_1024__undef_signed_index_inverted` varchar(1024) NULL,
              `col_varchar_1024__undef_signed_index_inverted_p_e` varchar(1024) NULL,
              `col_varchar_1024__undef_signed_index_inverted_p_u` varchar(1024) NULL,
              `col_varchar_1024__undef_signed_not_null` varchar(1024) NOT NULL,
              `col_varchar_1024__undef_signed_not_null_index_inverted` varchar(1024) NOT NULL,
              `col_varchar_1024__undef_signed_not_null_index_inverted_p_e` varchar(1024) NOT NULL,
              `col_varchar_1024__undef_signed_not_null_index_inverted_p_u` varchar(1024) NOT NULL,
              `col_boolean_undef_signed` boolean NULL,
              `col_boolean_undef_signed_not_null` boolean NOT NULL,
              `col_decimal_18__6__undef_signed` decimal(18,6) NULL,
              `col_decimal_18__6__undef_signed_index_inverted` decimal(18,6) NULL,
              `col_decimal_18__6__undef_signed_not_null` decimal(18,6) NOT NULL,
              `col_decimal_18__6__undef_signed_not_null_index_inverted` decimal(18,6) NOT NULL,
              `col_datetime_3__undef_signed` datetime(3) NULL,
              `col_datetime_3__undef_signed_index_inverted` datetime(3) NULL,
              `col_datetime_3__undef_signed_not_null` datetime(3) NOT NULL,
              `col_datetime_3__undef_signed_not_null_index_inverted` datetime(3) NOT NULL,
              `col_datetime_6__undef_signed` datetime(6) NULL,
              `col_datetime_6__undef_signed_index_inverted` datetime(6) NULL,
              `col_datetime_6__undef_signed_not_null` datetime(6) NOT NULL,
              `col_datetime_6__undef_signed_not_null_index_inverted` datetime(6) NOT NULL,
              `col_array_boolean__undef_signed` array<boolean> NULL,
              `col_array_boolean__undef_signed_index_inverted` array<boolean> NULL,
              `col_array_boolean__undef_signed_not_null` array<boolean> NOT NULL,
              `col_array_boolean__undef_signed_not_null_index_inverted` array<boolean> NOT NULL,
              `col_array_tinyint__undef_signed` array<tinyint> NULL,
              `col_array_tinyint__undef_signed_index_inverted` array<tinyint> NULL,
              `col_array_tinyint__undef_signed_not_null` array<tinyint> NOT NULL,
              `col_array_tinyint__undef_signed_not_null_index_inverted` array<tinyint> NOT NULL,
              `col_array_smallint__undef_signed` array<smallint> NULL,
              `col_array_smallint__undef_signed_index_inverted` array<smallint> NULL,
              `col_array_smallint__undef_signed_not_null` array<smallint> NOT NULL,
              `col_array_smallint__undef_signed_not_null_index_inverted` array<smallint> NOT NULL,
              `col_array_int__undef_signed` array<int> NULL,
              `col_array_int__undef_signed_index_inverted` array<int> NULL,
              `col_array_int__undef_signed_not_null` array<int> NOT NULL,
              `col_array_int__undef_signed_not_null_index_inverted` array<int> NOT NULL,
              `col_array_bigint__undef_signed` array<bigint> NULL,
              `col_array_bigint__undef_signed_index_inverted` array<bigint> NULL,
              `col_array_bigint__undef_signed_not_null` array<bigint> NOT NULL,
              `col_array_bigint__undef_signed_not_null_index_inverted` array<bigint> NOT NULL,
              `col_array_largeint__undef_signed` array<largeint> NULL,
              `col_array_largeint__undef_signed_index_inverted` array<largeint> NULL,
              `col_array_largeint__undef_signed_not_null` array<largeint> NOT NULL,
              `col_array_largeint__undef_signed_not_null_index_inverted` array<largeint> NOT NULL,
              `col_array_decimal_37_12___undef_signed` array<decimal(37,12)> NULL,
              `col_array_decimal_37_12___undef_signed_index_inverted` array<decimal(37,12)> NULL,
              `col_array_decimal_37_12___undef_signed_not_null` array<decimal(37,12)> NOT NULL,
              `col_array_decimal_37_12___undef_signed_not_null_index_inverted` array<decimal(37,12)> NOT NULL,
              `col_array_decimal_8_4___undef_signed` array<decimal(8,4)> NULL,
              `col_array_decimal_8_4___undef_signed_index_inverted` array<decimal(8,4)> NULL,
              `col_array_decimal_8_4___undef_signed_not_null` array<decimal(8,4)> NOT NULL,
              `col_array_decimal_8_4___undef_signed_not_null_index_inverted` array<decimal(8,4)> NOT NULL,
              `col_array_decimal_9_0___undef_signed` array<decimal(9,0)> NULL,
              `col_array_decimal_9_0___undef_signed_index_inverted` array<decimal(9,0)> NULL,
              `col_array_decimal_9_0___undef_signed_not_null` array<decimal(9,0)> NOT NULL,
              `col_array_decimal_9_0___undef_signed_not_null_index_inverted` array<decimal(9,0)> NOT NULL,
              `col_array_char_255___undef_signed` array<character(255)> NULL,
              `col_array_char_255___undef_signed_index_inverted` array<character(255)> NULL,
              `col_array_char_255___undef_signed_not_null` array<character(255)> NOT NULL,
              `col_array_char_255___undef_signed_not_null_index_inverted` array<character(255)> NOT NULL,
              `col_array_varchar_65533___undef_signed` array<varchar(65533)> NULL,
              `col_array_varchar_65533___undef_signed_index_inverted` array<varchar(65533)> NULL,
              `col_array_varchar_65533___undef_signed_not_null` array<varchar(65533)> NOT NULL,
              `col_array_varchar_65533___undef_signed_not_null_index_inverted` array<varchar(65533)> NOT NULL,
              `col_array_string__undef_signed` array<text> NULL,
              `col_array_string__undef_signed_index_inverted` array<text> NULL,
              `col_array_string__undef_signed_not_null` array<text> NOT NULL,
              `col_array_string__undef_signed_not_null_index_inverted` array<text> NOT NULL,
              `col_array_date__undef_signed` array<date> NULL,
              `col_array_date__undef_signed_index_inverted` array<date> NULL,
              `col_array_date__undef_signed_not_null` array<date> NOT NULL,
              `col_array_date__undef_signed_not_null_index_inverted` array<date> NOT NULL,
              `col_array_datetime__undef_signed` array<datetime> NULL,
              `col_array_datetime__undef_signed_index_inverted` array<datetime> NULL,
              `col_array_datetime__undef_signed_not_null` array<datetime> NOT NULL,
              `col_array_datetime__undef_signed_not_null_index_inverted` array<datetime> NOT NULL,
              `col_array_datetime_3___undef_signed` array<datetime(3)> NULL,
              `col_array_datetime_3___undef_signed_index_inverted` array<datetime(3)> NULL,
              `col_array_datetime_3___undef_signed_not_null` array<datetime(3)> NOT NULL,
              `col_array_datetime_3___undef_signed_not_null_index_inverted` array<datetime(3)> NOT NULL,
              `col_array_datetime_6___undef_signed` array<datetime(6)> NULL,
              `col_array_datetime_6___undef_signed_index_inverted` array<datetime(6)> NULL,
              `col_array_datetime_6___undef_signed_not_null` array<datetime(6)> NOT NULL,
              `col_array_datetime_6___undef_signed_not_null_index_inverted` array<datetime(6)> NOT NULL,
              INDEX col_int_undef_signed_index_inverted_idx (`col_int_undef_signed_index_inverted`) USING INVERTED,
              INDEX col_int_undef_signed_not_null_index_inverted_idx (`col_int_undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_date_undef_signed_index_inverted_idx (`col_date_undef_signed_index_inverted`) USING INVERTED,
              INDEX col_date_undef_signed_not_null_index_inverted_idx (`col_date_undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_varchar_1024__undef_signed_index_inverted_idx (`col_varchar_1024__undef_signed_index_inverted`) USING INVERTED,
              INDEX col_varchar_1024__undef_signed_index_inverted_p_e_idx (`col_varchar_1024__undef_signed_index_inverted_p_e`) USING INVERTED PROPERTIES("parser" = "english", "lower_case" = "true", "support_phrase" = "true"),
              INDEX col_varchar_1024__undef_signed_index_inverted_p_u_idx (`col_varchar_1024__undef_signed_index_inverted_p_u`) USING INVERTED PROPERTIES("parser" = "unicode", "lower_case" = "true", "support_phrase" = "true"),
              INDEX col_varchar_1024__undef_signed_not_null_index_inverted_idx (`col_varchar_1024__undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_varchar_1024__undef_signed_not_null_index_inverted_p_e_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_p_e`) USING INVERTED PROPERTIES("parser" = "english", "lower_case" = "true", "support_phrase" = "true"),
              INDEX col_varchar_1024__undef_signed_not_null_index_inverted_p_u_idx (`col_varchar_1024__undef_signed_not_null_index_inverted_p_u`) USING INVERTED PROPERTIES("parser" = "unicode", "lower_case" = "true", "support_phrase" = "true"),
              INDEX col_decimal_18__6__undef_signed_index_inverted_idx (`col_decimal_18__6__undef_signed_index_inverted`) USING INVERTED,
              INDEX col_decimal_18__6__undef_signed_not_null_index_inverted_idx (`col_decimal_18__6__undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_datetime_3__undef_signed_index_inverted_idx (`col_datetime_3__undef_signed_index_inverted`) USING INVERTED,
              INDEX col_datetime_3__undef_signed_not_null_index_inverted_idx (`col_datetime_3__undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_datetime_6__undef_signed_index_inverted_idx (`col_datetime_6__undef_signed_index_inverted`) USING INVERTED,
              INDEX col_datetime_6__undef_signed_not_null_index_inverted_idx (`col_datetime_6__undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_boolean__undef_signed_index_inverted_idx (`col_array_boolean__undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_boolean__undef_signed_not_null_index_inverted_idx (`col_array_boolean__undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_tinyint__undef_signed_index_inverted_idx (`col_array_tinyint__undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_tinyint__undef_signed_not_null_index_inverted_idx (`col_array_tinyint__undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_smallint__undef_signed_index_inverted_idx (`col_array_smallint__undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_smallint__undef_signed_not_null_index_inverted_idx (`col_array_smallint__undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_int__undef_signed_index_inverted_idx (`col_array_int__undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_int__undef_signed_not_null_index_inverted_idx (`col_array_int__undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_bigint__undef_signed_index_inverted_idx (`col_array_bigint__undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_bigint__undef_signed_not_null_index_inverted_idx (`col_array_bigint__undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_largeint__undef_signed_index_inverted_idx (`col_array_largeint__undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_largeint__undef_signed_not_null_index_inverted_idx (`col_array_largeint__undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_decimal_37_12___undef_signed_index_inverted_idx (`col_array_decimal_37_12___undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_decimal_37_12___undef_signed_n_0_idx (`col_array_decimal_37_12___undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_decimal_8_4___undef_signed_index_inverted_idx (`col_array_decimal_8_4___undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_decimal_8_4___undef_signed_not_1_idx (`col_array_decimal_8_4___undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_decimal_9_0___undef_signed_index_inverted_idx (`col_array_decimal_9_0___undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_decimal_9_0___undef_signed_not_2_idx (`col_array_decimal_9_0___undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_char_255___undef_signed_index_inverted_idx (`col_array_char_255___undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_char_255___undef_signed_not_null_index_inverted_idx (`col_array_char_255___undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_varchar_65533___undef_signed_index_inverted_idx (`col_array_varchar_65533___undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_varchar_65533___undef_signed_n_3_idx (`col_array_varchar_65533___undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_string__undef_signed_index_inverted_idx (`col_array_string__undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_string__undef_signed_not_null_index_inverted_idx (`col_array_string__undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_date__undef_signed_index_inverted_idx (`col_array_date__undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_date__undef_signed_not_null_index_inverted_idx (`col_array_date__undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_datetime__undef_signed_index_inverted_idx (`col_array_datetime__undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_datetime__undef_signed_not_null_index_inverted_idx (`col_array_datetime__undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_datetime_3___undef_signed_index_inverted_idx (`col_array_datetime_3___undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_datetime_3___undef_signed_not_null_index_inverted_idx (`col_array_datetime_3___undef_signed_not_null_index_inverted`) USING INVERTED,
              INDEX col_array_datetime_6___undef_signed_index_inverted_idx (`col_array_datetime_6___undef_signed_index_inverted`) USING INVERTED,
              INDEX col_array_datetime_6___undef_signed_not_null_index_inverted_idx (`col_array_datetime_6___undef_signed_not_null_index_inverted`) USING INVERTED
            ) ENGINE=OLAP
            DUPLICATE KEY(`pk`, `col_int_undef_signed_index_inverted`)
            DISTRIBUTED BY HASH(`pk`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "bloom_filter_columns" = "col_varchar_1024__undef_signed, col_date_undef_signed_not_null, col_date_undef_signed, col_int_undef_signed, col_varchar_1024__undef_signed_not_null, col_int_undef_signed_not_null",
            "storage_medium" = "hdd",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false"
            ); """

    streamLoad { 
	table 'table_30_un_pa_ke_pr_di4'
        set 'column_separator', '\t'
        file 'array_filter.csv'
        time 60000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(236, json.NumberTotalRows)
            assertEquals(236, json.NumberLoadedRows)
        }
    }

   sql """ delete from table_30_un_pa_ke_pr_di4 where col_varchar_1024__undef_signed_index_inverted_p_u = '始终';
           delete from table_30_un_pa_ke_pr_di4 where col_varchar_1024__undef_signed = '';
           delete from table_30_un_pa_ke_pr_di4 where col_varchar_1024__undef_signed_not_null = '--';
           delete from table_30_un_pa_ke_pr_di4 where col_int_undef_signed = -1;
           delete from table_30_un_pa_ke_pr_di4 where col_varchar_1024__undef_signed_not_null_index_inverted_p_u = '刘伟'; """
   order_qt_sql """ 
	SELECT array_contains(col_array_string__undef_signed_not_null, col_varchar_1024__undef_signed_index_inverted_p_e), (arrays_overlap(col_array_date__undef_signed_index_inverted, col_array_date__undef_signed_not_null)) and not (arrays_overlap(col_array_smallint__undef_signed_not_null, col_array_boolean__undef_signed_not_null) or (array_contains(col_array_decimal_8_4___undef_signed_index_inverted, col_int_undef_signed)) and (arrays_overlap(col_array_bigint__undef_signed, [ 32767, -9223372036854775808 ])) and ((case col_boolean_undef_signed when 0 then 1 when 1 then 2 else 0 end) = 0)) FROM table_30_un_pa_ke_pr_di4 where array_contains(col_array_largeint__undef_signed_not_null_index_inverted, col_boolean_undef_signed) or ((arrays_overlap(col_array_tinyint__undef_signed_not_null, col_array_largeint__undef_signed_not_null_index_inverted)) and (array_contains(col_array_date__undef_signed_not_null, col_date_undef_signed_not_null)) and not ((array_contains(col_array_largeint__undef_signed, col_int_undef_signed_index_inverted)) and not ((col_array_largeint__undef_signed_index_inverted is null) and col_array_decimal_8_4___undef_signed_not_null_index_inverted is not null or not (arrays_overlap(col_array_int__undef_signed_not_null, col_array_int__undef_signed_not_null) and not (case col_date_undef_signed_not_null_index_inverted when "2027-01-09" then 1 when "2023-01-15 08:32:59.123123" then 2 else 0 end) = 1)))) order by pk,col_int_undef_signed_index_inverted; """

}
