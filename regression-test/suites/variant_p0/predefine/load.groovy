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

suite("regression_test_variant_predefine_schema", "p0"){
    sql """DROP TABLE IF EXISTS test_predefine"""
    def count = new Random().nextInt(10) + 1;
    if (new Random().nextInt(100) < 50) {
        count = "1000"
    }
    sql """ set default_variant_max_subcolumns_count = ${count} """
    sql """ set enable_variant_flatten_nested = true """
    sql """
        CREATE TABLE `test_predefine` (
            `id` bigint NOT NULL,
            `type` varchar(30) NULL,
            `v1` variant<'a.b.c':int,'ss':string,'dcm':decimal(38, 9),'dt':datetime,'ip':ipv4,'a.b.d':double> NULL,
            INDEX idx_var_sub(`v1`) USING INVERTED PROPERTIES("parser" = "english") )
        ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1");
    """

    sql """insert into test_predefine values(1, '1', '{"a" : {"b" : {"c" : "123456", "d" : "11.111"}}, "ss" : 199991111, "dcm" : 123.456, "dt" : "2021-01-01 00:00:00", "ip" : "127.0.0.1"}')"""
    sql """insert into test_predefine values(2, '2', '{"a" : {"b" : {"c" : 678910, "d" : 22.222}}, "ss" : "29999111", "dcm" : "456.123", "dt" : "2022-01-01 11:11:11", "ip" : "127.0.0.1"}')"""
    sql """insert into test_predefine values(3, '3', '{"dcm" : 789.123, "dt" : "2025-01-01 11:11:11.1", "ip" : "127.0.0.1"}')"""
    sql """insert into test_predefine values(4, '4', '{"a" : {"b" : {"c" : "678910", "d" : "33.222"}}}')"""
    sql """insert into test_predefine values(5, '5', null)"""
    sql """insert into test_predefine values(6, '6', null)"""
    sql """insert into test_predefine values(7, '7', '{"xxx" : 12345}')"""
    sql """insert into test_predefine values(8, '8', '{"yyy" : 111.111}')"""
    sql """insert into test_predefine values(9, '2', '{"a" : {"b" : {"c" : 678910, "d" : 22.222}}, "ss" : "29999111", "dcm" : "456.123", "dt" : "2022-01-01 11:11:11", "ip" : "127.0.0.1"}')"""
    sql """insert into test_predefine values(10, '1', '{"a" : {"b" : {"c" : "123456", "d" : "11.111"}}, "ss" : 199991111, "dcm" : 123.456, "dt" : "2021-01-01 00:00:00", "ip" : "127.0.0.1"}')"""
    sql """insert into test_predefine values(12, '3', '{"dcm" : 789.123, "dt" : "2025-01-01 11:11:11.1", "ip" : "127.0.0.1"}')"""
    sql """insert into test_predefine values(11, '4', '{"a" : {"b" : {"c" : "678910", "d" : "33.222"}}}')"""
    qt_sql """select * from test_predefine order by id"""

    qt_sql """select cast(v1['ip'] as ipv4) from test_predefine where cast(v1['ip'] as ipv4) = '127.0.0.1';"""
    qt_sql """select cast(v1['dcm'] as decimal) from test_predefine where cast(v1['dcm'] as decimal) = '123.456';"""
    qt_sql """select v1['dcm'] from test_predefine order by id;"""
    qt_sql """select v1['dt'] from test_predefine where cast(v1['dt'] as datetime) = '2022-01-01 11:11:11';"""
    qt_sql """select v1['dt'] from test_predefine  where  cast(v1['dt'] as datetime) = '2022-01-01 11:11:11' order by id limit 10"""
    qt_sql """select * from test_predefine  where  cast(v1['dt'] as datetime) = '2022-01-01 11:11:11' order by id limit 10;"""
    qt_sql """select * from test_predefine  where  v1['dt'] is not null order by id limit 10;"""

    sql """DROP TABLE IF EXISTS test_predefine1"""
    sql """
        CREATE TABLE `test_predefine1` (
            `id` bigint NOT NULL,
            `v1` variant NULL,
            INDEX idx_var_sub(`v1`) USING INVERTED PROPERTIES("parser" = "english") )
        ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "variant_enable_flatten_nested" = "true");
    """ 
    sql """insert into test_predefine1 values(1, '{"predefine_col1" : 1024}')"""
    sql """insert into test_predefine1 values(2, '{"predefine_col2" : 1.11111}')"""
    sql """insert into test_predefine1 values(3, '{"predefine_col3" : "11111.00000"}')"""
    sql """insert into test_predefine1 values(4, '{"predefine_col4" : "2020-01-01-01"}')"""

    sql """insert into test_predefine1 values(5, '{"PREDEFINE_COL1" : 1024}')"""
    sql """insert into test_predefine1 values(6, '{"PREDEFINE_COL2" : 1.11111}')"""
    sql """insert into test_predefine1 values(7, '{"PREDEFINE_COL3" : "11111.00000"}')"""
    sql """insert into test_predefine1 values(8, '{"PREDEFINE_COL4" : "2020-01-01-01"}')"""
    sql """select * from test_predefine1 order by id limit 1"""
    qt_sql """select * from test_predefine1 order by id"""


    // complex types with scalar types
    sql "DROP TABLE IF EXISTS test_predefine2"
    sql """
        CREATE TABLE `test_predefine2` (
            `id` bigint NOT NULL,
            `v1` variant<
                'array_int':array<int>,
                'array_string':array<string>,
                'array_decimal':array<decimalv3(26,9)>,
                'array_datetime':array<datetime>,
                'array_datetimev2':array<datetimev2>,
                'array_date':array<date>,
                'array_datev2':array<datev2>,
                'array_ipv4':array<ipv4>,
                'array_ipv6':array<ipv6>,
                'array_float':array<decimalv3(26,9)>,
                'array_boolean':array<boolean>,
                'int_':int, 
                'string_':string, 
                'decimal_':decimalv3(26,9), 
                'datetime_':datetime,
                'datetimev2_':datetimev2(6),
                'date_':date,
                'datev2_':datev2,
                'ipv4_':ipv4,
                'ipv6_':ipv6,
                'float_':decimalv3(26,9),
                'boolean_':boolean,
                'varchar_': text,
                properties("variant_max_subcolumns_count" = "6", "variant_enable_typed_paths_to_sparse" = "false")
            > NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1");
    """
    def json1 = """
        {
                "array_int" : [1, 2, 3],
                "array_string" : ["a", "b", "c"],
                "array_decimal" : [1.1, 2.2, 3.3],
                "array_datetime" : ["2021-01-01 00:00:00", "2022-01-01 00:00:00", "2023-01-01 00:00:00"],
                "array_datetimev2" : ["2021-01-01 00:00:00", "2022-01-01 00:00:00", "2023-01-01 00:00:00"],
                "array_date" : ["2021-01-01", "2022-01-01", "2023-01-01"],
                "array_datev2" : ["2021-01-01", "2022-01-01", "2023-01-01"],
                "array_ipv4" : ["127.0.0.1", "172.0.1.1"],
                "array_ipv6" : ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe"],
                "array_float" : ["1.11111"],
                "array_boolean" : [true, false, true],
                "int_" : 11111122,
                "string_" : 12111222113.0,
                "decimal_" : 188118222.011121933,
                "datetime_" : "2022-01-01 11:11:11",
                "datetimev2_" : "2022-01-01 11:11:11.999999",
                "date_" : "2022-01-01",
                "datev2_" : "2022-01-01",
                "ipv4_" : "127.0.0.1",
                "ipv6_" : "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe",
                "float_" : "128.111",
                "boolean_" : true,
                "varchar_" : "hello world"
            }
    """
    def json2 = """
        {
                "array_int" : ["1", "2", 3],
                "array_string" : ["a", "b", "c"],
                "array_decimal" : [1.1, 2.2, 3.3],
                "array_datetime" : ["2021-01-01 00:00:00", "2022-01-01 00:00:00", "2023-01-01 00:00:00"],
                "array_datetimev2" : ["2021-01-01 00:00:00", "2022-01-01 00:00:00", "2023-01-01 00:00:00"],
                "array_date" : ["2021-01-01", "2022-01-01", "2023-01-01"],
                "array_datev2" : ["2021-01-01", "2022-01-01", "2023-01-01"],
                "array_ipv4" : ["127.0.0.1", "172.0.1.1"],
                "array_ipv6" : ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe"],
                "array_float" : [2.22222],
                "array_boolean" : [1, 0, 1, 0, 1],
                "int_" : "3333333333",
                "string_" : 12111222113.0,
                "decimal_" : "219911111111.011121933",
                "datetime_" : "2022-01-01 11:11:11",
                "datetimev2_" : "2022-01-01 11:11:11.999999",
                "date_" : "2022-01-01",
                "datev2_" : "2022-01-01",
                "ipv4_" : "127.0.0.1",
                "ipv6_" : "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe",
                "float_" : 1.111111111,
                "boolean_" : true,
                "varchar_" : "world hello"
            }
    """
    def json3 = """
        {
                "array_int" : ["1", "2", 3],
                "array_string" : ["a", "b", "c"],
                "array_datetimev2" : ["2021-01-01 00:00:00", "2022-01-01 00:00:00", "2023-01-01 00:00:00"],
                "int_" : "3333333333",
                "decimal_" : "219911111111.011121933",
                "date_" : "2022-01-01",
                "ipv4_" : "127.0.0.1",
                "float_" : 1.111111111,
                "boolean_" : true,
                "varchar_" : "world hello"
            }
    """
    def json4 = """
        {
                "array_int" : ["1", "2", 3],
                "array_string" : ["a", "b", "c"],
                "array_datetimev2" : ["2021-01-01 00:00:00", "2022-01-01 00:00:00", "2023-01-01 00:00:00"],
                "ipv4_" : "127.0.0.1",
                "float_" : 1.111111111,
                "varchar_" : "world hello",
                "ext_1" : 1.111111,
                "ext_2" : "this is an extra field",
                "ext_3" : [1, 2, 3]
            }
    """
    sql "insert into test_predefine2 values(1, '${json1}')"
    sql "insert into test_predefine2 values(2, '${json2}')"
    sql "insert into test_predefine2 values(3, '${json3}')"
    sql "insert into test_predefine2 values(4, '${json4}')"
       
    qt_sql """select * from test_predefine2 order by id"""

    for (int i = 10; i < 100; i++) {
        sql "insert into test_predefine2 values(${i}, '${json4}')"
    } 

    // // schema change
    // // 1. add column
    sql "alter table test_predefine1 add column v2 variant<'dcm':decimal(38, 9),'dt':datetime> default null"
    sql """insert into test_predefine1 values(101, '{"a" :1}', '{"dcm": 1111111}')""" 
    sql "alter table test_predefine1 add column v3 variant<'dcm':decimal(38, 9),'dt':datetime,'ip':ipv6> default null"
    sql """insert into test_predefine1 values(102, '{"a" :1}', '{"dcm": 1111111}', '{"dcm": 1111111}');"""
    // 2. todo support alter column type
    // sql "alter table test_predefine1 modify column v3 variant<dcm:decimal,dt:datetime,ip:ipv6>"
    sql """insert into test_predefine1 values(103, '{"a" :1}', '{"dcm": 1111111}', '{"dt": "2021-01-01 11:11:11"}');"""
    qt_sql """select * from test_predefine1 where id >= 100 order by id"""
    // 3. drop column
    sql "alter table test_predefine1 drop column v3"

    sql "DROP TABLE IF EXISTS test_predefine3"
    sql """CREATE TABLE `test_predefine3` (
            `id` bigint NOT NULL,
            `v` variant<'nested.a':string> NULL)
        ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "variant_enable_flatten_nested" = "false", "disable_auto_compaction" = "true");"""

    // test alter nested no effect at present
    sql "truncate table test_predefine3"
    sql """insert into test_predefine3 values (1, '{"nested" : [{"a" : 123, "b" : "456"}]}')"""
    // sql "alter table test_predefine3 modify column v variant<`nested.a`: string>"
    sql """insert into test_predefine3 values (1, '{"nested" : [{"a" : 123, "b" : "456"}]}')"""
    sql """insert into test_predefine3 values (1, '{"nested" : [{"a" : 123, "b" : "456"}]}')"""
    sql """insert into test_predefine3 values (1, '{"nested" : [{"a" : 123, "b" : "456"}]}')"""
    sql """insert into test_predefine3 values (1, '{"nested" : [{"a" : 123, "b" : "456"}]}')"""
    qt_sql "select * from test_predefine3"
    qt_sql "select v['nested'] from test_predefine3"
    qt_sql "select v['nested']['a'] from test_predefine3"

    // test use auto type detect first then alter to modify type
    sql "truncate table test_predefine3"
    sql """insert into test_predefine3 values (1, '{"auto_type" : 1234.1111}')"""
    // sql "alter table test_predefine3 modify column v variant<`auto_type`: int>"
    sql """insert into test_predefine3 values (1, '{"auto_type" : "124511111"}')"""
    sql """insert into test_predefine3 values (1, '{"auto_type" : 1111122334}')"""
    sql """insert into test_predefine3 values (1, '{"auto_type" : 111223341111}')"""
    sql """insert into test_predefine3 values (1, '{"auto_type" : true}')"""
    sql """insert into test_predefine3 values (1, '{"auto_type" : 1}')"""
    sql """insert into test_predefine3 values (1, '{"auto_type" : 256}')"""
    sql """insert into test_predefine3 values (1, '{"auto_type" : 12345}')"""
    sql """insert into test_predefine3 values (1, '{"auto_type" : 1.0}')"""
    trigger_and_wait_compaction("test_predefine3", "full")
    qt_sql """select variant_type(v) from test_predefine3"""

    // test array
    sql "DROP TABLE IF EXISTS region_insert"
    sql """
    CREATE TABLE `region_insert` (
      `k` bigint NULL,
      `var` variant<'c_acctbal':text,'c_address':text,'c_comment':text,'c_custkey':text,'c_mktsegment':text,'c_name':text,'c_nationkey':text,'c_phone':text,'p_brand':float,'p_comment':text,'p_container':text,'p_mfgr':text,'p_name':text,'p_partkey':text,'p_retailprice':text,'p_size':text,'p_type':text,'r_comment':text,'r_name':text,'r_regionkey':text,'ps_availqty':text,'ps_comment':text,'ps_none':text,'ps_partkey':text,'ps_suppkey':text,'ps_supplycost':text,'key_46':text,'key_47':text,'key_48':text,'o_clerk':text,'o_comment':text,'o_custkey':text,'o_orderdate':text,'o_orderkey':text,'o_orderpriority':text,'o_orderstatus':text,'o_shippriority':text,'o_totalprice':text,'key_80':array<boolean>> NULL,
      `OfvZr` variant NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`k`)
    DISTRIBUTED BY HASH(`k`) BUCKETS 5
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "min_load_replica_num" = "-1",
    "is_being_synced" = "false",
    "storage_medium" = "hdd",
    "storage_format" = "V2",
    "inverted_index_storage_format" = "V2",
    "light_schema_change" = "true",
    "store_row_column" = "true",
    "row_store_page_size" = "16384",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "group_commit_interval_ms" = "10000",
    "group_commit_data_bytes" = "134217728"
    );
    """
    sql """
        insert into region_insert (k,var,OfvZr) values(1550,'{"key_48":"2024-12-17 20:27:12","key_11":"2024-12-17 20:27:12","key_53":"2024-12-17 20:27:12","key_30":"2024-12-17 20:27:12","key_3":"2024-12-17 20:27:12","key_93":"1HYdNTPvNA","key_40":true,"key_61":"N5LU74i0Nb","key_55":"2024-12-17 20:27:12","key_45":"mMj4f8k8gH","key_58":"2024-12-17 20:27:12","key_71":true,"key_51":"2024-12-17 20:27:12","key_79":"2024-12-17 20:27:12","key_7":"8QJFB23Rug","key_75":31,"key_50":"2024-12-17 20:27:12","key_24":86,"key_33":98,"key_69":16,"key_57":86,"key_86":"2024-12-17 20:27:12","key_99":24,"key_66":"oTZgDxKvcc","key_18":false,"key_49":"2024-12-17 20:27:12","key_2":false,"key_64":"h3DxAvBG8D","key_87":87,"key_37":42,"key_29":"wb29lruo8E","key_96":88,"key_9":83,"key_52":6,"key_97":"X7y409riGJ","key_72":false,"key_26":"2024-12-17 20:27:12","key_12":66,"key_88":false,"key_32":false,"key_6":true,"key_80":false,"key_89":"2024-12-17 20:27:12","key_1":false,"key_35":"2024-12-17 20:27:12","key_23":70,"key_95":23,"key_76":false,"key_92":true,"key_47":"zYM9IJXSxk","key_22":"2024-12-17 20:27:12","key_38":"P9arsVnb3q","key_56":"LU4SdelM46","key_28":24,"key_4":"GKXCKn1Kf9","key_83":29,"key_20":90,"key_43":"VA8xyYskJ1","key_81":22,"key_16":"2024-12-17 20:27:12","key_82":true,"key_84":"2024-12-17 20:27:12"}','{"key_87":"900oLqWX9Q","key_32":63,"key_79":true,"key_42":3,"key_98":20,"key_35":false,"key_19":"2024-12-17 20:27:12","key_89":"NO0TLqKAvS","key_77":"2024-12-17 20:27:12","key_34":false,"key_43":false,"key_30":true,"key_21":"2024-12-17 20:27:12","key_3":"oDDa0SZ7Bs","key_72":"2024-12-17 20:27:12","key_67":38,"key_82":"2024-12-17 20:27:12","key_37":"VWLDmiZbMr","key_16":true,"key_58":"42Mju9EbAS","key_94":false,"key_50":"cqv3qYmYuJ","key_28":28,"key_78":43,"key_2":"omTAZB0CxT","key_75":"4tAlWmcvnY","key_40":50,"key_33":"2024-12-17 20:27:12","key_70":"2024-12-17 20:27:12","key_25":"2024-12-17 20:27:12","key_54":false,"key_11":"2024-12-17 20:27:12","key_5":"ritjh4q9pJ","key_51":"DzQGqKQ95I","key_73":false,"key_10":"bPI94fvfL4","key_26":"AF5DtNU5Dj","key_80":66,"key_9":69,"key_83":false,"key_59":48,"key_24":"2024-12-17 20:27:12","key_84":36,"key_17":true,"key_44":18,"key_97":"JBw2ZZhDtF","key_74":15,"key_96":true,"key_62":"2024-12-17 20:27:12","key_65":"6iWPCv8FDR","key_53":"2024-12-17 20:27:12","key_95":false,"key_56":"3zyjHDYMJG","key_60":false,"key_23":"2024-12-17 20:27:12","key_8":"zbNpgWWYWS","key_81":"2024-12-17 20:27:12"}')
    """
    sql "DROP TABLE IF EXISTS test_bf_with_bool"
    // test bf with bool
    sql """
        CREATE TABLE `test_bf_with_bool` (
      `k` bigint NULL,
      `var` variant<'c_bool':boolean>
    ) ENGINE=OLAP
    DUPLICATE KEY(`k`)
    DISTRIBUTED BY HASH(`k`) BUCKETS 5
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "min_load_replica_num" = "-1",
    "bloom_filter_columns" = "var"
    );
    """

    // array with nulls

    sql "DROP TABLE IF EXISTS test_array_with_nulls"
    // test bf with bool
    sql """
        CREATE TABLE `test_array_with_nulls` (
      `k` bigint NULL,
      `var` variant<match_name 'array_decimal':array<decimalv3(27,9)>>
    ) ENGINE=OLAP
    DUPLICATE KEY(`k`)
    DISTRIBUTED BY HASH(`k`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "min_load_replica_num" = "-1"
    );
    """
    sql """insert into test_array_with_nulls values(3, '{"array_decimal" : [null, 2.2, 3.3, 4.4]}')"""
    qt_sql_arr_null_1 "select * from test_array_with_nulls order by k"
    sql """insert into test_array_with_nulls values(1, '{"array_decimal" : [1.1, 2.2, 3.3, null]}')"""
    sql """insert into test_array_with_nulls values(2, '{"array_decimal" : [1.1, 2.2, null, 4.4]}')"""
    sql """insert into test_array_with_nulls values(4, '{"array_decimal" : [1.1, null, 3.3, 4.4]}')"""
    sql """insert into test_array_with_nulls values(5, '{"array_decimal" : [1.1, 2.2, 3.3, 4.4]}')"""
    sql """insert into test_array_with_nulls values(6, '{"array_decimal" : []}')"""
    sql """insert into test_array_with_nulls values(7, '{"array_decimal" : [null, null]}')"""
    qt_sql_arr_null_2 "select * from test_array_with_nulls order by k limit 5"

    // test variant_type
    sql "DROP TABLE IF EXISTS test_variant_type"
    sql """
        CREATE TABLE `test_variant_type` (
      `k` bigint NULL,
      `var` variant<match_name 'dcm' : decimal, 'db' : double, 'dt' : datetime, 'a.b.c' : array<int>>
    ) ENGINE=OLAP
    DUPLICATE KEY(`k`)
    DISTRIBUTED BY HASH(`k`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "min_load_replica_num" = "-1"
    );
    """
    sql """insert into test_variant_type values(1, '{"dcm" : 1.1, "db" : 2.2, "dt" : "2021-01-01 00:00:00", "a.b.c" : [1, 2, 3]}')"""
    sql """insert into test_variant_type values(1, null)"""
    qt_sql "select variant_type(var) from test_variant_type"

    sql "DROP TABLE IF EXISTS test_variant_type_not_null"
    sql """
        CREATE TABLE `test_variant_type_not_null` (
      `k` bigint NULL,
      `var` variant<match_name 'dcm' : decimal, 'db' : double, 'dt' : datetime, 'a.b.c' : array<int>> not null
    ) ENGINE=OLAP
    DUPLICATE KEY(`k`)
    DISTRIBUTED BY HASH(`k`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "min_load_replica_num" = "-1"
    );
    """
    sql """insert into test_variant_type_not_null values(1, '{"dcm" : 1.1, "db" : 2.2, "dt" : "2021-01-01 00:00:00", "a.b.c" : [1, 2, 3]}')"""
    qt_sql "select variant_type(var) from test_variant_type_not_null"
}
