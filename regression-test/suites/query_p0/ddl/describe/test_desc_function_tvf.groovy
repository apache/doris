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

suite("test_desc_function_tvf", "p0") {

    // ========== Static schema TVFs: no params needed ==========

    // numbers() - static schema
    order_qt_desc_numbers "DESC FUNCTION numbers('number'='10')"

    // backends() - static schema
    order_qt_desc_backends "DESC FUNCTION backends()"

    // frontends() - static schema
    order_qt_desc_frontends "DESC FUNCTION frontends()"

    // frontends_disks() - static schema
    order_qt_desc_frontends_disks "DESC FUNCTION frontends_disks()"

    // catalogs() - static schema
    order_qt_desc_catalogs "DESC FUNCTION catalogs()"

    // ========== Simple param TVFs ==========

    // partitions() - depends on 'catalog' param
    String dbName = context.config.getDbNameByFile(context.file)
    sql """ DROP TABLE IF EXISTS desc_func_test_table """
    sql """
        CREATE TABLE desc_func_test_table (
            k1 INT,
            k2 VARCHAR(20)
        ) DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    order_qt_desc_partitions "DESC FUNCTION partitions('catalog'='internal', 'database'='${dbName}', 'table'='desc_func_test_table')"

    // partitions() without catalog param (should use default internal)
    order_qt_desc_partitions_no_catalog "DESC FUNCTION partitions('database'='${dbName}', 'table'='desc_func_test_table')"

    // jobs() - depends on 'type' param
    order_qt_desc_jobs "DESC FUNCTION jobs('type'='insert')"

    // tasks() - depends on 'type' param
    order_qt_desc_tasks "DESC FUNCTION tasks('type'='insert')"

    // hudi_meta() - depends on 'query_type' param
    order_qt_desc_hudi_timeline "DESC FUNCTION hudi_meta('table'='ctl.db.tbl', 'query_type'='timeline')"

    // ========== Parquet metadata TVFs ==========

    // parquet_meta
    order_qt_desc_parquet_meta "DESC FUNCTION parquet_meta('path'='hdfs://namenode:8020/test.parquet')"

    // parquet_file_metadata
    order_qt_desc_parquet_file_meta "DESC FUNCTION parquet_file_metadata('path'='hdfs://namenode:8020/test.parquet')"

    // parquet_kv_metadata
    order_qt_desc_parquet_kv_meta "DESC FUNCTION parquet_kv_metadata('path'='hdfs://namenode:8020/test.parquet')"

    // ========== External file TVFs: csv_schema or __dummy_col ==========

    // s3() without csv_schema -> returns __dummy_col
    order_qt_desc_s3 "DESC FUNCTION s3('URI'='s3://bucket/path', 'ACCESS_KEY'='ak', 'SECRET_KEY'='sk')"

    // hdfs() without csv_schema -> returns __dummy_col
    order_qt_desc_hdfs "DESC FUNCTION hdfs('uri'='hdfs://namenode:8020/path', 'format'='parquet')"

    // local() without csv_schema -> returns __dummy_col
    order_qt_desc_local "DESC FUNCTION local('filepath'='/tmp/test.parquet', 'format'='parquet')"

    // http() without csv_schema -> returns __dummy_col
    order_qt_desc_http "DESC FUNCTION http('url'='http://localhost/test.parquet', 'format'='parquet')"

    // http_stream() without csv_schema -> returns __dummy_col
    order_qt_desc_http_stream "DESC FUNCTION http_stream('url'='http://localhost/stream', 'format'='csv')"

    // file() without csv_schema -> returns __dummy_col
    order_qt_desc_file "DESC FUNCTION file('path'='hdfs://namenode:8020/path', 'format'='parquet')"

    // cdc_stream() without csv_schema -> returns __dummy_col
    order_qt_desc_cdc_stream "DESC FUNCTION cdc_stream()"

    // ========== External file TVFs: with csv_schema ==========

    // s3() with csv_schema -> returns parsed schema columns
    order_qt_desc_s3_csv_schema "DESC FUNCTION s3('URI'='s3://bucket/path', 'ACCESS_KEY'='ak', 'SECRET_KEY'='sk', 'csv_schema'='k1:int;k2:string')"

    // hdfs() with csv_schema
    order_qt_desc_hdfs_csv_schema "DESC FUNCTION hdfs('uri'='hdfs://namenode:8020/path', 'format'='csv', 'csv_schema'='k1:int;k2:bigint')"

    // ========== Verify DESC FUNCTION does not trigger full TVF construction ==========

    // DESC FUNCTION numbers() without required 'number' param should still work
    // (previously this would fail because full construction requires 'number' param)
    order_qt_desc_numbers_no_param "DESC FUNCTION numbers()"

    // DESC FUNCTION s3() without any required params should still work
    // (previously this would fail because S3 constructor requires URI, ACCESS_KEY, etc.)
    order_qt_desc_s3_no_param "DESC FUNCTION s3()"

    // DESC FUNCTION hdfs() without any required params should still work
    order_qt_desc_hdfs_no_param "DESC FUNCTION hdfs()"

    // DESC FUNCTION parquet_metadata() without required 'path' param should still work
    order_qt_desc_parquet_meta_no_param "DESC FUNCTION parquet_meta()"

    // ========== Error cases ==========

    // Non-existent TVF name
    test {
        sql "DESC FUNCTION nonexistent_tvf()"
        exception "Could not find table function"
    }
}
