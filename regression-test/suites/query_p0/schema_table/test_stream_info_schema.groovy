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

suite("test_stream_info_schema") {
    sql "DROP DATABASE IF EXISTS test_stream_info_db"
    sql "CREATE DATABASE test_stream_info_db"
    sql "USE test_stream_info_db"

    sql """
        CREATE TABLE `tbl1` (
          `sid` int NULL,
          `sname` varchar(32) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`sid`)
        DISTRIBUTED BY HASH(`sid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); 
    """

    sql """
        CREATE STREAM `s1` ON TABLE tbl1
        COMMENT 'test stream 1'
        PROPERTIES('type' = 'min_delta');
    """

    sql """
        CREATE STREAM `s2` ON TABLE tbl1
        COMMENT 'test stream 2'
        PROPERTIES('type' = 'default');
    """

    sql """
        CREATE STREAM `s3` ON TABLE tbl1
        COMMENT 'test stream 3'
        PROPERTIES('type' = 'append_only');
    """

    qt_sql "select DB_NAME,STREAM_NAME,STREAM_TYPE,CONSUME_TYPE,STREAM_COMMENT,BASE_TABLE_NAME,BASE_TABLE_DB,BASE_TABLE_CTL,BASE_TABLE_TYPE,ENABLED,IS_STALE,STALE_REASON from information_schema.streams where DB_NAME = 'test_stream_info_db' order by STREAM_NAME;"
    sql "DROP DATABASE IF EXISTS test_stream_info_db"
}