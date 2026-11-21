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

suite("test_export_delete_disallow", "p0") {
    // create table and insert
    sql """ DROP TABLE IF EXISTS test_export_delete_disallow_tbl"""
    sql """
    CREATE TABLE IF NOT EXISTS test_export_delete_disallow_tbl (
        `id` int(11) NULL,
        `Name` string NULL
        )
        PROPERTIES("replication_num" = "1");
    """
    sql """insert into test_export_delete_disallow_tbl values(1, "abc");"""
    
    test {
        sql """EXPLAIN SELECT 1 INTO OUTFILE 'file:///tmp/doris_export_fixture/'"""
        exception "requires remote storage properties"
    }

    test {
        sql """EXPORT TABLE test_export_delete_disallow_tbl TO 'file:///tmp/doris_export_fixture/'"""
        exception "requires a remote storage destination"
    }

    test {
        sql """select * from test_export_delete_disallow_tbl limit 10 INTO OUTFILE "hdfs://nn/fixture/"
               properties("hadoop.username"="doris", "delete_existing_files"="true")"""
        exception """Deleting existing files is not allowed"""
    }

    test {
        sql """EXPORT TABLE test_export_delete_disallow_tbl TO "hdfs://nn/fixture/"
               PROPERTIES ("delete_existing_files"="true") WITH HDFS ("hadoop.username"="doris")"""
        exception """Deleting existing files is not allowe"""
    }
}
