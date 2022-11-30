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

suite("test_fnf") {
    try {
        sql """
            CREATE TABLE IF NOT EXISTS `test_fnf` (
              `test_varchar` varchar(1) NULL,
              `0test_varchar` varchar(1) NULL,
              `@test_varchar` varchar(1) NULL,
              `_test_varchar` varchar(1) NULL,
              `_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_var` varchar(1) NULL,
              `@test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_var` varchar(1) NULL,
              `0test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_var` varchar(1) NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`test_varchar`)
            DISTRIBUTED BY HASH(`test_varchar`) BUCKETS 3
              PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "in_memory" = "false",
              "storage_format" = "V2"
            )
            """
    } catch (java.sql.SQLException t){
        assertTrue(false)
    }

    try {
        sql """
            CREATE TABLE IF NOT EXISTS `test_fnf_error_1` (
              `test@_varchar` varchar(1) NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`test_varchar`)
            DISTRIBUTED BY HASH(`test_varchar`) BUCKETS 3
              PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "in_memory" = "false",
              "storage_format" = "V2"
            )
            """
    } catch (java.sql.SQLException t){
        assertTrue(true)
    }

    try {
        sql """
            CREATE TABLE IF NOT EXISTS `test_fnf_error_2` (
               `test_varchar` varchar(1) NULL,
               `_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varc` varchar(1) NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`test_varchar`)
            DISTRIBUTED BY HASH(`test_varchar`) BUCKETS 3
              PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "in_memory" = "false",
              "storage_format" = "V2"
            )
            """
    } catch (java.sql.SQLException t){
        assertTrue(true)
    } finally {
        sql """ DROP TABLE IF EXISTS test_fnf """

        sql """ DROP TABLE IF EXISTS test_fnf_error_1 """

        sql """ DROP TABLE IF EXISTS test_fnf_error_2 """
    }

}

