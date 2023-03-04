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

suite("test_unicode_name") {
    try {
    sql """
    set enable_unicode_name_support = true
    """
        sql """
    CREATE DATABASE IF NOT EXISTS `中文库名`
    """
    
        sql """
    CREATE TABLE IF NOT EXISTS `中文表名` (
      `字符串_字段名_test` varchar(150) NULL,
      `时间_字段名_test` datetime NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`字符串_字段名_test`)
    DISTRIBUTED BY HASH(`字符串_字段名_test`) BUCKETS 3
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "in_memory" = "false",
      "storage_format" = "V2"
    )
    """

        sql """
    CREATE TABLE IF NOT EXISTS `中文表名_1` LIKE `中文表名`
    """

        qt_select """SHOW CREATE TABLE `中文表名_1`"""
    } finally {
        sql """ DROP DATABASE IF EXISTS `中文库名` """
        sql """ DROP TABLE IF EXISTS `中文表名` """

        sql """ DROP TABLE IF EXISTS `中文表名_1` """
    }

}
