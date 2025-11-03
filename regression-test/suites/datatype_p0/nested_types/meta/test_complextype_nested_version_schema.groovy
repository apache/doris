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

suite("test_complextype_nested_version_schema") {
    // add array/map/struct
    sql "DROP TABLE IF EXISTS `complext_nested_version`;"
    def createTblSql = """
    CREATE TABLE `complext_nested_version` (
      `id` INT NULL,
      `c1` ARRAY<DECIMALV3(12, 1)> NULL,
      `c2` DECIMALV3(12, 1) NULL,
      `c3` MAP<DECIMALV3(12, 2),DATETIMEV2> NULL,
      `c4` STRUCT<f1:DATE,f2:DATETIME,f3:DATEV2,f4:DATETIMEV2,f5:DECIMALV3(9, 0)> NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 10
    PROPERTIES ("replication_allocation" = "tag.location.default: 1"); """

    sql createTblSql

    qt_sql """ desc complext_nested_version ;"""
    String res = sql """ show create table complext_nested_version ;"""
    assertTrue(!res.contains('DECIMALV3'))
    assertTrue(!res.contains('DATEV2'))
    assertTrue(!res.contains('DATETIMEV2'))
}
