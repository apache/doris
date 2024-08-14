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

suite("test_delete_unicode") {
    sql "set enable_unicode_name_support=true;"

    sql """
        CREATE TABLE `table_7298276` (
        `中文列名1` date NOT NULL,
        `中文列名2` int NOT NULL,
        `中文列名3` bigint NOT NULL,
        `中文列名4` largeint NOT NULL,
        INDEX 中文列名2 (`中文列名2`) USING INVERTED,
        INDEX 中文列名4 (`中文列名4`) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(`中文列名1`, `中文列名2`, `中文列名3`)
        DISTRIBUTED BY HASH(`中文列名1`, `中文列名2`, `中文列名3`) BUCKETS 4
        properties("replication_num" = "1");
    """

    sql """ insert into table_7298276 values ('2020-12-12',1,1,1);"""
    qt_sql1 "select * from table_7298276;"
    sql "delete from table_7298276 where 中文列名1 > '2012-08-17' and 中文列名2 > -68 and 中文列名3 in (1,2,3);"
    qt_sql2 "select * from table_7298276;"
}