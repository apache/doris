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

suite("test_time_lut") {
    for (def year = 1980; year < 2030; year++) {
        for (def month = 1; month < 13; month++) {
            qt_sql """ select weekofyear('${year}-${month}-1 23:59:59') """
            qt_sql """ select week('${year}-${month}-1 23:59:59') """
            qt_sql """ select yearweek('${year}-${month}-1 23:59:59') """
        }
    }

    def tableName = "test_time_to_sec_function"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                test_datetime datetime NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(test_datetime)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(test_datetime) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2"
            )
        """
    sql """ insert into ${tableName} values ("2019-08-01 13:21:03") """
    sql """ insert into ${tableName} values ("1989-03-21 13:00:00") """
    sql """ insert into ${tableName} values ("2015-03-13 10:30:00") """
    sql """ insert into ${tableName} values ("2015-03-13 12:36:38") """
    sql """ insert into ${tableName} values ("9999-11-11 12:12:00") """
    sql """ insert into ${tableName} values ("1989-03-21 13:11:11") """
    sql """ insert into ${tableName} values ("2013-04-02 15:16:52") """
    sql """ insert into ${tableName} values ("1989-03-21 13:11:00") """
    sql """ insert into ${tableName} values ("2013-04-02 10:16:52") """
    sql """ insert into ${tableName} values ("2023-04-03 19:54:23") """

    qt_sql_time_to_sec """ SELECT test_datetime,  time_to_sec(test_datetime) from ${tableName} order by test_datetime; """
}
