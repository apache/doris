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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.Assert;

suite("test_ifnull_array") {
    String suiteName = "test_ifnull_array"
    String tableName = "${suiteName}_table"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName}
          `class_code` varchar(50) NULL,
          `student_code` varchar(50) NULL,
          `adviser_code` varchar(50) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`class_code`)
        DISTRIBUTED BY HASH(`class_code`) BUCKETS 10
        PROPERTIES (
        'replication_num' = '1'
        );
        """
    sql """
            insert into ${tableName} values ('1','1','1');
        """
    qt_select "select (select array_distinct(array_agg(br.class_code)) from ${tableName} br where br.student_code = brv.student_code) as all_class_codes from ${tableName} brv;"
    sql """drop table if exists `${tableName}`"""
}
