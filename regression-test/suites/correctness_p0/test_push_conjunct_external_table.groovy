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

suite("test_push_conjunct_external_table") {
 sql """ DROP TABLE IF EXISTS dim_server; """
 sql """ admin set frontend config("enable_odbc_mysql_broker_table" = "true")"""

 sql """
     CREATE EXTERNAL TABLE `dim_server` (
    `col1` varchar(50) NOT NULL,
    `col2` varchar(50) NOT NULL
    ) ENGINE=mysql
    PROPERTIES
    (
    "host" = "127.0.0.1",
    "port" = "8239",
    "user" = "mysql_user",
    "password" = "mysql_passwd",
    "database" = "mysql_db_test",
    "table" = "mysql_table_test",
    "charset" = "utf8mb4"
    );
 """

 sql """
     DROP view if exists ads_oreo_sid_report;
 """

 sql """
     create view ads_oreo_sid_report
    (
    `col1` ,
        `col2`
    )
    AS
    select
    tmp.col1,tmp.col2
    from (
    select 'abc' as col1,'def' as col2
    ) tmp
    inner join dim_server ds on tmp.col1 = ds.col1  and tmp.col2 = ds.col2;
 """

 explain {
        sql """select * from ads_oreo_sid_report where col1='abc' and col2='def';"""
        contains "`ds`.`col1` = 'abc'"
        contains "`ds`.`col2` = 'def'"
    }

    sql """ DROP TABLE IF EXISTS dim_server; """
    sql """ DROP view if exists ads_oreo_sid_report; """
}

