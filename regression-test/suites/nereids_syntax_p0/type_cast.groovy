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

suite("type_cast") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    sql """ drop table if exists test_table2;"""
    sql """
        CREATE TABLE `test_table2`
        (
            `day` date
        ) ENGINE = OLAP DUPLICATE KEY(`day`)
        DISTRIBUTED BY HASH(`day`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """insert into test_table2 values('2020-05-25');"""

    def ret = sql"""explain verbose select * from test_table2 where day > CONVERT_tz('2020-05-25 00:00:00', 'Asia/Shanghai', 'Asia/Shanghai');"""
    // assertTrue(ret.toString().contains("CAST(day[#0] AS DATETIMEV2(6))")) 
    // https://github.com/apache/doris/pull/20863 
    // Since we have handled datetime string literals, we don't need to use cast for conversion.
    qt_sql """select count(*) from test_table2 where 'a' = 'a';"""
    qt_sql """select count(*) from test_table2 where cast('2020-01-01' as date) = cast('2020-01-01' as date);"""


    test {
        sql("""select id
            from( select 1 id ) a
            where case when id > 0 then 2 else 'abc' end = '2'""")
        result([[1]])
    }

    test {
        sql("select '12' = id from (select '1' as id)a")
        result([[false]])
    }

    sql """
        drop table if exists test_time_cast_to_string;
    """

    sql """
        create table test_time_cast_to_string (id int) distributed by random properties('replication_num'='1');
    """

    sql """
        select concat('a', sec_to_time(id)) from test_time_cast_to_string;
    """
}
