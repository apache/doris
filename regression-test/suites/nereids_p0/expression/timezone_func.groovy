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

suite("test_timezone_func") {

    String tableName = "timezone_func_table"

    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    def errorMessage = ""


    qt_sql_time_zone_8 "set time_zone='+8:00';"

    qt_sql_date_1 "select Date(datetime '2020-01-01 23:00:00+06:00','Asia/Shanghai');"
    qt_sql_date_2 "select Date('2020-01-01 23:00:00+06:00','Asia/Shanghai');"
    qt_sql_date_3 "select Date('2020-01-01 23:00:00','Asia/Shanghai');"
    qt_sql_date_4 "select Date('2020-01-01','Asia/Shanghai');"
    qt_sql_date_null_1 "select Date('2020-01-01',null);"
    qt_sql_date_null_2 "select Date(null,'Asia/Shanghai');"
    qt_sql_date_null_3 "select Date(null,null);"
    qt_sql_date_null_4 "select Date(datetime '2020-01-01 23:00:00+06:00','xxx');"

    errorMessage = "errCode = 2"
    expectExceptionLike ({sql "select Date('2020-01-01 23:99:00+06:00','Asia/Shanghai')"}, errorMessage)
    expectExceptionLike ({sql "select Date('2020-01-45 23:00:00','Asia/Shanghai')"}, errorMessage)
    expectExceptionLike ({sql "select Date('2020-14-01','Asia/Shanghai')"}, errorMessage)


    qt_sql_timestamp_1 "select TIMESTAMP(datetime '2020-01-01 23:00:00+06:00','Asia/Shanghai');"
    qt_sql_timestamp_2 "select TIMESTAMP('2020-01-01 23:00:00+06:00','Asia/Shanghai');"
    qt_sql_timestamp_3 "select TIMESTAMP('2020-01-01 23:00:00','Asia/Shanghai');"
    qt_sql_timestamp_4 "select TIMESTAMP('2020-01-01','Asia/Shanghai');"
    qt_sql_timestamp_null_1 "select TIMESTAMP('2020-01-01',null);"
    qt_sql_timestamp_null_2 "select TIMESTAMP(null,'Asia/Shanghai');"
    qt_sql_timestamp_null_3 "select TIMESTAMP(null,null);"
    qt_sql_timestamp_null_4 "select TIMESTAMP(datetime '2020-01-01 23:00:00+06:00','xxx');"

    errorMessage = "errCode = 2"
    expectExceptionLike ({sql "select TIMESTAMP('2020-01-01 23:99:00+06:00','Asia/Shanghai')"}, errorMessage)
    expectExceptionLike ({sql "select TIMESTAMP('2020-01-45 23:00:00','Asia/Shanghai')"}, errorMessage)
    expectExceptionLike ({sql "select TIMESTAMP('2020-14-01','Asia/Shanghai')"}, errorMessage)



    qt_sql_time_zone_10 "set time_zone='+10:00';"

    qt_sql_date_1 "select Date(datetime '2020-01-01 23:00:00+06:00','Asia/Shanghai');"
    qt_sql_date_2 "select Date('2020-01-01 23:00:00+06:00','Asia/Shanghai');"
    qt_sql_date_3 "select Date('2020-01-01 23:00:00','Asia/Shanghai');"
    qt_sql_date_4 "select Date('2020-01-01','Asia/Shanghai');"
    qt_sql_date_null_1 "select Date('2020-01-01',null);"
    qt_sql_date_null_2 "select Date(null,'Asia/Shanghai');"
    qt_sql_date_null_3 "select Date(null,null);"
    qt_sql_date_null_4 "select Date(datetime '2020-01-01 23:00:00+06:00','xxx');"

    errorMessage = "errCode = 2"
    expectExceptionLike ({sql "select Date('2020-01-01 23:99:00+06:00','Asia/Shanghai')"}, errorMessage)
    expectExceptionLike ({sql "select Date('2020-01-45 23:00:00','Asia/Shanghai')"}, errorMessage)
    expectExceptionLike ({sql "select Date('2020-14-01','Asia/Shanghai')"}, errorMessage)


    qt_sql_timestamp_1 "select TIMESTAMP(datetime '2020-01-01 23:00:00+06:00','Asia/Shanghai');"
    qt_sql_timestamp_2 "select TIMESTAMP('2020-01-01 23:00:00+06:00','Asia/Shanghai');"
    qt_sql_timestamp_3 "select TIMESTAMP('2020-01-01 23:00:00','Asia/Shanghai');"
    qt_sql_timestamp_4 "select TIMESTAMP('2020-01-01','Asia/Shanghai');"
    qt_sql_timestamp_null_1 "select TIMESTAMP('2020-01-01',null);"
    qt_sql_timestamp_null_2 "select TIMESTAMP(null,'Asia/Shanghai');"
    qt_sql_timestamp_null_3 "select TIMESTAMP(null,null);"
    qt_sql_timestamp_null_4 "select TIMESTAMP(datetime '2020-01-01 23:00:00+06:00','xxx');"

    errorMessage = "errCode = 2"
    expectExceptionLike ({sql "select TIMESTAMP('2020-01-01 23:99:00+06:00','Asia/Shanghai')"}, errorMessage)
    expectExceptionLike ({sql "select TIMESTAMP('2020-01-45 23:00:00','Asia/Shanghai')"}, errorMessage)
    expectExceptionLike ({sql "select TIMESTAMP('2020-14-01','Asia/Shanghai')"}, errorMessage)

    sql "drop table if exists ${tableName};"

    sql """
    create table ${tableName} (k int, z varchar(100), t datetime(2), t_notnull datetime)
        ENGINE=OLAP
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 2 properties("replication_num" = "1",
                "disable_auto_compaction" = "true"); 
    """       


    // 1 test empty table
    qt_sql_date_empty_1 "select Date(t,'Asia/Shanghai') from ${tableName};"
    qt_sql_date_empty_2 "select Date(t,null) from ${tableName};"
    qt_sql_date_empty_3 "select Date(t,'xxx') from ${tableName};"
    qt_sql_date_empty_4 "select Date(t, z) from ${tableName};"
    qt_sql_timestamp_empty_1 "select TIMESTAMP(t,'Asia/Shanghai') from ${tableName};"
    qt_sql_timestamp_empty_2 "select TIMESTAMP(t,null) from ${tableName};"
    qt_sql_timestamp_empty_3 "select TIMESTAMP(t,'xxx') from ${tableName};"
    qt_sql_timestamp_empty_4 "select TIMESTAMP(t, z) from ${tableName};"

    sql """
    insert into ${tableName} values(1, 'Asia/Shanghai', '2020-01-01 23:00:00.33', '2020-01-01 23:00:00'),
      (2, 'America/Los_Angeles','2025-01-01 11:11:11.11', '2025-01-01 11:11:11'),
      (3, 'xxxx','2025-01-01 11:11:11.11', '2025-01-01 11:11:11'),
      (4,'America/Los_Angeles', null, '2020-01-01 23:00:00');
    """
    // 2 test table with data

    qt_sql_time_zone_8 "set time_zone='+8:00';"
    // column null
    order_qt_sql_date_1 "select Date(t,'Asia/Shanghai') from ${tableName};"
    order_qt_sql_date_2 "select Date(t,null) from ${tableName};"
    order_qt_sql_date_3 "select Date(t,'xxx') from ${tableName};"
    order_qt_sql_date_4 "select Date(t, z) from ${tableName};"
    order_qt_sql_timestamp_1 "select TIMESTAMP(t,'Asia/Shanghai') from ${tableName};"
    order_qt_sql_timestamp_2 "select TIMESTAMP(t,null) from ${tableName};"
    order_qt_sql_timestamp_3 "select TIMESTAMP(t,'xxx') from ${tableName};"
    order_qt_sql_timestamp_4 "select TIMESTAMP(t, z) from ${tableName};"

    // column not null
    order_qt_sql_date_1 "select Date(t_notnull,'Asia/Shanghai') from ${tableName};"
    order_qt_sql_date_2 "select Date(t_notnull,null) from ${tableName};"
    order_qt_sql_date_3 "select Date(t_notnull,'xxx') from ${tableName};"
    order_qt_sql_date_4 "select Date(t_notnull, z) from ${tableName};"
    order_qt_sql_timestamp_1 "select TIMESTAMP(t_notnull,'Asia/Shanghai') from ${tableName};"
    order_qt_sql_timestamp_2 "select TIMESTAMP(t_notnull,null) from ${tableName};"
    order_qt_sql_timestamp_3 "select TIMESTAMP(t_notnull,'xxx') from ${tableName};"
    order_qt_sql_timestamp_4 "select TIMESTAMP(t_notnull, z) from ${tableName};"


    qt_sql_time_zone_10 "set time_zone='+10:00';"
    // column null
    order_qt_sql_date_1 "select Date(t,'Asia/Shanghai') from ${tableName};"
    order_qt_sql_date_2 "select Date(t,null) from ${tableName};"
    order_qt_sql_date_3 "select Date(t,'xxx') from ${tableName};"
    order_qt_sql_date_4 "select Date(t, z) from ${tableName};"
    order_qt_sql_timestamp_1 "select TIMESTAMP(t,'Asia/Shanghai') from ${tableName};"
    order_qt_sql_timestamp_2 "select TIMESTAMP(t,null) from ${tableName};"
    order_qt_sql_timestamp_3 "select TIMESTAMP(t,'xxx') from ${tableName};"
    order_qt_sql_timestamp_4 "select TIMESTAMP(t, z) from ${tableName};"

    // column not null
    order_qt_sql_date_1 "select Date(t_notnull,'Asia/Shanghai') from ${tableName};"
    order_qt_sql_date_2 "select Date(t_notnull,null) from ${tableName};"
    order_qt_sql_date_3 "select Date(t_notnull,'xxx') from ${tableName};"
    order_qt_sql_date_4 "select Date(t_notnull, z) from ${tableName};"
    order_qt_sql_timestamp_1 "select TIMESTAMP(t_notnull,'Asia/Shanghai') from ${tableName};"
    order_qt_sql_timestamp_2 "select TIMESTAMP(t_notnull,null) from ${tableName};"
    order_qt_sql_timestamp_3 "select TIMESTAMP(t_notnull,'xxx') from ${tableName};"
    order_qt_sql_timestamp_4 "select TIMESTAMP(t_notnull, z) from ${tableName};"
      
    
    //clean
    sql "drop table ${tableName};"
}

