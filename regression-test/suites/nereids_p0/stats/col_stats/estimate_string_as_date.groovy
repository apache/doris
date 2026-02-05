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

suite("estimate_string_as_date") {
    sql """
        set fe_debug = true;

        drop table if exists estimate_string_as_date;
        create table estimate_string_as_date (
        col_datetime_6__undef_signed_not_null datetime(6)  not null  ,
        pk int,
        col_date_undef_signed date  null  ,
        col_date_undef_signed_not_null date  not null  ,
        col_datetime_undef_signed datetime  null  ,
        col_datetime_undef_signed_not_null datetime  not null  ,
        col_datetime_3__undef_signed datetime(3)  null  ,
        col_datetime_3__undef_signed_not_null datetime(3)  not null  ,
        col_datetime_6__undef_signed datetime(6)  null  
        ) engine=olap
        DUPLICATE KEY(col_datetime_6__undef_signed_not_null, pk)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");

        insert into estimate_string_as_date(pk,col_date_undef_signed,col_date_undef_signed_not_null,col_datetime_undef_signed,col_datetime_undef_signed_not_null,col_datetime_3__undef_signed,col_datetime_3__undef_signed_not_null,col_datetime_6__undef_signed,col_datetime_6__undef_signed_not_null) 
        values (0,'9999-12-31','2017-03-05 16:53:44','2024-08-03 13:08:30','2006-06-22 11:25:27','2023-01-15 08:32:59','2024-08-03 13:08:30','9999-12-31 23:59:59','9999-12-31'),(1,null,'9999-12-31 23:59:59','2014-08-12','9999-12-31 23:59:59','2008-05-28','9999-12-31','2006-08-09','2018-06-25 02:20:36');

    """

    // no exception when convert date literal back to string literal in ExpresssoinEstimation
    sql """
        with cte1 as (
            select
                col_date_undef_signed AS col_alias18396,
                '2007-11-23 20:06:57' AS col_alias18397,
                col_datetime_3__undef_signed_not_null col_alias18398,
                approx_count_distinct (YEARWEEK(col_date_undef_signed, 6)) col_alias18399
            from
                estimate_string_as_date
            where
                col_date_undef_signed <> "2023-01-15 08:32:59"
            GROUP BY
                col_alias18396,
                col_alias18397,
                col_alias18398
            ORDER BY
                col_alias18396,
                col_alias18397,
                col_alias18398,
                col_alias18399
            LIMIT
                1 OFFSET 4
        )

            select
                *
            from
                cte1
            where
                col_alias18397 >= '2015-01-17 08:32:59';
        """
}