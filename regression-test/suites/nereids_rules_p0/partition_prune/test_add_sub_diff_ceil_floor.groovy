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

suite("test_add_sub_diff_ceil_floor") {
    sql "set disable_nereids_rules='REWRITE_FILTER_EXPRESSION'"
    sql "drop table if exists always_mono_func"
    sql """create table always_mono_func (a int, dt datetime, d date, c varchar(100)) duplicate key(a)
    partition by range(dt) (
            partition p1 values less than ("2017-01-01"),
            partition p2 values less than ("2018-01-01"),
            partition p3 values less than ("2019-01-01"),
            partition p4 values less than ("2020-01-01"),
            partition p5 values less than ("2021-01-01")
    ) distributed by hash(a) properties("replication_num"="1");"""
    sql """INSERT INTO always_mono_func SELECT number,
    date_add('2016-01-01 00:00:00', interval number month),
    cast(date_add('2022-01-01 00:00:00', interval number month) as date), cast(number as varchar(65533)) FROM numbers('number'='55');"""
    sql "INSERT INTO always_mono_func  values(3,null,null,null);"

    // xx_add
    explain {
        sql """select * from always_mono_func where years_add(dt,1) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where months_add(dt,2) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where days_add(dt,10) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where hours_add(dt,1) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where minutes_add(dt,2) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where seconds_add(dt,10) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where milliseconds_add(dt,2) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where microseconds_add(dt,10) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    // xx_sub
    explain {
        sql """select * from always_mono_func where years_sub(dt,1) <='2018-01-01' """
        contains("4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where months_sub(dt,2) <='2018-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where days_sub(dt,10) <='2018-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where hours_sub(dt,1) <='2018-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where minutes_sub(dt,2) <= '2018-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where seconds_sub(dt,10) <= '2018-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where milliseconds_sub(dt,2) <= '2018-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where microseconds_sub(dt,10) <= '2018-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }

    // xx_diff
    // first arg is dt. positive
    explain {
        sql """select * from always_mono_func where years_diff(dt,'2017-01-01') <2 """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where months_diff(dt,'2017-01-01') <2 """
        contains("partitions=2/5 (p1,p2)")
    }
    explain {
        sql """select * from always_mono_func where days_diff(dt,'2017-01-01') <2 """
        contains("partitions=2/5 (p1,p2)")
    }
    explain {
        sql """select * from always_mono_func where hours_diff(dt,'2017-01-01') <2 """
        contains("partitions=2/5 (p1,p2)")
    }
    explain {
        sql """select * from always_mono_func where minutes_diff(dt,'2017-01-01') <2 """
        contains("partitions=2/5 (p1,p2)")
    }
    explain {
        sql """select * from always_mono_func where seconds_diff(dt,'2017-01-01') <2 """
        contains("partitions=2/5 (p1,p2)")
    }
    explain {
        sql """select * from always_mono_func where milliseconds_diff(dt,'2017-01-01') <2 """
        contains("partitions=2/5 (p1,p2)")
    }
    explain {
        sql """select * from always_mono_func where microseconds_diff(dt,'2017-01-01') <2 """
        contains("partitions=2/5 (p1,p2)")
    }
    // second arg is dt. not positive
    explain {
        sql """select * from always_mono_func where years_diff('2021-01-01',dt) <2 """
        contains("partitions=2/5 (p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where months_diff('2021-01-01',dt) <2 """
        contains("partitions=1/5 (p5)")
    }
    explain {
        sql """select * from always_mono_func where days_diff('2021-01-01',dt) <2 """
        contains("partitions=1/5 (p5)")
    }
    explain {
        sql """select * from always_mono_func where hours_diff('2021-01-01',dt) <2 """
        contains("partitions=1/5 (p5)")
    }
    explain {
        sql """select * from always_mono_func where minutes_diff('2021-01-01',dt) <2 """
        contains("partitions=1/5 (p5)")
    }
    explain {
        sql """select * from always_mono_func where seconds_diff('2021-01-01',dt) <2 """
        contains("partitions=1/5 (p5)")
    }
    explain {
        sql """select * from always_mono_func where milliseconds_diff('2021-01-01',dt) <2 """
        contains("partitions=1/5 (p5)")
    }
    explain {
        sql """select * from always_mono_func where microseconds_diff('2021-01-01',dt) <2 """
        contains("partitions=1/5 (p5)")
    }
    explain {
        sql """select * from always_mono_func where years_diff('2021-01-01',dt) <=2 """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where months_diff('2020-01-01',dt) >2 """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where days_diff('2020-01-01',dt) >=2 """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }

    // xx_ceil
    explain {
        sql """select * from always_mono_func where year_ceil(dt) <'2019-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where month_ceil(dt) <'2019-02-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where day_ceil(dt) <'2019-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where hour_ceil(dt) <'2019-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where minute_ceil(dt) <'2019-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where second_ceil(dt) <'2019-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    // xx_ceil with other args
    explain {
        sql """select * from always_mono_func where year_ceil(dt,5) <'2019-01-01' """
        contains("partitions=1/5 (p1)")
    }
    explain {
        sql """select * from always_mono_func where year_ceil(dt,'2013-01-01') <'2019-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where year_ceil(dt,5,'2013-01-01') <'2019-01-01'"""
        contains(" partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where hour_ceil(dt,c) <'2019-01-01' """
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }

    // xx_floor
    explain {
        sql """select * from always_mono_func where year_floor(dt) <='2019-01-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where month_floor(dt) <='2019-02-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where day_floor(dt) <='2019-01-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where hour_floor(dt) <='2019-01-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where minute_floor(dt) <='2019-01-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where second_floor(dt) <'2019-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }

    // xx_floor with other args
    explain {
        sql """select * from always_mono_func where month_floor(dt,'2015-01-01') <='2019-02-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where month_floor(dt,5,'2015-01-01') <='2019-02-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where month_floor(dt,5) <='2019-02-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where hour_floor(dt,c,'2015-01-01') <='2019-01-01' """
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }

    // diff nest function
    explain {
        sql """select * from always_mono_func where years_diff('2021-01-01',month_ceil(hours_add(dt, 1))) <=2 """
        contains("partitions=4/5 (p2,p3,p4,p5)")
    }
    explain  {
        sql "select * from always_mono_func where years_diff('2021-01-01',month_ceil(hours_sub(dt, 1))) <=2"
        contains("partitions=3/5 (p3,p4,p5)")
    }
    // mixed with non-function predicates
    explain {
        sql "select * from always_mono_func where years_diff('2021-01-01',month_ceil(hours_sub(dt, 1))) <=2 and dt>'2019-06-01'"
        contains("partitions=2/5 (p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where years_diff('2021-01-01',month_ceil(hours_sub(dt, 1))) <=2 and date_trunc(dt,'day')>'2019-06-01' """
        contains("partitions=2/5 (p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where months_diff(months_add(dt,10), '2018-01-01') =2 """
        contains("partitions=1/5 (p2)")
    }

    // hours_add second arg is not literal, so will not do pruning
    explain {
        sql """select * from always_mono_func where hours_add(dt, years_diff(dt,'2018-01-01')) <'2018-01-01' """
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }
}