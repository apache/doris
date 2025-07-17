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
    sql "drop table if exists test_add_sub_diff_ceil_floor_t"
    sql """create table test_add_sub_diff_ceil_floor_t (a int, dt datetime, d date, c varchar(100)) duplicate key(a)
    partition by range(dt) (
            partition p1 values less than ("2017-01-01"),
            partition p2 values less than ("2018-01-01"),
            partition p3 values less than ("2019-01-01"),
            partition p4 values less than ("2020-01-01"),
            partition p5 values less than ("2021-01-01")
    ) distributed by hash(a) properties("replication_num"="1");"""
    sql """INSERT INTO test_add_sub_diff_ceil_floor_t SELECT number,
    date_add('2016-01-01 00:00:00', interval number month),
    cast(date_add('2022-01-01 00:00:00', interval number month) as date), cast(number as varchar(65533)) FROM numbers('number'='55');"""
    sql "INSERT INTO test_add_sub_diff_ceil_floor_t  values(3,null,null,null);"

    // xx_add
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where years_add(dt,1) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where months_add(dt,2) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where days_add(dt,10) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where hours_add(dt,1) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where minutes_add(dt,2) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where seconds_add(dt,10) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where milliseconds_add(dt,2) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where microseconds_add(dt,10) >'2019-01-01' """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    // xx_sub
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where years_sub(dt,1) <='2018-01-01' """
        contains("4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where months_sub(dt,2) <='2018-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where days_sub(dt,10) <='2018-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where hours_sub(dt,1) <='2018-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where minutes_sub(dt,2) <= '2018-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where seconds_sub(dt,10) <= '2018-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where milliseconds_sub(dt,2) <= '2018-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where microseconds_sub(dt,10) <= '2018-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }

    // xx_diff
    // first arg is dt. positive
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where years_diff(dt,'2017-01-01') <2 """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where months_diff(dt,'2017-01-01') <2 """
        contains("partitions=2/5 (p1,p2)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where days_diff(dt,'2017-01-01') <2 """
        contains("partitions=2/5 (p1,p2)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where hours_diff(dt,'2017-01-01') <2 """
        contains("partitions=2/5 (p1,p2)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where minutes_diff(dt,'2017-01-01') <2 """
        contains("partitions=2/5 (p1,p2)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where seconds_diff(dt,'2017-01-01') <2 """
        contains("partitions=2/5 (p1,p2)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where milliseconds_diff(dt,'2017-01-01') <2 """
        contains("partitions=2/5 (p1,p2)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where microseconds_diff(dt,'2017-01-01') <2 """
        contains("partitions=2/5 (p1,p2)")
    }
    // second arg is dt. not positive
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where years_diff('2021-01-01',dt) <2 """
        contains("partitions=2/5 (p4,p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where months_diff('2021-01-01',dt) <2 """
        contains("partitions=1/5 (p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where days_diff('2021-01-01',dt) <2 """
        contains("partitions=1/5 (p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where hours_diff('2021-01-01',dt) <2 """
        contains("partitions=1/5 (p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where minutes_diff('2021-01-01',dt) <2 """
        contains("partitions=1/5 (p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where seconds_diff('2021-01-01',dt) <2 """
        contains("partitions=1/5 (p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where milliseconds_diff('2021-01-01',dt) <2 """
        contains("partitions=1/5 (p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where microseconds_diff('2021-01-01',dt) <2 """
        contains("partitions=1/5 (p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where years_diff('2021-01-01',dt) <=2 """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where months_diff('2020-01-01',dt) >2 """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where days_diff('2020-01-01',dt) >=2 """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }

    // xx_ceil
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where year_ceil(dt) <'2019-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where month_ceil(dt) <'2019-02-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where day_ceil(dt) <'2019-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where hour_ceil(dt) <'2019-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where minute_ceil(dt) <'2019-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where second_ceil(dt) <'2019-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    // xx_ceil with other args
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where year_ceil(dt,5) <'2019-01-01' """
        contains("partitions=1/5 (p1)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where year_ceil(dt,'2013-01-01') <'2019-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where year_ceil(dt,5,'2013-01-01') <'2019-01-01'"""
        contains(" partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where hour_ceil(dt,c) <'2019-01-01' """
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }

    // xx_floor
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where year_floor(dt) <='2019-01-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where month_floor(dt) <='2019-02-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where day_floor(dt) <='2019-01-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where hour_floor(dt) <='2019-01-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where minute_floor(dt) <='2019-01-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where second_floor(dt) <'2019-01-01' """
        contains("partitions=3/5 (p1,p2,p3)")
    }

    // xx_floor with other args
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where month_floor(dt,'2015-01-01') <='2019-02-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where month_floor(dt,5,'2015-01-01') <='2019-02-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where month_floor(dt,5) <='2019-02-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where hour_floor(dt,c,'2015-01-01') <='2019-01-01' """
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }

    // diff nest function
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where years_diff('2021-01-01',month_ceil(hours_add(dt, 1))) <=2 """
        contains("partitions=4/5 (p2,p3,p4,p5)")
    }
    explain  {
        sql "select * from test_add_sub_diff_ceil_floor_t where years_diff('2021-01-01',month_ceil(hours_sub(dt, 1))) <=2"
        contains("partitions=4/5 (p1,p3,p4,p5)")
    }
    // mixed with non-function predicates
    explain {
        sql "select * from test_add_sub_diff_ceil_floor_t where years_diff('2021-01-01',month_ceil(hours_sub(dt, 1))) <=2 and dt>'2019-06-01'"
        contains("partitions=2/5 (p4,p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where years_diff('2021-01-01',month_ceil(hours_sub(dt, 1))) <=2 and date_trunc(dt,'day')>'2019-06-01' """
        contains("partitions=2/5 (p4,p5)")
    }
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where months_diff(months_add(dt,10), '2018-01-01') =2 """
        contains("partitions=1/5 (p2)")
    }

    // hours_add second arg is not literal, so will not do pruning
    explain {
        sql """select * from test_add_sub_diff_ceil_floor_t where hours_add(dt, years_diff(dt,'2018-01-01')) <'2018-01-01' """
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }

    // max
    sql "drop table if exists max_t"
    sql """create table max_t (a int, dt datetime, d date, c varchar(100)) duplicate key(a)
    partition by range(dt) (
            partition p1 values less than ("2017-01-01"),
            partition p2 values less than ("2018-01-01"),
            partition p3 values less than ("2019-01-01"),
            partition p4 values less than ("2020-01-01"),
            partition p5 values less than ("2021-01-01"),
            partition p6 values less than MAXVALUE
    ) distributed by hash(a) properties("replication_num"="1");"""
    sql """INSERT INTO max_t SELECT number,
    date_add('2016-01-01 00:00:00', interval number month),
    cast(date_add('2022-01-01 00:00:00', interval number month) as date), cast(number as varchar(65533)) FROM numbers('number'='100');"""
    sql "INSERT INTO max_t  values(3,null,null,null);"

    explain {
        sql "select * from max_t where years_diff('2021-01-01',month_ceil(hours_add(dt, 1),'1990-01-05')) <=2 ;"
        contains("partitions=5/6 (p2,p3,p4,p5,p6)")
    }
    explain {
        sql "select * from max_t where years_diff('2021-01-01',month_ceil(hours_add(dt, 1),10,'1990-01-05')) <=2 ;"
        contains("partitions=5/6 (p2,p3,p4,p5,p6)")
    }

    explain {
        sql """select * from max_t where years_diff('2021-01-01',month_ceil(hours_add(dt, 1),10,'1990-01-05')) <=2 and dt >'2018-01-01';"""
        contains("partitions=4/6 (p3,p4,p5,p6)")
    }

    explain {
        sql """select * from max_t where months_diff('2021-01-01',month_floor(hours_add(dt, 1),10,'1990-01-05')) <=2;"""
        contains("partitions=3/6 (p1,p5,p6)")
    }

    explain {
        sql """select * from max_t where months_diff('2021-01-01',month_floor(hours_add(dt, 1),12,'1000-01-01')) > 2"""
        contains("partitions=5/6 (p1,p2,p3,p4,p5)")
    }
    explain {
        sql """select * from max_t where months_diff('2021-01-01',month_floor(hours_add(dt, 1),12,'1000-01-01')) > 2 and month_floor(dt) >'2018-01-01' """
        contains("partitions=3/6 (p3,p4,p5)")
    }
    explain {
        sql """select * from max_t where hours_sub(hours_add(dt, 1),1) >'2018-01-01' and days_diff(hours_sub(hours_add(dt, 1),1),'2021-01-01') >2"""
        contains("partitions=1/6 (p6)")
    }

    explain {
        sql """select * from max_t where weeks_add(dt, 1) >'2018-01-01' """
        contains("partitions=5/6 (p2,p3,p4,p5,p6)")
    }
    explain {
        sql """select * from max_t where weeks_sub(dt, 10) >'2018-01-01' """
        contains("partitions=5/6 (p1,p3,p4,p5,p6)")
    }
    explain {
        sql """select * from max_t where weeks_diff(dt, '2018-01-01') >=10"""
        contains("partitions=4/6 (p3,p4,p5,p6)")
    }
    explain {
        sql """select * from max_t where weeks_diff('2018-01-01', dt) <=10"""
        contains("partitions=5/6 (p2,p3,p4,p5,p6)")
    }
    // yearweek
    explain {
        sql """select * from max_t where yearweek(dt) <201902"""
        contains("partitions=4/6 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from max_t where yearweek(dt,1) <201902"""
        contains("partitions=4/6 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from max_t where yearweek(dt,c) <20190206"""
        contains("partitions=6/6 (p1,p2,p3,p4,p5,p6)")
    }

    // yearweek
    explain {
        sql """select * from max_t where yearweek(dt) <20190206"""
        contains("partitions=6/6 (p1,p2,p3,p4,p5,p6)")
    }
    // from_days and unix_timestamp
    explain {
        sql """select * from max_t where unix_timestamp(dt) > 1547838847 """
        contains("partitions=3/6 (p4,p5,p6)")
    }

    sql "drop table if exists partition_int_from_days"
    sql """
    CREATE TABLE `partition_int_from_days` (
      `a` int NULL,
      `b` int NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`, `b`)
    PARTITION BY RANGE(`a`)
    (PARTITION p1 VALUES [("-2147483648"), ("100000")),
    PARTITION p2 VALUES [("100000"), ("738000")),
    PARTITION p3 VALUES [("738000"), ("90000000")),
    PARTITION p4 VALUES [("90000000"), (MAXVALUE)))
    DISTRIBUTED BY HASH(`a`) BUCKETS 10
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    ); """
    sql """
    insert into partition_int_from_days values(100,100),(100022,1002),(738004,33),(90000003,89);
    """
    explain {
        sql """select * from partition_int_from_days where from_days(a)>'2020-07-29' """
        contains("partitions=3/4 (p1,p3,p4)")
    }


    sql "drop table if exists unix_time_t"
    sql """create table unix_time_t (a int, dt datetime, d date, c varchar(100)) duplicate key(a)
    partition by range(dt) (
            partition p1 values less than ("1980-01-01"),
            partition p2 values less than ("2018-01-01"),
            partition p3 values less than ("2039-01-01"),
            partition p4 values less than MAXVALUE
    ) distributed by hash(a) properties("replication_num"="1");"""
    sql """INSERT INTO unix_time_t values(1,'1979-01-01','1979-01-01','abc'),(1,'2012-01-01','2012-01-01','abc'),(1,'2020-01-01','2020-01-01','abc'),(1,'2045-01-01','2045-01-01','abc')"""
    sql "INSERT INTO unix_time_t  values(3,null,null,null);"
    explain {
        sql """ select * from unix_time_t where unix_timestamp(dt) > 1514822400 """
        contains("partitions=2/4 (p3,p4)")
    }
    explain {
        sql """select * from unix_time_t where unix_timestamp(dt) < 2147454847;"""
        contains("partitions=4/4 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from unix_time_t where unix_timestamp(dt) = 2147454847"""
        contains("partitions=2/4 (p3,p4)")
    }
    explain {
        sql """select * from unix_time_t where unix_timestamp(dt) = 2147454847 and dt<'2038-01-01'"""
        contains("partitions=1/4 (p3)")
    }
    explain {
        sql """select * from unix_time_t where unix_timestamp(dt) <=0"""
        contains("partitions=3/4 (p1,p3,p4)")
    }

    explain {
        sql """select * from max_t where year(weeks_add(dt, 1)) >2019"""
        contains("partitions=3/6 (p4,p5,p6)")
    }
    explain {
        sql """select * from max_t where month(weeks_add(dt, 1)) >6"""
        contains("partitions=6/6 (p1,p2,p3,p4,p5,p6)")
    }
    explain {
        sql """select * from max_t where quarter(weeks_sub(dt, 1)) >3"""
        contains("partitions=6/6 (p1,p2,p3,p4,p5,p6)")
    }
    //explain {
    //    sql """select * from max_t where weeks_diff(dt, quarter(weeks_sub(dt, 1))) >'2020-01-01'"""
    //    contains("partitions=6/6 (p1,p2,p3,p4,p5,p6)")
    //}
}
