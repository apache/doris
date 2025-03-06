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

suite("month_quarter_cast_in_prune") {
    sql "drop table if exists test_month;"
    sql """create table test_month(a int,dt datetime(3))
    partition by range(dt) (
            partition p1 values less than ('2021-01-01'),
            partition p2 values less than ('2021-02-01'),
            partition p3 values less than ('2021-03-01'),
            partition p4 values less than ('2021-07-01'),
            partition p5 values less than ('2022-02-01'),
            partition p6 values less than ('2022-02-25'),
            partition p7 values less than ('2022-02-26 12:00:00'),
            partition p8 values less than ('2022-02-26 12:10:30'),
            partition p9 values less than ('2022-02-26 12:10:59'),
            partition p10 values less than ('2022-02-26 12:10:59.342'),
            partition p11 values less than (MAXVALUE)
    )
    distributed by hash(a)
    properties("replication_num"="1");"""

    sql """insert into test_month values(1,'2020-01-01'),(1,'2021-01-31'),(1,'2021-02-11'),(1,'2021-03-31'),(1,'2021-07-10'),(1,'2022-02-10'),(1,'2022-02-26'),
    (1,'2022-02-26 12:09:00'),(1,'2022-02-26 12:10:01'),(1,'2022-03-03'),(1,'2022-02-26 12:10:41'),(1,'2022-02-26 12:10:59.123');"""

    explain {
        sql "select * from test_month where month(dt)<3"
        contains("partitions=10/11 (p1,p2,p3,p5,p6,p7,p8,p9,p10,p11)")
    };
    explain {
        sql "select * from test_month where month(dt)>3"
        contains("partitions=4/11 (p1,p4,p5,p11)")
    };
    explain {
        sql "select * from test_month where quarter(dt)>1"
        contains("partitions=4/11 (p1,p4,p5,p11)")
    };
    explain {
        sql "select * from test_month where day(dt)>27"
        contains("partitions=6/11 (p1,p2,p3,p4,p5,p11)")
    };
    explain {
        sql "select * from test_month where hour(dt)<10"
        contains("partitions=8/11 (p1,p2,p3,p4,p5,p6,p7,p11)")
    };
    explain {
        sql "select * from test_month where second(dt)<20"
        contains("partitions=9/11 (p1,p2,p3,p4,p5,p6,p7,p8,p11)")
    };
    explain {
        sql "select * from test_month where microsecond(dt)>500000"
        contains("partitions=10/11 (p1,p2,p3,p4,p5,p6,p7,p8,p9,p11)")
    }
    explain {
        sql "select * from test_month where dayofyear(dt)>150"
        contains("partitions=4/11 (p1,p4,p5,p11)")
    };
    explain {
        sql "select * from test_month where dayofmonth(dt)<25"
        contains("partitions=7/11 (p1,p2,p3,p4,p5,p6,p11)")
    };
    explain {
        sql "select * from test_month where weekofyear(dt)>40"
        contains("partitions=11/11 (p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11)")
    };
    explain {
        sql "select * from test_month where cast(dt as date) > '2022-02-26'"
        contains("partitions=1/11 (p11)")
    }

    explain {
        sql "select * from test_month where month(dt)>3 and dt>'2021-01-02'"
        contains("partitions=3/11 (p4,p5,p11)")
    }
    explain {
        sql "select * from test_month where quarter(dt)>1 or cast(dt as date) > '2022-02-26'"
        contains("partitions=4/11 (p1,p4,p5,p11)")
    }
    explain {
        sql "select * from test_month where day(dt)>80"
        contains("partitions=6/11 (p1,p2,p3,p4,p5,p11)")
    }
    explain {
        sql "select * from test_month where hour(dt)<=100;"
        contains("partitions=11/11 (p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11)")
    }
    explain {
        sql "select * from test_month where dayofyear(dt)>150 or dt='2021-02-01'"
        contains("partitions=5/11 (p1,p3,p4,p5,p11)")
    }
    explain {
        sql "select * from test_month where dayofyear(dt)>150 or dt!='2021-02-01'"
        contains("partitions=11/11 (p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11)")
    }
    explain {
        sql "select * from test_month where dayofmonth(dt)<25 and dt>='2021-07-01'"
        contains("partitions=3/11 (p5,p6,p11)")
    }
    explain {
        sql "select * from test_month where dayofmonth(dt)=24"
        contains("partitions=7/11 (p1,p2,p3,p4,p5,p6,p11)")
    }

    sql "drop table if exists monotonic_function_t"
    sql """create table monotonic_function_t (a bigint, dt datetime, d date, c varchar(100)) duplicate key(a)
    partition by range(a) (
            partition p1 values less than ("100000"),
            partition p2 values less than ("1000000009999"),
            partition p3 values less than ("1000000009999999"),
            partition p4 values less than MAXVALUE
    ) distributed by hash(a) properties("replication_num"="1");"""
    sql """INSERT INTO monotonic_function_t values(10000,'1979-01-01','1979-01-01','abc'),(100000009999,'2012-01-01','2012-01-01','abc'),(100000009999999,'2020-01-01','2020-01-01','abc'),(10000000099999999,'2045-01-01','2045-01-01','abc')"""

    explain {
        sql """select * from monotonic_function_t where from_second(a) < '2001-09-09 12:33:19' """
        contains("partitions=4/4 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from monotonic_function_t where from_second(a) > '2001-09-09 12:33:19' """
        contains("partitions=4/4 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from monotonic_function_t where from_millisecond(a) < '2001-09-09 12:33:19' """
        contains("partitions=4/4 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from monotonic_function_t where from_millisecond(a) > '2001-09-09 12:33:19' """
        contains("partitions=3/4 (p1,p3,p4)")
    }
    explain {
        sql """select * from monotonic_function_t where from_microsecond(a) < '2000-09-09 12:33:19' """
        contains("partitions=3/4 (p1,p2,p3)")
    }
    explain {
        sql """select * from monotonic_function_t where from_microsecond(a) > '2002-09-09 12:33:19' """
        contains("partitions=2/4 (p1,p4)")
    }

    // test makedate
    sql "drop table if exists makedate_t"
    sql """create table makedate_t(a int, b int) partition by range(a)(
            partition p1 values less than ("2"),
            partition p2 values less than ("200"),
            partition p3 values less than("1000"),
            partition p4 values less than ("2100"),
            partition p5 values less than ("3500"),
            partition p6 values less than("5003"),
            partition p7 values less than(MAXVALUE)
    )
    distributed by hash(a) properties("replication_num"="1");"""

    sql "insert into makedate_t values(1,2),(100, 9),(500,10),(2000,101),(3000,103),(5000,101),(6000,1)"

    explain {
        sql "select * from makedate_t where makedate(a,10)<'2024-01-01'"
        contains("partitions=4/7 (p1,p2,p3,p4)")
    };
    explain {
        sql "select *  from makedate_t where makedate(2000,a)>'2024-01-01'"
        contains("partitions=2/7 (p1,p7)")
    };
    explain {
        sql "select *  from makedate_t where makedate(2023,a)>'2024-01-01'"
        contains("partitions=6/7 (p1,p3,p4,p5,p6,p7)")
    };
    explain {
        sql "select *  from makedate_t where makedate(2023,b)>'2024-01-01'"
        contains("partitions=7/7 (p1,p2,p3,p4,p5,p6,p7)")
    };
    explain {
        sql "select *  from makedate_t where makedate(a,b)>'2024-01-01'"
        contains("partitions=7/7 (p1,p2,p3,p4,p5,p6,p7)")
    };

    sql "drop table if exists from_unixtime_t;"
    sql """create table from_unixtime_t(a bigint, b int) partition by range(a)(
            partition p1 values less than ("50000000"),
            partition p2 values less than ("990000000"),
            partition p3 values less than("2000000000"),
            partition p4 values less than ("2500000000"),
            partition p5 values less than(MAXVALUE)
    )
    distributed by hash(a) properties("replication_num"="1");"""
    sql "insert into from_unixtime_t values(1000,10),(50000001,11),(990000001,12),(2000000002,13),(2500000003,14);"

    explain {
        sql """select * from from_unixtime_t where from_unixtime(a,"%Y-%m-%d %T") <'2001-05-16 16:00:00'"""
        contains("partitions=2/5 (p1,p2)")
    }
    explain {
        sql """select * from from_unixtime_t where from_unixtime(a,"%Y-%m-%d %T") <='2001-05-16 16:00:00'"""
        contains("partitions=3/5 (p1,p2,p3)")
    }

    explain {
        sql """select * from from_unixtime_t where from_unixtime(a,"yyyyMMdd") < '20330518'"""
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from from_unixtime_t where from_unixtime(a,"yyyyMMdd") > '20330518'"""
        contains("partitions=3/5 (p1,p4,p5)")

    }
    explain {
        sql """select * from from_unixtime_t where from_unixtime(a,"%yyyyMMdd") > '20330518'"""
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }
    explain {
        sql """select * from from_unixtime_t where from_unixtime(a,"yyyyMMdd %T") <='2001-05-16 16:00:00'"""
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }
}
