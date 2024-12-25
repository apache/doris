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
            partition p10 values less than (MAXVALUE)
    )
    distributed by hash(a)
    properties("replication_num"="1");"""

    sql """insert into test_month values(1,'2020-01-01'),(1,'2021-01-31'),(1,'2021-02-11'),(1,'2021-03-31'),(1,'2021-07-10'),(1,'2022-02-10'),(1,'2022-02-26'),
    (1,'2022-02-26 12:09:00'),(1,'2022-02-26 12:10:01'),(1,'2022-03-03'),(1,'2022-02-26 12:10:41');"""

    explain {
        sql "select * from test_month where month(dt)<3"
        contains("partitions=9/10 (p1,p2,p3,p5,p6,p7,p8,p9,p10)")
    };
    explain {
        sql "select * from test_month where month(dt)>3"
        contains("partitions=4/10 (p1,p4,p5,p10)")
    };
    explain {
        sql "select * from test_month where quarter(dt)>1"
        contains("partitions=4/10 (p1,p4,p5,p10)")
    };
    explain {
        sql "select * from test_month where day(dt)>27"
        contains("partitions=6/10 (p1,p2,p3,p4,p5,p10)")
    };
    explain {
        sql "select * from test_month where hour(dt)<10"
        contains("partitions=8/10 (p1,p2,p3,p4,p5,p6,p7,p10)")
    };
    explain {
        sql "select * from test_month where second(dt)<20"
        contains("partitions=9/10 (p1,p2,p3,p4,p5,p6,p7,p8,p10)")
    };
    explain {
        sql "select * from test_month where dayofyear(dt)>150"
        contains("partitions=4/10 (p1,p4,p5,p10)")
    };
    explain {
        sql "select * from test_month where dayofmonth(dt)<25"
        contains("partitions=7/10 (p1,p2,p3,p4,p5,p6,p10)")
    };
    explain {
        sql "select * from test_month where weekofyear(dt)>40"
        contains("partitions=4/10 (p1,p2,p5,p10)")
    };
    explain {
        sql "select * from test_month where cast(dt as date) > '2022-02-26'"
        contains("partitions=4/10 (p7,p8,p9,p10)")
    }

    explain {
        sql "select * from test_month where month(dt)>3 and dt>'2021-01-02'"
        contains("partitions=3/10 (p4,p5,p10)")
    }
    explain {
        sql "select * from test_month where quarter(dt)>1 or cast(dt as date) > '2022-02-26'"
        contains("partitions=7/10 (p1,p4,p5,p7,p8,p9,p10)")
    }
    explain {
        sql "select * from test_month where day(dt)>80"
        contains("partitions=6/10 (p1,p2,p3,p4,p5,p10)")
    }
    explain {
        sql "select * from test_month where hour(dt)<=100;"
        contains("partitions=10/10 (p1,p2,p3,p4,p5,p6,p7,p8,p9,p10)")
    }
    explain {
        sql "select * from test_month where dayofyear(dt)>150 or dt='2021-02-01'"
        contains("partitions=5/10 (p1,p3,p4,p5,p10)")
    }
    explain {
        sql "select * from test_month where dayofyear(dt)>150 or dt!='2021-02-01'"
        contains("partitions=10/10 (p1,p2,p3,p4,p5,p6,p7,p8,p9,p10)")
    }
    explain {
        sql "select * from test_month where dayofmonth(dt)<25 and dt>='2021-07-01'"
        contains("partitions=3/10 (p5,p6,p10)")
    }
    explain {
        sql "select * from test_month where dayofmonth(dt)=24"
        contains("partitions=7/10 (p1,p2,p3,p4,p5,p6,p10)")
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
        contains("partitions=3/4 (p2,p3,p4)")
    }
    explain {
        sql """select * from monotonic_function_t where from_millisecond(a) < '2001-09-09 12:33:19' """
        contains("partitions=4/4 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from monotonic_function_t where from_millisecond(a) > '2001-09-09 12:33:19' """
        contains("partitions=2/4 (p3,p4)")
    }
    explain {
        sql """select * from monotonic_function_t where from_microsecond(a) < '2000-09-09 12:33:19' """
        contains("partitions=3/4 (p1,p2,p3)")
    }
    explain {
        sql """select * from monotonic_function_t where from_microsecond(a) > '2002-09-09 12:33:19' """
        contains("partitions=2/4 (p1,p4)")
    }
}