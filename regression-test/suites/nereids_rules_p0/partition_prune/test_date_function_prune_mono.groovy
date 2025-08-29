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

suite("test_date_prune_mono") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "drop table if exists mal_test_partition_range5_date_mono"
    sql"""
        CREATE TABLE `mal_test_partition_range5_date_mono` (
          `a` INT NULL,
          `b` datetime not NULL,
          `c` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`, `b`, `c`)
        PARTITION BY RANGE(`b`)
        (PARTITION p1 VALUES [("2020-01-05 10:00:00"), ("2020-01-09 10:00:00")),
        PARTITION p2 VALUES [("2020-01-09 10:00:00"), ("2020-01-13 10:00:00")),
        PARTITION p3 VALUES [("2020-01-13 10:00:00"), ("2020-01-19 10:00:00")))
        DISTRIBUTED BY HASH(`a`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );"""
    sql """insert into mal_test_partition_range5_date_mono values(1,"2020-01-09 09:00:00",4),(1,"2020-01-09 11:00:00",4),
        (1,"2020-01-13 11:00:00",4),(1,"2020-01-13 09:00:00",4)"""
    // > >= < <= = <=>
    explain {
        sql """select * from mal_test_partition_range5_date_mono where date(b)<="2020-01-08" """
        contains("partitions=1/3 (p1)")
    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where date(b)<"2020-01-08" """
        contains("partitions=1/3 (p1)")
    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where "2020-01-08">=date(b)"""
        contains("partitions=1/3 (p1)")
    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where "2020-01-08" > date(b)"""
        contains("partitions=1/3 (p1)")
    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where date(b)="2020-01-08" """
        contains("partitions=1/3 (p1)")
    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where "2020-01-08" = date(b)"""
        contains("partitions=1/3 (p1)")
    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where date(b)>="2020-01-14" """
        contains("partitions=1/3 (p3)")
    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where date(b)>"2020-01-14" """
        contains("partitions=1/3 (p3)")
    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where "2020-01-14"<=date(b)"""
        contains("partitions=1/3 (p3)")
    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where "2020-01-14" < date(b)"""
        contains("partitions=1/3 (p3)")
    }

    explain {
        sql """select * from mal_test_partition_range5_date_mono where  date(b) in ("2020-01-13")"""
        contains("partitions=2/3 (p2,p3)")
    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where "2020-01-14" <=> date(b)"""
        contains("partitions=1/3 (p3)")
    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where  date(b) <=>"2020-01-14" """
        contains("partitions=1/3 (p3)")
    }

    // and or
    explain {
        sql """select * from mal_test_partition_range5_date_mono where date(b)>"2020-01-09"  and date(b) <"2020-01-13" """
        contains("partitions=1/3 (p2)")
    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where date(b)>"2020-01-09"  or date(b) <"2020-01-13" """
        contains("partitions=3/3 (p1,p2,p3)")
    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where date(b)>"2020-01-14"  or date(b) <"2020-01-06" """
        contains("partitions=2/3 (p1,p3)")
    }

    explain {
        sql """select * from mal_test_partition_range5_date_mono where date(b) between "2020-01-09"  and "2020-01-13" """
        contains("partitions=3/3 (p1,p2,p3)")

    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where date(b) between "2020-01-10"  and "2020-01-14" """
        contains("partitions=2/3 (p2,p3)")

    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where date(b) between "2020-01-10"  and "2020-01-12" """
        contains("partitions=1/3 (p2)")
    }

    // test not
    for (int i = 0; i < 2; i++) {
        if (i == 0) {
            // forbid rewrite not a>1 to a<=1
            sql "set disable_nereids_rules = 'REWRITE_FILTER_EXPRESSION'"
        } else {
            sql "set disable_nereids_rules = ''"
        }
        explain {
            sql """select * from mal_test_partition_range5_date_mono where not date(b)<="2020-01-14" """
            contains("partitions=1/3 (p3)")
        }
        explain {
            sql """ select * from mal_test_partition_range5_date_mono where not date(b)<"2020-01-14" """
            contains("partitions=1/3 (p3)")
        }
        explain {
            sql """select * from mal_test_partition_range5_date_mono where not "2020-01-14">=date(b)"""
            contains("partitions=1/3 (p3)")
        }
        explain {
            sql """select * from mal_test_partition_range5_date_mono where not "2020-01-14" > date(b)"""
            contains("partitions=1/3 (p3)")
        }
        explain {
            sql """select * from mal_test_partition_range5_date_mono where not date(b)="2020-01-08" """
            contains("partitions=3/3 (p1,p2,p3)")
        }
        explain {
            sql """select * from mal_test_partition_range5_date_mono where not "2020-01-08" = date(b)"""
            contains("partitions=3/3 (p1,p2,p3)")
        }
        explain {
            sql """select * from mal_test_partition_range5_date_mono where not date(b)>="2020-01-9" """
            contains("partitions=1/3 (p1)")
        }
        explain {
            sql """select * from mal_test_partition_range5_date_mono where not date(b)>"2020-01-9" """
            contains("partitions=2/3 (p1,p2)")
        }
        explain {
            sql """select * from mal_test_partition_range5_date_mono where not "2020-01-9"<=date(b)"""
            contains("partitions=1/3 (p1)")
        }
        explain {
            sql """select * from mal_test_partition_range5_date_mono where not "2020-01-9" < date(b)"""
            contains("partitions=2/3 (p1,p2)")
        }
        explain {
            sql """ select * from mal_test_partition_range5_date_mono where  not date(b) in ("2020-01-13 00:00:00")"""
            contains("partitions=3/3 (p1,p2,p3)")
        }
        explain {
            sql """ select * from mal_test_partition_range5_date_mono where not "2020-01-14" <=> date(b)"""
            contains("partitions=3/3 (p1,p2,p3)")
        }
        explain {
            sql """select * from mal_test_partition_range5_date_mono where not date(b) <=>"2020-01-14" """
            contains("partitions=3/3 (p1,p2,p3)")
        }
        explain {
            sql """ select * from mal_test_partition_range5_date_mono where not  (date(b)>"2020-01-09"  and date(b) <"2020-01-13") """
            contains("partitions=3/3 (p1,p2,p3)")
        }
        explain {
            sql """ select * from mal_test_partition_range5_date_mono where not (date(b)>="2020-01-13"  or date(b) <="2020-01-9") """
            contains("partitions=1/3 (p2)")
        }
        explain {
            sql """ select * from mal_test_partition_range5_date_mono where not date(b)<="2020-01-14"  or date(b) <"2020-01-06" """
            contains("partitions=2/3 (p1,p3)")
        }
        explain {
            sql """select * from mal_test_partition_range5_date_mono where not date(b) between "2020-01-09"  and "2020-01-13" """
            contains("partitions=2/3 (p1,p3)")
        }
        explain {
            sql """select * from mal_test_partition_range5_date_mono where not date(b) between "2020-01-10"  and "2020-01-14" """
            contains("partitions=3/3 (p1,p2,p3)")
        }
        explain {
            sql """select * from mal_test_partition_range5_date_mono where date(b) not between "2020-01-4"  and "2020-01-15" """
            contains("partitions=1/3 (p3)")
        }
    }

    // trunc(b) and b
    explain {
        sql """select * from mal_test_partition_range5_date_mono where (not date(b)<="2020-01-14") or b<"2020-01-9" """
        contains("partitions=2/3 (p1,p3)")
    }
    explain {
        sql """select * from mal_test_partition_range5_date_mono where  date(b)<"2020-01-13" and  b>"2020-01-10" """
        contains("partitions=1/3 (p2)")
    }
    explain {
        sql """ select * from mal_test_partition_range5_date_mono where  date(b)<"2020-01-13" and  b>"2020-01-9" """
        contains("partitions=2/3 (p1,p2)")
    }
    explain {
        sql """ select * from mal_test_partition_range5_date_mono where ( date(b)<="2020-01-14" and b >"2020-01-12") or b<"2020-01-9" """
        contains("partitions=3/3 (p1,p2,p3)")
    }
    explain {
        sql """ select * from mal_test_partition_range5_date_mono where ( date(b)<="2020-01-14" and b >"2020-01-19") or b<"2020-01-9" """
        contains("partitions=1/3 (p1)")
    }
    explain {
        sql """ select * from mal_test_partition_range5_date_mono where ( date(b)<="2020-01-14" and b >"2020-01-20") or b<"2020-01-9" """
        contains("partitions=1/3 (p1)")
    }
    explain {
        sql """ select * from mal_test_partition_range5_date_mono where ( not date(b)<="2020-01-14" or b >"2020-01-20") or b<"2020-01-9" """
        contains("partitions=2/3 (p1,p3)")
    }
    explain {
        sql """ select * from mal_test_partition_range5_date_mono where (  date(b) between "2020-01-14" and "2020-01-20") or b<"2020-01-9" """
        contains("partitions=2/3 (p1,p3)")
    }

    // is null, can support but now not
    sql "drop table if exists null_range_date_mono"
    sql """
        create table null_range_date_mono(
        k0 datetime null
        )
        partition by range (k0)
        (
        PARTITION p10 values less than ('2022-01-01 10:00:00'),
        PARTITION p100 values less than ('2022-01-04 10:00:00'),
        PARTITION pMAX values less than (maxvalue)
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 1 properties("replication_num"="1")
    """
    sql "insert into null_range_date_mono values('2022-01-03 10:00:00'),('2019-01-01 10:00:00'),('2022-01-02 10:00:00'),('2024-01-01 10:00:00'),(null);"
    explain {
        sql "select * from null_range_date_mono where date(k0) is null"
        contains("partitions=3/3 (p10,p100,pMAX)")
    }
    // test infinite range
    explain {
        sql "select * from null_range_date_mono where date(k0) <'2022-1-3'"
        contains("partitions=2/3 (p10,p100)")
    }
    explain {
        sql "select * from null_range_date_mono where date(k0) >'2022-1-3'"
        contains("partitions=2/3 (p100,pMAX)")
    }

    sql "drop table if exists mal_test_partition_range2_two_date_int_date_mono"
    sql """CREATE TABLE `mal_test_partition_range2_two_date_int_date_mono` (
          `dt` DATETIME NULL,
          `id` INT NULL,
          `c` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`dt`, `id`, `c`)
        PARTITION BY RANGE(`dt`, `id`)
        (PARTITION p201701_1000 VALUES [('0000-01-01', "-2147483648"), ('2017-02-01', "1000")),
        PARTITION p201702_2000 VALUES [('2017-02-01', "1000"), ('2017-03-01', "2000")),
        PARTITION p201703_all VALUES [('2017-03-01', "2000"), ('2017-04-01', "-2147483648")))
        DISTRIBUTED BY HASH(`dt`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );"""
    sql """insert into mal_test_partition_range2_two_date_int_date_mono values('2017-01-03 10:00:00', 3,23),('2017-02-04 10:00:00', 333,4),('2017-03-05 10:00:00', 1222,6);"""
    explain {
        sql """select * from mal_test_partition_range2_two_date_int_date_mono where date(dt) = '2017-2-1 00:00:00' and id>0 ;"""
        contains ("partitions=2/3 (p201701_1000,p201702_2000)")
    }
    explain {
        sql "select * from mal_test_partition_range2_two_date_int_date_mono where date(dt) > '2017-2-1  00:00:00' and id>100;"
        contains("partitions=2/3 (p201702_2000,p201703_all)")
    }

    explain {
        sql "select * from mal_test_partition_range2_two_date_int_date_mono where date(date_trunc(dt,'hour')) > '2017-2-1  00:00:00' and id>100;"
        contains("partitions=2/3 (p201702_2000,p201703_all)")
    }
    // test nest function
    explain {
        sql "select * from mal_test_partition_range2_two_date_int_date_mono where date(date_trunc(dt,'minute')) > '2017-2-1  00:00:00' and id>100;"
        contains("partitions=2/3 (p201702_2000,p201703_all)")
    }
    explain {
        sql "select * from mal_test_partition_range2_two_date_int_date_mono where date_trunc(date(dt),'minute') > '2017-2-1  00:00:00' and id>100;"
        contains("partitions=2/3 (p201702_2000,p201703_all)")
    }
}