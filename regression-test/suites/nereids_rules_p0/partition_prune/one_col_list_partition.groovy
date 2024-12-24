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

suite("one_col_list_partition") {
    sql "drop table if exists one_col_list_partition_date"
    sql """create table one_col_list_partition_date(a int, dt datetime, d date, c varchar(100)) duplicate key(a)
    partition by list(dt)
    (
            partition p1 values in ('2021-01-01 00:00:00', '2021-01-02 00:00:00'),
    partition p2 values in ('2021-01-03 00:00:00', '2021-01-04 00:00:00'),
    partition p3 values in ('2021-01-05 00:00:00', '2021-01-06 00:00:00'),
    partition p4 values in ('2021-01-07 00:00:00', '2021-01-08 00:00:00'),
    partition p5 values in ('2021-01-09 00:00:00', '2021-01-10 00:00:00'),
    partition p6 values in ('2021-01-11 00:00:00', '2021-01-12 00:00:00'),
    partition p7 values in ('2021-01-13 00:00:00', '2021-01-14 00:00:00'),
    partition p8 values in ('2021-01-15 00:00:00', '2021-01-16 00:00:00'),
    partition p9 values in ('2021-01-17 00:00:00', '2021-01-18 00:00:00')
    )
    distributed by hash(a)
    properties("replication_num"="1");"""

    sql """
    insert into one_col_list_partition_date values(1,'2021-01-01 00:00:00',null, 'abc'),(1,'2021-01-02 00:00:00',null, 'abc')
    ,(1,'2021-01-03 00:00:00',null, 'abc'),(1,'2021-01-04 00:00:00',null, 'abc')
    ,(1,'2021-01-05 00:00:00',null, 'abc'),(1,'2021-01-06 00:00:00',null, 'abc'),(1,'2021-01-07 00:00:00',null, 'abc'),(1,'2021-01-08 00:00:00',null, 'abc')
    ,(1,'2021-01-09 00:00:00',null, 'abc'),(1,'2021-01-11 00:00:00',null, 'abc'),(1,'2021-01-13 00:00:00',null, 'abc'),(1,'2021-01-15 00:00:00',null, 'abc')
    ,(1,'2021-01-10 00:00:00',null, 'abc'),(1,'2021-01-12 00:00:00',null, 'abc'),(1,'2021-01-14 00:00:00',null, 'abc'),(1,'2021-01-16 00:00:00',null, 'abc')
    """

    sql "drop table if exists one_col_list_partition_date_has_null"
    sql"""
    CREATE TABLE `one_col_list_partition_date_has_null` (
      `a` int NULL,
      `dt` datetime NULL,
      `d` date NULL,
      `c` varchar(100) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`)
    PARTITION BY LIST (`dt`)
    (PARTITION p10 VALUES IN (NULL),
    PARTITION p1 VALUES IN ('2021-01-01 00:00:00','2021-01-02 00:00:00'),
    PARTITION p2 VALUES IN ('2021-01-03 00:00:00','2021-01-04 00:00:00'),
    PARTITION p3 VALUES IN ('2021-01-05 00:00:00','2021-01-06 00:00:00'),
    PARTITION p4 VALUES IN ('2021-01-07 00:00:00','2021-01-08 00:00:00'),
    PARTITION p5 VALUES IN ('2021-01-09 00:00:00','2021-01-10 00:00:00'),
    PARTITION p6 VALUES IN ('2021-01-11 00:00:00','2021-01-12 00:00:00'),
    PARTITION p7 VALUES IN ('2021-01-13 00:00:00','2021-01-14 00:00:00'),
    PARTITION p8 VALUES IN ('2021-01-15 00:00:00','2021-01-16 00:00:00'),
    PARTITION p9 VALUES IN ('2021-01-17 00:00:00','2021-01-18 00:00:00'))
    DISTRIBUTED BY HASH(`a`) BUCKETS 10
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1")
    """

    sql """
    insert into one_col_list_partition_date_has_null values(1,'2021-01-01 00:00:00',null, 'abc'),(1,'2021-01-02 00:00:00',null, 'abc')
    ,(1,'2021-01-03 00:00:00',null, 'abc'),(1,'2021-01-04 00:00:00',null, 'abc')
    ,(1,'2021-01-05 00:00:00',null, 'abc'),(1,'2021-01-06 00:00:00',null, 'abc'),(1,'2021-01-07 00:00:00',null, 'abc'),(1,'2021-01-08 00:00:00',null, 'abc')
    ,(1,'2021-01-09 00:00:00',null, 'abc'),(1,'2021-01-11 00:00:00',null, 'abc'),(1,'2021-01-13 00:00:00',null, 'abc'),(1,'2021-01-15 00:00:00',null, 'abc')
    ,(1,'2021-01-10 00:00:00',null, 'abc'),(1,'2021-01-12 00:00:00',null, 'abc'),(1,'2021-01-14 00:00:00',null, 'abc'),(1,'2021-01-16 00:00:00',null, 'abc')
    ,(1,null,null,'bdb')
    """

    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE dt<'2021-01-10 00:00:00' and dt>'2021-01-7 00:00:00'"
        contains("partitions=2/9 (p4,p5)")
    }
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE dt<'2021-01-10 00:00:00' or dt>'2021-01-14 00:00:00'"
        contains("partitions=6/9 (p1,p2,p3,p4,p5,p8)")
    }

    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE (dt<'2021-01-10 00:00:00' or dt>'2021-01-10 00:00:00') and dt<=>'2021-01-01 00:00:00'"
        contains("partitions=1/9 (p1)")
    }

    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE dt regexp '2020-10-01 00:00:00'"
        contains("partitions=8/9 (p1,p2,p3,p4,p5,p6,p7,p8)")
    }
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE dt like '%2020-10-01 00:00:00'"
        contains("partitions=8/9 (p1,p2,p3,p4,p5,p6,p7,p8)")
    }

    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE dt like '2020-10-01 00:00:00'"
        contains("VEMPTYSET")
    }
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE dt in ('2021-01-10 00:00:00', '2021-01-13 00:00:00')"
        contains("partitions=2/9 (p5,p7)")

    }

    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE dt not in ('2021-01-01 00:00:00', '2021-01-02 00:00:00')"
        contains("partitions=7/9 (p2,p3,p4,p5,p6,p7,p8)")

    }

    explain {
        sql "select * from one_col_list_partition_date where dt is null"
        contains("VEMPTYSET")
    }
    explain {
        sql "select * from one_col_list_partition_date where dt is not null"
        contains("partitions=8/9 (p1,p2,p3,p4,p5,p6,p7,p8)")
    }
    explain {
        sql "select * from one_col_list_partition_date where not dt is null"
        contains("partitions=8/9 (p1,p2,p3,p4,p5,p6,p7,p8)")
    }
    explain {
        sql "select * from one_col_list_partition_date where !(dt is null)"
        contains("partitions=8/9 (p1,p2,p3,p4,p5,p6,p7,p8)")
    }

    explain {
        sql "select * from one_col_list_partition_date where dt <=> null"
        contains("VEMPTYSET")
    }
    explain {
        sql "select * from one_col_list_partition_date where !(dt <=> null)"
        contains("partitions=8/9 (p1,p2,p3,p4,p5,p6,p7,p8)")
    }
    explain {
        sql "select * from one_col_list_partition_date_has_null where dt is null"
        contains("partitions=1/10 (p10)")
    }
    explain {
        sql "select * from one_col_list_partition_date_has_null where dt is not null"
        contains("partitions=8/10 (p1,p2,p3,p4,p5,p6,p7,p8)")
    }
    explain {
        sql "select * from one_col_list_partition_date_has_null where not dt is null"
        contains("partitions=8/10 (p1,p2,p3,p4,p5,p6,p7,p8)")
    }
    explain {
        sql "select * from one_col_list_partition_date_has_null where !(dt is null)"
        contains("partitions=8/10 (p1,p2,p3,p4,p5,p6,p7,p8)")
    }

    explain {
        sql "select * from one_col_list_partition_date_has_null where dt <=> null"
        contains("partitions=1/10 (p10)")
    }
    explain {
        sql "select * from one_col_list_partition_date_has_null where !(dt <=> null)"
        contains("partitions=8/10 (p1,p2,p3,p4,p5,p6,p7,p8)")
    }


    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE dt<'2021-01-01 00:00:00'"
        contains("VEMPTYSET")
    }
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE dt<='2021-01-02 00:00:00'"
        contains("partitions=1/9 (p1)")
    }
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE dt>'2021-1-03 00:00:00'"
        contains("partitions=7/9 (p2,p3,p4,p5,p6,p7,p8)")
    }
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE dt>='2021-1-04 00:00:00'"
        contains("partitions=7/9 (p2,p3,p4,p5,p6,p7,p8)")
    }
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE dt='2021-1-05 00:00:00'"
        contains("partitions=1/9 (p3)")
    }
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE dt<=>'2021-1-01 00:00:00'"
        contains("partitions=1/9 (p1)")
    }
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE dt!='2021-1-01 00:00:00' and dt!='2021-1-02 00:00:00'"
        contains("partitions=7/9 (p2,p3,p4,p5,p6,p7,p8)")
    }


    // has condition function
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE coalesce(dt, null, '2020-01-01') <'2021-1-07 00:00:00'"
        contains("partitions=3/9 (p1,p2,p3)")
    }
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE coalesce(dt <'2021-1-07 00:00:00' , true, false)"
        contains("partitions=3/9 (p1,p2,p3)")
    }
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE coalesce(dt >'2021-1-07 00:00:00' , true, false)"
        contains("partitions=5/9 (p4,p5,p6,p7,p8)")
    }
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE coalesce(null ,dt< '2021-1-07 00:00:00', dt> '2020-10-01 00:00:00' )"
        contains("partitions=3/9 (p1,p2,p3)")
    }
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE if(dt<'2021-1-01 00:00:00', true, false)"
        contains("VEMPTYSET")
    }
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE if(a>1, dt<'2001-1-01 00:00:00', dt<'2001-1-01 00:00:00')"
        contains("partitions=8/9 (p1,p2,p3,p4,p5,p6,p7,p8)")
    }
    explain {
        sql "SELECT * FROM one_col_list_partition_date WHERE if(dt<'2021-1-01 00:00:00', dt<'2001-1-01 00:00:00', dt>'2031-1-01 00:00:00')"
        contains("VEMPTYSET")
    }
    explain {
        sql """SELECT * FROM one_col_list_partition_date WHERE case when dt<'2021-1-01 00:00:00' then false when dt<'2021-5-01' then false
        else true end;"""
        contains("VEMPTYSET")
    }
    explain {
        sql """SELECT * FROM one_col_list_partition_date WHERE case when dt<'2021-1-01 00:00:00' then false when dt<'2021-5-01' then true
        else true end;"""
        contains("partitions=8/9 (p1,p2,p3,p4,p5,p6,p7,p8)")
    }
    explain {
        sql """SELECT * FROM one_col_list_partition_date WHERE case when dt<'2022-01-01 00:00:00' then dt
        else '2023-01-01 00:00:00' end <'2021-01-06 00:00:00' ;"""
        contains("partitions=3/9 (p1,p2,p3)")
    }
}