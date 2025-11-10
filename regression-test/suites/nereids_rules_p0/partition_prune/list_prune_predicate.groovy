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

suite("list_prune_predicate") {
    multi_sql """
    drop table if exists test_list_date;
    create table  test_list_date (id int, d_date date)
    PARTITION BY LIST(d_date)
    (
    PARTITION `p20250101` VALUES IN ('2025-01-01'),
    PARTITION `p20250102` VALUES IN ('2025-01-02'),
    PARTITION `p20250103` VALUES IN ('2025-01-03'),
    PARTITION `p20250104` VALUES IN ('2025-01-04'),
    PARTITION `p20250105` VALUES IN ('2025-01-05'),
    PARTITION `p20250106` VALUES IN ('2025-01-06'),
    PARTITION `p20250107` VALUES IN ('2025-01-07'),
    PARTITION `p20250108` VALUES IN ('2025-01-08'),
    PARTITION `p20250109` VALUES IN ('2025-01-09'),
    PARTITION `p20250110` VALUES IN ('2025-01-10')
    )
    properties('replication_num'='1');

    insert into test_list_date values
    (1,'2025-01-01') , (2, '2025-01-02') , (3, '2025-01-03'), (4, '2025-01-04') , (5, '2025-01-05'), (6,'2025-01-06') ,
    (7, '2025-01-07') , (8, '2025-01-08'), (9, '2025-01-09') , (10, '2025-01-10'); """

    // =
    explain {
        sql "select * from test_list_date where d_date='2025-01-07';"
        contains("partitions=1/10 (p20250107)")
        notContains("PREDICATES")
    }
    // >
    explain {
        sql "select * from test_list_date where d_date>'2025-01-07';"
        contains("partitions=3/10 (p20250108,p20250109,p20250110)")
        notContains("PREDICATES")
    }

    // <
    explain {
        sql "select * from test_list_date where d_date<'2025-01-07';"
        contains("partitions=6/10 (p20250101,p20250102,p20250103,p20250104,p20250105,p20250106)")
        notContains("PREDICATES")
    }
    // >=
    explain {
        sql "select * from test_list_date where d_date>='2025-01-07';"
        contains("partitions=4/10 (p20250107,p20250108,p20250109,p20250110)")
        notContains("PREDICATES")
    }
    // <=
    explain {
        sql "select * from test_list_date where d_date<='2025-01-07';"
        contains("partitions=7/10 (p20250101,p20250102,p20250103,p20250104,p20250105,p20250106,p20250107)")
        notContains("PREDICATES")
    }
    // empty paritition
    explain {
        sql "select * from test_list_date where d_date<='2024-01-01'"
        contains("VEMPTYSET")
        notContains("PREDICATES")
    }

    // and
    explain {
        sql "select * from test_list_date where d_date>='2025-01-06' and d_date<='2025-01-07';"
        contains("partitions=2/10 (p20250106,p20250107)")
        notContains("PREDICATES")
    }
    // or
    explain {
        sql "select * from test_list_date where d_date>='2025-01-08' or d_date<='2025-01-03';"
        contains("partitions=6/10 (p20250101,p20250102,p20250103,p20250108,p20250109,p20250110)")
        notContains("PREDICATES")
    }

    // or
    explain {
        sql "select * from test_list_date where d_date>='2025-01-06' or d_date<='2025-01-07';"
        contains("partitions=10/10 (p20250101,p20250102,p20250103,p20250104,p20250105,p20250106,p20250107,p20250108,p20250109,p20250110)")
        notContains("PREDICATES")
    }

    // have other predicates
    // =
    explain {
        sql "select * from test_list_date where d_date='2025-01-07' and id > 8;"
        contains("partitions=1/10 (p20250107)")
        contains("PREDICATES: (id[#0] > 8)")
    }
    // >
    explain {
        sql "select * from test_list_date where d_date>'2025-01-07' and id > 8;"
        contains("partitions=3/10 (p20250108,p20250109,p20250110)")
        contains("PREDICATES: (id[#0] > 8)")
    }
    // and
    explain {
        sql "select * from test_list_date where d_date>='2025-01-06' and d_date<='2025-01-07' and id <2;"
        contains("partitions=2/10 (p20250106,p20250107)")
        contains("PREDICATES: (id[#0] < 2)")
    }
    // or
    explain {
        sql "select * from test_list_date where (d_date>='2025-01-08' or d_date<='2025-01-03') and id < 2;"
        contains("partitions=6/10 (p20250101,p20250102,p20250103,p20250108,p20250109,p20250110)")
        contains("PREDICATES: (id[#0] < 2)")
    }

    // or
    explain {
        sql "select * from test_list_date where d_date>='2025-01-08' or (d_date<='2025-01-03' and id < 2);"
        contains("partitions=6/10 (p20250101,p20250102,p20250103,p20250108,p20250109,p20250110)")
        contains("PREDICATES: ((d_date[#1] >= '2025-01-08') OR ((d_date[#1] <= '2025-01-03') AND (id[#0] < 2)))")
    }
    // or
    explain {
        sql "select * from test_list_date where (d_date>='2025-01-06' or d_date<='2025-01-07') and id < 2;"
        contains("PREDICATES: (id[#0] < 2)")
        contains("partitions=10/10 (p20250101,p20250102,p20250103,p20250104,p20250105,p20250106,p20250107,p20250108,p20250109,p20250110)")
    }

    // not in
    explain {
        sql "select * from test_list_date where d_date not in ('2025-01-01', '2025-01-02');"
        contains("partitions=8/10 (p20250103,p20250104,p20250105,p20250106,p20250107,p20250108,p20250109,p20250110)")
        notContains("PREDICATES")
    }

    // in
    explain {
        sql "select * from test_list_date where d_date in ('2025-01-01', '2025-01-03', '2025-01-05');"
        contains("partitions=3/10 (p20250101,p20250103,p20250105)")
        notContains("PREDICATES")
    }

    // between
    explain {
        sql "select * from test_list_date where d_date between '2025-01-03' and '2025-01-05';"
        contains("partitions=3/10 (p20250103,p20250104,p20250105)")
        notContains("PREDICATES")
    }

    // and or
    explain {
        sql "select * from test_list_date where (d_date >= '2025-01-03' and d_date <= '2025-01-05') or (d_date >= '2025-01-08' and d_date <= '2025-01-09');"
        contains("partitions=5/10 (p20250103,p20250104,p20250105,p20250108,p20250109)")
        notContains("PREDICATES")
    }

    explain {
        sql "select * from test_list_date where (d_date = '2025-01-05' or (d_date = '2025-01-06' and id > 5)) and id < 10;"
        contains("partitions=2/10 (p20250105,p20250106)")
    }

    explain {
        sql "select * from test_list_date where ((d_date >= '2025-01-03' and d_date <= '2025-01-05') or d_date = '2025-01-08') and (id = 3 or id = 4 or id = 8);"
        contains("partitions=4/10 (p20250103,p20250104,p20250105,p20250108)")
        contains("PREDICATES: id[#0] IN (3, 4, 8)")
    }

    explain {
        sql "select * from test_list_date where (d_date = '2025-01-03' and id > 2) or (d_date = '2025-01-07' and id < 8);"
        contains("partitions=2/10 (p20250103,p20250107)")
    }

    explain {
        sql "select * from test_list_date where (d_date in ('2025-01-02','2025-01-04') and id % 2 = 0) or (d_date = '2025-01-06' and id % 2 = 1);"
        contains("partitions=3/10 (p20250102,p20250104,p20250106)")
    }
    // is null
    explain {
        sql "select * from test_list_date where d_date is null;"
        contains("VEMPTYSET")
    }

    // func
    explain {
        sql "select * from test_list_date where year(d_date) = 2025;"
        contains("partitions=10/10 (p20250101,p20250102,p20250103,p20250104,p20250105,p20250106,p20250107,p20250108,p20250109,p20250110)")
    }

    // test date(dt) predicate rewrite before partition prune
    multi_sql """
    drop table if exists test_list_datetime;
    create table  test_list_datetime (id int, d_date datetime)
    PARTITION BY LIST(d_date)
    (
    PARTITION `p20250101` VALUES IN ('2025-01-01 00:00:00'),
    PARTITION `p20250102` VALUES IN ('2025-01-02 00:00:00'),
    PARTITION `p20250103` VALUES IN ('2025-01-03 00:00:00'),
    PARTITION `p20250104` VALUES IN ('2025-01-04 00:00:00'),
    PARTITION `p20250105` VALUES IN ('2025-01-05 00:00:00'),
    PARTITION `p20250106` VALUES IN ('2025-01-06 00:00:00'),
    PARTITION `p20250107` VALUES IN ('2025-01-07 00:00:00'),
    PARTITION `p20250108` VALUES IN ('2025-01-08 00:00:00'),
    PARTITION `p20250109` VALUES IN ('2025-01-09 00:00:00'),
    PARTITION `p20250110` VALUES IN ('2025-01-10 00:00:00')
    )
    properties('replication_num'='1');
    insert into test_list_datetime values
    (1,'2025-01-01') , (2, '2025-01-02') , (3, '2025-01-03'), (4, '2025-01-04') , (5, '2025-01-05'), (6,'2025-01-06') ,
    (7, '2025-01-07') , (8, '2025-01-08'), (9, '2025-01-09') , (10, '2025-01-10');"""

    explain {
        sql "select * from test_list_datetime where date(d_date) in ('2025-01-09','2025-01-10');"
        contains("partitions=2/10 (p20250109,p20250110)")
        notContains("PREDICATES")
    }

    // multi column partition
    multi_sql """
    drop table if exists test_list_multi_column;
    create table  test_list_multi_column (id int, d_date date)
    PARTITION BY LIST(id,d_date)
    (
    PARTITION `p20250101` VALUES IN ((1,'2025-01-01')),
    PARTITION `p20250102` VALUES IN ((2,'2025-01-02')),
    PARTITION `p20250103` VALUES IN ((3,'2025-01-03')),
    PARTITION `p20250104` VALUES IN ((4,'2025-01-04')),
    PARTITION `p20250105` VALUES IN ((5,'2025-01-05')),
    PARTITION `p20250106` VALUES IN ((6,'2025-01-06')),
    PARTITION `p20250107` VALUES IN ((7,'2025-01-07')),
    PARTITION `p20250108` VALUES IN ((8,'2025-01-08')),
    PARTITION `p20250109` VALUES IN ((9,'2025-01-09')),
    PARTITION `p20250110` VALUES IN ((10,'2025-01-10'))
    )
    properties('replication_num'='1');
    insert into test_list_multi_column values
    (1,'2025-01-01') , (2, '2025-01-02') , (3, '2025-01-03'), (4, '2025-01-04') , (5, '2025-01-05'), (6,'2025-01-06') ,
    (7, '2025-01-07') , (8, '2025-01-08'), (9, '2025-01-09') , (10, '2025-01-10');
    """
    explain {
        sql "select * from test_list_multi_column where id=1 and d_date='2025-01-01';"
        contains("partitions=1/10 (p20250101)")
        notContains("PREDICATES")
    }
    explain {
        sql "select * from test_list_multi_column where id=1 and d_date='2025-01-02';"
        contains("VEMPTYSET")
        notContains("PREDICATES")
    }
    explain {
        sql "select * from test_list_multi_column where (id=1 and d_date='2025-01-01') or id=2;"
        contains("partitions=2/10 (p20250101,p20250102)")
        notContains("PREDICATES")
    }

    explain {
        sql "select * from test_list_multi_column where (id = 1 and d_date = '2025-01-01') or (id = 2 and d_date = '2025-01-02');"
        contains("partitions=2/10 (p20250101,p20250102)")
        notContains("PREDICATES")
    }

    explain {
        sql "select * from test_list_multi_column where id = 1 and (d_date = '2025-01-01' or d_date = '2025-01-02');"
        contains("partitions=1/10 (p20250101)")
        notContains("PREDICATES")
    }

    explain {
        sql "select * from test_list_multi_column where (id = 1 or id = 2) and d_date = '2025-01-01';"
        contains("partitions=1/10 (p20250101)")
        notContains("PREDICATES")
    }

    // test default partition
    multi_sql """
    drop table if exists list_par_data_migration;
    CREATE TABLE IF NOT EXISTS list_par_data_migration ( 
        k1 tinyint NOT NULL, 
        k2 smallint NOT NULL, 
        k3 int NOT NULL, 
        k4 bigint NOT NULL, 
        k5 decimal(9, 3) NOT NULL,
        k8 double max NOT NULL, 
        k9 float sum NOT NULL ) 
    AGGREGATE KEY(k1,k2,k3,k4,k5)
    PARTITION BY LIST(k1) ( 
        PARTITION p1 VALUES IN ("1","2","3","4"), 
        PARTITION p2 VALUES IN ("5","6","7","8"), 
        PARTITION p3 ) 
    DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1");
    """
    sql """insert into list_par_data_migration values (1,1,1,1,24453.325,1,1)"""
    sql """insert into list_par_data_migration values (10,1,1,1,24453.325,1,1)"""
    sql """insert into list_par_data_migration values (11,1,1,1,24453.325,1,1)"""
    qt_default_partition """
    select * from list_par_data_migration partition p3 where k1=10 order by k1;"""
    // test manuallySpecifiedPartitions
    explain {
        sql "select * from list_par_data_migration partition p2 where k1=2 order by k1;"
        contains("VEMPTYSET")
    }
    explain {
        sql "select * from list_par_data_migration partition p1 where k1=2 order by k1;"
        contains("partitions=1/3 (p1)")
        // k1=2 can not be deleted because k1=2 is not always true in p1
        contains("PREDICATES: (k1[#0] = 2)")
    }
    explain {
        sql "select * from list_par_data_migration partition p1 where k1=1 or k1=2 or k1=3 or k1=4 order by k1;"
        contains("partitions=1/3 (p1)")
        notContains("PREDICATES")
    }

    // test variable skip_prune_predicate
    sql "set skip_prune_predicate = true"
    explain {
        sql "select * from list_par_data_migration partition p1 where k1=1 or k1=2 or k1=3 or k1=4 order by k1;"
        contains("partitions=1/3 (p1)")
        contains("PREDICATES")
    }
    sql "set skip_prune_predicate = false"

    sql " drop table if exists test_list_multi_column_unique;"
    sql """ create table  test_list_multi_column_unique (id int, d_date date, c02 int default 1) unique key (id, d_date)
    PARTITION BY LIST(id,d_date)
    (
    PARTITION `p20250101` VALUES IN ((1,'2025-01-01')),
    PARTITION `p20250102` VALUES IN ((2,'2025-01-02')),
    PARTITION `p20250103` VALUES IN ((3,'2025-01-03')),
    PARTITION `p20250104` VALUES IN ((4,'2025-01-04')),
    PARTITION `p20250105` VALUES IN ((5,'2025-01-05')),
    PARTITION `p20250106` VALUES IN ((6,'2025-01-06')),
    PARTITION `p20250107` VALUES IN ((7,'2025-01-07')),
    PARTITION `p20250108` VALUES IN ((8,'2025-01-08')),
    PARTITION `p20250109` VALUES IN ((9,'2025-01-09')),
    PARTITION `p20250110` VALUES IN ((10,'2025-01-10'))
    ) DISTRIBUTED BY HASH(id) BUCKETS 10
    properties('replication_num'='1');"""
    sql """insert into test_list_multi_column_unique(id, d_date) values
    (1,'2025-01-01') , (2, '2025-01-02') , (3, '2025-01-03'), (4, '2025-01-04') , (5, '2025-01-05'), (6,'2025-01-06') ,
    (7, '2025-01-07') , (8, '2025-01-08'), (9, '2025-01-09') , (10, '2025-01-10');
    """

    // add test for delete(not support optimize)
    explain {
        sql "delete from  test_list_multi_column_unique where id=1 and d_date='2025-01-01';"
        contains("partitions=1/10 (p20250101)")
        contains("(id[#0] = 1) AND (d_date[#1] = '2025-01-01')")
    }

    // add test for update(support optimize)
    explain {
        sql "update test_list_multi_column_unique set c02 = 1 where id=1 and d_date='2025-01-01';"
        contains("partitions=1/10 (p20250101)")
        notContains("(id[#0] = 1) AND (d_date[#1] = '2025-01-01')")
    }
    sql """update test_list_multi_column_unique set c02 = 10 where id=1 and d_date='2025-01-01';"""
    qt_update "select * from test_list_multi_column_unique order by id;"
}