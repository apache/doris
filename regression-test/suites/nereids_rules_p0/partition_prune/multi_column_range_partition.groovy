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

suite("multi_column_range_partition") {
    sql "drop table if exists t_multi_column_range_partition"
    sql """
    create table t_multi_column_range_partition(a int, dt datetime, v int) partition by range(a, dt)
    (
    partition p0 values [(0,'2024-01-01 00:00:00'), (10,'2024-01-10 00:00:00')),
    partition p10 values [(10,'2024-01-10 00:00:00'), (20,'2024-01-20 00:00:00')),
    partition p20 values [(20,'2024-01-20 00:00:00'), (30,'2024-01-31 00:00:00')),
    partition p30 values [(30,'2024-01-31 00:00:00'), (40,'2024-02-10 00:00:00')),
    partition p40 values [(40,'2024-02-10 00:00:00'), (50,'2024-02-20 00:00:00'))
    )
    distributed by hash(a) properties("replication_num"="1"); """

    sql """insert into t_multi_column_range_partition values(0,'2024-01-01 00:00:00',2),(1,'2024-01-01 00:00:00',2),(1,'2025-01-01 00:00:00',2),
    (10,'2024-01-10 00:00:00',3),(10,'2024-01-11 00:00:00',200),(12,'2021-01-01 00:00:00',2),
    (25,'2024-01-10 00:00:00',3),(20,'2024-01-11 00:00:00',200),(30,'2021-01-01 00:00:00',2),
    (40,'2024-01-01 00:00:00',2),(40,'2024-01-31 00:00:00',2),(10,'2024-01-9 00:00:00',1000),(10,'2024-01-10 00:00:00',1000),(10,'2024-01-10 01:00:00',1000),
    (2,'2023-01-10 01:00:00',1000)
    """

    sql "drop table if exists t_multi_column_partition_datetime_first"
    sql """
    create table t_multi_column_partition_datetime_first(a int, dt datetime, v int not null) partition by range(dt,a)
    (
    partition p0 values [('2024-01-01 00:00:00',0), ('2024-01-10 00:00:00',10)),
    partition p10 values [('2024-01-10 00:00:00',10), ('2024-01-20 00:00:00',20)),
    partition p20 values [('2024-01-20 00:00:00',20), ('2024-01-31 00:00:00',30)),
    partition p30 values [('2024-01-31 00:00:00',30), ('2024-02-10 00:00:00',40)),
    partition p40 values [('2024-02-10 00:00:00',40), ('2024-02-20 00:00:00',50))
    )
    distributed by hash(a) properties("replication_num"="1");"""

    sql "insert into t_multi_column_partition_datetime_first values(0,'2024-01-01 00:00:00',2)"
    sql "insert into t_multi_column_partition_datetime_first values(1,'2024-01-01 00:00:00',2)"
    sql "insert into t_multi_column_partition_datetime_first values(10,'2024-01-10 00:00:00',3),(10,'2024-01-11 00:00:00',200)"
    sql "insert into t_multi_column_partition_datetime_first values(25,'2024-01-10 00:00:00',3),(20,'2024-01-11 00:00:00',200)"
    sql "insert into t_multi_column_partition_datetime_first values(40,'2024-01-01 00:00:00',2),(40,'2024-01-31 00:00:00',2)"
    sql "insert into t_multi_column_partition_datetime_first values(10,'2024-01-9 00:00:00',1000),(10,'2024-01-10 00:00:00',1000),(10,'2024-01-10 01:00:00',1000)"

    explain {
        sql "select * from t_multi_column_range_partition where a=10 and date_trunc(dt, 'day') <'2024-01-10'"
        contains("partitions=1/5 (p0)")
    }
    sql "set PARTITION_PRUNING_EXPAND_THRESHOLD=5;"
    explain {
        sql "select * from t_multi_column_range_partition where a=10 and date_trunc(dt, 'day') <'2024-01-10'"
        contains("partitions=2/5 (p0,p10)")
    }
    for (int i = 0; i < 2; ++i) {
        if (i == 1) {
            sql "set PARTITION_PRUNING_EXPAND_THRESHOLD=5;"
        } else if (i == 0) {
            sql "set PARTITION_PRUNING_EXPAND_THRESHOLD=30;"
        }
        explain {
            sql "select * from t_multi_column_range_partition where a=10 and date_trunc(dt, 'day') ='2024-01-10'"
            contains("partitions=2/5 (p0,p10)")
        }
        explain {
            sql "select * from t_multi_column_range_partition where a<19 and (date_trunc(dt, 'day') <'2024-01-20' OR date_trunc(dt, 'day') >'2024-02-10')"
            contains("partitions=2/5 (p0,p10)")
        }
        explain {
            sql "select * from t_multi_column_range_partition where a>1 and a<30 and (date_trunc(dt, 'day') <'2024-01-20' OR date_trunc(dt, 'day') >'2024-02-10')"
            contains("partitions=3/5 (p0,p10,p20)")
        }
        explain {
            sql "select * from t_multi_column_range_partition where a>1 and a<30 and (date_trunc(dt, 'day') >'2024-01-20' and date_trunc(dt, 'day') <'2024-02-10')"
            contains("partitions=3/5 (p0,p10,p20)")
        }

        explain {
            sql "select * from t_multi_column_range_partition where !(a > 1 and a < 30 and(date_trunc(dt, 'day') > '2024-01-20' and date_trunc(dt, 'day') < '2024-02-10') )"
            contains("partitions=4/5 (p0,p10,p20,p30)")
        }
        explain {
            sql "select * from t_multi_column_range_partition where !(a >= 10 or date_trunc(dt, 'day') < '2024-01-10' )"
            contains("partitions=1/5 (p0)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_range_partition WHERE a>1 and date_trunc(dt, 'day') regexp '2020-10-01 00:00:00'"
            contains("partitions=4/5 (p0,p10,p20,p30)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_range_partition WHERE a>1 and !date_trunc(dt, 'day') regexp '2020-10-01 00:00:00'"
            contains("partitions=4/5 (p0,p10,p20,p30)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_range_partition WHERE a=1 and !date_trunc(dt, 'day') regexp '2020-10-01 00:00:00'"
            contains("partitions=1/5 (p0)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_range_partition WHERE a!=1 and !date_trunc(dt, 'day') like '2020-10-01 00:00:00'"
            contains("partitions=4/5 (p0,p10,p20,p30)")
        }

        explain {
            sql "SELECT * FROM t_multi_column_range_partition WHERE a in (7,8) and date_trunc(dt, 'day') in ('2024-01-01', '2024-10-02')"
            contains("partitions=1/5 (p0)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_range_partition WHERE a not in (7,8) and date_trunc(dt, 'day') not in ('2024-01-01', '2024-10-02')"
            contains("partitions=4/5 (p0,p10,p20,p30)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_range_partition WHERE a in (7,8) and date_trunc(dt, 'day') not in ('2024-01-01', '2024-10-02')"
            contains("partitions=1/5 (p0)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_range_partition WHERE a in (7,8,15) and date_trunc(dt, 'day') <'2024-01-20'"
            contains("partitions=2/5 (p0,p10)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_range_partition WHERE (a in (7,8,15) or a>=31) and date_trunc(dt, 'day') >'2024-01-20'"
            contains("partitions=3/5 (p0,p10,p30)")
        }

        explain {
            sql "select * from t_multi_column_partition_datetime_first where dt is null"
            contains("VEMPTYSET")
        }
        explain {
            sql "select * from t_multi_column_partition_datetime_first where date_trunc(dt,'day') is null"
            contains("partitions=3/5 (p0,p10,p30)")
        }
        explain {
            sql "select * from t_multi_column_partition_datetime_first where date_trunc(dt,'day') is not null"
            contains("partitions=3/5 (p0,p10,p30)")
        }
        explain {
            sql "select * from t_multi_column_partition_datetime_first where a=1 and date_trunc(dt,'month') is null"
            contains("partitions=1/5 (p30)")
        }
        explain {
            sql "select * from t_multi_column_partition_datetime_first where a>1 and date_trunc(dt,'month') is null"
            contains("partitions=1/5 (p30)")
        }
        explain {
            sql "select * from t_multi_column_partition_datetime_first where date_trunc(dt,'month') is null"
            contains("partitions=1/5 (p30)")
        }
        explain {
            sql "select * from t_multi_column_partition_datetime_first where a=1 and date_trunc(dt,'month') is not null"
            contains("partitions=3/5 (p0,p10,p30)")
        }

        explain {
            sql "SELECT * FROM t_multi_column_partition_datetime_first WHERE date_trunc(dt,'day')<'2024-1-19 00:00:00'"
            contains("partitions=2/5 (p0,p10)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_partition_datetime_first WHERE date_trunc(dt,'day')<='2024-1-10 00:00:00'"
            contains("partitions=2/5 (p0,p10)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_partition_datetime_first WHERE date_trunc(dt,'day')>'2024-1-10 00:00:00'"
            contains("partitions=2/5 (p10,p30)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_partition_datetime_first WHERE date_trunc(dt,'day')>'2024-1-11 00:00:00'"
            contains("partitions=2/5 (p10,p30)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_partition_datetime_first WHERE date_trunc(dt,'day')>='2024-1-10 00:00:00'"
            contains("partitions=3/5 (p0,p10,p30)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_partition_datetime_first WHERE date_trunc(dt,'day')='2024-1-2 00:00:00'"
            contains("partitions=1/5 (p0)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_partition_datetime_first WHERE date_trunc(dt,'day')='2020-10-01 00:00:00'"
            contains("VEMPTYSET")
        }
        explain {
            sql "SELECT * FROM t_multi_column_partition_datetime_first WHERE date_trunc(dt,'day')<=>'2020-10-01 00:00:00'"
            contains("VEMPTYSET")
        }
        explain {
            sql "SELECT * FROM t_multi_column_partition_datetime_first WHERE date_trunc(dt,'day')!='2020-10-01 00:00:00'"
            contains("partitions=3/5 (p0,p10,p30)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_partition_datetime_first WHERE date_trunc(dt,'day')<'2024-1-19 00:00:00' and a>10"
            contains("partitions=2/5 (p0,p10)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_partition_datetime_first WHERE date_trunc(dt,'day')<='2024-1-10 00:00:00' and a=10"
            contains("partitions=2/5 (p0,p10)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_partition_datetime_first WHERE date_trunc(dt,'day')>'2024-1-10 00:00:00' and a<2"
            contains("partitions=2/5 (p10,p30)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_partition_datetime_first WHERE date_trunc(dt,'day')='2024-1-10 00:00:00' and a >100"
            contains("partitions=2/5 (p0,p10)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_partition_datetime_first WHERE date_trunc(dt,'day')='2024-1-1 00:00:00' and a <0"
            contains("partitions=1/5 (p0)")
        }


        explain {
            sql "SELECT * FROM t_multi_column_partition_datetime_first WHERE coalesce(date_trunc(dt,'day'), null, '2020-01-01') <'2021-1-07 00:00:00'"
            contains("partitions=3/5 (p0,p10,p30)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_range_partition WHERE coalesce(date_trunc(dt,'day') <'2021-1-07 00:00:00' , true, false) and a=9"
            contains("partitions=1/5 (p0)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_range_partition WHERE if(date_trunc(dt,'day') <'2021-1-07 00:00:00', true, false) and a<20"
            contains("partitions=2/5 (p0,p10)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_range_partition WHERE if(a>1, dt<'2001-1-01 00:00:00', dt<'2001-1-01 00:00:00')"
            contains("partitions=4/5 (p0,p10,p20,p30)")
        }
        explain {
            sql "SELECT * FROM t_multi_column_range_partition WHERE if(dt<'2021-1-01 00:00:00', dt<'2001-1-01 00:00:00', dt>'2031-1-01 00:00:00')"
            contains("partitions=4/5 (p0,p10,p20,p30)")
        }

        explain {
            sql """SELECT * FROM t_multi_column_range_partition WHERE case when dt<'2024-1-10 00:00:00' then false when dt<'2024-1-20 00:00:00' then false
    else true end and a=10;"""
            contains("partitions=2/5 (p0,p10)")
        }

        explain {
            sql """SELECT * FROM t_multi_column_range_partition WHERE case when date_trunc(dt,'day')<'2024-1-10 00:00:00' then false when date_trunc(dt,'day')<'2024-1-20 00:00:00' then true
    else true end and a<20;"""
            contains("partitions=2/5 (p0,p10)")
        }
        explain {
            sql """SELECT * FROM t_multi_column_range_partition WHERE case when date_trunc(dt,'day')<'2024-1-10 00:00:00' then dt
    else '2023-01-01 00:00:00' end <'2021-01-06 00:00:00' and a<20;"""
            contains("partitions=2/5 (p0,p10)")
        }
    }
}