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

suite("one_col_range_partition") {
    sql "set ENABLE_FOLD_CONSTANT_BY_BE=false"
    sql "drop table if exists one_col_range_partition_date"
    sql """
    create table one_col_range_partition_date(a int, dt datetime, d date, c varchar(100)) duplicate key(a)
    partition by range(dt)
    (
            from ("2020-01-01") to ("2024-12-31") interval 1 day
    )
    distributed by hash(a)
    properties("replication_num"="1");
    """
    sql """
    INSERT INTO one_col_range_partition_date SELECT number, 
    date_add('2020-01-01 00:00:00', interval number hour), 
    cast(date_add('2020-01-01 00:00:00', interval number hour) as date), cast(number as varchar(65533)) FROM numbers('number'='10000');
    """

    sql "drop table if exists one_col_range_partition_date_has_null"
    sql """create table one_col_range_partition_date_has_null (a int, dt datetime, d date, c varchar(100)) duplicate key(a)
    partition by range(dt) (
            partition p1 values less than ("2017-01-01"),
            partition p2 values less than ("2018-01-01"),
            partition p3 values less than ("2019-01-01"),
            partition p4 values less than ("2020-01-01"),
            partition p5 values less than ("2021-01-01")
    )distributed by hash(a)
    properties("replication_num"="1");"""
    sql """INSERT INTO one_col_range_partition_date_has_null SELECT number,
    date_add('2017-01-01 00:00:00', interval number day),
    cast(date_add('2022-01-01 00:00:00', interval number day) as date), cast(number as varchar(65533)) FROM numbers('number'='1000');"""
    sql """INSERT INTO one_col_range_partition_date_has_null  values(3,null,null,null);"""



    // or and
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt<'2020-10-01 00:00:00' and dt>'2020-9-01 00:00:00'"
        contains("partitions=30/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt<'2020-10-01 00:00:00' or dt>'2020-9-01 00:00:00'"
        contains("partitions=417/1826")
    }

    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE (dt<'2020-10-01 00:00:00' or dt>'2020-9-01 00:00:00') and dt<=>'2020-9-5 00:00:00'"
        contains("partitions=1/1826 (p_20200905)")
    }

    explain {
        sql """SELECT count(*) FROM one_col_range_partition_date WHERE
        !(dt<'2020-10-01 00:00:00' and dt>'2020-9-01 00:00:00' or date_trunc(dt,'month')<'2020-7-01' and date_trunc(dt,'month')>'2020-6-01' )"""
        contains("partitions=388/1826")
    }

    explain {
        sql """SELECT count(*) FROM one_col_range_partition_date WHERE
        (dt>='2020-10-01 00:00:00' or dt<='2020-9-01 00:00:00' ) and (date_trunc(dt,'month')>='2020-9-01' or date_trunc(dt,'month')<='2020-6-01' )"""
        contains("partitions=327/1826")
    }

    // !
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE !('2020-09-01' not like '%2020-10-01 00:00:00%')"
        contains("partitions=417/1826 ")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE  !('2020-09-01' not in ('2020-10-01','2020-09-01'))"
        contains("partitions=417/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE  !('2020-09-01' in ('2020-10-01','2020-09-01'))"
        contains("VEMPTYSET")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE !('2020-10-02'>'2020-10-01')"
        contains("VEMPTYSET")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE !(dt>='2020-10-01 00:00:00' and a<100)"
        contains("partitions=417/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE !(a<100 and  c>10)"
        contains("partitions=417/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE !(a<100 or  c>10)"
        contains("partitions=417/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE !(dt>='2020-10-01 00:00:00' or a<100 )"
        contains("partitions=274/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE !(dt<'2020-2-01 1:00:00' or (a<100 and dt>='2020-10-01 12:00:00'))"
        contains("partitions=386/1826")
    }

    // like regexp
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt regexp '2020-10-01 00:00:00'"
        contains("partitions=417/1826")
    }

    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt like '2020-10-01 00:00:00'"
        contains("partitions=417/1826")
    }

    // in
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt in ('2020-10-01 00:10:00', '2020-10-02 00:00:00')"
        contains("partitions=2/1826")
    }

    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt not in ('2020-10-01 00:10:00', '2020-10-02 00:00:00')"
        contains("partitions=417/1826")
    }

    // is null
    //partition key has null value
    explain {
        sql "select * from one_col_range_partition_date_has_null where dt is null"
        contains("partitions=1/5 (p1)")
    }
    explain {
        sql "select * from one_col_range_partition_date_has_null where dt is not null"
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql "select * from one_col_range_partition_date_has_null where not dt is null"
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql "select * from one_col_range_partition_date_has_null where !(dt is null)"
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }

    explain {
        sql "select * from one_col_range_partition_date_has_null where dt <=> null"
        contains("partitions=1/5 (p1)")
    }
    explain {
        sql "select * from one_col_range_partition_date_has_null where !(dt <=> null)"
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql "select * from one_col_range_partition_date where dt is null"
        contains("VEMPTYSET")
    }
    explain {
        sql "select * from one_col_range_partition_date where dt is not null"
        contains("partitions=417/1826")
    }
    explain {
        sql "select * from one_col_range_partition_date where not dt is null"
        contains("partitions=417/1826")
    }
    explain {
        sql "select * from one_col_range_partition_date where !(dt is null)"
        contains("partitions=417/1826")
    }

    explain {
        sql "select * from one_col_range_partition_date where dt <=> null"
        contains("VEMPTYSET")
    }
    explain {
        sql "select * from one_col_range_partition_date where !(dt <=> null)"
        contains("partitions=417/1826")
    }

    // op
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt<'2020-10-01 00:00:00'"
        contains("partitions=274/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt<='2020-10-01 00:00:00'"
        contains("partitions=275/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt>'2020-10-01 00:00:00'"
        contains("partitions=143/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt>='2020-10-01 00:00:00'"
        contains("partitions=143/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt='2020-10-01 00:00:00'"
        contains("partitions=1/1826 (p_20201001)")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt<=>'2020-10-01 00:00:00'"
        contains("partitions=1/1826 (p_20201001)")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt!='2020-10-01 00:00:00'"
        contains("partitions=417/1826")
    }

    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE coalesce(dt, null, '2020-01-01') <'2020-10-01 00:00:00'"
        contains("partitions=417/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE coalesce(dt <'2020-2-01 00:00:00' , true, false)"
        contains("partitions=31/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE coalesce(dt >'2020-2-01 00:00:00' , true, false)"
        contains("partitions=386/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE coalesce(null ,dt< '2001-1-01 00:00:00', dt> '2020-10-01 00:00:00' )"
        contains("partitions=143/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE if(dt<'2001-1-01 00:00:00', true, false)"
        contains("VEMPTYSET")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE if(a>1, dt<'2001-1-01 00:00:00', dt>'2001-1-01 00:00:00')"
        contains("partitions=417/1826 ")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE if(dt<'2021-1-01 00:00:00', dt<'2001-1-01 00:00:00', dt>'2001-1-01 00:00:00')"
        contains("partitions=417/1826")
    }
    explain {
        sql """SELECT * FROM one_col_range_partition_date WHERE case when dt<'2021-1-01 00:00:00' then false when dt<'2021-5-01' then false
        else true end;"""
        contains("partitions=417/1826")
    }
    explain {
        sql """SELECT * FROM one_col_range_partition_date WHERE case when dt<'2022-1-01 00:00:00' then dt
        else '2023-1-01 00:00:00' end <'2021-1-01 00:00:00' ;"""
        contains("partitions=417/1826")
    }

    // predicates has non partition column
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE '2020-09-01' not like '%2020-10-01 00:00:00%'"
        contains("partitions=417/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE  '2020-09-01' not in ('2020-10-01','2020-09-01')"
        contains("VEMPTYSET")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE  '2020-09-01' in ('2020-10-01','2020-09-01')"
        contains("partitions=417/1826")
    }

    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE '2020-10-02'>'2020-10-01'"
        contains("partitions=417/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt>='2020-10-01 00:00:00' and a<100"
        contains("partitions=143/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE a<100 and  c>10"
        contains("partitions=417/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE a<100 or  c>10"
        contains("partitions=417/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt>='2020-10-01 00:00:00' or a<100"
        contains("partitions=417/1826")
    }

    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE not (dt>='2020-10-01 00:00:00' and a<100)"
        contains("partitions=417/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt='2020-10-01 00:00:00' or a<100"
        contains("partitions=417/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE not(dt!='2020-10-01 00:00:00' or a<100)"
        contains("partitions=1/1826 (p_20201001)")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt<'2020-1-01 1:00:00' or a<100 and dt>='2020-10-01 12:00:00'"
        contains("partitions=144/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt>='2020-10-01 00:00:00' and dt=d"
        contains("partitions=143/1826")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date WHERE dt>='2020-10-01 00:00:00' or dt=d"
        contains("partitions=417/1826")
    }

}