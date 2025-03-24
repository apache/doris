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

suite("varchar_as_date_partition_col") {
    sql "set ENABLE_FOLD_CONSTANT_BY_BE=false"
    sql"""drop table if exists partition_varchar;"""
    sql """CREATE TABLE partition_varchar(a int, dt varchar(10), rdt datetime) PARTITION BY list(dt) (
    partition p20240101 values in ("20240101","20240102"),
    partition p20240103 values  in ("20240103","20240104"),
    partition p20240105 values  in  ("20240105","20240106"),
    partition p20240107 values  in ("20240107","20240108"),
    partition p20240109 values  in ("20240109","20240110"),
    partition p20240111 values  in ("20240111","20240112"),
    partition p20240113 values  in ("20240113","20240114"),
    partition p20240115 values  in ("20240115","20240116"),
    partition p20240117 values  in ("20240117","20240118")
    )
    distributed BY hash(a)
    properties(
    "replication_num"="1"
    );"""
    sql """
    insert into partition_varchar 
    SELECT number, DATE_FORMAT(date_add('2024-01-01', interval number day),"yyyyMMdd"),date_add('2024-01-01', interval number day)
    FROM numbers('number'='10');"""

    sql "drop table if exists partition_varchar_has_null"
    sql """CREATE TABLE partition_varchar_has_null(a int, dt varchar(10), rdt datetime) PARTITION BY list(dt) (
    partition p20240101 values in ("20240101","20240102",null),
    partition p20240103 values  in ("20240103","20240104"),
    partition p20240105 values  in  ("20240105","20240106"),
    partition p20240107 values  in ("20240107","20240108"),
    partition p20240109 values  in ("20240109","20240110"),
    partition p20240111 values  in ("20240111","20240112"),
    partition p20240113 values  in ("20240113","20240114"),
    partition p20240115 values  in ("20240115","20240116"),
    partition p20240117 values  in ("20240117","20240118")
    )
    distributed BY hash(a)
    properties(
    "replication_num"="1"
    );"""

    sql """insert into partition_varchar_has_null 
    SELECT number, DATE_FORMAT(date_add('2024-01-01', interval number day),"yyyyMMdd"),date_add('2024-01-01', interval number day)
    FROM numbers('number'='10');"""
    sql """insert into partition_varchar_has_null values(11,null,null)"""


    explain {
        sql "SELECT * FROM partition_varchar WHERE dt<'20240110' and dt>'20240104'"
        contains("partitions=3/9 (p20240105,p20240107,p20240109)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE dt<'20240104' or dt>'20240110'"
        contains("partitions=2/9 (p20240101,p20240103)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE dt<date_format(now(),'yyyyMMdd') or dt > date_format(cast(now() as date) - interval 12 month,'yyyyMMdd')"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE date_trunc(dt,'day')< '20240105'"
        contains("partitions=2/9 (p20240101,p20240103)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE date_trunc(dt,'day')< date_format(cast(now() as date) - interval 10 month,'yyyyMMdd')"
        contains(" partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE (date_trunc(dt,'day')<'20240105' or dt>='20240108') or dt<=>'20240110';"
        contains("partitions=4/9 (p20240101,p20240103,p20240107,p20240109)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE (dt<'20240107' and dt>='20240104') or dt<=>'20240109'"
        contains("partitions=3/9 (p20240103,p20240105,p20240109)")
    }
    explain {
        sql "SELECT count(*) FROM partition_varchar WHERE (date_trunc(dt,'day')<'2024-1-03' and date_trunc(dt,'day')>='2024-1-01' )"
        contains("partitions=1/9 (p20240101)")
    }

    explain {
        sql """
        SELECT count(*) FROM partition_varchar WHERE
        !(date_trunc(dt,'day')<'2024-1-03' and date_trunc(dt,'day')>='2024-1-01' )
        """
        contains("partitions=4/9 (p20240103,p20240105,p20240107,p20240109)")
    }


    explain {
        sql "SELECT * FROM partition_varchar WHERE !('2020-09-01' not like '%2020-10-01 00:00:00%')"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE  !('2020-09-01' not in ('2020-10-01','2020-09-01'))"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE  !('2020-09-01' in ('2020-10-01','2020-09-01'))"
        contains("VEMPTYSET")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE !('2020-10-02'>'2020-10-01')"
        contains("VEMPTYSET")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE !(dt>='20241001' and a<100)"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE !(a<100 )"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }

    explain {
        sql "SELECT * FROM partition_varchar WHERE !(dt>='20240102' or a<100 )"
        contains("partitions=1/9 (p20240101)")
    }

    explain {
        sql "SELECT * FROM partition_varchar WHERE !(dt>='20240102' or (a<100 and dt<'20240801'))"
        contains("partitions=1/9 (p20240101)")
    }

    explain {
        sql "SELECT * FROM partition_varchar WHERE dt regexp '2024-10-01'"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }

    explain {
        sql "SELECT * FROM partition_varchar WHERE dt like '%20240101'"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE dt like '20240101'"
        contains("partitions=1/9 (p20240101)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE dt in ('20240101', '20241002')"
        contains("partitions=1/9 (p20240101)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE dt not in ('20241001', '20241002')"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }

    //partition key has null value
    explain {
        sql "select * from partition_varchar_has_null where dt is null"
        contains("partitions=1/9 (p20240101)")
    }

    explain {
        sql "select * from partition_varchar_has_null where dt is not null"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }

    explain {
        sql "select * from partition_varchar_has_null where not dt is null"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }
    explain {
        sql "select * from partition_varchar_has_null where !(dt is null)"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }
    explain {
        sql "select * from partition_varchar_has_null where dt <=> null"
        contains("partitions=1/9 (p20240101)")
    }
    explain {
        sql "select * from partition_varchar_has_null where !(dt <=> null)"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }
    //partition key has no null value
    explain {
        sql "select * from partition_varchar where dt is null"
        contains("VEMPTYSET")
    }
    explain {
        sql "select * from partition_varchar where dt is not null"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }
    explain {
        sql "select * from partition_varchar where not dt is null"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }
    explain {
        sql "select * from partition_varchar where !(dt is null)"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }
    explain {
        sql "select * from partition_varchar where dt <=> null"
        contains("VEMPTYSET")
    }
    explain {
        sql "select * from partition_varchar where !(dt <=> null)"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }
    explain {
        sql "select * from partition_varchar where  date_trunc(dt,'month') is null"
        contains("VEMPTYSET")
    }
    explain {
        sql "select * from partition_varchar where  date_trunc(dt,'month') is not null"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE dt<'20240103'"
        contains("partitions=1/9 (p20240101)")
    }

    explain {
        sql "SELECT * FROM partition_varchar WHERE dt<='20240103'"
        contains("partitions=2/9 (p20240101,p20240103)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE dt>'20240103'"
        contains("partitions=4/9 (p20240103,p20240105,p20240107,p20240109)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE dt>='20240103'"
        contains("partitions=4/9 (p20240103,p20240105,p20240107,p20240109)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE dt='20240103'"
        contains("partitions=1/9 (p20240103)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE dt<=>'20240103'"
        contains("partitions=1/9 (p20240103)")
    }
    explain {
        sql "SELECT * FROM partition_varchar WHERE dt!='20240103'"
        contains("partitions=5/9 (p20240101,p20240103,p20240105,p20240107,p20240109)")
    }
}