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

suite("int_as_date_partition_col") {
    sql "set ENABLE_FOLD_CONSTANT_BY_BE=false"
    sql "drop table if exists partition_int"
    sql """CREATE TABLE partition_int(a int, dt int) PARTITION BY range(dt) (
            partition p20240101 values less than ("20240101"),
            partition p20240201 values less than ("20240201"),
            partition p20240301 values less than ("20240301"),
            partition p20240401 values less than ("20240401"),
            partition p20240501 values less than ("20240501"),
            partition p20240601 values less than ("20240601")
    )
    distributed BY hash(a)
    properties("replication_num"="1");"""

    sql """insert into partition_int
    SELECT number,cast(date_add('2023-11-05', interval number day) as int) FROM numbers('number'='150');"""
    sql "drop table if exists partition_int_has_null"
    sql """
    CREATE TABLE partition_int_has_null(a int, dt int) PARTITION BY range(dt) (
            partition p20240101 values less than ("20240101"),
            partition p20240201 values less than ("20240201"),
            partition p20240301 values less than ("20240301"),
            partition p20240401 values less than ("20240401"),
            partition p20240501 values less than ("20240501"),
            partition p20240601 values less than ("20240601")
    )
    distributed BY hash(a) properties("replication_num"="1");"""

    sql """insert into partition_int_has_null
    SELECT number,cast(date_add('2023-11-05', interval number day) as int) FROM numbers('number'='120');"""
    
    explain {
        sql "SELECT * FROM partition_int WHERE dt<'20240501' and dt>'20240301'"
        contains("partitions=2/6 (p20240401,p20240501)")
    }
    explain {
        sql "SELECT * FROM partition_int WHERE dt<'20240701' or dt>='20241001'"
        contains("partitions=5/6 (p20240101,p20240201,p20240301,p20240401,p20240501)")
    }

    explain {
        sql "SELECT * FROM partition_int WHERE (dt<'20240301' or dt>='20241001') or dt<=>'20240905'"
        contains("partitions=3/6 (p20240101,p20240201,p20240301)")
    }
    explain {
        sql "SELECT * FROM partition_int WHERE (dt<'20240301' and dt>='20240201') or dt<=>'20240905'"
        contains("partitions=1/6 (p20240301)")
    }

    explain {
        sql """SELECT count(*) FROM partition_int WHERE
        (date_trunc(dt,'month')<'2024-8-01' and date_trunc(dt,'month')>'2024-6-01' )"""
        contains("partitions=5/6 (p20240101,p20240201,p20240301,p20240401,p20240501)")
    }

    explain {
        sql """SELECT count(*) FROM partition_int WHERE date_trunc(dt,'month')<'2024-07-01' ;"""
        contains("partitions=5/6 (p20240101,p20240201,p20240301,p20240401,p20240501)")
    }

    explain {
        sql """SELECT count(*) FROM partition_int WHERE
        !(date_trunc(dt,'month')<'2024-8-01' and date_trunc(dt,'month')>'2024-6-01' );"""
        contains("partitions=5/6 (p20240101,p20240201,p20240301,p20240401,p20240501)")
    }

    explain {
        sql """
        SELECT count(*) FROM partition_int WHERE
        !(dt<'20241001'  or date_trunc(dt,'month')<'2024-7-01' and date_trunc(dt,'month')>'2024-6-01' )
        """
        contains("VEMPTYSET")
    }

    explain {
        sql "SELECT * FROM partition_int WHERE !('2020-09-01' not like '%2020-10-01 00:00:00%')"
        contains("partitions=5/6 (p20240101,p20240201,p20240301,p20240401,p20240501)")
    }
    explain {
        sql "SELECT * FROM partition_int WHERE  !('2020-09-01' not in ('2020-10-01','2020-09-01'))"
        contains("partitions=5/6 (p20240101,p20240201,p20240301,p20240401,p20240501)")
    }
    explain {
        sql "SELECT * FROM partition_int WHERE  !('2020-09-01' in ('2020-10-01','2020-09-01'))"
        contains("VEMPTYSET")
    }
    explain {
        sql "SELECT * FROM partition_int WHERE !('2020-10-02'>'2020-10-01')"
        contains("VEMPTYSET")
    }
    explain {
        sql "SELECT * FROM partition_int WHERE !(dt>='20240301' and a<100) "
        contains("partitions=5/6 (p20240101,p20240201,p20240301,p20240401,p20240501")
    }
    explain {
        sql "SELECT * FROM partition_int WHERE !(a<100 )"
        contains("partitions=5/6 (p20240101,p20240201,p20240301,p20240401,p20240501")
    }

    explain {
        sql "SELECT * FROM partition_int WHERE !(dt>='20240301' or a<100 )"
        contains("partitions=3/6 (p20240101,p20240201,p20240301)")
    }

    explain {
        sql "SELECT * FROM partition_int WHERE !(dt>='20240301' or (a<100 and dt<'20240801'))"
        contains("partitions=3/6 (p20240101,p20240201,p20240301)")
    }

    explain {
        sql "SELECT * FROM partition_int WHERE dt regexp '2024-10-01'"
        contains("partitions=5/6 (p20240101,p20240201,p20240301,p20240401,p20240501)")
    }

    explain {
        sql "SELECT * FROM partition_int WHERE dt like '20241001'"
        contains("partitions=5/6 (p20240101,p20240201,p20240301,p20240401,p20240501)")
    }
    explain {
        sql "SELECT * FROM partition_int WHERE dt in  ('20240301', '20240302')"
        contains("partitions=1/6 (p20240401)")
    }

    explain {
        sql "SELECT * FROM partition_int WHERE dt not in ('20240201', '20240202')"
        contains("partitions=5/6 (p20240101,p20240201,p20240301,p20240401,p20240501)")
    }

    explain {
        sql "select * from partition_int_has_null where dt is null"
        contains("partitions=1/6 (p20240101)")
    }
    explain {
        sql "select * from partition_int_has_null where dt is not null"
        contains("partitions=4/6 (p20240101,p20240201,p20240301,p20240401)")
    }
    explain {
        sql "select * from partition_int_has_null where not dt is null"
        contains("partitions=4/6 (p20240101,p20240201,p20240301,p20240401)")
    }
    explain {
        sql "select * from partition_int_has_null where !(dt is null)"
        contains("partitions=4/6 (p20240101,p20240201,p20240301,p20240401)")
    }
    explain {
        sql "select * from partition_int_has_null where dt <=> null"
        contains("partitions=1/6 (p20240101)")
    }
    explain {
        sql "select * from partition_int_has_null where !(dt <=> null)"
        contains("partitions=4/6 (p20240101,p20240201,p20240301,p20240401)")
    }

    explain {
        sql "SELECT * FROM partition_int WHERE dt<'20240301'"
        contains("partitions=3/6 (p20240101,p20240201,p20240301)")
    }
    explain {
        sql "SELECT * FROM partition_int WHERE dt<='20240301'"
        contains("partitions=4/6 (p20240101,p20240201,p20240301,p20240401)")
    }
    explain {
        sql "SELECT * FROM partition_int WHERE dt>'20240301'"
        contains("partitions=2/6 (p20240401,p20240501)")
    }
    explain {
        sql "SELECT * FROM partition_int WHERE dt>='20240301'"
        contains("partitions=2/6 (p20240401,p20240501)")
    }
    explain {
        sql "SELECT * FROM partition_int WHERE dt='20240301'"
        contains("partitions=1/6 (p20240401)")
    }
    explain {
        sql "SELECT * FROM partition_int WHERE dt<=>'20240302'"
        contains("partitions=1/6 (p20240401)")
    }
    explain {
        sql "SELECT * FROM partition_int WHERE dt!='20241001'"
        contains("partitions=5/6 (p20240101,p20240201,p20240301,p20240401,p20240501)")
    }
}