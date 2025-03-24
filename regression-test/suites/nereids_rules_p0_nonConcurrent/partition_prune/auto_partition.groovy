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

suite("auto_partition") {
    sql "drop table if exists one_col_range_partition_date_func"
    sql """create table one_col_range_partition_date_func (a int, dt datetime not null, d date, c varchar(100)) duplicate key(a)
    auto partition by range(date_trunc(dt,'day')) () distributed by hash(a) properties("replication_num"="1");"""
    sql """INSERT INTO one_col_range_partition_date_func SELECT number,
    date_add('2020-01-01 00:00:00', interval number hour),
    cast(date_add('2020-01-01 00:00:00', interval number hour) as date), cast(number as varchar(65533)) FROM numbers('number'='10000');"""
    sql "drop table if exists one_col_range_partition_date_func_month"
    sql """create table one_col_range_partition_date_func_month(a int, dt datetime not null, d date, c varchar(100)) duplicate key(a)
    auto partition by range(date_trunc(dt,'month')) () distributed by hash(a) properties("replication_num"="1");"""
    sql """INSERT INTO one_col_range_partition_date_func_month SELECT number,
    date_add('2020-01-01 00:00:00', interval number hour),
    cast(date_add('2020-01-01 00:00:00', interval number hour) as date), cast(number as varchar(65533)) FROM numbers('number'='10000');"""
    // auto partition
    // partition by range(date_trunc(dt, 'day'))
    explain {
        sql "SELECT * FROM one_col_range_partition_date_func WHERE dt<'2020-10-01 00:00:00'"
        contains("partitions=274/417")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date_func WHERE dt<='2020-10-01 00:00:00'"
        contains("partitions=275/417")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date_func WHERE dt>'2020-10-01 00:00:00'"
        contains("partitions=143/417")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date_func WHERE dt>='2020-10-01 00:00:00'"
        contains("partitions=143/417")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date_func WHERE dt='2020-10-01 00:00:00'"
        contains("partitions=1/417")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date_func WHERE dt<=>'2020-10-01 00:00:00'"
        contains("partitions=1/417")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date_func WHERE dt!='2020-10-01 00:00:00'"
        contains("partitions=417/417")
    }

    // partition by range(date_trunc(dt, 'month'))
    explain {
        sql "SELECT * FROM one_col_range_partition_date_func_month WHERE dt<'2020-10-01 00:00:00'"
        contains("partitions=9/14")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date_func_month WHERE dt<='2020-10-01 00:00:00'"
        contains("partitions=10/14")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date_func_month WHERE dt>'2020-10-01 00:00:00'"
        contains("partitions=5/14")
    }

    explain {
        sql "SELECT * FROM one_col_range_partition_date_func_month WHERE dt>='2020-10-01 00:00:00'"
        contains("partitions=5/14")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date_func_month WHERE dt='2020-10-01 00:00:00'"
        contains("partitions=1/14 (p20201001000000)")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date_func_month WHERE dt<=>'2020-10-01 00:00:00'"
        contains("partitions=1/14 (p20201001000000)")
    }
    explain {
        sql "SELECT * FROM one_col_range_partition_date_func_month WHERE dt!='2020-10-01 00:00:00'"
        contains("partitions=14/14")
    }
}