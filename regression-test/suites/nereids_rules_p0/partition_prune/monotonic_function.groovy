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

suite("monotonic_function") {
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
        sql """explain select * from monotonic_function_t where from_millisecond(a) > '2001-09-09 12:33:19' """
        contains("partitions=2/4 (p3,p4)")
    }
    explain {
        sql """explain select * from monotonic_function_t where from_microsecond(a) < '2000-09-09 12:33:19' """
        contains("partitions=3/4 (p1,p2,p3)")
    }
    explain {
        sql """explain select * from monotonic_function_t where from_microsecond(a) > '2002-09-09 12:33:19' """
        contains("partitions=2/4 (p1,p4)")
    }
}