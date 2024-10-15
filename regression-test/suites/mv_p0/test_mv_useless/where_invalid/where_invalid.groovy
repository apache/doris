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

suite ("where_invalid") {
    sql """drop table if exists a_table;"""

    sql """
        create table a_table(
            k1 int null,
            k2 int not null,
            k3 bigint null,
            k4 bigint sum null,
            k5 bitmap bitmap_union ,
            k6 hll hll_union 
        )
        aggregate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
        """

    sql "insert into a_table select 1,1,1,1,to_bitmap(1),hll_hash(1);"

    createMV("create materialized view ma1 as select k1,bitmap_union(k5) from a_table group by k1;")

    sql "insert into a_table select 2,2,2,2,to_bitmap(2),hll_hash(2);"

    test {
        sql "delete from a_table where k2=1;"
        exception "errCode = 2,"
    }
    sql "delete from a_table where k1=1;"

    qt_test "select k1,bitmap_count(bitmap_union(k5)) from a_table group by k1;"

    test {
        sql "create materialized view where_1 as select k1,k4 from a_table where k4 =1;"
        exception "errCode = 2,"
    }

    test {
        sql "create materialized view where_2 as select k1,sum(k4) from a_table where k4 =1 group by k1;"
        exception "errCode = 2,"
    }

    test {
        sql "create materialized view where_2 as select k1,sum(k4) from a_table where k1+k4 =1 group by k1;"
        exception "errCode = 2,"
    }
}
