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

// AUTO RANGE PARTITION on a nullable column. NULL orders below every value, so NULL rows live in the
// partition whose range starts at the type's minimum, 0000-01-01 (named p00000101000000).
suite("test_auto_range_partition_nullable") {
    def partitionNames = { String table ->
        (sql "show partitions from ${table}").collect { it[1] as String }.sort()
    }
    def countIn = { String table, String partition ->
        (sql "select count(*) from ${table} partition(${partition})")[0][0] as long
    }

    sql "drop table if exists auto_range_nullable"
    sql """
        create table auto_range_nullable (
            k0 int not null,
            d date null
        )
        duplicate key(k0)
        auto partition by range (date_trunc(d, 'month')) ()
        distributed by hash(k0) buckets 1
        properties("replication_num" = "1");
    """

    sql """ insert into auto_range_nullable values (1, null), (2, '2024-03-05'), (3, null), (4, '2024-04-30'); """
    assertEquals(["p00000101000000", "p20240301000000", "p20240401000000"], partitionNames("auto_range_nullable"))
    assertEquals(2, countIn("auto_range_nullable", "p00000101000000"))

    // later NULLs and the type's minimum reuse the same partition
    sql """ insert into auto_range_nullable values (5, null), (6, '0000-01-15'); """
    assertEquals(3, partitionNames("auto_range_nullable").size())
    assertEquals(4, countIn("auto_range_nullable", "p00000101000000"))

    // predicates: IS NULL finds the NULL rows (partition pruning keeps their partition), equality never matches NULL
    assertEquals(3, (sql "select count(*) from auto_range_nullable where d is null")[0][0] as long)
    assertEquals(3, (sql "select count(*) from auto_range_nullable where d is not null")[0][0] as long)
    assertEquals(1, (sql "select count(*) from auto_range_nullable where d = '2024-03-05'")[0][0] as long)

    // INSERT OVERWRITE with auto-detected partitions replaces the NULL partition like any other
    sql """ insert overwrite table auto_range_nullable partition(*) values (7, null); """
    assertEquals(1, countIn("auto_range_nullable", "p00000101000000"))
    assertEquals(1, countIn("auto_range_nullable", "p20240301000000"))

    // datetime with a sub-second scale and an hourly function
    sql "drop table if exists auto_range_nullable_dt"
    sql """
        create table auto_range_nullable_dt (
            k0 datetime(6) null
        )
        auto partition by range (date_trunc(k0, 'hour')) ()
        distributed by hash(k0) buckets 1
        properties("replication_num" = "1");
    """
    sql """ insert into auto_range_nullable_dt values (null), ('2024-01-01 10:20:30.123456'); """
    assertEquals(["p00000101000000", "p20240101100000"], partitionNames("auto_range_nullable_dt"))
    assertEquals(1, countIn("auto_range_nullable_dt", "p00000101000000"))

    // an explicit first partition with an open lower bound already holds NULL: nothing new is created
    sql "drop table if exists auto_range_nullable_lt"
    sql """
        create table auto_range_nullable_lt (
            k0 int not null,
            d datetime null
        )
        duplicate key(k0)
        auto partition by range (date_trunc(d, 'day')) (
            partition p_before values less than ('2020-01-01 00:00:00')
        )
        distributed by hash(k0) buckets 1
        properties("replication_num" = "1");
    """
    sql """ insert into auto_range_nullable_lt values (1, null), (2, '2024-01-01 00:00:00'); """
    assertEquals(["p20240101000000", "p_before"], partitionNames("auto_range_nullable_lt"))
    assertEquals(1, countIn("auto_range_nullable_lt", "p_before"))

    // allow_partition_column_nullable still forbids a nullable partition column when it is off
    sql "set allow_partition_column_nullable = false"
    try {
        test {
            sql """
                create table auto_range_nullable_off (
                    d date null
                )
                auto partition by range (date_trunc(d, 'month')) ()
                distributed by hash(d) buckets 1
                properties("replication_num" = "1");
            """
            exception "The partition column must be NOT NULL with allow_partition_column_nullable OFF"
        }
    } finally {
        sql "set allow_partition_column_nullable = true"
    }
}
