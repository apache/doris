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

suite("test_nanosecond_datev2_datetimev2") {
    sql "drop table if exists nanosecond_datev2_datetimev2"
    sql """
        create table nanosecond_datev2_datetimev2 (
            id int,
            date_value datev2,
            datetime_value datetimev2(6)
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into nanosecond_datev2_datetimev2 values
            (1, '0000-01-01', '0000-01-01 00:00:00.000001'),
            (2, '9999-12-31', '9999-12-31 23:59:59.999999'),
            (3, '1970-01-01', '1970-01-01 00:00:00.000000'),
            (4, '2026-08-27', '2026-08-27 17:50:00.123456'),
            (5, null, null)
    """

    def constantSql = """
        select
            nanosecond(cast('0000-01-01' as datev2)),
            nanosecond(cast('9999-12-31' as datev2)),
            nanosecond(cast('1970-01-01' as datev2)),
            nanosecond(cast('2026-08-27' as datev2)),
            nanosecond(cast('0000-01-01 00:00:00.000001' as datetimev2(6))),
            nanosecond(cast('9999-12-31 23:59:59.999999' as datetimev2(6))),
            nanosecond(cast('1970-01-01 00:00:00.000000' as datetimev2(6))),
            nanosecond(cast('2026-08-27 17:50:00.123456' as datetimev2(6))),
            nanosecond(cast(null as datev2)),
            nanosecond(cast(null as datetimev2(6)))
    """

    sql "set debug_skip_fold_constant = false"
    qt_nanosecond_datev2_datetimev2_fold constantSql
    sql "set debug_skip_fold_constant = true"
    qt_nanosecond_datev2_datetimev2_runtime constantSql
    sql "set debug_skip_fold_constant = false"

    order_qt_nanosecond_datev2_datetimev2_columns """
        select id, nanosecond(date_value), nanosecond(datetime_value)
        from nanosecond_datev2_datetimev2
        order by id
    """
}
