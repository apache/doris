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

suite("test_timestamp_ns_join") {
    sql "drop table if exists timestamp_ns_join_left"
    sql "drop table if exists timestamp_ns_join_right"
    for (def tableName : ["timestamp_ns_join_left", "timestamp_ns_join_right"]) {
        sql """
            create table ${tableName} (
                id int,
                dt timestamp_ns
            )
            duplicate key(id)
            distributed by hash(id) buckets 2
            properties("replication_num" = "1")
        """
    }
    sql """
        insert into timestamp_ns_join_left values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1970-01-01 00:00:00.000000000'),
        (3, '1970-01-01 00:00:00.000000001'),
        (4, null)
    """
    sql """
        insert into timestamp_ns_join_right values
        (11, '1677-09-21 00:12:43.145224192'),
        (12, '1970-01-01 00:00:00.000000001'),
        (13, '2262-04-11 23:47:16.854775807'),
        (14, null)
    """

    order_qt_inner_join """
        select l.id, l.dt, r.id
        from timestamp_ns_join_left l
        join timestamp_ns_join_right r on l.dt = r.dt
        order by l.id, r.id
    """
    order_qt_left_join """
        select l.id, l.dt, r.id
        from timestamp_ns_join_left l
        left join timestamp_ns_join_right r on l.dt = r.dt
        order by l.id, r.id
    """
}
