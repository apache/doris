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

suite("test_timestamp_ns_statistics") {
    sql "drop table if exists timestamp_ns_statistics"
    sql """
        create table timestamp_ns_statistics (
            id int,
            ts timestamp_ns
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_statistics values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1969-12-31 23:59:59.999999999'),
        (3, '1970-01-01 00:00:00.000000000'),
        (4, '2024-02-29 12:34:56.123456789'),
        (5, '2262-04-11 23:47:16.854775807'),
        (6, null)
    """

    sql "analyze table timestamp_ns_statistics(ts) with sync"
    sql "show column stats timestamp_ns_statistics(ts)"

    order_qt_data_after_analyze """
        select id, ts
        from timestamp_ns_statistics
        order by id
    """
}
