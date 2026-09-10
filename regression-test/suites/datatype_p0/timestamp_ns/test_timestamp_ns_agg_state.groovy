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

suite("test_timestamp_ns_agg_state") {
    sql "set enable_agg_state = true"
    sql "drop table if exists timestamp_ns_agg_state"
    sql """
        create table timestamp_ns_agg_state (
            id int,
            dt_state agg_state<max(timestamp_ns not null)> generic
        )
        aggregate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_agg_state values
        (1, max_state(cast('1677-09-21 00:12:43.145224192' as timestamp_ns))),
        (1, max_state(cast('1970-01-01 00:00:00.000000000' as timestamp_ns))),
        (1, max_state(cast('2262-04-11 23:47:16.854775807' as timestamp_ns)))
    """
    order_qt_aggregate_state """
        select id, max_merge(dt_state)
        from timestamp_ns_agg_state
        group by id
        order by id
    """
}
