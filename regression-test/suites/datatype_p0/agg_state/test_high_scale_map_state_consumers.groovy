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

suite("test_high_scale_map_state_consumers") {
    sql "set enable_agg_state=true"
    sql "set enable_decimal256=true"
    sql "drop table if exists high_scale_map_state_consumers"
    sql """create table high_scale_map_state_consumers (
        k int, s agg_state<avg_map(map<int,decimal(76,40)> not null)> generic
    ) aggregate key(k) distributed by hash(k) buckets 1
    properties("replication_num"="1")"""

    // Empty tables are sufficient: rebuilding AVG_MAP's nested signature used to fail during planning.
    // The stored result remains Decimal(76,40) even though a new producer would use the current session.
    sql "set enable_decimal256=false"
    explain {
        sql "select avg_map_merge(s) from high_scale_map_state_consumers"
        contains "avg_map_merge"
    }
    explain {
        sql "select avg_map_union(s) from high_scale_map_state_consumers"
        contains "avg_map_union"
    }
    explain {
        sql "select avg_map_finalize(avg_map_union(s)) from high_scale_map_state_consumers"
        contains "avg_map_finalize"
    }
}
