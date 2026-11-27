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

// This test changes an FE config. Run it without concurrent suites and restore the config.
suite("test_nullable_state_metadata_version", "nonConcurrent") {
    context.reconnectToMasterFe()
    sql "set enable_agg_state=true"
    sql "set agg_phase=2"
    sql "drop table if exists nullable_state_metadata_version"
    setFeConfigTemporary(["be_exec_version": 20]) {
        sql """
            create table nullable_state_metadata_version (
                k int not null,
                s agg_state<bitmap_intersect(bitmap)> generic
            ) aggregate key(k) distributed by hash(k) buckets 1
            properties("replication_num"="1")
        """
        // One stored value is sufficient. The other UNION branch has no input rows.
        sql """
            insert into nullable_state_metadata_version
            select 1,bitmap_intersect_state(to_bitmap(1))
        """
    }
    for (int queryVersion : [19, 20]) {
        setFeConfigTemporary(["be_exec_version": queryVersion]) {
            // The stored type remains version 20 even when the query uses version 19.
            // Its empty UNION state must stay [0], which contributes no bitmap.
            // If it becomes an actual empty bitmap, the final intersection loses {1}.
            "order_qt_empty_union_${queryVersion}"("""
                select bitmap_to_string(bitmap_intersect_merge(s))
                from (
                    select bitmap_intersect_union(s) s
                    from nullable_state_metadata_version where k<0
                    union all
                    select s from nullable_state_metadata_version where k=1
                ) states
            """)
        }
    }
}
