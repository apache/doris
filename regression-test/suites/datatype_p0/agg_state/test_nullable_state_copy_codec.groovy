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

// Change the FE version only while creating or querying these tables. The helper restores it.
suite("test_nullable_state_copy_codec", "nonConcurrent") {
    context.reconnectToMasterFe()
    sql "set enable_agg_state=true"
    sql "drop table if exists nullable_state_copy_old"
    sql "drop table if exists nullable_state_copy_new"
    setFeConfigTemporary(["be_exec_version": 19]) {
        sql """
            create table nullable_state_copy_old (
                k int not null,
                s agg_state<sum(bigint null)> generic,
                n agg_state<sum(bigint not null)> generic,
                w agg_state<avg_weighted(double null, double null)> generic,
                b agg_state<bitmap_intersect(bitmap null)> generic
            ) aggregate key(k) distributed by hash(k) buckets 1
            properties("replication_num"="1")
        """
        // Two rows distinguish valid and internally empty states. Stored AGG_STATE columns
        // are NOT NULL; an outer SQL NULL is tested below with an expression.
        // SUM n also checks the fixed-length storage column. The other columns use byte codecs.
        sql """
            insert into nullable_state_copy_old values
                (1, sum_state(nullable(cast(7 as bigint))), sum_state(cast(7 as bigint)),
                    avg_weighted_state(nullable(cast(2 as double)), nullable(cast(3 as double))),
                    bitmap_intersect_state(nullable(to_bitmap(1)))),
                (2, sum_state(cast(null as bigint)), sum_state(cast(0 as bigint)),
                    avg_weighted_state(cast(null as double), nullable(cast(3 as double))),
                    bitmap_intersect_state(cast(null as bitmap)))
        """
    }
    setFeConfigTemporary(["be_exec_version": 20]) {
        sql """
            create table nullable_state_copy_new (
                k int not null,
                s agg_state<sum(bigint null)> generic,
                n agg_state<sum(bigint not null)> generic,
                w agg_state<avg_weighted(double null, double null)> generic,
                b agg_state<bitmap_intersect(bitmap null)> generic
            ) aggregate key(k) distributed by hash(k) buckets 1
            properties("replication_num"="1")
        """
        // The inputs are stored slots, not STATE producers. The cast must accept the old
        // storage payload without changing the source slot's version or rebuilding a value.
        sql "insert into nullable_state_copy_new select k, s, n, w, b from nullable_state_copy_old"
        // IF creates an outer SQL NULL without changing the stored column. The cast
        // must preserve that NULL and must not decode its unused nested state bytes.
        qt_nullable_state_cast """
            select k,
                   sum_finalize(cast(if(k = 2, null, s) as agg_state<sum(bigint null)>)),
                   cast(if(k = 2, null, s) as agg_state<sum(bigint null)>) is null
            from nullable_state_copy_old order by k
        """
        // The forward compatibility rule must not become a general reverse cast. A new
        // Bitmap no-input marker is not part of the old writer's published format.
        test {
            sql "insert into nullable_state_copy_old select k, s, n, w, b from nullable_state_copy_new"
            exception "cannot cast agg_state"
        }
    }
    for (int queryVersion : [19, 20]) {
        setFeConfigTemporary(["be_exec_version": queryVersion]) {
            quickRunTest("copy_values_${queryVersion}", """
                select k, sum_finalize(s), s is null, sum_finalize(n),
                       avg_weighted_finalize(w), w is null,
                       bitmap_to_string(bitmap_intersect_finalize(b)), b is null
                from nullable_state_copy_new order by k
            """, true)
            // Row 2 was written as an old empty Bitmap (010100). It must remain an empty
            // Bitmap after the copy, not become the new no-input marker (00). These bytes
            // are all 0 or 1, so JDBC UTF-8 conversion preserves them for the hex check.
            quickRunTest("copy_bitmap_bytes_${queryVersion}", """
                select k, b from nullable_state_copy_new where k = 2 order by k
            """, true, { row -> [row[0], row[1].getBytes("UTF-8").encodeHex().toString()] })
        }
    }
}
