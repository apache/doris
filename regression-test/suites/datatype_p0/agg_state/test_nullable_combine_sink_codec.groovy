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

// This suite changes the FE codec version. Run it without concurrent suites.
suite("test_nullable_combine_sink_codec", "nonConcurrent") {
    context.reconnectToMasterFe()
    sql "set enable_agg_state=true"
    sql "drop table if exists nullable_combine_input"
    sql "drop table if exists nullable_combine_sink"
    sql """
        create table nullable_combine_input (k int not null, v int null, w double null)
        duplicate key(k) distributed by hash(k) buckets 1 properties("replication_num"="1")
    """
    // Two rows are sufficient: one valid group and one NULL-only group. The weight
    // is valid in both rows, so AVG_WEIGHTED must test the first nullable argument.
    sql "insert into nullable_combine_input values (1, 2, 3), (2, null, 3)"
    setFeConfigTemporary(["be_exec_version": 19]) {
        sql """
            create table nullable_combine_sink (
                k int not null,
                a agg_state<avg(int null)> generic,
                w agg_state<avg_weighted(double null, double null)> generic,
                b agg_state<bitmap_intersect(bitmap null)> generic
            ) aggregate key(k) distributed by hash(k) buckets 1 properties("replication_num"="1")
        """
    }
    setFeConfigTemporary(["be_exec_version": 20]) {
        for (int phase : [1, 2]) {
            sql "set agg_phase=${phase}"
            // COMBINE consumes raw rows. Its output must use version 19 before the
            // sink receives it. A CAST of the completed state cannot change its bytes.
            // Separate keys preserve each phase's result for inspection after the test.
            // TO_BITMAP returns an empty Bitmap for SQL NULL. Use IF to pass an actual
            // nullable Bitmap, so the second group contains no valid input state.
            sql """
                insert into nullable_combine_sink
                select k + ${phase} * 10, avg_combine(v),
                       avg_weighted_combine(cast(v as double), w),
                       bitmap_intersect_combine(if(v is null, null, to_bitmap(v)))
                from nullable_combine_input group by k
            """
        }
    }
    for (int queryVersion : [19, 20]) {
        setFeConfigTemporary(["be_exec_version": queryVersion]) {
            // Each valid group has AVG = 2 and AVG_WEIGHTED = (2 * 3) / 3 = 2.
            // The NULL-only group has NULL numeric results and an old empty Bitmap.
            quickRunTest("combine_values_${queryVersion}", """
                select k, avg_finalize(a), a is null, avg_weighted_finalize(w),
                       bitmap_to_string(bitmap_intersect_finalize(b))
                from nullable_combine_sink order by k
            """, true)
            // Old Bitmap empty-state bytes are 010100. A version-20 producer would
            // write 00 for this NULL-only group. Final values alone cannot detect it.
            // These bytes survive JDBC's UTF-8 conversion because each is 0 or 1.
            quickRunTest("combine_bytes_${queryVersion}", """
                select k, b from nullable_combine_sink where k in (12, 22) order by k
            """, true, { row -> [row[0], row[1].getBytes("UTF-8").encodeHex().toString()] })
        }
    }
}
