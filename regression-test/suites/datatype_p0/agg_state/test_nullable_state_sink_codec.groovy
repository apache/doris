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

// This suite changes an FE config. Run it alone and restore the config after each block.
suite("test_nullable_state_sink_codec", "nonConcurrent") {
    context.reconnectToMasterFe()
    sql "set enable_agg_state=true"
    // AGG_STATE cannot be cast to String in SQL. Its String serde sends raw bytes
    // through MySQL. These two empty-state encodings contain only bytes 0 and 1,
    // so JDBC's UTF-8 string conversion preserves every byte. Display them as hex.
    def bitmapBytesToHex = { row ->
        [row[0], row[1].getBytes("UTF-8").encodeHex().toString()] + row.drop(2)
    }
    sql "drop table if exists nullable_state_sink_codec"
    setFeConfigTemporary(["be_exec_version": 19]) {
        sql """
            create table nullable_state_sink_codec (
                k int not null,
                b agg_state<bitmap_intersect(bitmap null)> generic,
                s agg_state<sum(bigint null)> generic
            ) aggregate key(k) distributed by hash(k) buckets 1
            properties("replication_num"="1")
        """
    }
    setFeConfigTemporary(["be_exec_version": 20]) {
        // Without a sink, this version 20 producer writes the no-input marker 00.
        // Inserting the same expression below must select the stored column's codec.
        quickRunTest("source_codec_20", """
            select 1 k, bitmap_intersect_state(cast(null as bitmap)) order by k
        """, true, bitmapBytesToHex)
        // One row per INSERT path is sufficient. The Bitmap argument is NULL so
        // the state has no input. Its version 19 bytes must be 010100, not 00.
        // SUM has no input in the first row and one valid input in the second row.
        // The argument types and nullability already match the stored types.
        // Thus only a codec mismatch can make the sink rebuild these producers.
        sql """
            insert into nullable_state_sink_codec
            select 1, bitmap_intersect_state(cast(null as bitmap)),
                      sum_state(cast(null as bigint))
        """
        sql """
            insert into nullable_state_sink_codec
            values (2, bitmap_intersect_state(cast(null as bitmap)),
                       sum_state(nullable(cast(1 as bigint))))
        """
    }
    for (int queryVersion : [19, 20]) {
        setFeConfigTemporary(["be_exec_version": queryVersion]) {
            // Check stored bytes as well as final values. The new reader accepts
            // both Bitmap formats, so a result-only check would miss a wrong codec.
            quickRunTest("stored_codec_${queryVersion}", """
                select k, b,
                       bitmap_to_string(bitmap_intersect_finalize(b)), sum_finalize(s)
                from nullable_state_sink_codec order by k
            """, true, bitmapBytesToHex)
        }
    }
}
