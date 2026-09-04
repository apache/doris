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

import org.apache.doris.regression.suite.ClusterOptions

// Reproduces the INSERT OVERWRITE ... PARTITION(*) hang on a multi-BE cluster.
//
// The race is WITHIN A SINGLE LOAD, on the receiver's per-LoadStream close path.
// A PARTITION(*) auto-detect overwrite that creates NEW partitions mid-load opens
// both non-incremental (base) and incremental (new partition) streams. On
// CLOSE_LOAD the buggy _dispatch does, per stream:
//     close();  _report_result();          // count + send this stream's EOS
//     lock; if incremental: _closing_stream_ids.push_back(id);
//            else: StreamClose(id);
//            if (all_closed) { StreamClose all in _closing_stream_ids; clear(); }
// The push_back of an incremental stream happens in a SEPARATE lock scope from
// close()'s counting. If the last (non-incremental) stream reaches all_closed and
// drains+clears the list before a counted-but-delayed incremental stream does its
// push_back, that incremental stream is never StreamClose'd -> the load's brpc
// streams never finish -> the load hangs forever.
//
// In a fast single-host docker cluster that window is microseconds, so it will
// not open organically. The debug point LoadStream.close_load.delay_incremental_register
// (BE-only, test build) sleeps an incremental stream between close() and push_back,
// deterministically opening the window. On the FIXED binary registration and the
// all-received check are under one lock, so no stream is orphaned and the load
// completes even with the delay.
//
// Expectation: buggy binary -> load hangs -> this suite throws (RED).
//              fixed binary -> load completes -> suite passes (GREEN).
suite("test_iot_overwrite_partition_star_hang", "docker") {
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 3
    options.cloudMode = false
    options.beConfigs += [
        'enable_debug_points=true'
    ]

    docker(options) {
        sql "set enable_auto_create_when_overwrite = true;"
        sql " drop table if exists iot_star_hang; "
        sql """
            create table iot_star_hang(
                k0 int null
            )
            auto partition by list (k0)
            (
                PARTITION p1 values in ((0))
            )
            DISTRIBUTED BY HASH(`k0`) BUCKETS 1
            properties("replication_num" = "1");
        """
        // Seed ONLY the base partition. With replication_num=1 + BUCKETS 1 it lands on
        // a single BE, so the base load opens a non-incremental stream to just one
        // backend. New partitions created mid-overwrite land on the OTHER BEs as fresh
        // backends -> those are incremental streams (num_incremental_streams > 0), which
        // is exactly the condition the receiver's deferred-close race needs.
        sql """ insert into iot_star_hang values (0); """

        def deadline = 60000

        try {
            // Delay incremental-stream registration on all BEs to force the orphan.
            GetDebugPoint().enableDebugPointForAllBEs("LoadStream.close_load.delay_incremental_register")
            log.info("debug point enabled: delay incremental register")

            // Expose the JDBC statement so the watchdog can cancel the blocked
            // socket I/O before tearing the thread down (Thread.interrupt does
            // not reliably unblock Connector/J). Also hold the connection
            // reference so it can be closed if a cancel arrives before the
            // worker finishes.
            def stmtRef = null
            def connRef = null
            def loadEx = null

            def t = Thread.start {
                def conn = null
                try {
                    conn = context.getConnection()
                    connRef = conn
                    def stmt = conn.createStatement()
                    stmtRef = stmt
                    stmt.execute("set enable_auto_create_when_overwrite = true;")
                    // key 0 -> existing base partition (non-incremental stream);
                    // keys 1..49 -> new partitions (incremental streams). One load, both kinds.
                    stmt.execute("""
                        insert overwrite table iot_star_hang partition(*)
                        select number from numbers("number" = "50");
                    """)
                } catch (Throwable ex) {
                    loadEx = ex
                } finally {
                    if (conn != null) conn.close()
                }
            }

            t.join(deadline)
            if (t.isAlive()) {
                try {
                    if (stmtRef != null) stmtRef.cancel()
                    if (connRef != null) connRef.close()
                } catch (Throwable ignore) {}
                t.join(5000)
                if (t.isAlive()) {
                    t.interrupt()
                }
                throw new Exception("INSERT OVERWRITE PARTITION(*) hung: load did not finish within ${deadline}ms (orphaned incremental stream never closed)")
            }
            if (loadEx != null) {
                throw new Exception("INSERT OVERWRITE PARTITION(*) failed: ${loadEx.message}")
            }
        } finally {
            try {
                GetDebugPoint().disableDebugPointForAllBEs("LoadStream.close_load.delay_incremental_register")
            } catch (Throwable ignore) {}
        }

        // Assert the load both completed and published the correct
        // deterministic result (keys 0..49) so the suite proves bounded
        // completion AND correct commit visibility, not just non-hang.
        qt_sql """select count(*) from iot_star_hang;"""
        qt_sql """select count(distinct k0), min(k0), max(k0) from iot_star_hang;"""
    }
}
