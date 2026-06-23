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

package org.apache.doris.cdcclient.source.reader.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.shyiko.mysql.binlog.GtidSet;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Guard for MySQL GTID sets that carry more than one interval per server uuid (a "split
 * interval", e.g. {@code uuid:1-3:5-7} left behind when the gap transaction 4 was purged).
 *
 * <p>We delegate all GTID parsing/serialization to the binlog connector's {@link GtidSet} and
 * never split the string ourselves, so the go-mysql #550 class of bug (a single-interval regex
 * that silently merges split intervals and persists a corrupt resume position → ERROR 1236)
 * cannot regress unnoticed. These tests pin that the split interval survives both GtidSet
 * parsing/round-trip and our FE offset-map persistence without the gap being swallowed.
 */
class GtidMultiIntervalOffsetTest {

    private static final String SERVER_UUID = "24bc7850-2c16-11e6-a073-0242ac110002";
    // Two disjoint intervals for the same uuid, with a purged gap at transaction 4.
    private static final String MULTI_INTERVAL_GTID = SERVER_UUID + ":1-3:5-7";

    @Test
    void splitIntervalParsesAsTwoIntervalsNotMergedIntoOne() {
        List<GtidSet.Interval> intervals =
                new GtidSet(MULTI_INTERVAL_GTID).getUUIDSet(SERVER_UUID).getIntervals();
        // The gap at txn 4 must keep this as two intervals, never collapsed into a single 1-7.
        assertEquals(2, intervals.size());
        assertEquals(1, intervals.get(0).getStart());
        assertEquals(3, intervals.get(0).getEnd());
        assertEquals(5, intervals.get(1).getStart());
        assertEquals(7, intervals.get(1).getEnd());
    }

    @Test
    void splitIntervalSurvivesGtidSetSerializationRoundTrip() {
        // Re-serializing then re-parsing must not lose the gap (toString -> parse -> two intervals).
        String reSerialized = new GtidSet(MULTI_INTERVAL_GTID).toString();
        assertEquals(
                2, new GtidSet(reSerialized).getUUIDSet(SERVER_UUID).getIntervals().size());
    }

    @Test
    void splitIntervalSurvivesFeOffsetMapRoundTrip() {
        // FE persists/restores the binlog offset as a string-valued map; rebuild from it.
        Map<String, String> feOffset = new HashMap<>();
        feOffset.put(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY, "mysql-bin.000004");
        feOffset.put(BinlogOffset.BINLOG_POSITION_OFFSET_KEY, "1024");
        feOffset.put(BinlogOffset.GTID_SET_KEY, MULTI_INTERVAL_GTID);

        BinlogOffset restored = new BinlogOffset(new HashMap<>(feOffset));

        // The gtids value is opaque to JSON and must come back byte-for-byte.
        assertEquals(MULTI_INTERVAL_GTID, restored.getGtidSet());
        // And it must still parse into two intervals after the round trip.
        assertEquals(
                2, new GtidSet(restored.getGtidSet()).getUUIDSet(SERVER_UUID).getIntervals().size());
    }

    @Test
    void purgedGapTransactionStaysOutsideTheSplitInterval() {
        GtidSet multiInterval = new GtidSet(MULTI_INTERVAL_GTID);
        // Transactions inside the two intervals are contained; the purged gap (txn 4) is not.
        assertTrue(new GtidSet(SERVER_UUID + ":2-2").isContainedWithin(multiInterval));
        assertTrue(new GtidSet(SERVER_UUID + ":6-6").isContainedWithin(multiInterval));
        assertFalse(new GtidSet(SERVER_UUID + ":4-4").isContainedWithin(multiInterval));
    }
}
