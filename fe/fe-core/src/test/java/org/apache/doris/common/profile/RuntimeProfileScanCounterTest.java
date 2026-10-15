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

package org.apache.doris.common.profile;

import org.apache.doris.thrift.TPlanNodeRuntimeStatsItem;
import org.apache.doris.thrift.TUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Unit test of the profile counter a scan records its rows under. The learned hbo entries compare the
 * rows a scan read with the row count of the table, so the conversion has to read the counters a scan
 * operator actually reports ({@code RowsRead} / {@code ScanRows}), not only the {@code InputRows} of a
 * sink operator - a scan which recorded zero was judged stale on the next identical query.
 */
public class RuntimeProfileScanCounterTest {

    private static AggCounter counter(long value) {
        AggCounter counter = new AggCounter(TUnit.UNIT);
        counter.sum.setValue(value);
        // the sink case divides by the number of instances
        counter.number = 1;
        return counter;
    }

    @Test
    public void testTheRowsAScanReadBecomeItsInputRows() {
        RuntimeProfile query = new RuntimeProfile("QUERY");
        RuntimeProfile scan = new RuntimeProfile("OLAP_SCAN_OPERATOR (id=0)", 0);
        scan.addCounter("RowsRead", counter(1234), RuntimeProfile.ROOT_COUNTER);
        query.addChild(scan, false);

        List<TPlanNodeRuntimeStatsItem> items = RuntimeProfile.toTPlanNodeRuntimeStatsItem(query, null);
        Assertions.assertEquals(1234, inputRowsOf(items), "the rows a scan read were not recorded");
    }

    @Test
    public void testScanRowsIsReadAsWell() {
        RuntimeProfile query = new RuntimeProfile("QUERY");
        RuntimeProfile scan = new RuntimeProfile("OLAP_SCAN_OPERATOR (id=0)", 0);
        scan.addCounter("ScanRows", counter(4321), RuntimeProfile.ROOT_COUNTER);
        query.addChild(scan, false);

        Assertions.assertEquals(4321, inputRowsOf(RuntimeProfile.toTPlanNodeRuntimeStatsItem(query, null)));
    }

    @Test
    public void testTheInputRowsOfASinkWinOverAScanCounter() {
        RuntimeProfile query = new RuntimeProfile("QUERY");
        RuntimeProfile sink = new RuntimeProfile("HASH_JOIN_SINK_OPERATOR (id=1)", 1);
        sink.addCounter("InputRows", counter(7), RuntimeProfile.ROOT_COUNTER);
        sink.addCounter("RowsRead", counter(999), RuntimeProfile.ROOT_COUNTER);
        query.addChild(sink, false);

        Assertions.assertEquals(7, inputRowsOf(RuntimeProfile.toTPlanNodeRuntimeStatsItem(query, null)));
    }

    private static long inputRowsOf(List<TPlanNodeRuntimeStatsItem> items) {
        long rows = -1;
        for (TPlanNodeRuntimeStatsItem item : items) {
            if (item.isSetInputRows() && item.getInputRows() > rows) {
                rows = item.getInputRows();
            }
        }
        return rows;
    }
}
