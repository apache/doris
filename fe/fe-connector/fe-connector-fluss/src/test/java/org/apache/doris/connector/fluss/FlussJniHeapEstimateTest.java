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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.spi.scan.ConnectorScanRange;

import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FlussJniHeapEstimateTest {

    /**
     * The 13 columns the performance tests read, whose rows the BE-side heap was measured against: a
     * GenericRow and its Object[13] (16 + 72), then two BIGINTs, an INT, two strings, a DECIMAL, an INT,
     * a DOUBLE, a BOOLEAN, a DATE, a TIMESTAMP and two more strings.
     */
    private static final RowType EVENTS = RowType.of(
            DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(),
            DataTypes.DECIMAL(12, 2), DataTypes.INT(), DataTypes.DOUBLE(), DataTypes.BOOLEAN(),
            DataTypes.DATE(), DataTypes.TIMESTAMP(3), DataTypes.STRING(), DataTypes.STRING());

    private static final long STRING = 88 + 80;

    @Test
    public void rowIsItsGenericRowItsArrayAndEveryField() {
        List<Integer> all = IntStream.range(0, EVENTS.getFieldCount()).boxed().collect(Collectors.toList());
        long fields = 24 + 24 + 16 + STRING + STRING + 136 + 16 + 24 + 0 + 16 + 24 + STRING + STRING;
        Assertions.assertEquals(16 + 72 + fields, FlussJniHeapEstimate.rowBytes(EVENTS, all));
        // Only the columns read are kept: the key alone is a row with one BIGINT.
        Assertions.assertEquals(16 + 24 + 24, FlussJniHeapEstimate.rowBytes(EVENTS, Collections.singletonList(0)));
    }

    @Test
    public void everyTypeHasItsBoxedSize() {
        Assertions.assertEquals(0, FlussJniHeapEstimate.fieldBytes(DataTypes.BOOLEAN()));
        Assertions.assertEquals(0, FlussJniHeapEstimate.fieldBytes(DataTypes.TINYINT()));
        Assertions.assertEquals(16, FlussJniHeapEstimate.fieldBytes(DataTypes.SMALLINT()));
        Assertions.assertEquals(16, FlussJniHeapEstimate.fieldBytes(DataTypes.FLOAT()));
        Assertions.assertEquals(16, FlussJniHeapEstimate.fieldBytes(DataTypes.TIME()));
        Assertions.assertEquals(24, FlussJniHeapEstimate.fieldBytes(DataTypes.TIMESTAMP_LTZ()));
        Assertions.assertEquals(136, FlussJniHeapEstimate.fieldBytes(DataTypes.DECIMAL(38, 10)));
        Assertions.assertEquals(80, FlussJniHeapEstimate.fieldBytes(DataTypes.BYTES()));
        Assertions.assertEquals(STRING, FlussJniHeapEstimate.fieldBytes(DataTypes.CHAR(3)));
        Assertions.assertEquals(STRING, FlussJniHeapEstimate.fieldBytes(DataTypes.ARRAY(DataTypes.INT())));
    }

    @Test
    public void primaryKeyRangesDeclareTheirRecordsTimesARowAndAnEntry() {
        long row = 100;
        FlussScanRange.Partition none = FlussScanRange.Partition.NONE;
        List<ConnectorScanRange> declared = FlussJniHeapEstimate.declare(Arrays.asList(
                FlussScanRange.pkFull(none, 0, 3L, 40L, 140L),
                // Never snapshotted: replayed from the first record of its log.
                FlussScanRange.pkFull(none, 1, FlussScanRange.NO_KV_SNAPSHOT, LogScanner.EARLIEST_OFFSET, 25L),
                FlussScanRange.pkTail(none, 2, 500L, 530L),
                FlussScanRange.log(none, 3, 0L, 1000L),
                // Its snapshot holds everything: no change log to replay, nothing to declare.
                FlussScanRange.pkFull(none, 4, 9L, 90L, 90L)), row);

        Assertions.assertEquals(100L * (row + 112), heapOf(declared.get(0)));
        Assertions.assertEquals(25L * (row + 112), heapOf(declared.get(1)));
        Assertions.assertEquals(30L * (row + 96), heapOf(declared.get(2)));
        // A log range streams.
        Assertions.assertEquals(0L, heapOf(declared.get(3)));
        Assertions.assertEquals(0L, heapOf(declared.get(4)));
        // The rest of each range is untouched.
        Assertions.assertEquals("140", declared.get(0).getProperties().get(FlussScanRange.PROP_LOG_STOP_OFFSET));
    }

    private static long heapOf(ConnectorScanRange range) {
        return ((FlussScanRange) range).getJniHeapBytes();
    }
}
