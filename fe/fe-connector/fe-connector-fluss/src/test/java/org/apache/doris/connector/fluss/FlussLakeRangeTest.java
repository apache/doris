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
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TPaimonFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The wrapper that puts a lake split under the scan's own table format, with or without a log tail.
 *
 * <p>Everything it does NOT change is what makes the lake half worth having — its file extent, its split
 * weight, its deletion vectors — so the delegation is asserted method by method against a stand-in that
 * answers something distinctive for each. A silently swallowed answer (a zero weight, an empty path)
 * would not fail any assertion about rows; it would just make the lake half read or schedule badly.
 */
public class FlussLakeRangeTest {

    private static final FlussScanRange.Partition DT_20260101 =
            FlussScanRange.Partition.of(77L, "dt=20260101",
                    Collections.singletonMap("dt", "20260101"));

    /** A sibling range whose every answer is distinctive, so a dropped delegation is visible. */
    private static final class LakeSplit implements ConnectorScanRange {
        private static final long serialVersionUID = 1L;

        @Override
        public Optional<String> getPath() {
            return Optional.of("/warehouse/db/tbl/bucket-2/data-1.parquet");
        }

        @Override
        public long getStart() {
            return 4096L;
        }

        @Override
        public long getLength() {
            return 8192L;
        }

        @Override
        public long getFileSize() {
            return 16384L;
        }

        @Override
        public String getFileFormat() {
            return "parquet";
        }

        @Override
        public long getModificationTime() {
            return 1735689600L;
        }

        @Override
        public long getSelfSplitWeight() {
            return 7L;
        }

        @Override
        public long getTargetSplitSize() {
            return 134217728L;
        }

        @Override
        public List<String> getHosts() {
            return Arrays.asList("be-1", "be-2");
        }

        @Override
        public Map<String, String> getProperties() {
            Map<String, String> props = new LinkedHashMap<>();
            props.put("paimon.bucket", "2");
            props.put("paimon.schema_id", "5");
            return props;
        }

        @Override
        public Map<String, String> getPartitionValues() {
            return Collections.singletonMap("dt", "20260101");
        }

        @Override
        public boolean isPartitionBearing() {
            return true;
        }

        @Override
        public long getPushDownRowCount() {
            return 42L;
        }

        @Override
        public boolean isNativeReadRange() {
            return true;
        }

        @Override
        public String getTableFormatType() {
            return "paimon";
        }

        @Override
        public void populateRangeParams(TTableFormatFileDesc formatDesc, TFileRangeDesc rangeDesc) {
            formatDesc.setPaimonParams(new TPaimonFileDesc());
            rangeDesc.setColumnsFromPathKeys(Collections.singletonList("dt"));
            rangeDesc.setColumnsFromPath(Collections.singletonList("20260101"));
        }
    }

    private static FlussLakeRange suppressed() {
        return FlussLakeRange.suppressed(new LakeSplit(),
                new FlussLakeRange.Tail(DT_20260101, 2, 100L, 105L));
    }

    @Test
    public void everythingThatDecidesHowTheSplitIsReadStaysTheSiblingsAnswer() {
        ConnectorScanRange inner = new LakeSplit();
        for (FlussLakeRange wrapped : Arrays.asList(suppressed(),
                FlussLakeRange.plain(new LakeSplit()))) {
            Assertions.assertEquals(inner.getPath(), wrapped.getPath());
            Assertions.assertEquals(inner.getStart(), wrapped.getStart());
            Assertions.assertEquals(inner.getLength(), wrapped.getLength());
            Assertions.assertEquals(inner.getFileSize(), wrapped.getFileSize());
            Assertions.assertEquals(inner.getFileFormat(), wrapped.getFileFormat());
            Assertions.assertEquals(inner.getModificationTime(), wrapped.getModificationTime());
            Assertions.assertEquals(inner.getSelfSplitWeight(), wrapped.getSelfSplitWeight());
            Assertions.assertEquals(inner.getTargetSplitSize(), wrapped.getTargetSplitSize());
            Assertions.assertEquals(inner.getHosts(), wrapped.getHosts());
            Assertions.assertEquals(inner.getProperties(), wrapped.getProperties());
            Assertions.assertEquals(inner.getPartitionValues(), wrapped.getPartitionValues());
            Assertions.assertEquals(inner.isPartitionBearing(), wrapped.isPartitionBearing());
            Assertions.assertEquals(inner.getPushDownRowCount(), wrapped.getPushDownRowCount());
            Assertions.assertEquals(inner.isNativeReadRange(), wrapped.isNativeReadRange());
        }
    }

    /**
     * The one answer that changes. Every range of a fluss scan carries the scan's own format: BE builds
     * one fluss reader from it and dispatches this split to the sibling's stack by its range type. The
     * sibling's own name would land the split on a reader the scan does not have.
     */
    @Test
    public void theFormatNameIsTheScansOwn() {
        Assertions.assertEquals("fluss", suppressed().getTableFormatType());
        Assertions.assertEquals("fluss", FlussLakeRange.plain(new LakeSplit()).getTableFormatType());
    }

    /**
     * The payload is both halves, in their own thrift fields: whatever the sibling wrote, untouched, plus
     * the fluss descriptor. Asserted whole rather than by spot check — this is an untyped string map that
     * nothing between here and BE type-checks.
     */
    @Test
    public void theDescriptorCarriesTheSiblingsPayloadAndTheTail() {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();

        suppressed().populateRangeParams(formatDesc, rangeDesc);

        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("fluss.range_type", "LAKE_SUPPRESS");
        expected.put("fluss.union.tail", "77:2:100:105");
        Assertions.assertEquals(expected, formatDesc.getFlussParams());
        // The sibling's own payload has to survive beside it: the paimon reader fails outright without it.
        Assertions.assertTrue(formatDesc.isSetPaimonParams());
        Assertions.assertEquals(Collections.singletonList("20260101"), rangeDesc.getColumnsFromPath());
    }

    /**
     * A plain lake split says only what kind it is. Writing a tail key here — even an empty one — would
     * make BE read a tail that does not exist; writing nothing at all would make BE unable to route it.
     */
    @Test
    public void plainSplitsDescriptorSaysLakeAndNamesNoTail() {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();

        FlussLakeRange.plain(new LakeSplit()).populateRangeParams(formatDesc, new TFileRangeDesc());

        Assertions.assertEquals(Collections.singletonMap("fluss.range_type", "LAKE"),
                formatDesc.getFlussParams());
        Assertions.assertTrue(formatDesc.isSetPaimonParams());
    }

    /**
     * An unpartitioned table's tail leaves the partition segment empty rather than writing a sentinel id.
     * The string is also the cache key BE builds the suppression set under, so "no partition" and
     * "partition -1" must not be the same key.
     */
    @Test
    public void anUnpartitionedTailLeavesThePartitionSegmentEmpty() {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        FlussLakeRange.suppressed(new LakeSplit(),
                new FlussLakeRange.Tail(FlussScanRange.Partition.NONE, 3, 0L, 9L))
                .populateRangeParams(formatDesc, new TFileRangeDesc());

        Assertions.assertEquals(":3:0:9", formatDesc.getFlussParams().get("fluss.union.tail"));
    }

    /** An empty tail suppresses nothing; producing one would mean planning wrapped a split for no reason. */
    @Test
    public void anEmptyTailIsRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new FlussLakeRange.Tail(DT_20260101, 0, 5L, 5L));
    }
}
