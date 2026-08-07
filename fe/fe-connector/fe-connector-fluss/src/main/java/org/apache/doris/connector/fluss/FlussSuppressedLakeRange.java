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
import org.apache.doris.thrift.TTableFormatFileDesc;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A lake split of a primary-key table, with the log tail that supersedes part of it.
 *
 * <p>A tiered primary-key table is read as its lake plus the change log written since the lake snapshot.
 * The two halves overlap by key, not by row: a row the lake holds may since have been updated or deleted
 * in the log, and returning both copies is wrong in a way no row count reveals. This range is the lake
 * half of that arrangement — the sibling connector's own split, unchanged in every respect that decides
 * how it is read and scheduled — carrying the one extra fact BE needs: which slice of which bucket's log
 * may contradict it. BE reads that slice's keys once per bucket and drops the lake rows they name; the
 * surviving state of the tail is contributed separately, by a {@link FlussScanRange.RangeType#PK_TAIL}
 * range, so each row is produced exactly once.
 *
 * <p><b>Delegation is the whole design.</b> Everything except the format name and the extra payload is
 * the sibling's answer, because everything else is what makes the lake half worth having: its file path
 * and byte extent (native columnar readers), its split weight and target size (scheduling), its deletion
 * vectors, its schema id, its partition values. Overriding any of them here would silently reshape the
 * sibling's own read. That is also why this class must not grow a "smarter" version of any delegated
 * method — the one thing it knows better than the sibling is the tail.
 */
public class FlussSuppressedLakeRange implements ConnectorScanRange {

    private static final long serialVersionUID = 1L;

    /**
     * The {@code table_format_type} BE dispatches on. It is neither {@code fluss} nor {@code paimon}: the
     * range is read by the sibling's reader stack WRAPPED in the suppression, and BE picks that composite
     * reader by this name. Both plain names would land on a reader that ignores half the payload.
     */
    public static final String TABLE_FORMAT_TYPE = "fluss_union";

    /** {@code fluss.range_type} of a wrapped lake split; read by BE's C++ side, never by the scanner. */
    public static final String RANGE_TYPE_LAKE_SUPPRESS = "LAKE_SUPPRESS";

    /** The log slice whose keys suppress rows of this split, as {@code partitionId:bucket:start:stop}. */
    public static final String PROP_TAIL = "fluss.union.tail";

    private final ConnectorScanRange inner;
    private final Tail tail;

    public FlussSuppressedLakeRange(ConnectorScanRange inner, Tail tail) {
        this.inner = Objects.requireNonNull(inner, "inner");
        this.tail = Objects.requireNonNull(tail, "tail");
    }

    /** The wrapped sibling range, for a caller that has to look at what the lake half actually is. */
    public ConnectorScanRange getInner() {
        return inner;
    }

    public Tail getTail() {
        return tail;
    }

    /**
     * One bucket's log tail: the half-open offset range that the lake snapshot does not cover yet.
     *
     * <p>Its encoding is also its identity on the BE side — the same string keys the cache that holds the
     * suppression keys, so every split of the same bucket shares one read of the tail. Partition id is
     * empty for an unpartitioned table rather than a sentinel number, so that the key of an unpartitioned
     * table's bucket cannot collide with a partitioned one's.
     */
    public static final class Tail implements java.io.Serializable {

        private static final long serialVersionUID = 1L;

        private final FlussScanRange.Partition partition;
        private final int bucketId;
        private final long startOffset;
        private final long stopOffset;

        public Tail(FlussScanRange.Partition partition, int bucketId, long startOffset, long stopOffset) {
            this.partition = Objects.requireNonNull(partition, "partition");
            if (startOffset >= stopOffset) {
                throw new IllegalArgumentException("a suppressing tail must contain something, but bucket "
                        + bucketId + " was given [" + startOffset + ", " + stopOffset + ")");
            }
            this.bucketId = bucketId;
            this.startOffset = startOffset;
            this.stopOffset = stopOffset;
        }

        public int getBucketId() {
            return bucketId;
        }

        public long getStartOffset() {
            return startOffset;
        }

        public long getStopOffset() {
            return stopOffset;
        }

        String encode() {
            return (partition.isPartitioned() ? String.valueOf(partition.getId()) : "")
                    + ":" + bucketId + ":" + startOffset + ":" + stopOffset;
        }

        @Override
        public String toString() {
            return "Tail{" + encode() + "}";
        }
    }

    // ------------------------------------------------------------------ the two things that differ

    @Override
    public String getTableFormatType() {
        return TABLE_FORMAT_TYPE;
    }

    /**
     * The sibling fills its own payload first — serialized split or schema id and deletion files, file
     * format, columns from path — and the tail is appended beside it. The two ride in different fields of
     * the same descriptor ({@code paimon_params} and {@code fluss_params}), so neither has to know about
     * the other.
     */
    @Override
    public void populateRangeParams(TTableFormatFileDesc formatDesc, TFileRangeDesc rangeDesc) {
        inner.populateRangeParams(formatDesc, rangeDesc);
        Map<String, String> params = new LinkedHashMap<>();
        params.put(FlussScanRange.PROP_RANGE_TYPE, RANGE_TYPE_LAKE_SUPPRESS);
        params.put(PROP_TAIL, tail.encode());
        formatDesc.setFlussParams(params);
    }

    // ------------------------------------------------------------------ everything else is the sibling's

    @Override
    public Optional<String> getPath() {
        return inner.getPath();
    }

    @Override
    public long getStart() {
        return inner.getStart();
    }

    @Override
    public long getLength() {
        return inner.getLength();
    }

    @Override
    public long getFileSize() {
        return inner.getFileSize();
    }

    @Override
    public String getFileFormat() {
        return inner.getFileFormat();
    }

    @Override
    public long getModificationTime() {
        return inner.getModificationTime();
    }

    @Override
    public long getSelfSplitWeight() {
        return inner.getSelfSplitWeight();
    }

    @Override
    public long getTargetSplitSize() {
        return inner.getTargetSplitSize();
    }

    @Override
    public List<String> getHosts() {
        return inner.getHosts();
    }

    @Override
    public Map<String, String> getProperties() {
        return inner.getProperties();
    }

    @Override
    public Map<String, String> getPartitionValues() {
        return inner.getPartitionValues();
    }

    @Override
    public boolean isPartitionBearing() {
        return inner.isPartitionBearing();
    }

    @Override
    public long getPushDownRowCount() {
        return inner.getPushDownRowCount();
    }

    @Override
    public boolean isNativeReadRange() {
        return inner.isNativeReadRange();
    }

    @Override
    public String toString() {
        return "FlussSuppressedLakeRange{" + tail + ", " + inner + "}";
    }
}
