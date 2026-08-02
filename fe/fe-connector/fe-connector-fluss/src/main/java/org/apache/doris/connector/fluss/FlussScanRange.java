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

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One unit of fluss reading: a single bucket of a single partition, read one of three ways.
 *
 * <p>The payload is an untyped string map rather than a thrift struct of its own. The C++ layer holds
 * no fluss logic — it hands the map straight to the java scanner in be-java-extensions — so a typed
 * struct would buy nothing but a transcription step in the middle (which is what paimon pays for,
 * {@code paimon_jni_reader.cpp} rebuilding a string map field by field). {@code es_params} and
 * {@code jdbc_params} are the same shape for the same reason.
 *
 * <p>Only what VARIES per split lives here. Bootstrap servers, table identity, client and table
 * options and the projection are identical for every split of a scan, so they are written once at
 * scan-node level ({@code TFileScanRangeParams.fluss_properties}); a table with 100 partitions and
 * 128 buckets would otherwise serialize them 12800 times.
 *
 * <p>Partition columns are NOT part of what the scanner returns. The connector declares the partition
 * keys as {@code path_partition_keys}, which makes the engine leave them out of the scanner's
 * projection and materialize them from {@link #getPartitionValues()} instead. That is forced by the
 * union read: the lake half of a union comes from paimon ranges, and the paimon connector already
 * declares its partition keys that way — the file-slot / partition-slot split is decided once per scan
 * node, so both halves have to agree on it.
 */
public class FlussScanRange implements ConnectorScanRange {

    private static final long serialVersionUID = 1L;

    /** How the scanner reads this range; the wire value is the enum name. */
    public enum RangeType {
        /** Log-only read of one bucket over {@code [logStart, logStop)}. */
        LOG,
        /** Full read of one bucket of a primary-key table: kv snapshot plus the log after it. */
        PK_FULL,
        /** Primary-key read merging the lake's splits for one bucket with the log tail after them. */
        UNION_PK
    }

    public static final String PROP_RANGE_TYPE = "fluss.range_type";
    public static final String PROP_PARTITION_ID = "fluss.partition_id";
    public static final String PROP_PARTITION_NAME = "fluss.partition_name";
    public static final String PROP_BUCKET_ID = "fluss.bucket_id";
    public static final String PROP_LOG_START_OFFSET = "fluss.log_start_offset";
    public static final String PROP_LOG_STOP_OFFSET = "fluss.log_stop_offset";
    public static final String PROP_KV_SNAPSHOT_ID = "fluss.kv_snapshot_id";
    public static final String PROP_LAKE_SNAPSHOT_ID = "fluss.lake_snapshot_id";
    public static final String PROP_LAKE_SPLITS = "fluss.lake_splits";

    /** {@code kv_snapshot_id} for a bucket that has never been snapshotted. */
    public static final long NO_KV_SNAPSHOT = -1L;

    /**
     * Separator for {@link #PROP_LAKE_SPLITS}. Each element is base64, whose alphabet
     * ({@code A-Za-z0-9+/=}) has no comma, so joining is unambiguous. The factory rejects any element
     * that contains one rather than trusting the caller.
     */
    private static final String LAKE_SPLIT_SEPARATOR = ",";

    private final RangeType rangeType;
    private final Partition partition;
    private final int bucketId;
    private final Map<String, String> properties;

    private FlussScanRange(RangeType rangeType, Partition partition, int bucketId,
            Map<String, String> properties) {
        this.rangeType = rangeType;
        this.partition = partition;
        this.bucketId = bucketId;
        this.properties = Collections.unmodifiableMap(properties);
    }

    /**
     * A log range over {@code [logStartOffset, logStopOffset)} of one bucket. {@code logStartOffset} is
     * either a real offset or fluss's {@code LogScanner.EARLIEST_OFFSET} sentinel, passed through
     * verbatim — the scanner hands it back to fluss, which is the only side that interprets it.
     */
    public static FlussScanRange log(Partition partition, int bucketId,
            long logStartOffset, long logStopOffset) {
        Map<String, String> props = baseProps(RangeType.LOG, partition, bucketId,
                logStartOffset, logStopOffset);
        return new FlussScanRange(RangeType.LOG, partition, bucketId, props);
    }

    /**
     * A full primary-key range: the kv snapshot {@code kvSnapshotId} plus the log from
     * {@code logStartOffset} (the offset that snapshot was taken at) up to {@code logStopOffset}.
     * {@code kvSnapshotId} is {@link #NO_KV_SNAPSHOT} when the bucket has never been snapshotted, in
     * which case the whole state is replayed from the log.
     */
    public static FlussScanRange pkFull(Partition partition, int bucketId, long kvSnapshotId,
            long logStartOffset, long logStopOffset) {
        Map<String, String> props = baseProps(RangeType.PK_FULL, partition, bucketId,
                logStartOffset, logStopOffset);
        props.put(PROP_KV_SNAPSHOT_ID, String.valueOf(kvSnapshotId));
        return new FlussScanRange(RangeType.PK_FULL, partition, bucketId, props);
    }

    /**
     * A union primary-key range: this bucket's splits of lake snapshot {@code lakeSnapshotId}, merged
     * with the log from {@code logStartOffset} (where that lake snapshot ended, exclusive) up to
     * {@code logStopOffset}. Each element of {@code lakeSplits} is a base64-encoded serialized lake
     * split.
     */
    public static FlussScanRange unionPk(Partition partition, int bucketId, long lakeSnapshotId,
            List<String> lakeSplits, long logStartOffset, long logStopOffset) {
        Objects.requireNonNull(lakeSplits, "lakeSplits");
        if (lakeSplits.isEmpty()) {
            throw new IllegalArgumentException(
                    "a UNION_PK range needs at least one lake split; a bucket with none is a plain log read");
        }
        for (String split : lakeSplits) {
            if (split == null || split.contains(LAKE_SPLIT_SEPARATOR)) {
                // Would silently split one entry into two on the scanner side.
                throw new IllegalArgumentException(
                        "lake split is not base64 (null or contains '" + LAKE_SPLIT_SEPARATOR + "'): " + split);
            }
        }
        Map<String, String> props = baseProps(RangeType.UNION_PK, partition, bucketId,
                logStartOffset, logStopOffset);
        props.put(PROP_LAKE_SNAPSHOT_ID, String.valueOf(lakeSnapshotId));
        props.put(PROP_LAKE_SPLITS, String.join(LAKE_SPLIT_SEPARATOR, lakeSplits));
        return new FlussScanRange(RangeType.UNION_PK, partition, bucketId, props);
    }

    private static Map<String, String> baseProps(RangeType rangeType, Partition partition,
            int bucketId, long logStartOffset, long logStopOffset) {
        Objects.requireNonNull(partition, "partition");
        Map<String, String> props = new LinkedHashMap<>();
        props.put(PROP_RANGE_TYPE, rangeType.name());
        if (partition.isPartitioned()) {
            props.put(PROP_PARTITION_ID, String.valueOf(partition.id));
            props.put(PROP_PARTITION_NAME, partition.name);
        }
        props.put(PROP_BUCKET_ID, String.valueOf(bucketId));
        props.put(PROP_LOG_START_OFFSET, String.valueOf(logStartOffset));
        props.put(PROP_LOG_STOP_OFFSET, String.valueOf(logStopOffset));
        return props;
    }

    public RangeType getRangeType() {
        return rangeType;
    }

    public Partition getPartition() {
        return partition;
    }

    @Override
    public String getTableFormatType() {
        return "fluss";
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Map<String, String> getPartitionValues() {
        return partition.values;
    }

    /**
     * Always true: a fluss range's partition values come from fluss metadata, never from a file path.
     * On an unpartitioned table that keeps the engine from falling back to parsing a path that does not
     * exist (the range has no path at all).
     */
    @Override
    public boolean isPartitionBearing() {
        return true;
    }

    @Override
    public void populateRangeParams(TTableFormatFileDesc formatDesc, TFileRangeDesc rangeDesc) {
        formatDesc.setFlussParams(new LinkedHashMap<>(properties));
        Map<String, String> partitionValues = partition.values;
        if (!partitionValues.isEmpty()) {
            List<String> keys = new ArrayList<>(partitionValues.size());
            List<String> values = new ArrayList<>(partitionValues.size());
            List<Boolean> isNull = new ArrayList<>(partitionValues.size());
            for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
                keys.add(entry.getKey());
                values.add(entry.getValue());
                // Never null: fluss rejects a null partition value at write time
                // (PartitionGetter.getResolvedPartitionSpec), so there is no null sentinel to decode.
                isNull.add(false);
            }
            rangeDesc.setColumnsFromPathKeys(keys);
            rangeDesc.setColumnsFromPath(values);
            rangeDesc.setColumnsFromPathIsNull(isNull);
        }
    }

    @Override
    public String toString() {
        return "FlussScanRange{" + rangeType
                + (partition.isPartitioned() ? ", partition=" + partition.name : "")
                + ", bucket=" + bucketId
                + ", " + properties + "}";
    }

    /**
     * Which partition a range belongs to, as the three facts that must agree: fluss's partition id
     * (what the scanner subscribes by), its Hive-style {@code k=v/k=v} name (what the user sees) and
     * the per-column values (what the engine materializes the partition columns from). Bundled so an
     * unpartitioned table cannot be described half-way — {@link #NONE} is the only way to say "none".
     */
    public static final class Partition implements java.io.Serializable {

        private static final long serialVersionUID = 1L;

        /** The single pseudo-partition of an unpartitioned table. */
        public static final Partition NONE = new Partition(-1L, null, Collections.emptyMap());

        private final long id;
        private final String name;
        private final Map<String, String> values;

        private Partition(long id, String name, Map<String, String> values) {
            this.id = id;
            this.name = name;
            this.values = values;
        }

        /**
         * @param id     fluss's partition id
         * @param name   the Hive-style {@code k=v/k=v} partition name Doris uses
         * @param values partition column name to value, in partition-key order
         */
        public static Partition of(long id, String name, Map<String, String> values) {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(values, "values");
            if (values.isEmpty()) {
                throw new IllegalArgumentException(
                        "partition " + name + " has no column values; use Partition.NONE for an "
                                + "unpartitioned table");
            }
            return new Partition(id, name, Collections.unmodifiableMap(new LinkedHashMap<>(values)));
        }

        /**
         * Whether this is a real partition rather than {@link #NONE}. Asked of the field, not of
         * object identity: a range crosses a java-serialization boundary, which would hand back a
         * copy of {@code NONE} that {@code ==} no longer recognizes.
         */
        public boolean isPartitioned() {
            return name != null;
        }

        public long getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public Map<String, String> getValues() {
            return values;
        }

        @Override
        public String toString() {
            return this == NONE ? "Partition.NONE" : name + "(" + id + ")";
        }
    }
}
