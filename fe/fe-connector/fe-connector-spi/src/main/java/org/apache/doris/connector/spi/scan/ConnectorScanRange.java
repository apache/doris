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

package org.apache.doris.connector.spi.scan;

import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Represents a unit of work (a split/range) for scanning a connector table.
 *
 * <p>Each scan range maps to one BE scan task. The Thrift shape is chosen by the range itself: override
 * {@link #populateRangeParams} to build a format-specific {@code TTableFormatFileDesc} (iceberg, hudi, paimon
 * and es all do), and use {@link #getTableFormatType()} to select the BE-side reader. The default
 * {@code populateRangeParams} dumps {@link #getProperties()} into the generic {@code jdbc_params} map, which
 * is what the JNI-based readers consume.</p>
 *
 * <p>Connectors produce scan ranges via {@link ConnectorScanPlanProvider#planScan},
 * and the engine converts them to {@code TScanRangeLocations} for dispatch.</p>
 */
public interface ConnectorScanRange extends Serializable {

    /** Returns the file path, if applicable. */
    default Optional<String> getPath() {
        return Optional.empty();
    }

    /** Returns the byte offset to start reading from. */
    default long getStart() {
        return 0;
    }

    /**
     * Returns this range's size, or -1 when the connector does not quantify it (the default).
     *
     * <p><b>The unit is connector-defined, NOT universally bytes.</b> A file-backed connector reports the byte
     * count to read from {@link #getStart()} (hive, iceberg), but a connector whose split is not a byte range
     * reports what its own SDK gave it — MaxCompute's row-offset splits carry a ROW count, and its default
     * splits, like Paimon's JNI ranges, carry -1. BE is told the value only through the range's own thrift, so
     * each connector stays self-consistent.</p>
     *
     * <p>Consequently the ENGINE must not read this as a byte size to drive a generic size-based decision
     * (sampling, split merging, parallelism): a value meaning rows would silently mis-size the plan for that
     * connector. Such a feature is gated on an explicit capability the connector opts into (mirroring
     * {@code supportsTableSample}), and only a connector whose {@code getLength} is genuinely a byte count may
     * opt in.</p>
     */
    default long getLength() {
        return -1;
    }

    /**
     * Returns the file format (e.g., "parquet", "orc", "csv", "jni").
     *
     * <p>For a range read by a native file reader this determines that reader. For a range served through a
     * JNI scanner (jdbc, es, and the system-table paths), return "jni".</p>
     */
    default String getFileFormat() {
        return "jni";
    }

    /** Returns the total file size in bytes, or -1 if unknown. */
    default long getFileSize() {
        return -1;
    }

    /** Returns the last modification time of the file in milliseconds, or 0 if unknown. */
    default long getModificationTime() {
        return 0;
    }

    /**
     * Returns this split's weight numerator for proportional BE assignment, or {@code -1} when the
     * connector provides no weight.
     *
     * <p>The engine forms a proportional split weight {@code getSelfSplitWeight() / getTargetSplitSize()}
     * (clamped) only when BOTH this and {@link #getTargetSplitSize()} are provided; otherwise it falls back
     * to {@code SplitWeight.standard()} (uniform). A connector with no size-based weight model keeps the
     * {@code -1} default and is unaffected. {@code 0} is a legitimate weight (e.g. an empty file or a
     * zero-row system-table split), distinct from the {@code -1} "not provided" sentinel.</p>
     */
    default long getSelfSplitWeight() {
        return -1;
    }

    /**
     * Returns the weight denominator (scan-level target split size) used with {@link #getSelfSplitWeight()}
     * to form the proportional split weight, or {@code -1} when not provided.
     *
     * <p>Proportional weighting is applied only when this is positive AND {@link #getSelfSplitWeight()} is
     * non-negative; otherwise the engine uses {@code SplitWeight.standard()}.</p>
     */
    default long getTargetSplitSize() {
        return -1;
    }

    /** Returns preferred host locations for data locality. */
    default List<String> getHosts() {
        return Collections.emptyList();
    }

    /** Returns additional connector-specific properties. */
    Map<String, String> getProperties();

    /**
     * Returns the table format type string sent to BE in {@code TTableFormatFileDesc}.
     *
     * <p>This determines which BE reader/scanner is used for the scan range.
     * Examples: "jdbc" for JDBC connections, "hive" for Hive tables,
     * "plugin_driven" (default) for generic plugin-driven scans.</p>
     */
    default String getTableFormatType() {
        return "plugin_driven";
    }

    /**
     * Returns partition column values for this scan range.
     * Keys are partition column names; values are the partition values.
     */
    default Map<String, String> getPartitionValues() {
        return Collections.emptyMap();
    }

    /**
     * Whether this range belongs to a partitioned table whose partition values come from the connector's
     * metadata (NOT encoded in the file path). When {@code true}, an <em>empty</em> {@link #getPartitionValues()}
     * map means "this file genuinely has no path-derived partition values" and the engine must use it verbatim
     * instead of falling back to Hive-style path parsing — which would fail for connectors (e.g. Iceberg) whose
     * data files are not laid out as {@code key=value} directories. The default {@code false} preserves the
     * legacy behavior (an empty map is treated as "no partition info", letting the engine path-parse).
     */
    default boolean isPartitionBearing() {
        return false;
    }

    /**
     * Returns the precomputed pushed-down COUNT(*) row count this range carries, or {@code -1} when
     * the range carries no precomputed count.
     *
     * <p>When a no-grouping {@code COUNT(*)} is pushed down, a connector that can produce a precomputed
     * row count (e.g. Paimon's collapsed count range) surfaces the summed total here so the scan node
     * can render the EXPLAIN {@code pushdown agg=COUNT (n)} line. Ranges with no precomputed count keep
     * the {@code -1} default, which renders as the {@code (-1)} sentinel.</p>
     */
    default long getPushDownRowCount() {
        return -1;
    }

    /**
     * Whether this range is read by BE's NATIVE (ORC/Parquet) reader rather than the JNI scanner.
     *
     * <p>Used by a connector that distinguishes native vs JNI sub-splits (e.g. Paimon) so the scan
     * node can accumulate the native/total split counts for the EXPLAIN
     * {@code paimonNativeReadSplits=<native>/<total>} line. The default is {@code false} (JNI), so
     * connectors without a native read path are unaffected.</p>
     */
    default boolean isNativeReadRange() {
        return false;
    }

    /**
     * Populates per-range Thrift params from this scan range's data.
     *
     * <p>Connectors that need typed Thrift structs (e.g., Hudi, Paimon)
     * override this to construct their format-specific Thrift descriptor.
     * The default implementation puts all properties into the generic
     * {@code jdbc_params} map, which is suitable for JNI-based readers
     * and simple formats.</p>
     *
     * @param formatDesc the TTableFormatFileDesc to populate with format-specific data
     * @param rangeDesc  the TFileRangeDesc, may be mutated for format downgrade
     */
    default void populateRangeParams(TTableFormatFileDesc formatDesc,
            TFileRangeDesc rangeDesc) {
        Map<String, String> props = new HashMap<>(getProperties());
        props.put("connector_file_format", getFileFormat());
        Map<String, String> partValues = getPartitionValues();
        if (partValues != null && !partValues.isEmpty()) {
            for (Map.Entry<String, String> entry : partValues.entrySet()) {
                props.put("partition." + entry.getKey(), entry.getValue());
            }
        }
        formatDesc.setJdbcParams(props);
    }
}
