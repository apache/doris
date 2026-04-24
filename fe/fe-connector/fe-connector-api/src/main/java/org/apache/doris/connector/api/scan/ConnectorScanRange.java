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

package org.apache.doris.connector.api.scan;

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
 * <p>Each scan range maps to one BE scan task. The {@link #getRangeType() range type}
 * determines how the engine converts this range into the appropriate Thrift
 * scan range structure for BE execution.</p>
 *
 * <p>Connectors produce scan ranges via {@link ConnectorScanPlanProvider#planScan},
 * and the engine converts them to {@code TScanRangeLocations} for dispatch.</p>
 */
public interface ConnectorScanRange extends Serializable {

    /** Returns the scan range type, which determines BE processing. */
    ConnectorScanRangeType getRangeType();

    /** Returns the file path, if applicable. */
    default Optional<String> getPath() {
        return Optional.empty();
    }

    /** Returns the byte offset to start reading from. */
    default long getStart() {
        return 0;
    }

    /** Returns the number of bytes to read, or -1 for the entire file. */
    default long getLength() {
        return -1;
    }

    /**
     * Returns the file format (e.g., "parquet", "orc", "csv", "jni").
     *
     * <p>For {@link ConnectorScanRangeType#FILE_SCAN}, this determines the
     * BE reader. For non-file types (JDBC, ES, etc.), return "jni" to use
     * the JNI scanner framework.</p>
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
     * Returns delete files associated with this scan range.
     * Used by Iceberg merge-on-read tables for positional/equality deletes.
     */
    default List<ConnectorDeleteFile> getDeleteFiles() {
        return Collections.emptyList();
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
        props.put("connector_scan_range_type", getRangeType().name());
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
