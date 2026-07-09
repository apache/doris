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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Opaque table handle for a Hudi table, carrying database/table coordinates,
 * base path for MetaClient, Hudi table type (COW/MOR), and scan-related
 * metadata populated by {@link HudiConnectorMetadata#getTableHandle}.
 *
 * <p>After {@code applyFilter}, a new handle may carry
 * {@link #getPrunedPartitionPaths()} for partition-pruned scans.</p>
 */
public class HudiTableHandle implements ConnectorTableHandle {

    private static final long serialVersionUID = 1L;

    private final String dbName;
    private final String tableName;
    private final String basePath;
    private final String hudiTableType;

    // Scan-related metadata (populated from HmsTableInfo)
    private final String inputFormat;
    private final String serdeLib;
    private final List<String> partitionKeyNames;
    private final Map<String, String> tableParameters;

    // Set after applyFilter for partition pruning
    private final List<String> prunedPartitionPaths;

    // Set after applySnapshot for FOR TIME AS OF time travel: the completed-timeline instant the scan reads
    // BEFORE-OR-ON (a String like "20240101120000", not a numeric snapshot id). Null = no time travel; the scan
    // reads the latest completed instant (byte-identical to a plain snapshot read).
    private final String queryInstant;

    // Set after applySnapshot for an @incr incremental read: the resolved (begin, end] completed-timeline window
    // (String instants like "20240101120000"; empty timeline / "earliest" collapse to "000") the scan reads.
    // Both bounds are FULLY resolved by HudiConnectorMetadata.resolveTimeTravel (EARLIEST -> "000", an omitted /
    // "latest" end -> the latest completed instant), so downstream file selection + the synthetic commit-time
    // filter consume them directly. A non-null beginInstant is the incremental-read marker; null = not an
    // incremental read.
    private final String beginInstant;
    private final String endInstant;

    // Set after applySnapshot for an @incr incremental read: the RAW @incr option params fe-core threaded via
    // getIncrementalParams() (e.g. hoodie.datasource.read.incr.path.glob / ...fallback.fulltablescan.enable /
    // hoodie.read.timeline.holes.resolution.policy). planScan feeds this map straight to the ported
    // IncrementalRelation constructors as their optParams (and derives HollowCommitHandling from it), so the
    // relations read the glob / fallback / policy exactly as legacy did. Empty for a non-incremental read.
    private final Map<String, String> incrementalParams;

    private HudiTableHandle(Builder builder) {
        this.dbName = builder.dbName;
        this.tableName = builder.tableName;
        this.basePath = builder.basePath;
        this.hudiTableType = builder.hudiTableType;
        this.inputFormat = builder.inputFormat;
        this.serdeLib = builder.serdeLib;
        this.partitionKeyNames = builder.partitionKeyNames != null
                ? Collections.unmodifiableList(builder.partitionKeyNames)
                : Collections.emptyList();
        this.tableParameters = builder.tableParameters != null
                ? Collections.unmodifiableMap(builder.tableParameters)
                : Collections.emptyMap();
        this.prunedPartitionPaths = builder.prunedPartitionPaths;
        this.queryInstant = builder.queryInstant;
        this.beginInstant = builder.beginInstant;
        this.endInstant = builder.endInstant;
        this.incrementalParams = builder.incrementalParams != null
                ? Collections.unmodifiableMap(builder.incrementalParams)
                : Collections.emptyMap();
    }

    /** Legacy constructor for Phase 1 compatibility (metadata-only). */
    public HudiTableHandle(String dbName, String tableName, String basePath, String hudiTableType) {
        this(new Builder(dbName, tableName, basePath, hudiTableType));
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getBasePath() {
        return basePath;
    }

    public String getHudiTableType() {
        return hudiTableType;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public String getSerdeLib() {
        return serdeLib;
    }

    public List<String> getPartitionKeyNames() {
        return partitionKeyNames;
    }

    public Map<String, String> getTableParameters() {
        return tableParameters;
    }

    public List<String> getPrunedPartitionPaths() {
        return prunedPartitionPaths;
    }

    /** The FOR TIME AS OF instant the scan reads before-or-on, or {@code null} for a latest read. */
    public String getQueryInstant() {
        return queryInstant;
    }

    /**
     * The resolved incremental-read begin instant (exclusive lower bound of the {@code (begin, end]} window),
     * or {@code null} for a non-incremental read. A non-null value is the incremental-read marker.
     */
    public String getBeginInstant() {
        return beginInstant;
    }

    /** The resolved incremental-read end instant (inclusive upper bound), or {@code null} if non-incremental. */
    public String getEndInstant() {
        return endInstant;
    }

    /**
     * The raw {@code @incr} option params (glob / fallback-full-table-scan / hollow-commit policy), fed verbatim
     * to the ported incremental relations at scan time. Empty (never null) for a non-incremental read.
     */
    public Map<String, String> getIncrementalParams() {
        return incrementalParams;
    }

    /** Returns a builder pre-populated with this handle's state, for creating modified copies. */
    public Builder toBuilder() {
        Builder b = new Builder(dbName, tableName, basePath, hudiTableType);
        b.inputFormat = this.inputFormat;
        b.serdeLib = this.serdeLib;
        b.partitionKeyNames = this.partitionKeyNames;
        b.tableParameters = this.tableParameters;
        b.prunedPartitionPaths = this.prunedPartitionPaths;
        b.queryInstant = this.queryInstant;
        b.beginInstant = this.beginInstant;
        b.endInstant = this.endInstant;
        b.incrementalParams = this.incrementalParams;
        return b;
    }

    @Override
    public String toString() {
        return "HudiTableHandle{" + dbName + "." + tableName
                + ", basePath=" + basePath + ", type=" + hudiTableType + "}";
    }

    /**
     * Builder for constructing HudiTableHandle with scan metadata.
     */
    public static final class Builder {
        private final String dbName;
        private final String tableName;
        private final String basePath;
        private final String hudiTableType;
        private String inputFormat;
        private String serdeLib;
        private List<String> partitionKeyNames;
        private Map<String, String> tableParameters;
        private List<String> prunedPartitionPaths;
        private String queryInstant;
        private String beginInstant;
        private String endInstant;
        private Map<String, String> incrementalParams;

        public Builder(String dbName, String tableName, String basePath, String hudiTableType) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.basePath = basePath;
            this.hudiTableType = hudiTableType;
        }

        public Builder inputFormat(String val) {
            this.inputFormat = val;
            return this;
        }

        public Builder serdeLib(String val) {
            this.serdeLib = val;
            return this;
        }

        public Builder partitionKeyNames(List<String> val) {
            this.partitionKeyNames = val;
            return this;
        }

        public Builder tableParameters(Map<String, String> val) {
            this.tableParameters = val;
            return this;
        }

        public Builder prunedPartitionPaths(List<String> val) {
            this.prunedPartitionPaths = val;
            return this;
        }

        public Builder queryInstant(String val) {
            this.queryInstant = val;
            return this;
        }

        public Builder beginInstant(String val) {
            this.beginInstant = val;
            return this;
        }

        public Builder endInstant(String val) {
            this.endInstant = val;
            return this;
        }

        public Builder incrementalParams(Map<String, String> val) {
            this.incrementalParams = val;
            return this;
        }

        public HudiTableHandle build() {
            return new HudiTableHandle(this);
        }
    }
}
