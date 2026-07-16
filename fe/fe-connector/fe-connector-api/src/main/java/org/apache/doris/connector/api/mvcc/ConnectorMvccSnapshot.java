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

package org.apache.doris.connector.api.mvcc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable description of a point-in-time snapshot taken from an MVCC-capable
 * external table (Iceberg, Paimon, Hudi, ...).
 *
 * <p>Returned by {@code ConnectorMetadata.beginQuerySnapshot} and friends.
 * Used by the engine as the MVCC pin for all subsequent reads of the same
 * table handle within a query, and serialized into BE scan ranges so the
 * read path sees a consistent version.</p>
 */
public final class ConnectorMvccSnapshot {

    private final long snapshotId;
    private final long timestampMillis;
    private final String description;
    private final long schemaId;
    private final Map<String, String> properties;
    private final boolean lastModifiedFreshness;

    private ConnectorMvccSnapshot(Builder b) {
        this.snapshotId = b.snapshotId;
        this.timestampMillis = b.timestampMillis;
        this.description = b.description;
        this.schemaId = b.schemaId;
        this.properties = b.properties.isEmpty()
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(b.properties));
        this.lastModifiedFreshness = b.lastModifiedFreshness;
    }

    /** Connector-assigned snapshot identifier (e.g. Iceberg snapshot id). */
    public long getSnapshotId() {
        return snapshotId;
    }

    /** Wall-clock time at which the snapshot was committed, in ms since epoch. */
    public long getTimestampMillis() {
        return timestampMillis;
    }

    /** Optional human-readable description; may be empty, never null. */
    public String getDescription() {
        return description;
    }

    /**
     * Schema version of this snapshot (e.g. paimon schemaId). {@code -1} = unknown
     * &rArr; schema-aware reads fall back to the latest schema.
     */
    public long getSchemaId() {
        return schemaId;
    }

    /** Connector-specific metadata propagated to BE. Unmodifiable, never null. */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Whether this table's MTMV freshness is a last-modified TIMESTAMP rather than a snapshot id. When
     * {@code true} the generic model ({@code PluginDrivenMvccExternalTable}) serves the table/partition MTMV
     * snapshots from {@code ConnectorMetadata.getTableFreshness} / {@code getPartitionFreshnessMillis} (fetched
     * on the MTMV refresh path only); when {@code false} (the default, e.g. paimon/iceberg — a snapshot-id
     * connector) it keeps the snapshot-id / pin-timestamp freshness with NO extra metadata call. The flag rides
     * on the query-begin pin so fe-core reads it off the pin it already holds — a snapshot-id connector pays
     * zero extra round-trips.
     */
    public boolean isLastModifiedFreshness() {
        return lastModifiedFreshness;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private long snapshotId;
        private long timestampMillis;
        private String description = "";
        private long schemaId = -1;
        private final Map<String, String> properties = new HashMap<>();
        private boolean lastModifiedFreshness;

        public Builder snapshotId(long snapshotId) {
            this.snapshotId = snapshotId;
            return this;
        }

        /** Marks this table's MTMV freshness as last-modified (see {@link #isLastModifiedFreshness()}). */
        public Builder lastModifiedFreshness(boolean lastModifiedFreshness) {
            this.lastModifiedFreshness = lastModifiedFreshness;
            return this;
        }

        public Builder timestampMillis(long timestampMillis) {
            this.timestampMillis = timestampMillis;
            return this;
        }

        public Builder schemaId(long schemaId) {
            this.schemaId = schemaId;
            return this;
        }

        public Builder description(String description) {
            this.description = Objects.requireNonNull(description, "description");
            return this;
        }

        public Builder property(String key, String value) {
            this.properties.put(
                    Objects.requireNonNull(key, "key"),
                    Objects.requireNonNull(value, "value"));
            return this;
        }

        public Builder properties(Map<String, String> properties) {
            this.properties.putAll(Objects.requireNonNull(properties, "properties"));
            return this;
        }

        public ConnectorMvccSnapshot build() {
            return new ConnectorMvccSnapshot(this);
        }
    }
}
