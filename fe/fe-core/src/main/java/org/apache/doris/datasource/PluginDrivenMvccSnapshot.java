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

package org.apache.doris.datasource;

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccSnapshot;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Generic MVCC snapshot for plugin-driven (connector SPI) tables.
 *
 * <p>Pins a single point-in-time view of an MVCC-capable connector table for the whole duration of
 * a query: the scalar {@link ConnectorMvccSnapshot} (the snapshot-id pin used for reads) plus the
 * materialized partition view listed at that moment. Holding the materialized view here means every
 * MTMV/MvccTable accessor that receives this snapshot reads the SAME partition set with NO extra
 * connector round-trip (single-pin invariant).</p>
 *
 * <p>Source-agnostic: it carries already-rendered partition names/items and per-partition staleness
 * timestamps, so no data-source-specific logic lives in fe-core. Parity with the legacy
 * {@code PaimonMvccSnapshot}/{@code PaimonPartitionInfo} pair.</p>
 *
 * <p>For an explicit time-travel pin it ALSO carries the schema AS OF the pinned snapshot
 * ({@code pinnedSchema}), so reads under schema evolution see the historical columns; a {@code null}
 * pinnedSchema means "use the latest schema" (parity with legacy
 * {@code PaimonExternalTable.getSchemaCacheValue} reading the context-pinned snapshot's schema).</p>
 */
public class PluginDrivenMvccSnapshot implements MvccSnapshot {

    private final ConnectorMvccSnapshot connectorSnapshot;
    private final Map<String, PartitionItem> nameToPartitionItem;
    private final Map<String, Long> nameToLastModifiedMillis;
    private final SchemaCacheValue pinnedSchema;

    /**
     * @param connectorSnapshot        the scalar snapshot pin (snapshot id used for reads)
     * @param nameToPartitionItem      rendered partition name -&gt; built {@link PartitionItem}
     * @param nameToLastModifiedMillis rendered partition name -&gt; last-modified epoch millis (one
     *                                 entry per listed partition, BEFORE any per-partition item build
     *                                 failure dropped a name from {@code nameToPartitionItem})
     */
    public PluginDrivenMvccSnapshot(ConnectorMvccSnapshot connectorSnapshot,
            Map<String, PartitionItem> nameToPartitionItem,
            Map<String, Long> nameToLastModifiedMillis) {
        this(connectorSnapshot, nameToPartitionItem, nameToLastModifiedMillis, null);
    }

    /**
     * @param connectorSnapshot        the scalar snapshot pin (snapshot id used for reads)
     * @param nameToPartitionItem      rendered partition name -&gt; built {@link PartitionItem}
     * @param nameToLastModifiedMillis rendered partition name -&gt; last-modified epoch millis
     * @param pinnedSchema             the schema AS OF the pinned snapshot (schema-at-snapshot under
     *                                 schema evolution); {@code null} = use the latest schema
     */
    public PluginDrivenMvccSnapshot(ConnectorMvccSnapshot connectorSnapshot,
            Map<String, PartitionItem> nameToPartitionItem,
            Map<String, Long> nameToLastModifiedMillis,
            SchemaCacheValue pinnedSchema) {
        this.connectorSnapshot = connectorSnapshot;
        this.nameToPartitionItem = nameToPartitionItem == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(nameToPartitionItem));
        this.nameToLastModifiedMillis = nameToLastModifiedMillis == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(nameToLastModifiedMillis));
        this.pinnedSchema = pinnedSchema;
    }

    public ConnectorMvccSnapshot getConnectorSnapshot() {
        return connectorSnapshot;
    }

    /**
     * The schema AS OF the pinned snapshot for time-travel under schema evolution; {@code null} for
     * the latest pin (B5a query-begin) or the no-handle path, meaning the caller uses the latest
     * schema.
     */
    public SchemaCacheValue getPinnedSchema() {
        return pinnedSchema;
    }

    /** Convenience: the schema version of the pinned connector snapshot ({@code -1} = unknown). */
    public long getSchemaId() {
        return connectorSnapshot.getSchemaId();
    }

    public Map<String, PartitionItem> getNameToPartitionItem() {
        return nameToPartitionItem;
    }

    public Map<String, Long> getNameToLastModifiedMillis() {
        return nameToLastModifiedMillis;
    }

    /**
     * True when at least one listed partition failed to build into a {@link PartitionItem} (its
     * rendered name could not be parsed), i.e. the built item map is short of the listed partition
     * set, so the caller falls back to UNPARTITIONED rather than silently pruning to a partial set.
     * Both maps are keyed by the rendered partition name, so this compares like-for-like: a connector
     * emitting two partitions that render to the same name collapses both maps equally and is NOT
     * flagged invalid. Parity with legacy {@code PaimonPartitionInfo.isPartitionInvalid}.
     */
    public boolean isPartitionInvalid() {
        return nameToLastModifiedMillis.size() != nameToPartitionItem.size();
    }
}
