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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.persist.gson.GsonPostProcessable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/** Schema identity captured under the table read lock while preparing a write. */
public final class TxnWriteSchemaSnapshot implements GsonPostProcessable {
    @SerializedName("tableId")
    private final long tableId;
    @SerializedName("versions")
    private Map<Long, Integer> indexSchemaVersions;
    @SerializedName("mappings")
    private Map<Long, ChangelogWriteMapping> changelogMappings;
    @SerializedName("partialUpdate")
    private final boolean partialUpdate;
    private transient PartialUpdateSchema partialSchema;

    // Width checks are the only commit-time consumers of the original columns. Freeze the
    // scalar values so a later change to a Column or its ScalarType cannot affect this write.
    static final class PartialUpdateSchema {
        private final int version;
        private final ImmutableListMultimap<Integer, Integer> stringWidths;

        private PartialUpdateSchema(MaterializedIndexMeta base) {
            version = base.getSchemaVersion();
            ImmutableListMultimap.Builder<Integer, Integer> widths = ImmutableListMultimap.builder();
            for (Column column : base.getSchema()) {
                if (column.isVisible() && column.getType().isStringType()) {
                    widths.put(column.getUniqueId(), column.getStrLen());
                }
            }
            stringWidths = widths.build();
        }

        boolean isCompatibleWith(MaterializedIndexMeta current) {
            if (current.getSchemaVersion() <= version || stringWidths.isEmpty()) {
                return true;
            }
            // Schemas without allocated UIDs can repeat an ID. Preserve the first-match
            // lookup used by the commit check, including when that column is not a string.
            Map<Integer, Column> columnsById = new HashMap<>();
            for (Column column : current.getSchema()) {
                columnsById.putIfAbsent(column.getUniqueId(), column);
            }
            for (Map.Entry<Integer, Integer> width : stringWidths.entries()) {
                Column column = columnsById.get(width.getKey());
                if (column != null && column.getType().isStringType() && column.getStrLen() != width.getValue()) {
                    return false;
                }
            }
            return true;
        }
    }

    public TxnWriteSchemaSnapshot(long tableId, Map<Long, Integer> indexSchemaVersions,
            Map<Long, ChangelogWriteMapping> changelogMappings, boolean partialUpdate) {
        Preconditions.checkArgument(!indexSchemaVersions.isEmpty(), "Write schema requires at least one index");
        Preconditions.checkArgument(indexSchemaVersions.keySet().containsAll(changelogMappings.keySet()),
                "Changelog source indexes must belong to the write schema");
        this.tableId = tableId;
        this.indexSchemaVersions = ImmutableMap.copyOf(indexSchemaVersions);
        this.changelogMappings = ImmutableMap.copyOf(changelogMappings);
        this.partialUpdate = partialUpdate;
    }

    public static TxnWriteSchemaSnapshot capture(OlapTable table, boolean partialUpdate) {
        Map<Long, Integer> versions = schemaVersions(table);
        Map<Long, ChangelogWriteMapping> mappings = new TreeMap<>();
        table.getIndexIdToMeta().forEach((id, meta) -> {
            if (meta.getChangelogIndexId() > 0) {
                mappings.put(id, ChangelogWriteMapping.fromColumns(meta.getSchema(true),
                        table.getIndexMetaByIndexId(meta.getChangelogIndexId()).getSchema(true),
                        table.getChangelogConfig().getNeedHistoricalValue()));
            }
        });
        TxnWriteSchemaSnapshot snapshot = new TxnWriteSchemaSnapshot(table.getId(), versions, mappings, partialUpdate);
        if (partialUpdate) {
            snapshot.partialSchema = new PartialUpdateSchema(table.getIndexMetaByIndexId(table.getBaseIndexId()));
        }
        return snapshot;
    }

    public static Map<Long, Integer> schemaVersions(OlapTable table) {
        Map<Long, Integer> versions = new TreeMap<>();
        table.getIndexIdToMeta(true).forEach((id, meta) -> versions.put(id, meta.getSchemaVersion()));
        return ImmutableMap.copyOf(versions);
    }

    TxnWriteSchemaSnapshot persistentIdentity() {
        return new TxnWriteSchemaSnapshot(tableId, indexSchemaVersions, changelogMappings, partialUpdate);
    }

    public long getTableId() {
        return tableId;
    }

    public Set<Long> getIndexIds() {
        return indexSchemaVersions.keySet();
    }

    public Map<Long, Integer> getIndexSchemaVersions() {
        return indexSchemaVersions;
    }

    public Map<Long, ChangelogWriteMapping> getChangelogMappings() {
        return changelogMappings;
    }

    public boolean isPartialUpdate() {
        return partialUpdate;
    }

    PartialUpdateSchema getPartialSchema() {
        return partialSchema;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        indexSchemaVersions = ImmutableMap.copyOf(indexSchemaVersions);
        changelogMappings = ImmutableMap.copyOf(changelogMappings);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TxnWriteSchemaSnapshot)) {
            return false;
        }
        TxnWriteSchemaSnapshot snapshot = (TxnWriteSchemaSnapshot) other;
        return tableId == snapshot.tableId && partialUpdate == snapshot.partialUpdate
                && indexSchemaVersions.equals(snapshot.indexSchemaVersions)
                && changelogMappings.equals(snapshot.changelogMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, indexSchemaVersions, changelogMappings, partialUpdate);
    }
}
