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
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

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
    private transient List<Column> partialSchema;
    private transient int partialSchemaVersion;

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
            MaterializedIndexMeta base = table.getIndexMetaByIndexId(table.getBaseIndexId());
            snapshot.partialSchema = ImmutableList.copyOf(base.getSchema().stream()
                    .map(TxnWriteSchemaSnapshot::copyColumn).collect(Collectors.toList()));
            snapshot.partialSchemaVersion = base.getSchemaVersion();
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

    static Column copyColumn(Column column) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(column), Column.class);
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

    List<Column> getPartialSchema() {
        return partialSchema == null ? null : partialSchema.stream()
                .map(TxnWriteSchemaSnapshot::copyColumn).collect(Collectors.toList());
    }

    int getPartialSchemaVersion() {
        return partialSchemaVersion;
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
