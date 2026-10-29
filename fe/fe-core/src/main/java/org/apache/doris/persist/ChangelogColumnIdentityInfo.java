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

package org.apache.doris.persist;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/** Exact column identity transition; does not change the light schema change property. */
public class ChangelogColumnIdentityInfo implements Writable {
    @SerializedName("dbId")
    private long dbId;
    @SerializedName("tableId")
    private long tableId;
    @SerializedName("columnIds")
    private Map<Long, Map<String, Integer>> columnIds;
    @SerializedName("previousVersions")
    private Map<Long, Integer> previousVersions;
    @SerializedName("schemaVersions")
    private Map<Long, Integer> schemaVersions;
    @SerializedName("maxColumnIds")
    private Map<Long, Integer> maxColumnIds;

    public ChangelogColumnIdentityInfo(long dbId, long tableId, Map<Long, Map<String, Integer>> columnIds,
            Map<Long, Integer> previousVersions, Map<Long, Integer> schemaVersions, Map<Long, Integer> maxColumnIds) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.columnIds = columnIds;
        this.previousVersions = previousVersions;
        this.schemaVersions = schemaVersions;
        this.maxColumnIds = maxColumnIds;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public void apply(OlapTable table) {
        Preconditions.checkState(table.getId() == tableId, "column identity table mismatch");
        Preconditions.checkState(table.getIndexIdToMeta(true).keySet().equals(columnIds.keySet()),
                "column identity index mismatch");
        Preconditions.checkState(previousVersions.keySet().equals(columnIds.keySet())
                        && schemaVersions.keySet().equals(columnIds.keySet())
                        && maxColumnIds.keySet().equals(columnIds.keySet()), "incomplete column identity journal");
        // Validate every index before mutating any column, including on replay.
        for (Map.Entry<Long, Map<String, Integer>> entry : columnIds.entrySet()) {
            MaterializedIndexMeta meta = table.getIndexMetaByIndexId(entry.getKey());
            int version = meta.getSchemaVersion();
            boolean alreadyApplied = version == schemaVersions.get(entry.getKey());
            Preconditions.checkState(alreadyApplied || version == previousVersions.get(entry.getKey()),
                    "column identity schema version mismatch");
            Map<String, Integer> ids = entry.getValue();
            Preconditions.checkState(ids.keySet().equals(meta.getSchema(true).stream().map(Column::getName)
                    .collect(Collectors.toSet())), "column identity column mismatch");
            Preconditions.checkState(ids.values().stream().allMatch(id -> id >= 0)
                    && ids.values().stream().distinct().count() == ids.size(), "invalid column identities");
            for (Column column : meta.getSchema(true)) {
                int id = ids.get(column.getName());
                Preconditions.checkState((!alreadyApplied && column.getUniqueId() < 0)
                        || column.getUniqueId() == id, "column identity changed");
                Preconditions.checkState(id <= maxColumnIds.get(entry.getKey()), "invalid maximum column ID");
            }
        }
        columnIds.forEach((indexId, ids) -> {
            MaterializedIndexMeta meta = table.getIndexMetaByIndexId(indexId);
            meta.getSchema(true).forEach(column -> column.setUniqueId(ids.get(column.getName())));
            meta.setMaxColUniqueId(maxColumnIds.get(indexId));
            meta.setSchemaVersion(schemaVersions.get(indexId));
        });
        table.rebuildFullSchema();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static ChangelogColumnIdentityInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), ChangelogColumnIdentityInfo.class);
    }
}
