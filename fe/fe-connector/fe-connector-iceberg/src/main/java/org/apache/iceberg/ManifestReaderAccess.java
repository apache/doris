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

package org.apache.iceberg;

import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.parquet.ManifestMetadataAccess;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.doris.connector.iceberg.IcebergV4FileIO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Package bridge for filtering borrowed manifest entries before copying retained files. */
public final class ManifestReaderAccess {
    private ManifestReaderAccess() {
    }

    /** Freeze each transported task's actual reference instead of serializing table-wide read history. */
    public static FileScanTask prepareTask(FileScanTask task) {
        if (task instanceof BaseFilesTable.ManifestReadTask) {
            BaseFilesTable.ManifestReadTask files = (BaseFilesTable.ManifestReadTask) task;
            if (files.io() instanceof IcebergV4FileIO) {
                return new BaseFilesTable.ManifestReadTask(files.dataTableSchema(),
                        ((IcebergV4FileIO) files.io()).forManifest(files.manifest()), files.specsById(),
                        files.manifest(), files.projection(), files.residual());
            }
        } else if (task instanceof BaseEntriesTable.ManifestReadTask) {
            BaseEntriesTable.ManifestReadTask entries = (BaseEntriesTable.ManifestReadTask) task;
            if (entries.io() instanceof IcebergV4FileIO) {
                return new BaseEntriesTable.ManifestReadTask(entries.dataTableSchema(),
                        ((IcebergV4FileIO) entries.io()).forManifest(entries.manifest()), entries.specsById(),
                        entries.manifest(), entries.projection(), entries.residual());
            }
        } else if (task instanceof AllManifestsTable.ManifestListReadTask) {
            AllManifestsTable.ManifestListReadTask lists = (AllManifestsTable.ManifestListReadTask) task;
            if (lists.io() instanceof IcebergV4FileIO) {
                return new AllManifestsTable.ManifestListReadTask(lists.dataTableSchema(),
                        ((IcebergV4FileIO) lists.io()).forManifestList(
                                lists.manifestList(), lists.referenceSnapshotId()),
                        lists.schema(), lists.specsById(), lists.manifestList(), lists.residual(),
                        lists.referenceSnapshotId());
            }
        }
        return task;
    }

    /** Metadata-only probes must not decode unused partitions or require their expired source schemas. */
    public static ManifestReader<DataFile> readDataMetadata(ManifestFile manifest, Table table, List<String> columns) {
        return ManifestFiles.read(manifest, table.io(), table.specs()).select(columns);
    }

    public static ManifestReader<DeleteFile> readDeleteMetadata(ManifestFile manifest, Table table,
            List<String> columns) {
        return ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs()).select(columns);
    }

    /** Recover physical partition types, consulting the header when commit metadata lacks a source. */
    public static Map<Integer, PartitionSpec> readSpecs(ManifestFile manifest, Table table, FileIO io)
            throws IOException {
        PartitionSpec spec = table.specs().get(manifest.partitionSpecId());
        // Fixed-result transforms (bucket/year/month/day/hour) remain readable without their source
        // schema. Iceberg resolves their result from UNKNOWN; do not require expired source schemas.
        boolean missingSource = spec.rawPartitionType().fields().stream()
                .anyMatch(field -> field.type().typeId() == org.apache.iceberg.types.Type.TypeID.UNKNOWN);
        if (!missingSource) {
            return Collections.singletonMap(spec.specId(), spec);
        }
        Long commitSnapshotId = manifest.snapshotId();
        Snapshot commitSnapshot = commitSnapshotId == null ? null : table.snapshot(commitSnapshotId);
        Schema commitSchema = commitSnapshot == null ? null : table.schemas().get(commitSnapshot.schemaId());
        if (commitSchema == null
                || spec.fields().stream().anyMatch(field -> commitSchema.findField(field.sourceId()) == null)) {
            // A rewrite can retain older physical types while committing after source removal. Recover the
            // header types so data and delete partitions use the same canonical representation after adaptation.
            PartitionSpec readable = ManifestReaderAccess.readPartitionSpec(manifest, io);
            return Collections.singletonMap(readable.specId(), readable);
        }
        Map<Integer, Types.NestedField> sources = new LinkedHashMap<>();
        Set<String> reservedNames = new HashSet<>();
        spec.fields().forEach(field -> reservedNames.add(field.name()));
        for (PartitionField field : spec.fields()) {
            Types.NestedField source = commitSchema.findField(field.sourceId());
            String name = "source_" + field.sourceId();
            while (!reservedNames.add(name)) {
                name += "_";
            }
            sources.put(field.sourceId(), Types.NestedField.optional(field.sourceId(), name, source.type()));
        }
        PartitionSpec readable = spec.toUnbound().bind(new Schema(
                new ArrayList<>(sources.values())), true);
        return Collections.singletonMap(readable.specId(), readable);
    }

    /** Recover header types without rejecting historical fields whose source columns were dropped. */
    public static PartitionSpec readPartitionSpec(ManifestFile manifest, FileIO io) throws IOException {
        // A rewritten manifest can carry the current table schema and an older partition spec. The SDK's
        // header reader binds strictly, unlike table metadata loading, and rejects that valid combination.
        Map<String, String> metadata;
        if (FileFormat.fromFileName(manifest.path()) == FileFormat.PARQUET) {
            // V4 moves these header keys to the Parquet footer; an Avro reader cannot recover them.
            metadata = ManifestMetadataAccess.read(io.newInputFile(manifest));
        } else {
            try (CloseableIterable<?> header = InternalData.read(FileFormat.AVRO, io.newInputFile(manifest))
                    .project(new Schema(ManifestEntry.STATUS)).build()) {
                metadata = ((AvroIterable<?>) header).getMetadata();
            }
        }
        ObjectNode specJson = JsonUtil.mapper().createObjectNode();
        specJson.put("spec-id", manifest.partitionSpecId());
        specJson.set("fields", JsonUtil.mapper().readTree(metadata.get("partition-spec")));
        return PartitionSpecParser.fromJson(specJson).bind(SchemaParser.fromJson(metadata.get("schema")), true);
    }

    /**
     * The reader owns all underlying streams and must remain open during iteration. Files are reused;
     * callers must finish inspecting each file and copy any retained value before advancing.
     */
    public static <F extends ContentFile<F>> Iterable<F> liveFiles(ManifestReader<F> reader) {
        return CloseableIterable.transform(reader.liveEntries(), ManifestEntry::file);
    }
}
