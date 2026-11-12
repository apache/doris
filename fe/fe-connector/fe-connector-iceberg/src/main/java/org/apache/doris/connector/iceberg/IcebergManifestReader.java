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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.ManifestReaderAccess;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/** Reads immutable manifest contents independently of whether the result is retained in a cache. */
final class IcebergManifestReader {
    /** One manifest's request-local predicate and partition adapter, confined to its loading worker. */
    static final class ReadFilter {
        private final Types.StructType partitionType;
        private final Predicate<ContentFile<?>> predicate;
        private final Runnable skipped;
        private boolean partitionTypeChecked;
        private IcebergPartitionValueAdapter adapter;

        ReadFilter(PartitionSpec spec, Predicate<ContentFile<?>> predicate, Runnable skipped) {
            this.partitionType = spec.rawPartitionType();
            this.predicate = predicate;
            this.skipped = skipped;
        }

        private void checkPartitionType(ContentFile<?> file) {
            if (!partitionTypeChecked) {
                Types.StructType physical = ((PartitionData) file.partition()).getPartitionType();
                if (!physical.equals(partitionType)) {
                    adapter = new IcebergPartitionValueAdapter(physical, partitionType);
                }
                partitionTypeChecked = true;
            }
        }

        DataFile adapt(DataFile file) {
            checkPartitionType(file);
            return adapter == null ? file : adapter.adapt(file);
        }

        DeleteFile adapt(DeleteFile file) {
            checkPartitionType(file);
            return adapter == null ? file : adapter.adapt(file);
        }

        boolean matches(ContentFile<?> file) {
            if (predicate.test(file)) {
                return true;
            }
            skipped.run();
            return false;
        }
    }

    private IcebergManifestReader() {
    }

    static ManifestCacheValue read(ManifestFile manifest, Table table,
            ManifestContent content, boolean estimateWeight) {
        return read(manifest, table, table.io(), content, estimateWeight, null, null, () -> { });
    }

    static Function<ManifestFile, ManifestCacheValue> uncachedLoader(Table table, List<String> columns,
            Runnable checkCancelled) {
        return uncachedLoader(table, columns, null, checkCancelled);
    }

    static Function<ManifestFile, ManifestCacheValue> uncachedLoader(Table table, List<String> columns,
            Function<ManifestFile, ReadFilter> filters, Runnable checkCancelled) {
        // One request-local IO view, shared by this scan's loads, never by the catalog/cache.
        FileIO io = new IcebergScanFileIO(table.io(), checkCancelled);
        return manifest -> read(manifest, table, io, manifest.content(), false, columns, filters, checkCancelled);
    }

    private static ManifestCacheValue read(ManifestFile manifest, Table table, FileIO io,
            ManifestContent content, boolean estimateWeight, List<String> columns,
            Function<ManifestFile, ReadFilter> filters, Runnable checkCancelled) {
        checkCancelled.run();
        ReadFilter filter = filters == null ? null : filters.apply(manifest);
        try {
            if (content == ManifestContent.DELETES) {
                return ManifestCacheValue.forDeleteFiles(
                        loadDeleteFiles(manifest, table, io, filter, checkCancelled), estimateWeight);
            }
            return ManifestCacheValue.forDataFiles(
                    loadDataFiles(manifest, table, io, columns, filter, checkCancelled), estimateWeight);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read iceberg manifest " + manifest.path(), e);
        }
    }

    private static List<DataFile> loadDataFiles(ManifestFile manifest, Table table, FileIO io,
            List<String> columns, ReadFilter filter, Runnable checkCancelled) throws IOException {
        List<DataFile> dataFiles = new ArrayList<>();
        // .copy() is mandatory: the ManifestReader iterator reuses the same object across iterations.
        try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io,
                manifestReadSpecs(manifest, table))) {
            if (columns != null) {
                reader.select(columns);
            }
            for (DataFile borrowed : ManifestReaderAccess.liveFiles(reader)) {
                checkCancelled.run();
                DataFile file = filter == null ? borrowed : filter.adapt(borrowed);
                if (filter == null || filter.matches(file)) {
                    dataFiles.add(file.copy());
                }
            }
        }
        return dataFiles;
    }

    /** Recover the manifest writer's partition source types, or read its header when that schema expired. */
    private static Map<Integer, PartitionSpec> manifestReadSpecs(ManifestFile manifest, Table table) {
        PartitionSpec spec = table.specs().get(manifest.partitionSpecId());
        // Fixed-result transforms (bucket/year/month/day/hour) remain readable without their source
        // schema. Iceberg resolves their result from UNKNOWN; do not require expired source schemas.
        boolean missingSource = spec.rawPartitionType().fields().stream()
                .anyMatch(field -> field.type().typeId() == org.apache.iceberg.types.Type.TypeID.UNKNOWN);
        if (!missingSource) {
            return Collections.singletonMap(spec.specId(), spec);
        }
        Long writerSnapshotId = manifest.snapshotId();
        Snapshot writerSnapshot = writerSnapshotId == null ? null : table.snapshot(writerSnapshotId);
        Schema writerSchema = writerSnapshot == null ? null : table.schemas().get(writerSnapshot.schemaId());
        if (writerSchema == null) {
            // The manifest header retains its physical partition type after snapshot/schema expiration.
            return null;
        }
        Map<Integer, Types.NestedField> sources = new LinkedHashMap<>();
        Set<String> reservedNames = new HashSet<>();
        spec.fields().forEach(field -> reservedNames.add(field.name()));
        for (PartitionField field : spec.fields()) {
            Types.NestedField source = writerSchema.findField(field.sourceId());
            if (source == null) {
                return null;
            }
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

    private static List<DeleteFile> loadDeleteFiles(ManifestFile manifest, Table table, FileIO io,
            ReadFilter filter, Runnable checkCancelled) throws IOException {
        List<DeleteFile> deleteFiles = new ArrayList<>();
        try (ManifestReader<DeleteFile> reader =
                ManifestFiles.readDeleteManifest(manifest, io, manifestReadSpecs(manifest, table))) {
            for (DeleteFile borrowed : ManifestReaderAccess.liveFiles(reader)) {
                checkCancelled.run();
                DeleteFile file = filter == null ? borrowed : filter.adapt(borrowed);
                if (filter == null || filter.matches(file)) {
                    deleteFiles.add(filter == null ? file.copy() : copyDeleteForIndex(file));
                }
            }
        }
        return deleteFiles;
    }

    /** Match the SDK index's retention policy after predicates have inspected the complete statistics. */
    static DeleteFile copyDeleteForIndex(DeleteFile file) {
        Set<Integer> columns = file.content() == FileContent.POSITION_DELETES
                ? Collections.singleton(MetadataColumns.DELETE_FILE_PATH.fieldId())
                : new HashSet<>(file.equalityFieldIds());
        return ContentFileUtil.copy(file, true, columns);
    }

}
