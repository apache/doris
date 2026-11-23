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
import org.apache.iceberg.util.JsonUtil;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Map;

/** Package bridge for filtering borrowed manifest entries before copying retained files. */
public final class ManifestReaderAccess {
    private ManifestReaderAccess() {
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
