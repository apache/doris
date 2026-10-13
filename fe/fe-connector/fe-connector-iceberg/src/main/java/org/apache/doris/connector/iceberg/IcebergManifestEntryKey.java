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

import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Types;

import java.util.Objects;

/**
 * Cache key for one iceberg manifest entry (T08).
 *
 * <p>Ported verbatim from the legacy fe-core
 * {@code org.apache.doris.datasource.iceberg.IcebergManifestEntryKey}. The key carries only stable identity
 * dimensions — the manifest path plus its content type — so two tables that share a manifest path hit the same
 * cached payload, and a table-level invalidation (REFRESH TABLE) intentionally does NOT drop it (legacy
 * {@code testInvalidateTableKeepsManifestCache} parity). Runtime loader context (the manifest/table instances)
 * must not be stored here.
 *
 * <p>The key also carries the partition type the manifest is decoded with: the reader converts partition values to
 * the bound spec's types, and after a type promotion (e.g. INT to BIGINT) a scan pinned to the old schema and a scan
 * on the current one bind the same manifest to different types. Each binding needs its own decoded copy.
 */
public class IcebergManifestEntryKey {
    private final String manifestPath;
    private final ManifestContent content;
    private final Types.StructType partitionType;

    public IcebergManifestEntryKey(String manifestPath, ManifestContent content, Types.StructType partitionType) {
        this.manifestPath = Objects.requireNonNull(manifestPath, "manifestPath can not be null");
        this.content = Objects.requireNonNull(content, "content can not be null");
        this.partitionType = Objects.requireNonNull(partitionType, "partitionType can not be null");
    }

    /** The key for {@code manifest} decoded with {@code boundSpec}, the scan's spec for its partition spec id. */
    public static IcebergManifestEntryKey of(ManifestFile manifest, PartitionSpec boundSpec) {
        return new IcebergManifestEntryKey(manifest.path(), manifest.content(), boundSpec.partitionType());
    }

    public String getManifestPath() {
        return manifestPath;
    }

    public ManifestContent getContent() {
        return content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergManifestEntryKey)) {
            return false;
        }
        IcebergManifestEntryKey that = (IcebergManifestEntryKey) o;
        return Objects.equals(manifestPath, that.manifestPath)
                && content == that.content
                && partitionType.equals(that.partitionType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(manifestPath, content, partitionType);
    }
}
