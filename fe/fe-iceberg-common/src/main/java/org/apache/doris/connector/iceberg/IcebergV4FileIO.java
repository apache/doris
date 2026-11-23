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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestListFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Serializable, borrowed read view for table-relative format v4 locations. */
public final class IcebergV4FileIO implements FileIO {
    private static final long serialVersionUID = 1L;
    private static final Pattern SCHEME = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]*:.*", Pattern.DOTALL);
    private final FileIO delegate;
    private final String tableLocation;
    private final String metadataFileLocation;
    private final Map<Long, String> inlineSnapshotLists;
    private transient volatile TableMetadata metadata;
    private transient LoadingCache<String, TableMetadata> metadataFiles;
    private transient LoadingCache<String, Integer> manifestVersions;
    private transient LoadingCache<Long, MetadataHeader> snapshotHeaders;
    private transient ConcurrentMap<String, MetadataLocation> metadataLocations;

    public IcebergV4FileIO(FileIO delegate, String tableLocation) {
        this(delegate, tableLocation, null);
    }

    public IcebergV4FileIO(FileIO delegate, String tableLocation, TableMetadata metadata) {
        this(delegate, tableLocation, metadata, metadata == null ? null : metadata.metadataFileLocation(),
                metadata != null && metadata.metadataFileLocation() == null ? metadata.snapshots().stream()
                        .filter(snapshot -> snapshot.manifestListLocation() != null)
                        .collect(Collectors.toMap(Snapshot::snapshotId, Snapshot::manifestListLocation)) : Map.of());
    }

    private IcebergV4FileIO(FileIO delegate, String tableLocation,
            TableMetadata metadata, String metadataFileLocation, Map<Long, String> inlineSnapshotLists) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.tableLocation = tableLocation;
        this.metadata = metadata;
        this.metadataFileLocation = metadataFileLocation;
        // REST responses may supply inline metadata without a metadata-file identity to reopen on the BE.
        this.inlineSnapshotLists = Map.copyOf(inlineSnapshotLists);
    }

    public FileIO delegate() {
        return delegate;
    }

    public IcebergV4FileIO withDelegate(FileIO replacement) {
        return new IcebergV4FileIO(replacement, tableLocation, metadata, metadataFileLocation, inlineSnapshotLists);
    }

    private TableMetadata metadata() {
        if (metadata == null && metadataFileLocation != null) {
            metadata = metadataFiles().get(metadataFileLocation);
        }
        return metadata;
    }

    private synchronized LoadingCache<String, TableMetadata> metadataFiles() {
        if (metadataFiles == null) {
            // Transport only the immutable metadata-file identity, not all snapshot history in every JNI task.
            // The cache coalesces lazy remote loads without doing I/O under this initialization lock.
            metadataFiles = Caffeine.newBuilder().maximumSize(1)
                    .build(path -> TableMetadataParser.read(delegate, path));
        }
        return metadataFiles;
    }

    public static String resolve(String tableLocation, String path) {
        Objects.requireNonNull(path, "path");
        if (isAbsolute(path)) {
            return path;
        }
        if (tableLocation == null || tableLocation.isEmpty()) {
            throw new IllegalArgumentException("Relative Iceberg v4 path requires a table location: " + path);
        }
        // Duplicate separators are part of an object-store key, including at the concatenation boundary.
        return tableLocation + "/" + path;
    }

    private static boolean isAbsolute(String path) {
        return SCHEME.matcher(path).matches();
    }

    public String resolveLocation(String path) {
        return resolve(tableLocation, path);
    }

    public String resolveContentFile(ContentFile<?> file) {
        return isAbsolute(file.location()) || isLegacyEntry(file)
                ? file.location() : resolve(tableLocation, file.location());
    }

    private boolean isLegacyEntry(ContentFile<?> file) {
        String manifest = file.manifestLocation();
        // Encoding alone is insufficient: the SDK can also write a V4 manifest using Avro.
        return manifest != null && FileFormat.fromFileName(manifest) == FileFormat.AVRO
                && versions().get(manifest) < 4;
    }

    private synchronized LoadingCache<String, Integer> versions() {
        if (manifestVersions == null) {
            // Keep only small version numbers; never serialize a query's cache into every metadata task.
            manifestVersions = Caffeine.newBuilder().maximumSize(256).build(this::readManifestVersion);
        }
        return manifestVersions;
    }

    private int readManifestVersion(String manifest) {
        MetadataLocation reference = manifest.startsWith("/") ? locations().get(manifest) : null;
        if (manifest.startsWith("/") && reference == null) {
            throw new IllegalStateException("Manifest reference has not been bound: " + manifest);
        }
        String location = reference == null ? resolve(tableLocation, manifest) : reference.location;
        return readHeader(delegate.newInputFile(location), FileFormat.AVRO).version;
    }

    /** Bind provenance even on a parsed-manifest cache hit, where the SDK will not open the manifest again. */
    public void prepareManifest(ManifestFile file) {
        if (file.path().startsWith("/")) {
            metadataLocation(file);
        }
    }

    /** A cached-only prefetch probe runs under its queue lock and must never bind provenance through I/O. */
    public boolean isManifestPrepared(ManifestFile file) {
        if (!file.path().startsWith("/")) {
            return true;
        }
        MetadataLocation bound = locations().get(file.path());
        return bound != null && Objects.equals(bound.snapshotId, file.snapshotId());
    }

    private synchronized ConcurrentMap<String, MetadataLocation> locations() {
        if (metadataLocations == null) {
            // Query-owned provenance, not a cross-query cache. Retain only ambiguous slash-leading references.
            metadataLocations = new ConcurrentHashMap<>();
        }
        return metadataLocations;
    }

    private String remember(String path, String location, Long snapshotId) {
        MetadataLocation previous = locations().putIfAbsent(path, new MetadataLocation(location, snapshotId));
        if (previous != null && !previous.location.equals(location)) {
            throw new IllegalArgumentException("Ambiguous legacy and V4 metadata reference: " + path);
        }
        return location;
    }

    private synchronized LoadingCache<Long, MetadataHeader> snapshots() {
        if (snapshotHeaders == null) {
            snapshotHeaders = Caffeine.newBuilder().maximumSize(256).build(id -> {
                TableMetadata current = metadata();
                Snapshot snapshot = current == null ? null : current.snapshot(id);
                String path = snapshot == null ? inlineSnapshotLists.get(id) : snapshot.manifestListLocation();
                return path == null ? null : snapshotHeader(path, id);
            });
        }
        return snapshotHeaders;
    }

    private MetadataHeader snapshotHeader(String path, long snapshotId) {
        String relative = resolve(tableLocation, path);
        MetadataHeader header = existingHeader(relative, FileFormat.AVRO);
        if (header != null && Objects.equals(header.snapshotId, snapshotId)) {
            if (path.startsWith("/")) {
                remember(path, relative, snapshotId);
            }
            return header;
        }
        if (path.startsWith("/")) {
            header = existingHeader(path, FileFormat.AVRO);
            if (header != null && Objects.equals(header.snapshotId, snapshotId)) {
                remember(path, path, snapshotId);
                return header;
            }
        }
        throw new NotFoundException("Cannot find manifest list for snapshot %s: %s", snapshotId, path);
    }

    private String metadataLocation(ManifestFile file) {
        String path = file.path();
        MetadataLocation bound = locations().get(path);
        if (bound != null && Objects.equals(bound.snapshotId, file.snapshotId())) {
            return bound.location;
        }
        MetadataHeader source = file.snapshotId() == null ? null : snapshots().get(file.snapshotId());
        // Only a pre-V4 producer proves that a slash-leading reference is filesystem-rooted.
        // New V4 references remain relative, including when an unrelated root path also exists.
        if (source != null && source.version < 4) {
            return remember(path, path, file.snapshotId());
        }
        String relative = resolve(tableLocation, path);
        if (existingInput(relative) != null) {
            return remember(path, relative, file.snapshotId());
        }
        if (source != null && source.version >= 4 && !source.location.startsWith("/")) {
            throw new NotFoundException("Cannot find relative V4 manifest: %s", relative);
        }
        // SDK 1.12 retains unqualified write.metadata.path locations after subsequent V4 commits.
        // Rewrites may contain only EXISTING entries, so their snapshot ID cannot be proved from ADDED rows.
        // A physically rooted list in the same directory establishes the SDK's filesystem metadata convention.
        InputFile root = existingInput(path);
        if (root != null && (hasRootedMetadataDirectory(path)
                || containsAddedSnapshot(root, FileFormat.fromFileName(path), file.snapshotId()))) {
            return remember(path, path, file.snapshotId());
        }
        throw new NotFoundException("Cannot resolve manifest for snapshot %s: %s", file.snapshotId(), path);
    }

    private boolean hasRootedMetadataDirectory(String path) {
        String directory = path.substring(0, path.lastIndexOf('/') + 1);
        TableMetadata current = metadata();
        Snapshot latest = current == null ? null : current.currentSnapshot();
        String list = latest == null ? null : latest.manifestListLocation();
        Long snapshotId = latest == null ? null : latest.snapshotId();
        if (current == null) {
            Map.Entry<Long, String> inline = inlineSnapshotLists.entrySet().stream()
                    .filter(entry -> entry.getValue().startsWith("/")
                            && directory.equals(entry.getValue().substring(0, entry.getValue().lastIndexOf('/') + 1)))
                    .findFirst().orElse(null);
            if (inline == null) {
                return false;
            }
            list = inline.getValue();
            snapshotId = inline.getKey();
        }
        if (list == null || !list.startsWith("/")
                || !directory.equals(list.substring(0, list.lastIndexOf('/') + 1))) {
            return false;
        }
        return snapshots().get(snapshotId).location.equals(list);
    }

    private InputFile existingInput(String location) {
        try {
            InputFile input = delegate.newInputFile(location);
            return input.exists() ? input : null;
        } catch (NotFoundException missing) {
            return null;
        }
    }

    private MetadataHeader existingHeader(String location, FileFormat format) {
        InputFile input = existingInput(location);
        return input == null ? null : readHeader(input, format);
    }

    private static MetadataHeader readHeader(InputFile input, FileFormat format) {
        try {
            Map<String, String> metadata;
            try (CloseableIterable<?> header = InternalData.read(format, input)
                    .project(new Schema()).build()) {
                metadata = ((AvroIterable<?>) header).getMetadata();
            }
            String snapshot = metadata.get("snapshot-id");
            return new MetadataHeader(Integer.parseInt(metadata.getOrDefault("format-version", "1")),
                    snapshot == null || snapshot.equals("null") ? null : Long.valueOf(snapshot), input.location());
        } catch (IOException failure) {
            throw new UncheckedIOException("Failed to read Iceberg metadata header", failure);
        }
    }

    private static boolean containsAddedSnapshot(InputFile input, FileFormat format, Long snapshotId) {
        Schema projection = new Schema(Types.NestedField.required(0, "status", Types.IntegerType.get()),
                Types.NestedField.optional(1, "snapshot_id", Types.LongType.get()));
        try (CloseableIterable<StructLike> entries = InternalData.read(format, input).project(projection).build()) {
            for (StructLike entry : entries) {
                if (entry.get(0, Integer.class) == 1 && snapshotId != null
                        && snapshotId.equals(entry.get(1, Long.class))) {
                    return true;
                }
            }
            return false;
        } catch (IOException failure) {
            throw new UncheckedIOException("Failed to read Iceberg manifest provenance: " + input.location(), failure);
        }
    }

    private static final class MetadataHeader {
        private final int version;
        private final Long snapshotId;
        private final String location;

        private MetadataHeader(int version, Long snapshotId, String location) {
            this.version = version;
            this.snapshotId = snapshotId;
            this.location = location;
        }
    }

    private static final class MetadataLocation {
        private final String location;
        private final Long snapshotId;

        private MetadataLocation(String location, Long snapshotId) {
            this.location = location;
            this.snapshotId = snapshotId;
        }
    }

    @Override
    public InputFile newInputFile(String path) {
        return delegate.newInputFile(resolve(tableLocation, path));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        return delegate.newInputFile(resolve(tableLocation, path), length);
    }

    // Absolute descriptors may carry encryption metadata understood by the delegate. Relative V4 encrypted
    // descriptors retain the SDK's rejection rather than silently falling back to plaintext reads.
    @Override
    public InputFile newInputFile(DataFile file) {
        return isAbsolute(file.location()) || isLegacyEntry(file)
                ? delegate.newInputFile(file) : FileIO.super.newInputFile(file);
    }

    @Override
    public InputFile newInputFile(DeleteFile file) {
        return isAbsolute(file.location()) || isLegacyEntry(file)
                ? delegate.newInputFile(file) : FileIO.super.newInputFile(file);
    }

    @Override
    public InputFile newInputFile(ManifestFile file) {
        if (!file.path().startsWith("/")) {
            return isAbsolute(file.path()) ? delegate.newInputFile(file) : FileIO.super.newInputFile(file);
        }
        return metadataLocation(file).equals(file.path())
                ? delegate.newInputFile(file) : FileIO.super.newInputFile(file);
    }

    @Override
    public InputFile newInputFile(ManifestListFile file) {
        String path = file.location();
        if (!path.startsWith("/")) {
            return isAbsolute(path) ? delegate.newInputFile(file) : FileIO.super.newInputFile(file);
        }
        TableMetadata current = metadata();
        Long snapshotId = current == null ? inlineSnapshotLists.entrySet().stream()
                .filter(entry -> path.equals(entry.getValue())).map(Map.Entry::getKey).findFirst().orElse(null)
                : current.snapshots().stream()
                .filter(snapshot -> path.equals(snapshot.manifestListLocation()))
                .map(Snapshot::snapshotId).findFirst().orElse(null);
        if (snapshotId == null) {
            return FileIO.super.newInputFile(file);
        }
        snapshots().get(snapshotId);
        return locations().get(path).location.equals(path)
                ? delegate.newInputFile(file) : FileIO.super.newInputFile(file);
    }

    @Override
    public Map<String, String> properties() {
        return delegate.properties();
    }

    @Override
    public OutputFile newOutputFile(String path) {
        throw new UnsupportedOperationException("Iceberg v4 read view is read-only");
    }

    @Override
    public void deleteFile(String path) {
        throw new UnsupportedOperationException("Iceberg v4 read view is read-only");
    }

    @Override
    public void close() {
        // The table lease owns the underlying IO; closing a scan must not close shared catalog clients.
    }
}
