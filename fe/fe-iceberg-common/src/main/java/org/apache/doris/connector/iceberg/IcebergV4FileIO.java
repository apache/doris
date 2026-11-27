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

import com.fasterxml.jackson.core.JsonGenerator;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.avro.InvalidAvroMagicException;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestListFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.util.JsonUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

/** Serializable, borrowed read view for table-relative format v4 locations. */
public final class IcebergV4FileIO implements FileIO {
    private static final long serialVersionUID = 1L;
    private static final Pattern SCHEME = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]*:.*", Pattern.DOTALL);
    private final FileIO delegate;
    private final String tableLocation;
    private final Map<String, MetadataLocation> taskLocations;
    private final transient TableMetadata metadata;
    private transient LoadingCache<String, Integer> manifestVersions;
    private transient LoadingCache<Long, MetadataHeader> snapshotHeaders;
    private transient ConcurrentMap<String, MetadataLocation> metadataLocations;

    public IcebergV4FileIO(FileIO delegate, String tableLocation) {
        this(delegate, tableLocation, null);
    }

    public IcebergV4FileIO(FileIO delegate, String tableLocation, TableMetadata metadata) {
        this(delegate, tableLocation, metadata, Map.of());
    }

    private IcebergV4FileIO(FileIO delegate, String tableLocation,
            TableMetadata metadata, Map<String, MetadataLocation> taskLocations) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.tableLocation = tableLocation;
        this.metadata = metadata;
        this.taskLocations = Map.copyOf(taskLocations);
    }

    /** Exposes borrowed decorators without moving file operations outside their authentication scope. */
    public interface Delegating extends FileIO {
        FileIO delegate();
    }

    public static IcebergV4FileIO find(FileIO io) {
        while (io instanceof Delegating) {
            io = ((Delegating) io).delegate();
        }
        return io instanceof IcebergV4FileIO ? (IcebergV4FileIO) io : null;
    }

    public FileIO delegate() {
        return delegate;
    }

    public IcebergV4FileIO withDelegate(FileIO replacement) {
        IcebergV4FileIO view = new IcebergV4FileIO(replacement, tableLocation, metadata, taskLocations);
        // Per-consumer authentication changes the IO delegate, not this statement's frozen path provenance.
        view.metadataLocations = locations();
        return view;
    }

    /** Bind just this task's concrete reference; neither metadata JSON nor snapshot history crosses the wire. */
    public IcebergV4FileIO forManifest(ManifestFile file) {
        prepareManifest(file);
        MetadataLocation location = locations().get(file.path());
        return new IcebergV4FileIO(delegate, tableLocation, null,
                location == null ? Map.of() : Map.of(file.path(), location));
    }

    public IcebergV4FileIO forManifestList(ManifestListFile file, long snapshotId) {
        if (!file.location().startsWith("/")) {
            return new IcebergV4FileIO(delegate, tableLocation);
        }
        snapshotHeader(file.location(), snapshotId);
        return new IcebergV4FileIO(delegate, tableLocation, null,
                Map.of(file.location(), locations().get(file.location())));
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

    /** Resolve index keys only; position-delete payloads still compare the original stored file spelling. */
    public String contentFileIdentity(ContentFile<?> owner, String path) {
        String resolved = isAbsolute(path) || isLegacyEntry(owner) ? path : resolve(tableLocation, path);
        FileIO storage = delegate;
        while (storage instanceof Delegating) {
            storage = ((Delegating) storage).delegate();
        }
        if (storage instanceof HadoopFileIO || (storage instanceof ResolvingFileIO
                && HadoopFileIO.class.isAssignableFrom(((ResolvingFileIO) storage).ioClass(resolved)))) {
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(resolved);
            java.net.URI uri = hadoopPath.toUri();
            if (uri.getScheme() != null && (uri.getAuthority() != null || "file".equals(uri.getScheme()))) {
                return hadoopPath.toString();
            }
            // Resolve legacy roots in their actual default filesystem, through the authenticated factory.
            HadoopInputFile input = (HadoopInputFile) delegate.newInputFile(resolved);
            return hadoopIdentity(input);
        }
        return resolved;
    }

    private static String hadoopIdentity(HadoopInputFile input) {
        return input.getFileSystem().makeQualified(input.getPath()).toString();
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
            metadataLocations = new ConcurrentHashMap<>(taskLocations);
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
                Snapshot snapshot = metadata == null ? null : metadata.snapshot(id);
                String path = snapshot == null ? null : snapshot.manifestListLocation();
                return path == null ? null : snapshotHeader(path, id);
            });
        }
        return snapshotHeaders;
    }

    private MetadataHeader snapshotHeader(String path, long snapshotId) {
        String relative = hasBase() || isAbsolute(path) ? resolve(tableLocation, path) : null;
        MetadataHeader header = null;
        RuntimeIOException nonAvroCandidate = null;
        try {
            header = relative == null ? null : existingHeader(relative, FileFormat.AVRO);
        } catch (RuntimeIOException failure) {
            if (!path.startsWith("/") || !(failure.getCause() instanceof InvalidAvroMagicException)) {
                throw failure;
            }
            nonAvroCandidate = failure;
        }
        if (header != null && Objects.equals(header.snapshotId, snapshotId)) {
            if (path.startsWith("/")) {
                remember(path, relative, snapshotId);
            }
            return header;
        }
        if (path.startsWith("/")) {
            header = existingHeader(path, FileFormat.AVRO);
            // Only a matching legacy producer proves that non-Avro bytes belong to an unrelated candidate.
            if (header != null && Objects.equals(header.snapshotId, snapshotId)
                    && (nonAvroCandidate == null || (header.version >= 1 && header.version < 4))) {
                remember(path, path, snapshotId);
                return header;
            }
        }
        if (nonAvroCandidate != null) {
            throw nonAvroCandidate;
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
        String relative = hasBase() ? resolve(tableLocation, path) : null;
        // A retained producer proves the namespace. Existence alone never identifies a manifest.
        if (source != null && source.version < 4) {
            return remember(path, matchingManifest(path, file), file.snapshotId());
        }
        if (source != null && !source.location.startsWith("/")) {
            return remember(path, matchingManifest(relative, file), file.snapshotId());
        }
        InputFile relativeInput = relative == null ? null : existingInput(relative);
        InputFile rootedInput;
        try {
            // Object-store FileIOs may reject a filesystem-root spelling at the factory boundary.
            rootedInput = delegate.newInputFile(path);
        } catch (IllegalArgumentException | NotFoundException unsupportedRoot) {
            rootedInput = null;
        }
        if (rootedInput != null && !rootedInput.exists()) {
            rootedInput = null;
        }
        boolean relativeMatches = relativeInput != null && relativeInput.getLength() == file.length();
        boolean rootedMatches = rootedInput != null && rootedInput.getLength() == file.length();
        // After expiry, the immutable descriptor is the remaining evidence; never guess between two matches.
        if (relativeMatches && rootedMatches) {
            // Hadoop roots and qualified paths can alias one object; ambiguity requires distinct identities.
            if (relativeInput instanceof HadoopInputFile && rootedInput instanceof HadoopInputFile
                    && hadoopIdentity((HadoopInputFile) relativeInput)
                            .equals(hadoopIdentity((HadoopInputFile) rootedInput))) {
                return remember(path, relative, file.snapshotId());
            }
            throw new IllegalArgumentException("Ambiguous legacy and V4 manifest reference: " + path);
        }
        if (relativeMatches || rootedMatches) {
            return remember(path, relativeMatches ? relative : path, file.snapshotId());
        }
        if (relativeInput != null || rootedInput != null) {
            throw new IllegalArgumentException("Manifest length does not match its descriptor: " + path);
        }
        if (relative == null) {
            resolve(tableLocation, path);
        }
        throw new NotFoundException("Cannot find manifest at %s or retained rooted location %s", relative, path);
    }

    private String matchingManifest(String location, ManifestFile file) {
        InputFile input = existingInput(location);
        if (input == null) {
            throw new NotFoundException("Cannot find manifest: %s", location);
        }
        if (input.getLength() != file.length()) {
            throw new IllegalArgumentException("Manifest length does not match its descriptor: " + location);
        }
        return location;
    }

    private boolean hasBase() {
        return tableLocation != null && !tableLocation.isEmpty();
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

    private static final class MetadataLocation implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String location;
        private final Long snapshotId;

        private MetadataLocation(String location, Long snapshotId) {
            this.location = location;
            this.snapshotId = snapshotId;
        }
    }

    @Override
    public InputFile newInputFile(String path) {
        return delegate.newInputFile(inputLocation(path));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        return delegate.newInputFile(inputLocation(path), length);
    }

    private String inputLocation(String path) {
        if (path.startsWith("/")) {
            MetadataLocation bound = locations().get(path);
            if (bound != null) {
                return bound.location;
            }
        }
        if (path.startsWith("/") && metadata != null && path.equals(metadata.metadataFileLocation())) {
            String relative = hasBase() ? resolve(tableLocation, path) : null;
            InputFile candidate = relative == null ? null : existingInput(relative);
            if (candidate != null) {
                return remember(path, relative, null);
            }
            // A catalog may retain a rooted pointer after an upgrade. Existence alone cannot prove
            // that a rooted object belongs to this table or to its frozen metadata generation.
            candidate = existingInput(path);
            if (candidate != null && matchesFrozenMetadata(candidate)) {
                return remember(path, path, null);
            }
            throw new NotFoundException("Cannot find frozen table metadata: %s", path);
        }
        // MANIFESTS also sizes its list through the string overload before opening the typed descriptor.
        if (path.startsWith("/") && metadata != null) {
            for (Snapshot snapshot : metadata.snapshots()) {
                if (path.equals(snapshot.manifestListLocation())) {
                    return snapshotHeader(path, snapshot.snapshotId()).location;
                }
            }
        }
        return resolve(tableLocation, path);
    }

    private boolean matchesFrozenMetadata(InputFile candidate) {
        TableMetadata loaded;
        if (candidate.location().contains(".metadata.json")) {
            loaded = TableMetadataParser.read(candidate);
        } else {
            // Catalog-supplied JSON pointers need not follow the SDK's codec-bearing file-name convention.
            try (InputStream input = candidate.newStream()) {
                loaded = TableMetadataParser.fromJson(candidate.location(), JsonUtil.mapper().readTree(input));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        // UUID and snapshot alone cannot distinguish metadata-only schema or property updates.
        return Arrays.equals(metadataDigest(metadata), metadataDigest(loaded));
    }

    private static byte[] metadataDigest(TableMetadata metadata) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            // Stream the SDK encoding: metadata histories must not become additional whole JSON trees/strings.
            try (JsonGenerator json = JsonUtil.factory().createGenerator(
                    new DigestOutputStream(OutputStream.nullOutputStream(), digest))) {
                TableMetadataParser.toJson(metadata, json);
            }
            return digest.digest();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
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
        MetadataLocation bound = locations().get(path);
        if (bound == null) {
            Long snapshotId = metadata == null ? null : metadata.snapshots().stream()
                    .filter(snapshot -> path.equals(snapshot.manifestListLocation()))
                    .map(Snapshot::snapshotId).findFirst().orElse(null);
            if (snapshotId != null) {
                snapshotHeader(path, snapshotId);
            } else {
                // Directly serialized IO has no table history. Production metadata tasks bind the expected
                // list identity on the FE; standalone readers still validate the candidate's list header.
                String relative = hasBase() ? resolve(tableLocation, path) : null;
                MetadataHeader header = relative == null ? null : existingHeader(relative, FileFormat.AVRO);
                if (header != null && header.snapshotId != null) {
                    remember(path, relative, header.snapshotId);
                } else {
                    header = existingHeader(path, FileFormat.AVRO);
                    if (header == null || header.snapshotId == null) {
                        throw new NotFoundException("Cannot find manifest list: %s", path);
                    }
                    remember(path, path, header.snapshotId);
                }
            }
            bound = locations().get(path);
        }
        return bound.location.equals(path) ? delegate.newInputFile(file) : FileIO.super.newInputFile(file);
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
