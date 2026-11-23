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
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/** Serializable, borrowed read view for table-relative format v4 locations. */
public final class IcebergV4FileIO implements FileIO {
    private static final long serialVersionUID = 1L;
    private static final Pattern SCHEME = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]*:.*", Pattern.DOTALL);
    private final FileIO delegate;
    private final String tableLocation;
    private transient LoadingCache<String, Integer> manifestVersions;

    public IcebergV4FileIO(FileIO delegate, String tableLocation) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.tableLocation = tableLocation;
    }

    public FileIO delegate() {
        return delegate;
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
        try (CloseableIterable<?> header = InternalData.read(FileFormat.AVRO,
                delegate.newInputFile(resolve(tableLocation, manifest))).project(new Schema()).build()) {
            return Integer.parseInt(((AvroIterable<?>) header).getMetadata().getOrDefault("format-version", "1"));
        } catch (IOException failure) {
            throw new UncheckedIOException("Failed to read Iceberg manifest version", failure);
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
        return isAbsolute(file.path())
                ? delegate.newInputFile(file) : FileIO.super.newInputFile(file);
    }

    @Override
    public InputFile newInputFile(ManifestListFile file) {
        return isAbsolute(file.location())
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
