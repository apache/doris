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

package org.apache.doris.fs;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SPI-compatible replacement for the legacy {@code SwitchingFileSystem}.
 *
 * <p>Implements {@link FileSystem} and routes each operation to the appropriate
 * {@link FileSystem} based on the URI scheme / authority of the path operand.  The
 * storage-type → {@link StorageProperties} mapping comes from the catalog's
 * {@code storagePropertiesMap}.
 *
 * <p>Resolved {@link FileSystem} instances are cached per {@link StorageProperties} reference
 * (identity-based) to avoid recreating connections on every call.
 */
public class SpiSwitchingFileSystem implements FileSystem {

    private static final Logger LOG = LogManager.getLogger(SpiSwitchingFileSystem.class);

    private final Map<StorageProperties.Type, StorageProperties> storagePropertiesMap;
    /** Non-null only when created via the test constructor — all paths delegate here. */
    private FileSystem testDelegate;
    /**
     * Cache: StorageProperties (identity) → spi.FileSystem.
     * Using identity comparison is correct because the properties map values are stable
     * objects owned by the catalog instance.
     */
    private final Map<StorageProperties, FileSystem> cache = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public SpiSwitchingFileSystem(Map<StorageProperties.Type, StorageProperties> storagePropertiesMap) {
        this.storagePropertiesMap = storagePropertiesMap;
    }

    /**
     * Testing constructor: routes every path to the supplied {@link FileSystem} delegate.
     * Should never be used in production code.
     */
    @VisibleForTesting
    public SpiSwitchingFileSystem(FileSystem testDelegate) {
        this.storagePropertiesMap = java.util.Collections.emptyMap();
        this.testDelegate = testDelegate;
    }

    /** Resolves the appropriate {@link FileSystem} for the given URI string. */
    public FileSystem forPath(String uri) throws IOException {
        if (testDelegate != null) {
            return testDelegate;
        }
        LocationPath lp = LocationPath.of(uri, storagePropertiesMap);
        StorageProperties sp = lp.getStorageProperties();
        if (sp == null) {
            throw new IOException("No StorageProperties found for path: " + uri);
        }
        try {
            return cache.computeIfAbsent(sp, props -> {
                try {
                    return FileSystemFactory.getFileSystem(props);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    /** Resolves the appropriate {@link FileSystem} for the given {@link Location}. */
    public FileSystem forLocation(Location location) throws IOException {
        return forPath(location.uri());
    }

    // -----------------------------------------------------------------------
    // FileSystem interface — each method delegates to the per-path filesystem
    // -----------------------------------------------------------------------

    @Override
    public boolean exists(Location location) throws IOException {
        return forLocation(location).exists(location);
    }

    @Override
    public void mkdirs(Location location) throws IOException {
        forLocation(location).mkdirs(location);
    }

    @Override
    public void delete(Location location, boolean recursive) throws IOException {
        forLocation(location).delete(location, recursive);
    }

    @Override
    public void rename(Location src, Location dst) throws IOException {
        forLocation(src).rename(src, dst);
    }

    @Override
    public FileIterator list(Location location) throws IOException {
        return forLocation(location).list(location);
    }

    @Override
    public List<FileEntry> listFiles(Location dir) throws IOException {
        return forLocation(dir).listFiles(dir);
    }

    @Override
    public List<FileEntry> listFilesRecursive(Location dir) throws IOException {
        return forLocation(dir).listFilesRecursive(dir);
    }

    @Override
    public Set<String> listDirectories(Location dir) throws IOException {
        return forLocation(dir).listDirectories(dir);
    }

    @Override
    public void renameDirectory(Location src, Location dst, Runnable whenSrcNotExists)
            throws IOException {
        forLocation(src).renameDirectory(src, dst, whenSrcNotExists);
    }

    @Override
    public DorisInputFile newInputFile(Location location) throws IOException {
        return forLocation(location).newInputFile(location);
    }

    @Override
    public DorisInputFile newInputFile(Location location, long length) throws IOException {
        return forLocation(location).newInputFile(location, length);
    }

    @Override
    public DorisOutputFile newOutputFile(Location location) throws IOException {
        return forLocation(location).newOutputFile(location);
    }

    @Override
    public GlobListing globListWithLimit(Location path, String startAfter, long maxBytes,
            long maxFiles) throws IOException {
        return forLocation(path).globListWithLimit(path, startAfter, maxBytes, maxFiles);
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return; // idempotent
        }
        List<FileSystem> snapshot = new ArrayList<>(cache.values());
        cache.clear();
        IOException firstError = null;
        for (FileSystem fs : snapshot) {
            try {
                fs.close();
            } catch (IOException e) {
                if (firstError == null) {
                    firstError = e;
                } else {
                    firstError.addSuppressed(e);
                }
                LOG.warn("Error closing cached FileSystem: {}", fs, e);
            }
        }
        if (firstError != null) {
            throw firstError;
        }
    }
}
