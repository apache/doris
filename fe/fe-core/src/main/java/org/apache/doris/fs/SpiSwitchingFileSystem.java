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
import org.apache.doris.filesystem.DorisInputStream;
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
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
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
 *
 * <p><strong>Legacy cross-scheme fallback translation.</strong> {@link LocationPath} documents a
 * compatibility fallback where a path whose scheme does not directly match any configured storage
 * is served by an S3-compatible storage named "s3" (e.g. a {@code cos://} path routed to an S3
 * configuration). Concrete filesystems validate URI schemes against their own whitelist, so the
 * delegate must not see the foreign scheme. When that fallback fires — and only then — this class
 * translates the operand URI to the storage's native scheme before delegating and translates any
 * returned paths back to the caller's original scheme, so callers keep comparing paths against
 * their metadata (e.g. HMS locations) unchanged. Translation activates only when the rewrite is a
 * pure scheme-prefix swap and is therefore exactly reversible; in every other case the original
 * URI is passed through untouched.
 */
public class SpiSwitchingFileSystem implements FileSystem {

    private static final Logger LOG = LogManager.getLogger(SpiSwitchingFileSystem.class);

    private final Map<StorageProperties.Type, StorageProperties> storagePropertiesMap;
    /** Non-null only when created via the test constructor — all paths delegate here. */
    private FileSystem testDelegate;
    /**
     * Cache: StorageProperties → spi.FileSystem.
     * Uses value-based equality (via ConnectionProperties.equals/hashCode on origProps),
     * so logically identical configurations share the same FileSystem instance.
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
        return resolve(Location.of(uri)).fs;
    }

    /** Resolves the appropriate {@link FileSystem} for the given {@link Location}. */
    @Override
    public FileSystem forLocation(Location location) throws IOException {
        return resolve(location).fs;
    }

    /**
     * Resolves the filesystem for {@code location} and, when the legacy cross-scheme fallback
     * fired, the scheme translation to apply on the way in and out. The filesystem choice and
     * the translation come from the same {@link LocationPath} resolution — the normalized URI
     * is never re-routed.
     */
    private Resolved resolve(Location location) throws IOException {
        if (testDelegate != null) {
            return new Resolved(testDelegate, null, null);
        }
        String uri = location.uri();
        LocationPath lp = LocationPath.of(uri, storagePropertiesMap);
        StorageProperties sp = lp.getStorageProperties();
        if (sp == null) {
            throw new IOException("No StorageProperties found for path: " + uri);
        }
        FileSystem fs;
        try {
            fs = cache.computeIfAbsent(sp, props -> {
                try {
                    return FileSystemFactory.getFileSystem(props);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
        String[] prefixes = compatSchemePrefixes(uri, lp, sp);
        if (prefixes == null) {
            return new Resolved(fs, null, null);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Legacy scheme fallback for path {}: delegating as {}...", uri, prefixes[1]);
        }
        return new Resolved(fs, prefixes[0], prefixes[1]);
    }

    /**
     * Returns {@code [callerPrefix, delegatePrefix]} (e.g. {@code ["cos://", "s3://"]}) when the
     * legacy cross-scheme fallback fired and the normalization is a pure scheme-prefix swap, or
     * {@code null} when no translation must happen. The two conditions:
     * <ul>
     *   <li>the storage type selected by {@link LocationPath} differs from the type the path's
     *       scheme maps to — i.e. the compatibility fallback in
     *       {@code LocationPath.findStorageProperties} picked the storage, not a direct match;</li>
     *   <li>the normalized URI equals the original with only the scheme prefix replaced, so the
     *       translation is exactly reversible for URIs the delegate returns.</li>
     * </ul>
     */
    private static String[] compatSchemePrefixes(String uri, LocationPath lp, StorageProperties sp) {
        StorageProperties.Type schemeType = LocationPath.fromSchemaWithContext(uri, lp.getSchema());
        if (schemeType == sp.getType()) {
            return null;
        }
        String normalized = lp.getNormalizedLocation();
        int uriSchemeEnd = uri.indexOf("://");
        int normSchemeEnd = normalized == null ? -1 : normalized.indexOf("://");
        if (uriSchemeEnd < 0 || normSchemeEnd < 0) {
            return null;
        }
        String callerPrefix = uri.substring(0, uriSchemeEnd + 3);
        String delegatePrefix = normalized.substring(0, normSchemeEnd + 3);
        if (callerPrefix.equalsIgnoreCase(delegatePrefix)) {
            return null;
        }
        if (!uri.substring(uriSchemeEnd + 3).equals(normalized.substring(normSchemeEnd + 3))) {
            return null;
        }
        return new String[] {callerPrefix, delegatePrefix};
    }

    // -----------------------------------------------------------------------
    // FileSystem interface — each method delegates to the per-path filesystem
    // -----------------------------------------------------------------------

    @Override
    public boolean exists(Location location) throws IOException {
        Resolved r = resolve(location);
        return r.fs.exists(r.toDelegate(location));
    }

    @Override
    public void mkdirs(Location location) throws IOException {
        Resolved r = resolve(location);
        r.fs.mkdirs(r.toDelegate(location));
    }

    @Override
    public void delete(Location location, boolean recursive) throws IOException {
        Resolved r = resolve(location);
        r.fs.delete(r.toDelegate(location), recursive);
    }

    @Override
    public void rename(Location src, Location dst) throws IOException {
        Resolved r = resolve(src);
        r.fs.rename(r.toDelegate(src), r.toDelegate(dst));
    }

    @Override
    public FileIterator list(Location location) throws IOException {
        Resolved r = resolve(location);
        FileIterator iterator = r.fs.list(r.toDelegate(location));
        if (!r.translated()) {
            return iterator;
        }
        return new FileIterator() {
            @Override
            public boolean hasNext() throws IOException {
                return iterator.hasNext();
            }

            @Override
            public FileEntry next() throws IOException {
                return r.toCaller(iterator.next());
            }

            @Override
            public void close() throws IOException {
                iterator.close();
            }
        };
    }

    @Override
    public List<FileEntry> listFiles(Location dir) throws IOException {
        Resolved r = resolve(dir);
        return r.toCaller(r.fs.listFiles(r.toDelegate(dir)));
    }

    @Override
    public List<FileEntry> listFilesRecursive(Location dir) throws IOException {
        Resolved r = resolve(dir);
        return r.toCaller(r.fs.listFilesRecursive(r.toDelegate(dir)));
    }

    @Override
    public Set<String> listDirectories(Location dir) throws IOException {
        Resolved r = resolve(dir);
        Set<String> dirs = r.fs.listDirectories(r.toDelegate(dir));
        if (!r.translated()) {
            return dirs;
        }
        Set<String> mapped = new LinkedHashSet<>();
        for (String d : dirs) {
            mapped.add(r.toCaller(d));
        }
        return mapped;
    }

    @Override
    public void renameDirectory(Location src, Location dst, Runnable whenSrcNotExists)
            throws IOException {
        Resolved r = resolve(src);
        r.fs.renameDirectory(r.toDelegate(src), r.toDelegate(dst), whenSrcNotExists);
    }

    @Override
    public DorisInputFile newInputFile(Location location) throws IOException {
        Resolved r = resolve(location);
        DorisInputFile file = r.fs.newInputFile(r.toDelegate(location));
        return r.translated() ? new TranslatedInputFile(file, location) : file;
    }

    @Override
    public DorisInputFile newInputFile(Location location, long length) throws IOException {
        Resolved r = resolve(location);
        DorisInputFile file = r.fs.newInputFile(r.toDelegate(location), length);
        return r.translated() ? new TranslatedInputFile(file, location) : file;
    }

    @Override
    public DorisOutputFile newOutputFile(Location location) throws IOException {
        Resolved r = resolve(location);
        DorisOutputFile file = r.fs.newOutputFile(r.toDelegate(location));
        return r.translated() ? new TranslatedOutputFile(file, location) : file;
    }

    @Override
    public GlobListing globListWithLimit(Location path, String startAfter, long maxBytes,
            long maxFiles) throws IOException {
        Resolved r = resolve(path);
        // startAfter and GlobListing's bucket/prefix/maxFile are bucket-relative keys,
        // not URIs — only the file entries carry scheme-qualified locations.
        GlobListing listing = r.fs.globListWithLimit(r.toDelegate(path), startAfter, maxBytes, maxFiles);
        if (!r.translated()) {
            return listing;
        }
        return new GlobListing(r.toCaller(listing.getFiles()), listing.getBucket(),
                listing.getPrefix(), listing.getMaxFile());
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

    /**
     * A resolved delegate filesystem plus the (optional) scheme translation to apply. When
     * {@code callerPrefix}/{@code delegatePrefix} are null, all methods are identity pass-throughs.
     */
    private static final class Resolved {
        final FileSystem fs;
        /** Scheme prefix as the caller wrote it (e.g. {@code "cos://"}); null when no translation. */
        private final String callerPrefix;
        /** Native scheme prefix of the serving storage (e.g. {@code "s3://"}); null when no translation. */
        private final String delegatePrefix;

        Resolved(FileSystem fs, String callerPrefix, String delegatePrefix) {
            this.fs = fs;
            this.callerPrefix = callerPrefix;
            this.delegatePrefix = delegatePrefix;
        }

        boolean translated() {
            return callerPrefix != null;
        }

        /** Caller URI → the scheme the delegate filesystem natively accepts. */
        Location toDelegate(Location location) {
            if (!translated()) {
                return location;
            }
            String uri = location.uri();
            if (!uri.regionMatches(true, 0, callerPrefix, 0, callerPrefix.length())) {
                return location;
            }
            return Location.of(delegatePrefix + uri.substring(callerPrefix.length()));
        }

        /** Delegate-returned URI → the scheme the caller originally used. */
        String toCaller(String uri) {
            if (!translated() || uri == null
                    || !uri.regionMatches(true, 0, delegatePrefix, 0, delegatePrefix.length())) {
                return uri;
            }
            return callerPrefix + uri.substring(delegatePrefix.length());
        }

        Location toCaller(Location location) {
            return translated() ? Location.of(toCaller(location.uri())) : location;
        }

        FileEntry toCaller(FileEntry entry) {
            if (!translated()) {
                return entry;
            }
            return new FileEntry(toCaller(entry.location()), entry.length(), entry.isDirectory(),
                    entry.modificationTime(), entry.blocks());
        }

        List<FileEntry> toCaller(List<FileEntry> entries) {
            if (!translated()) {
                return entries;
            }
            List<FileEntry> mapped = new ArrayList<>(entries.size());
            for (FileEntry entry : entries) {
                mapped.add(toCaller(entry));
            }
            return mapped;
        }
    }

    /**
     * Wraps a delegate {@link DorisInputFile} so {@link #location()} reports the caller's
     * original URI instead of the scheme-translated one the delegate was opened with —
     * callers may feed that location back into this switching filesystem.
     */
    private static final class TranslatedInputFile implements DorisInputFile {
        private final DorisInputFile delegate;
        private final Location callerLocation;

        TranslatedInputFile(DorisInputFile delegate, Location callerLocation) {
            this.delegate = delegate;
            this.callerLocation = callerLocation;
        }

        @Override
        public Location location() {
            return callerLocation;
        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public boolean exists() throws IOException {
            return delegate.exists();
        }

        @Override
        public long lastModifiedTime() throws IOException {
            return delegate.lastModifiedTime();
        }

        @Override
        public DorisInputStream newStream() throws IOException {
            return delegate.newStream();
        }
    }

    /** Output-side counterpart of {@link TranslatedInputFile}. */
    private static final class TranslatedOutputFile implements DorisOutputFile {
        private final DorisOutputFile delegate;
        private final Location callerLocation;

        TranslatedOutputFile(DorisOutputFile delegate, Location callerLocation) {
            this.delegate = delegate;
            this.callerLocation = callerLocation;
        }

        @Override
        public Location location() {
            return callerLocation;
        }

        @Override
        public OutputStream create() throws IOException {
            return delegate.create();
        }

        @Override
        public OutputStream createOrOverwrite() throws IOException {
            return delegate.createOrOverwrite();
        }
    }
}
