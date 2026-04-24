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

package org.apache.doris.filesystem.s3;

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisInputStream;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.spi.ObjFileSystem;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/* S3-backed FileSystem implementation for the Doris FE filesystem SPI.
 * Does not depend on fe-core, fe-common, or fe-catalog.
 */
public class S3FileSystem extends ObjFileSystem {

    private static final Logger LOG = LogManager.getLogger(S3FileSystem.class);

    // S3 does not have real directories; use a zero-byte marker with trailing slash.
    private static final String DIR_MARKER_SUFFIX = "/";

    public S3FileSystem(S3ObjStorage objStorage) {
        super("S3", objStorage);
    }

    @Override
    protected boolean isNotFoundError(IOException e) {
        return e instanceof java.io.FileNotFoundException;
    }

    @Override
    public void mkdirs(Location location) throws IOException {
        // S3 has no real directories; emulate POSIX mkdir -p:
        //   1) Idempotent: if the dir marker already exists, do nothing.
        //   2) Refuse to clobber: if a real (non-marker) key already exists at the same
        //      bare path, fail rather than silently turn the file into a 0-byte marker.
        //   3) Write a marker for every missing ancestor prefix between the bucket root
        //      and the leaf, so that subsequent list/exists semantics see the chain.
        // Full atomicity is impossible on S3; HEAD-then-PUT and never-overwrite is our
        // best-effort guarantee against concurrent deletes.
        String uri = location.uri();
        String marker = uri.endsWith(DIR_MARKER_SUFFIX) ? uri : uri + DIR_MARKER_SUFFIX;
        String bareKey = uri.endsWith(DIR_MARKER_SUFFIX)
                ? uri.substring(0, uri.length() - 1) : uri;

        if (objectExists(marker)) {
            return;
        }
        if (objectExists(bareKey)) {
            throw new IOException("mkdirs: a non-directory object already exists at " + bareKey);
        }

        // Walk parent prefixes (excluding bucket root and the leaf marker itself) and
        // PUT a 0-byte marker for any that are missing. Keys are derived from the parsed
        // S3Uri so this is correct for both virtual-hosted and path-style URIs.
        S3Uri parsed = parseUri(uri);
        String base = uriBase(uri, parsed);
        String dirKey = parsed.key().endsWith(DIR_MARKER_SUFFIX)
                ? parsed.key() : parsed.key() + DIR_MARKER_SUFFIX;
        List<String> ancestorKeys = new ArrayList<>();
        int idx = dirKey.length() - 1;
        while (true) {
            int slash = dirKey.lastIndexOf('/', idx - 1);
            if (slash <= 0) {
                break;
            }
            ancestorKeys.add(dirKey.substring(0, slash + 1));
            idx = slash;
        }
        Collections.reverse(ancestorKeys); // top-down for predictability
        for (String ancestor : ancestorKeys) {
            String ancestorUri = base + ancestor;
            if (!objectExists(ancestorUri)) {
                objStorage.putObject(ancestorUri,
                        RequestBody.of(InputStream.nullInputStream(), 0));
            }
        }
        objStorage.putObject(marker, RequestBody.of(InputStream.nullInputStream(), 0));
    }

    private boolean objectExists(String fullUri) throws IOException {
        try {
            objStorage.headObject(fullUri);
            return true;
        } catch (IOException e) {
            if (isNotFoundError(e)) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public void delete(Location location, boolean recursive) throws IOException {
        if (recursive) {
            // Delete all objects under this prefix
            String prefix = location.uri().endsWith(DIR_MARKER_SUFFIX)
                    ? location.uri() : location.uri() + DIR_MARKER_SUFFIX;
            deleteRecursive(prefix);
            // #25: do NOT issue an additional deleteObject(location.uri()) here.
            // The recursive listing already swept the directory marker (key ending in '/')
            // and every child key. For a non-existent bare key the extra call would just
            // produce a swallowed 404; for a real key it is already covered by the listing.
            return;
        }
        // Per FileSystem contract, recursive=false must fail on a non-empty directory.
        // S3 has no real directories, so probe the prefix for any non-marker child.
        String prefix = location.uri().endsWith(DIR_MARKER_SUFFIX)
                ? location.uri() : location.uri() + DIR_MARKER_SUFFIX;
        String markerKey = parseUri(prefix).key();
        RemoteObjects probe = ((S3ObjStorage) objStorage).listObjects(prefix, null, 2);
        for (RemoteObject obj : probe.getObjectList()) {
            if (!obj.getKey().equals(markerKey)) {
                throw new IOException("Directory not empty: " + location.uri());
            }
        }
        // Delete the exact object (or marker). 404 is benign and swallowed.
        try {
            objStorage.deleteObject(location.uri());
        } catch (IOException e) {
            if (!isNotFoundError(e)) {
                throw e;
            }
        }
    }

    /**
     * Batched implementation of {@link org.apache.doris.filesystem.FileSystem#deleteFiles}.
     * Groups input locations by bucket, then issues a single
     * {@link S3ObjStorage#deleteObjectsByKeys(String, java.util.List)} call per bucket
     * (which already chunks at the S3 1000-key DeleteObjects limit). Uses the same
     * URI parsing as the rest of this class so path-style URIs work transparently.
     */
    @Override
    public void deleteFiles(Collection<Location> locations) throws IOException {
        if (locations == null || locations.isEmpty()) {
            return;
        }
        // Preserve insertion order across buckets for stable test assertions.
        Map<String, List<String>> byBucket = new LinkedHashMap<>();
        for (Location loc : locations) {
            S3Uri u = parseUri(loc.uri());
            byBucket.computeIfAbsent(u.bucket(), b -> new ArrayList<>()).add(u.key());
        }
        S3ObjStorage s3 = (S3ObjStorage) objStorage;
        for (Map.Entry<String, List<String>> e : byBucket.entrySet()) {
            s3.deleteObjectsByKeys(e.getKey(), e.getValue());
        }
    }

    private void deleteRecursive(String prefix) throws IOException {
        // Single batched DeleteObjects per page (up to 1000 keys), instead of one
        // DeleteObject HTTP call per key. The deleteObjectsByKeys helper itself chunks
        // requests at the S3 1000-key limit.
        S3Uri prefixUri = parseUri(prefix);
        String bucket = prefixUri.bucket();
        S3ObjStorage s3 = (S3ObjStorage) objStorage;
        String continuationToken = null;
        do {
            RemoteObjects batch = s3.listObjects(prefix, continuationToken);
            if (!batch.getObjectList().isEmpty()) {
                List<String> keys = new ArrayList<>(batch.getObjectList().size());
                for (RemoteObject obj : batch.getObjectList()) {
                    keys.add(obj.getKey());
                }
                s3.deleteObjectsByKeys(bucket, keys);
            }
            continuationToken = batch.isTruncated() ? batch.getContinuationToken() : null;
        } while (continuationToken != null);
    }

    /** Parses {@code uri} respecting the underlying client's path-style configuration. */
    private S3Uri parseUri(String uri) {
        return S3Uri.parse(uri, ((S3ObjStorage) objStorage).isUsePathStyle());
    }

    /**
     * Returns the URI prefix that, when appended with a full object key, reconstructs the
     * fully-qualified URI: e.g. {@code "s3://bucket/"} or {@code "https://endpoint/bucket/"}.
     * Computed by stripping the parsed key off the original URI string, so it is correct
     * for both virtual-hosted and path-style URIs without re-deriving the host.
     */
    private static String uriBase(String uri, S3Uri parsed) {
        String key = parsed.key();
        String base = key.isEmpty() ? uri : uri.substring(0, uri.length() - key.length());
        return base.endsWith("/") ? base : base + "/";
    }

    private String reconstructUri(String prefix, String fullKey) {
        return uriBase(prefix, parseUri(prefix)) + fullKey;
    }

    /**
     * S3 has no notion of a real directory; the bare HEAD only finds files.  Fall back to
     * a 1-key prefix listing so that a "directory" that exists solely as a set of children
     * (e.g. an upload tree written by Hive/Spark without explicit markers) still reports
     * existence. Returns true for any non-empty prefix on object stores.
     */
    @Override
    public boolean exists(Location location) throws IOException {
        if (super.exists(location)) {
            return true;
        }
        return prefixHasChildren(location.uri());
    }

    private boolean prefixHasChildren(String uri) throws IOException {
        String prefix = uri.endsWith(DIR_MARKER_SUFFIX) ? uri : uri + DIR_MARKER_SUFFIX;
        RemoteObjects page = ((S3ObjStorage) objStorage).listObjects(prefix, null, 1);
        return !page.getObjectList().isEmpty();
    }

    /**
     * Renames a single object key.
     *
     * <p>This rename is implemented as <em>copy-then-delete</em> and is therefore
     * <strong>not atomic</strong>: a crash between the two calls will leave both copies.
     *
     * <p>Behaviour:
     * <ul>
     *   <li>Throws {@link FileAlreadyExistsException} if {@code dst} already exists.</li>
     *   <li>Refuses to rename a "directory" (a prefix with children but no key) and
     *       throws {@link IOException}; callers must use
     *       {@link #renameDirectory(Location, Location, Runnable)} for that case.</li>
     * </ul>
     */
    @Override
    public void rename(Location src, Location dst) throws IOException {
        // 1) Reject if destination already exists.
        boolean dstExists;
        try {
            objStorage.headObject(dst.uri());
            dstExists = true;
        } catch (IOException e) {
            if (!isNotFoundError(e)) {
                throw e;
            }
            dstExists = false;
        }
        if (dstExists) {
            throw new FileAlreadyExistsException(dst.uri());
        }
        // 2) Refuse directory-prefix renames so children are not silently orphaned.
        boolean srcIsKey;
        try {
            objStorage.headObject(src.uri());
            srcIsKey = true;
        } catch (IOException e) {
            if (!isNotFoundError(e)) {
                throw e;
            }
            srcIsKey = false;
        }
        if (!srcIsKey && prefixHasChildren(src.uri())) {
            throw new IOException("rename: source is a directory prefix with children; "
                    + "use renameDirectory(): " + src.uri());
        }
        objStorage.copyObject(src.uri(), dst.uri());
        objStorage.deleteObject(src.uri());
    }

    /**
     * Atomically-named (but non-atomic in implementation) directory move for object stores.
     *
     * <p>S3 has no real directories, so this method:
     * <ol>
     *   <li>Lists every key under {@code src + "/"} (paginated).</li>
     *   <li>If the listing is empty, runs {@code whenSrcNotExists} and returns.</li>
     *   <li>Verifies that {@code dst + "/"} has no objects yet; otherwise throws
     *       {@link FileAlreadyExistsException} <em>before</em> any copy.</li>
     *   <li>Copies each child to the matching destination key.</li>
     *   <li>Batch-deletes the source keys via
     *       {@link S3ObjStorage#deleteObjectsByKeys(String, java.util.List)}.</li>
     * </ol>
     *
     * <p>The default implementation in {@link org.apache.doris.filesystem.FileSystem} is unsafe on
     * S3 because it relies on {@code exists(src)} (which is false for marker-less directories)
     * and {@link #rename(Location, Location)} (which only handles a single key).
     */
    @Override
    public void renameDirectory(Location src, Location dst, Runnable whenSrcNotExists)
            throws IOException {
        String srcUri = src.uri();
        String dstUri = dst.uri();
        String srcPrefix = srcUri.endsWith(DIR_MARKER_SUFFIX) ? srcUri : srcUri + DIR_MARKER_SUFFIX;
        String dstPrefix = dstUri.endsWith(DIR_MARKER_SUFFIX) ? dstUri : dstUri + DIR_MARKER_SUFFIX;

        S3ObjStorage s3 = (S3ObjStorage) objStorage;

        // 1. Collect all source keys (paginated).
        List<String> srcKeys = new ArrayList<>();
        String continuation = null;
        do {
            RemoteObjects page = s3.listObjects(srcPrefix, continuation);
            for (RemoteObject obj : page.getObjectList()) {
                srcKeys.add(obj.getKey());
            }
            continuation = page.isTruncated() ? page.getContinuationToken() : null;
        } while (continuation != null);

        if (srcKeys.isEmpty()) {
            whenSrcNotExists.run();
            return;
        }

        // 2. Refuse to clobber a non-empty destination prefix.
        RemoteObjects dstProbe = s3.listObjects(dstPrefix, null, 1);
        if (!dstProbe.getObjectList().isEmpty()) {
            throw new FileAlreadyExistsException(dstUri);
        }

        // 3. Copy every key under src to the matching destination key.
        S3Uri srcParsed = parseUri(srcPrefix);
        S3Uri dstParsed = parseUri(dstPrefix);
        String srcKeyPrefix = srcParsed.key();
        String dstKeyPrefix = dstParsed.key();
        String srcBase = uriBase(srcPrefix, srcParsed);
        String dstBase = uriBase(dstPrefix, dstParsed);
        String bucket = srcParsed.bucket();
        for (String key : srcKeys) {
            String relative = key.startsWith(srcKeyPrefix)
                    ? key.substring(srcKeyPrefix.length()) : key;
            String dstKey = dstKeyPrefix + relative;
            objStorage.copyObject(srcBase + key, dstBase + dstKey);
        }

        // 4. Batch-delete the original keys.
        s3.deleteObjectsByKeys(bucket, srcKeys);
    }

    @Override
    public FileIterator list(Location location) throws IOException {
        // S3 list-objects is prefix-based, not directory-based: listing
        // "s3://bucket/foo" would also return objects under "foo_bar/" because
        // they share the same string prefix. The FileSystem.list contract
        // specifies a directory, so enforce a trailing '/' to constrain the
        // prefix to a true directory boundary.
        String uri = location.uri();
        if (!uri.endsWith(DIR_MARKER_SUFFIX)) {
            uri = uri + DIR_MARKER_SUFFIX;
        }
        return new S3FileIterator(uri);
    }

    /**
     * Returns the non-directory entries directly under {@code location} (non-recursive),
     * matching the {@link org.apache.doris.filesystem.FileSystem#listFiles} contract.
     *
     * <p>Unlike {@link #list(Location)} (which is a flat recursive listing on object stores),
     * this override uses S3 delimiter-mode listing so that only objects in this single
     * "directory level" are returned. Sub-directories (S3 {@code CommonPrefixes}) are
     * intentionally <em>not</em> exposed here — callers needing them must use the iterator
     * from {@link #list(Location)} or a separate listing method. The synthetic directory
     * marker created by {@link #mkdirs(Location)} (a zero-byte object whose key ends in
     * {@code "/"}) is filtered out of the result.
     *
     * <p>Glob handling is split to mirror HDFS {@code DFSFileSystem.listFiles} semantics:
     * <ul>
     *   <li><b>Single-level glob</b> (wildcards confined to the last path segment, e.g.
     *       {@code s3://b/data/2024_*.parquet}) — list immediate children of the parent
     *       prefix via delimiter-mode {@code listObjectsNonRecursive} and filter by the
     *       basename glob. This matches HDFS's behaviour where a last-segment glob
     *       returns only direct children of the parent directory.</li>
     *   <li><b>Cross-segment glob</b> (wildcards in any non-last segment, e.g.
     *       {@code s3://b/a/*\/file.csv}) — fall back to
     *       {@link #globListWithLimit(Location, String, long, long)} which performs a
     *       recursive prefix scan with regex filtering. S3 cannot evaluate wildcards
     *       per-segment cheaply, so we explicitly defer for that case.</li>
     * </ul>
     */
    @Override
    public List<FileEntry> listFiles(Location location) throws IOException {
        // BrokerLoadPendingTask passes paths with glob characters (e.g. _*).
        // S3 list-objects does not expand globs; reuse globListWithLimit for non
        // single-level glob paths (cross-segment globs need the recursive scan).
        String locationStr = location.toString();
        if (containsGlob(locationStr)) {
            if (isSingleLevelGlob(locationStr)) {
                return listFilesSingleLevelGlob(location);
            }
            // Cross-segment glob: keep the existing recursive prefix scan.
            // 0 (FileSystem contract: 0 = unlimited) for both byte and file limits
            // so a single listFiles() call returns every match without paging.
            GlobListing listing = globListWithLimit(location, null, 0L, 0L);
            return listing.getFiles();
        }
        String uri = location.uri();
        String prefix = uri.endsWith(DIR_MARKER_SUFFIX) ? uri : uri + DIR_MARKER_SUFFIX;
        String prefixKey = parseUri(prefix).key();

        S3ObjStorage s3 = (S3ObjStorage) objStorage;
        List<FileEntry> result = new ArrayList<>();
        String continuation = null;
        do {
            RemoteObjects page = s3.listObjectsNonRecursive(prefix, continuation);
            for (RemoteObject obj : page.getObjectList()) {
                // Skip the directory marker placeholder: key equals the listing prefix
                // or otherwise ends with "/".
                if (obj.getKey().equals(prefixKey)
                        || obj.getKey().endsWith(DIR_MARKER_SUFFIX)) {
                    continue;
                }
                result.add(new FileEntry(
                        Location.of(reconstructUri(prefix, obj.getKey())),
                        obj.getSize(), false, obj.getModificationTime(), List.of()));
            }
            continuation = page.isTruncated() ? page.getContinuationToken() : null;
        } while (continuation != null);
        return result;
    }

    /**
     * Single-level glob branch of {@link #listFiles(Location)}: lists the immediate
     * children of the parent prefix and filters them by the last-segment glob,
     * mirroring the HDFS {@code globStatus} → "non-directory entries" pipeline for
     * last-segment patterns. Sub-directory markers (keys ending in {@code "/"}) are
     * skipped so cross-prefix entries cannot leak through.
     */
    private List<FileEntry> listFilesSingleLevelGlob(Location location) throws IOException {
        String uri = location.uri();
        // containsGlob + isSingleLevelGlob guarantee a glob in the last segment,
        // and Location URIs always have at least one '/' (from "scheme://").
        int lastSlash = uri.lastIndexOf('/');
        String parentPrefix = uri.substring(0, lastSlash + 1);
        String basenameGlob = uri.substring(lastSlash + 1);
        Pattern matcher = Pattern.compile(globToRegex(basenameGlob));
        String parentKey = parseUri(parentPrefix).key();

        S3ObjStorage s3 = (S3ObjStorage) objStorage;
        List<FileEntry> result = new ArrayList<>();
        String continuation = null;
        do {
            RemoteObjects page = s3.listObjectsNonRecursive(parentPrefix, continuation);
            for (RemoteObject obj : page.getObjectList()) {
                String key = obj.getKey();
                // Skip the synthetic directory marker for the parent itself, and any
                // other "directory" placeholder objects.
                if (key.equals(parentKey) || key.endsWith(DIR_MARKER_SUFFIX)) {
                    continue;
                }
                int keyLastSlash = key.lastIndexOf('/');
                String basename = keyLastSlash >= 0 ? key.substring(keyLastSlash + 1) : key;
                if (!matcher.matcher(basename).matches()) {
                    continue;
                }
                result.add(new FileEntry(
                        Location.of(reconstructUri(parentPrefix, key)),
                        obj.getSize(), false, obj.getModificationTime(), List.of()));
            }
            continuation = page.isTruncated() ? page.getContinuationToken() : null;
        } while (continuation != null);
        return result;
    }

    // TODO(audit#6): listDirectories may also be improved to surface S3 CommonPrefixes;
    // skipped here to keep the scope of this change small. See plan-doc/s3.md.

    /**
     * Returns true if {@code path} contains glob wildcards that S3's prefix-based
     * list cannot evaluate. Only {@code *} and {@code ?} are treated as glob
     * metacharacters here. {@code [} and {@code {} are legal characters in S3 keys
     * — treating them as globs would route literal-key requests through
     * {@link #globListWithLimit} and cause spurious empty results. Users that need
     * character classes or alternations in their pattern still get them via the
     * dedicated glob entry point ({@link #globListWithLimit}); they are not
     * auto-detected from a generic listFiles() argument.
     */
    private static boolean containsGlob(String path) {
        return path.indexOf('*') >= 0 || path.indexOf('?') >= 0;
    }

    /**
     * Returns true iff {@code pathStr} contains a glob (only {@code *} and {@code ?} are
     * recognised, matching {@link #containsGlob}) confined entirely to the last path
     * segment — i.e. the basename has at least one wildcard and the parent prefix has
     * none. This is the case where S3 can satisfy the {@code listFiles} contract with a
     * single delimiter-mode listing of the parent prefix, mirroring HDFS semantics.
     * Cross-segment globs (wildcards in any non-last segment) return false and fall back
     * to the recursive {@link #globListWithLimit} scan.
     */
    static boolean isSingleLevelGlob(String pathStr) {
        int lastSlash = pathStr.lastIndexOf('/');
        String basename = lastSlash >= 0 ? pathStr.substring(lastSlash + 1) : pathStr;
        if (basename.indexOf('*') < 0 && basename.indexOf('?') < 0) {
            return false;
        }
        String parent = lastSlash >= 0 ? pathStr.substring(0, lastSlash) : "";
        return parent.indexOf('*') < 0 && parent.indexOf('?') < 0;
    }

    @Override
    public DorisInputFile newInputFile(Location location) throws IOException {
        return new S3InputFile(location);
    }

    @Override
    public DorisOutputFile newOutputFile(Location location) throws IOException {
        return new S3OutputFile(location);
    }

    /** Lazy-loading, paginating FileIterator over S3 list results. */
    private class S3FileIterator implements FileIterator {
        private final String prefix;
        private String continuationToken;
        private List<FileEntry> buffer = new ArrayList<>();
        private int bufferIdx = 0;
        private boolean done = false;

        S3FileIterator(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public boolean hasNext() throws IOException {
            if (bufferIdx < buffer.size()) {
                return true;
            }
            if (done) {
                return false;
            }
            fetchNextPage();
            return bufferIdx < buffer.size();
        }

        private void fetchNextPage() throws IOException {
            RemoteObjects page = objStorage.listObjects(prefix, continuationToken);
            buffer = new ArrayList<>();
            bufferIdx = 0;
            for (RemoteObject obj : page.getObjectList()) {
                // Skip directory-marker placeholders (e.g. created by mkdirs):
                // those are zero-byte objects whose key ends with "/" or equals the
                // listing prefix's bare key.  Emitting them as FileEntry causes
                // downstream readers to attempt opening a non-existent file.
                if (obj.getKey().endsWith(DIR_MARKER_SUFFIX)) {
                    continue;
                }
                Location loc = Location.of(reconstructUri(prefix, obj.getKey()));
                buffer.add(new FileEntry(loc, obj.getSize(), false, obj.getModificationTime(), List.of()));
            }
            if (page.isTruncated()) {
                continuationToken = page.getContinuationToken();
            } else {
                done = true;
            }
        }

        @Override
        public FileEntry next() throws IOException {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            return buffer.get(bufferIdx++);
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }

    /**
     * S3-backed {@link DorisInputFile}.
     *
     * <p>The first call to {@link #length()}, {@link #exists()}, {@link #lastModifiedTime()}
     * or {@link #newStream()} issues a single S3 HEAD; the result (length, last-modified,
     * existence) is cached on this instance so subsequent calls do not re-issue HEAD. The
     * cache is a snapshot view: if the underlying object changes after this {@code
     * S3InputFile} was created, callers will keep observing the values from the first HEAD.
     * Construct a fresh {@code S3InputFile} when an up-to-date view is required.
     */
    private class S3InputFile implements DorisInputFile {
        private final Location location;
        // Cached HEAD response state. {@code metadataLoaded} flips to true on the first
        // (successful or NotFound) HEAD; {@code exists} discriminates the two outcomes.
        private boolean metadataLoaded;
        private boolean exists;
        private long length;
        private long lastModifiedTime;

        S3InputFile(Location location) {
            this.location = location;
        }

        @Override
        public Location location() {
            return location;
        }

        @Override
        public long length() throws IOException {
            ensureMetadata();
            if (!exists) {
                throw new java.io.FileNotFoundException("Object not found: " + location.uri());
            }
            return length;
        }

        @Override
        public boolean exists() throws IOException {
            ensureMetadata();
            return exists;
        }

        @Override
        public long lastModifiedTime() throws IOException {
            ensureMetadata();
            if (!exists) {
                throw new java.io.FileNotFoundException("Object not found: " + location.uri());
            }
            return lastModifiedTime;
        }

        @Override
        public DorisInputStream newStream() throws IOException {
            ensureMetadata();
            if (!exists) {
                throw new java.io.FileNotFoundException("Object not found: " + location.uri());
            }
            return new S3SeekableInputStream(location.uri(), (S3ObjStorage) objStorage, length);
        }

        /**
         * Issues a HEAD against {@link #location} on the first call and caches the result
         * (length, last-modified, existence). NotFound is cached as {@code exists=false}
         * rather than re-thrown, so a subsequent {@link #exists()} call does not retry.
         */
        private void ensureMetadata() throws IOException {
            if (metadataLoaded) {
                return;
            }
            try {
                RemoteObject obj = objStorage.headObject(location.uri());
                length = obj.getSize();
                lastModifiedTime = obj.getModificationTime();
                exists = true;
            } catch (IOException e) {
                if (!isNotFoundError(e)) {
                    throw e;
                }
                exists = false;
            }
            metadataLoaded = true;
        }
    }

    /**
     * Seekable input stream for S3 objects.
     * Uses HTTP Range requests to seek without downloading the entire object.
     *
     * <p>#24: each underlying {@code GetObject} stream is wrapped in a
     * {@link java.io.BufferedInputStream} of {@link #READ_AHEAD_BYTES} bytes so that
     * subsequent small reads after a seek are served from the in-memory buffer instead of
     * issuing one socket read per byte. The read-ahead buffer is discarded on every
     * {@link #seek(long)} (a fresh range request is opened on the next read) and on
     * {@link #close()}, so semantics (EOF, partial reads, exceptions) are unchanged.
     */
    private static class S3SeekableInputStream extends DorisInputStream {
        // Read-ahead buffer size. 64 KiB matches HDFS / S3A defaults and is small enough to
        // amortise socket overhead without holding meaningful memory per open file.
        private static final int READ_AHEAD_BYTES = 64 * 1024;

        private final String remotePath;
        private final S3ObjStorage objStorage;
        private final long fileLength;
        private long position;
        private InputStream current;
        private boolean closed;

        S3SeekableInputStream(String remotePath, S3ObjStorage objStorage, long fileLength) {
            this.remotePath = remotePath;
            this.objStorage = objStorage;
            this.fileLength = fileLength;
        }

        private void checkOpen() throws IOException {
            if (closed) {
                throw new IOException("Stream already closed: " + remotePath);
            }
        }

        /** Opens a range-based GET stream starting at {@link #position}. */
        private void openStream() throws IOException {
            if (current != null) {
                current.close();
                current = null;
            }
            // Wrap with BufferedInputStream so single-byte reads after seek are served from a
            // 64 KiB in-memory window instead of one socket round-trip per byte.
            current = new java.io.BufferedInputStream(
                    objStorage.openInputStreamAt(remotePath, position), READ_AHEAD_BYTES);
        }

        @Override
        public long getPos() throws IOException {
            checkOpen();
            return position;
        }

        @Override
        public void seek(long pos) throws IOException {
            checkOpen();
            if (pos < 0 || pos > fileLength) {
                throw new IOException("Seek position out of range [0, " + fileLength + "]: " + pos);
            }
            if (pos == position) {
                return;
            }
            // Close the current stream; a new range request will be issued on next read.
            if (current != null) {
                current.close();
                current = null;
            }
            position = pos;
        }

        @Override
        public int read() throws IOException {
            checkOpen();
            if (position >= fileLength) {
                return -1;
            }
            ensureOpen();
            int b = current.read();
            if (b >= 0) {
                position++;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            checkOpen();
            if (position >= fileLength) {
                return -1;
            }
            ensureOpen();
            int n = current.read(b, off, len);
            if (n > 0) {
                position += n;
            }
            return n;
        }

        private void ensureOpen() throws IOException {
            if (current == null) {
                openStream();
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            if (current != null) {
                current.close();
                current = null;
            }
        }
    }

    /** S3-backed DorisOutputFile. */
    private class S3OutputFile implements DorisOutputFile {
        private final Location location;

        S3OutputFile(Location location) {
            this.location = location;
        }

        @Override
        public Location location() {
            return location;
        }

        @Override
        public OutputStream create() throws IOException {
            // Per DorisOutputFile contract, create() must fail if the target object
            // already exists. Probe with headObject and translate not-found into a
            // signal that we may proceed with the PUT.
            try {
                objStorage.headObject(location.uri());
            } catch (IOException e) {
                if (isNotFoundError(e)) {
                    return createOrOverwrite();
                }
                throw e;
            }
            throw new FileAlreadyExistsException(location.uri());
        }

        @Override
        public OutputStream createOrOverwrite() throws IOException {
            // Use a buffered in-memory stream; flush triggers PutObject on close
            return new S3OutputStream(location.uri(), (S3ObjStorage) objStorage);
        }
    }

    /**
     * Returns the longest key-prefix of {@code globPattern} that contains no glob metacharacters
     * ({@code * ? [ { \}).  Used as the {@code prefix} parameter for S3 {@code ListObjectsV2}.
     */
    static String longestNonGlobPrefix(String globPattern) {
        int earliest = globPattern.length();
        for (char c : new char[]{'*', '?', '[', '{', '\\'}) {
            int idx = globPattern.indexOf(c);
            if (idx >= 0 && idx < earliest) {
                earliest = idx;
            }
        }
        return globPattern.substring(0, earliest);
    }

    /**
     * Expands {@code {N..M}} numeric range syntax in a glob pattern to the equivalent
     * comma-separated alternation {@code {N,N+1,...,M}} that Java's PathMatcher understands.
     * Supports simple non-negative ranges like {@code {1..3}}, reverse ranges like {@code {3..1}},
     * and mixed comma-separated patterns like {@code {1..2,3,1..3}} or {@code {Refrain,1..3}}.
     * For simple (no-comma) brace groups, only non-negative ranges are expanded;
     * negative-start ranges like {@code {-1..1}} are left unchanged.
     * Duplicate values are removed. For example:
     * <ul>
     *   <li>{@code data_{1..3}.csv} → {@code data_{1,2,3}.csv}</li>
     *   <li>{@code data_{1..2,3,1..3}.csv} → {@code data_{1,2,3}.csv}</li>
     *   <li>{@code data_{Refrain,1..3}.csv} → {@code data_{Refrain,1,2,3}.csv}</li>
     *   <li>{@code data_{-1..1}.csv} → unchanged (no expansion)</li>
     * </ul>
     */
    private static String expandNumericRanges(String pattern) {
        java.util.regex.Pattern rangeSegment = java.util.regex.Pattern.compile(
                "(-?\\d+)\\.\\.(-?\\d+)");
        java.util.regex.Pattern simpleRange = java.util.regex.Pattern.compile(
                "\\{(\\d+)\\.\\.(\\d+)\\}");
        // Match any brace group that contains at least one N..M range
        java.util.regex.Pattern braceGroup = java.util.regex.Pattern.compile(
                "\\{([^}]*\\d+\\.\\.\\d+[^}]*)\\}");
        java.util.regex.Matcher m = braceGroup.matcher(pattern);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String content = m.group(1);
            boolean isMixed = content.contains(",");
            if (!isMixed) {
                // Simple brace group (no comma): only expand non-negative ranges
                java.util.regex.Matcher sm = simpleRange.matcher(m.group(0));
                if (!sm.matches()) {
                    // Not a simple non-negative range (e.g., {-1..1}) — leave unchanged
                    continue;
                }
            }
            String[] segments = content.split(",", -1);
            java.util.LinkedHashSet<String> values = new java.util.LinkedHashSet<>();
            for (String seg : segments) {
                java.util.regex.Matcher rm = rangeSegment.matcher(seg.trim());
                if (rm.matches()) {
                    int from = Integer.parseInt(rm.group(1));
                    int to = Integer.parseInt(rm.group(2));
                    int step = from <= to ? 1 : -1;
                    for (int i = from; step > 0 ? i <= to : i >= to; i += step) {
                        values.add(String.valueOf(i));
                    }
                } else {
                    values.add(seg.trim());
                }
            }
            StringBuilder expansion = new StringBuilder("{");
            expansion.append(String.join(",", values));
            expansion.append('}');
            m.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(expansion.toString()));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    @Override
    public GlobListing globListWithLimit(Location path, String startAfter, long maxBytes,
            long maxFiles) throws IOException {
        // Parse the URI via the same path-style-aware helper as the rest of this class.
        String uri = path.uri();
        S3Uri parsed = parseUri(uri);
        String bucket = parsed.bucket();
        String keyPattern = parsed.key();
        String base = uriBase(uri, parsed);

        String expandedKeyPattern = expandNumericRanges(keyPattern);
        // Translate the glob to a regex and match against the raw S3 key. We do NOT
        // route through java.nio.file.Paths because (a) on Windows the filesystem
        // separator is '\' which would corrupt S3 keys, and (b) Paths.get rejects keys
        // containing characters illegal in the host OS path syntax (':', '\', etc.).
        Pattern matcher = Pattern.compile(globToRegex(expandedKeyPattern));
        String listPrefix = longestNonGlobPrefix(expandedKeyPattern);

        S3ObjStorage s3 = (S3ObjStorage) objStorage;
        ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(listPrefix);
        if (startAfter != null && !startAfter.isEmpty()) {
            reqBuilder.startAfter(startAfter);
        }
        ListObjectsV2Request request = reqBuilder.build();

        List<FileEntry> files = new ArrayList<>();
        long totalSize = 0L;
        boolean reachLimit = false;
        // nextMatchAfterLimit: the first matching key found after the page limit was reached.
        // Empty string means no such key was found yet (scanning still in progress or no more keys).
        String nextMatchAfterLimit = "";
        String lastMatchedKey = "";
        boolean isTruncated;

        try {
            do {
                ListObjectsV2Response response = s3.getClient().listObjectsV2(request);
                for (S3Object obj : response.contents()) {
                    if (reachLimit) {
                        // After hitting limit: find the first matching key so callers know more data exists.
                        if (nextMatchAfterLimit.isEmpty()
                                && matcher.matcher(obj.key()).matches()) {
                            nextMatchAfterLimit = obj.key();
                        }
                        continue;
                    }

                    if (!matcher.matcher(obj.key()).matches()) {
                        continue;
                    }

                    files.add(new FileEntry(
                            Location.of(base + obj.key()),
                            obj.size(),
                            false,
                            obj.lastModified() != null ? obj.lastModified().toEpochMilli() : 0L,
                            null));
                    totalSize += obj.size();
                    lastMatchedKey = obj.key();

                    if ((maxFiles > 0 && files.size() >= maxFiles)
                            || (maxBytes > 0 && totalSize >= maxBytes)) {
                        reachLimit = true;
                    }
                }

                isTruncated = response.isTruncated();
                if (isTruncated) {
                    request = request.toBuilder()
                            .continuationToken(response.nextContinuationToken())
                            .build();
                }
                // Continue paginating after limit until we find the next matching key,
                // so callers can use it as a pagination cursor.
            } while (isTruncated && (!reachLimit || nextMatchAfterLimit.isEmpty()));
        } catch (NoSuchKeyException e) {
            LOG.info("NoSuchKey when listing s3://{}/{}, treating as empty", bucket, listPrefix);
            return new GlobListing(List.of(), bucket, listPrefix, "");
        } catch (Exception e) {
            throw new IOException("Failed to list S3 objects at " + uri + ": " + e.getMessage(), e);
        }

        // maxFile is the next matching key after the returned page (if found), or the last returned key.
        String maxFile = nextMatchAfterLimit.isEmpty() ? lastMatchedKey : nextMatchAfterLimit;
        return new GlobListing(files, bucket, listPrefix, maxFile);
    }

    /**
     * Translates a Java NIO glob pattern to a regular expression that can be matched
     * against raw S3 object keys. Mirrors the JDK's glob → regex conversion but operates
     * on plain strings instead of {@link java.nio.file.Path}, so it tolerates keys with
     * characters that the OS-level FileSystem refuses (e.g. {@code :}, {@code \}, spaces
     * with arbitrary code points). Supports:
     * <ul>
     *   <li>{@code *} → any run of non-{@code /} characters;</li>
     *   <li>{@code **} → any run of characters including {@code /};</li>
     *   <li>{@code ?} → any single non-{@code /} character;</li>
     *   <li>{@code [...]} character class (with {@code !} for negation);</li>
     *   <li>{@code {a,b,c}} alternation;</li>
     *   <li>{@code \X} escapes the next character literally.</li>
     * </ul>
     * All other regex metacharacters are escaped, and {@code /} is always literal so that
     * a single {@code *} does not cross directory levels.
     */
    static String globToRegex(String glob) {
        StringBuilder sb = new StringBuilder("^");
        boolean inClass = false;
        boolean inGroup = false;
        int i = 0;
        while (i < glob.length()) {
            char c = glob.charAt(i);
            if (c == '\\') {
                if (i + 1 < glob.length()) {
                    sb.append(Pattern.quote(String.valueOf(glob.charAt(i + 1))));
                    i += 2;
                } else {
                    sb.append("\\\\");
                    i++;
                }
                continue;
            }
            if (inClass) {
                if (c == ']') {
                    inClass = false;
                    sb.append(']');
                } else if (c == '\\' || c == '[') {
                    sb.append('\\').append(c);
                } else {
                    sb.append(c);
                }
                i++;
                continue;
            }
            switch (c) {
                case '*':
                    if (i + 1 < glob.length() && glob.charAt(i + 1) == '*') {
                        sb.append(".*");
                        i += 2;
                    } else {
                        sb.append("[^/]*");
                        i++;
                    }
                    break;
                case '?':
                    sb.append("[^/]");
                    i++;
                    break;
                case '[':
                    inClass = true;
                    sb.append('[');
                    i++;
                    if (i < glob.length() && glob.charAt(i) == '!') {
                        sb.append('^');
                        i++;
                    }
                    break;
                case '{':
                    inGroup = true;
                    sb.append("(?:");
                    i++;
                    break;
                case '}':
                    inGroup = false;
                    sb.append(')');
                    i++;
                    break;
                case ',':
                    if (inGroup) {
                        sb.append('|');
                    } else {
                        sb.append(',');
                    }
                    i++;
                    break;
                default:
                    if ("\\.^$|+()".indexOf(c) >= 0) {
                        sb.append('\\').append(c);
                    } else {
                        sb.append(c);
                    }
                    i++;
                    break;
            }
        }
        sb.append('$');
        return sb.toString();
    }
}
