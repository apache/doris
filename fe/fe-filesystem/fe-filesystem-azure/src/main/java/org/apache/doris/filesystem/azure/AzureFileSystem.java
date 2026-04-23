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

package org.apache.doris.filesystem.azure;

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
import org.apache.doris.filesystem.spi.UploadPartResult;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Azure Blob Storage-backed {@link org.apache.doris.filesystem.FileSystem} implementation.
 *
 * <p>Does not depend on fe-core, fe-common, or fe-catalog.
 * Azure does not support atomic directory renames; {@link #rename} is limited to single blobs.
 */
public class AzureFileSystem extends ObjFileSystem {

    private static final Logger LOG = LogManager.getLogger(AzureFileSystem.class);

    private static final String DIR_MARKER_SUFFIX = "/";

    public AzureFileSystem(AzureObjStorage objStorage) {
        super("AZURE", objStorage);
    }

    /**
     * Recognises 404-style errors strictly via the typed {@link java.io.FileNotFoundException}.
     *
     * <p>{@link AzureObjStorage} translates every Azure 404 it surfaces (head/open/list)
     * into {@code FileNotFoundException}; non-404 BlobStorageExceptions are wrapped as
     * generic {@link IOException}. Matching only on the typed exception avoids false
     * positives where an unrelated error happens to embed the substring {@code "404"}
     * in its message.
     */
    @Override
    protected boolean isNotFoundError(IOException e) {
        return e instanceof java.io.FileNotFoundException;
    }

    @Override
    public void mkdirs(Location location) throws IOException {
        // Azure is a flat namespace; create a zero-byte directory marker for compatibility.
        String path = location.uri().endsWith(DIR_MARKER_SUFFIX)
                ? location.uri()
                : location.uri() + DIR_MARKER_SUFFIX;
        // Idempotent: if a marker blob already exists at <path>/, don't re-upload it
        // (putObject uses overwrite=true and would truncate any pre-existing blob with
        // exactly the same key). Only proceed when the marker is genuinely missing.
        try {
            objStorage.headObject(path);
            return;
        } catch (IOException e) {
            if (!isNotFoundError(e)) {
                throw e;
            }
        }
        objStorage.putObject(path, RequestBody.of(InputStream.nullInputStream(), 0));
    }

    /**
     * Returns true iff the location resolves to either an exact blob or a virtual
     * directory (any blob exists whose key starts with {@code <uri>/}).  The default
     * {@link ObjFileSystem#exists(Location)} only HEADs the literal key, so it would
     * report false for prefix-style directories that have no marker blob.
     */
    @Override
    public boolean exists(Location location) throws IOException {
        try {
            objStorage.headObject(location.uri());
            return true;
        } catch (IOException e) {
            if (!isNotFoundError(e)) {
                throw e;
            }
        }
        String prefix = location.uri().endsWith(DIR_MARKER_SUFFIX)
                ? location.uri() : location.uri() + DIR_MARKER_SUFFIX;
        return hasChildUnder(prefix);
    }

    /**
     * Deletes a blob or virtual directory.
     *
     * <p>When {@code recursive=true}, paginates the listing under {@code <uri>/} and deletes
     * each entry — including the directory marker blob {@code <uri>/} itself when present.
     * The literal key {@code <uri>} (without trailing slash) is intentionally NOT deleted by
     * the recursive branch: in Azure's flat namespace, deleting a directory must not also
     * remove a sibling blob whose name happens to equal the directory path.
     *
     * <p>When {@code recursive=false}, first checks whether any child blobs exist under
     * {@code <uri>/}; if so, throws {@link IOException} ("Directory not empty"). Otherwise
     * issues a best-effort {@code deleteObject(uri)} on the literal key. A 404 (target does
     * not exist) is swallowed so that deleting a missing target is idempotent — matching
     * the contract honored by {@code S3FileSystem}.
     */
    @Override
    public void delete(Location location, boolean recursive) throws IOException {
        String prefix = location.uri().endsWith(DIR_MARKER_SUFFIX)
                ? location.uri() : location.uri() + DIR_MARKER_SUFFIX;
        if (recursive) {
            deleteRecursive(prefix);
            return;
        }
        if (hasChildUnder(prefix)) {
            throw new IOException("Directory not empty: " + location);
        }
        try {
            objStorage.deleteObject(location.uri());
        } catch (IOException e) {
            if (!isNotFoundError(e)) {
                throw e;
            }
        }
    }

    /**
     * Returns true if {@code prefix} (a URI ending in {@code /}) has at least one
     * blob whose name is strictly longer than the prefix's key portion (i.e. a child).
     * The directory marker blob whose key equals the prefix's key portion is not
     * considered a child.
     */
    private boolean hasChildUnder(String prefix) throws IOException {
        String prefixKey = AzureUri.parse(prefix).key();
        RemoteObjects firstPage = objStorage.listObjects(prefix, null);
        for (RemoteObject obj : firstPage.getObjectList()) {
            if (obj.getKey().length() > prefixKey.length()) {
                return true;
            }
        }
        return false;
    }

    private void deleteRecursive(String prefix) throws IOException {
        String continuationToken = null;
        do {
            RemoteObjects batch = objStorage.listObjects(prefix, continuationToken);
            for (RemoteObject obj : batch.getObjectList()) {
                objStorage.deleteObject(rebuildUri(prefix, obj.getKey()));
            }
            continuationToken = batch.isTruncated() ? batch.getContinuationToken() : null;
        } while (continuationToken != null);
    }

    /**
     * Azure Blob Storage does not support atomic directory renames.
     * Single-blob renames are supported via copy-then-delete.
     *
     * <p>Overwrite policy: this method REFUSES to silently overwrite an existing
     * destination blob. If a blob already exists at {@code dst}, an
     * {@link IOException} is thrown before any copy is attempted. This is stronger
     * than the underlying Azure SDK behaviour (which would clobber {@code dst})
     * and matches the safer "no clobber" contract used by other Doris file systems.
     *
     * @throws IOException if the source appears to be a directory prefix,
     *                     or if the destination already exists
     */
    @Override
    public void rename(Location src, Location dst) throws IOException {
        if (src.uri().endsWith(DIR_MARKER_SUFFIX)) {
            throw new IOException(
                    "Renaming directories is not supported in Azure Blob Storage: " + src);
        }
        // Refuse rename when src is a virtual directory (children exist under <src>/);
        // a copy-then-delete on the literal blob name would silently leave children behind.
        if (hasChildUnder(src.uri() + DIR_MARKER_SUFFIX)) {
            throw new IOException(
                    "Renaming directories is not supported in Azure Blob Storage: " + src);
        }
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
            throw new IOException("Rename destination already exists: " + dst);
        }
        objStorage.copyObject(src.uri(), dst.uri());
        try {
            objStorage.deleteObject(src.uri());
        } catch (IOException e) {
            IOException composed = new IOException(
                    "rename failed during source delete; attempted to remove dst for compensation: "
                            + src + " -> " + dst, e);
            try {
                objStorage.deleteObject(dst.uri());
            } catch (IOException de) {
                composed.addSuppressed(de);
            }
            throw composed;
        }
    }

    /**
     * Azure Blob Storage has no atomic multi-blob rename. Iterating copy+delete across
     * an entire prefix would lose atomicity and leak partially-renamed state on failure,
     * so this implementation refuses the operation when the source directory exists and
     * lets the caller branch.  When the source has neither a marker blob nor any
     * children, the {@code whenSrcNotExists} callback is run and the call returns
     * normally — matching the {@code FileSystem#renameDirectory} contract.
     */
    @Override
    public void renameDirectory(Location src, Location dst, Runnable whenSrcNotExists)
            throws IOException {
        String prefix = src.uri().endsWith(DIR_MARKER_SUFFIX)
                ? src.uri() : src.uri() + DIR_MARKER_SUFFIX;
        if (hasChildUnder(prefix)) {
            throw new UnsupportedOperationException(
                    "renameDirectory is not supported by AzureFileSystem; "
                            + "Azure Blob Storage has no atomic directory rename");
        }
        try {
            objStorage.headObject(prefix);
        } catch (IOException e) {
            if (isNotFoundError(e)) {
                whenSrcNotExists.run();
                return;
            }
            throw e;
        }
        throw new UnsupportedOperationException(
                "renameDirectory is not supported by AzureFileSystem; "
                        + "Azure Blob Storage has no atomic directory rename");
    }

    /**
     * Glob-aware {@code listFiles}: a path containing glob metacharacters ({@code *} or
     * {@code ?}) is dispatched to the appropriate scan rather than being passed verbatim
     * to the prefix-based listing (which would silently return no matches because Azure
     * list-blobs treats {@code *} as a literal character).
     *
     * <ul>
     *   <li>No glob → fall through to the default {@link FileSystem#listFiles}, which
     *       iterates the directory listing produced by {@link #list(Location)}.</li>
     *   <li>Single-segment glob (wildcards confined to the basename) → list the parent
     *       prefix and filter immediate children by the basename glob.</li>
     *   <li>Cross-segment glob (wildcards in any non-last segment) → defer to
     *       {@link #globListWithLimit(Location, String, long, long)}, which performs a
     *       recursive prefix scan with regex filtering.</li>
     * </ul>
     */
    @Override
    public List<FileEntry> listFiles(Location location) throws IOException {
        String uriStr = location.uri();
        if (!containsGlob(uriStr)) {
            List<FileEntry> result = new ArrayList<>();
            try (FileIterator it = list(location)) {
                while (it.hasNext()) {
                    FileEntry e = it.next();
                    if (!e.isDirectory()) {
                        result.add(e);
                    }
                }
            }
            return result;
        }
        if (isSingleLevelGlob(uriStr)) {
            return listFilesSingleLevelGlob(location);
        }
        return globListWithLimit(location, "", 0L, 0L).getFiles();
    }

    /**
     * Single-level glob branch of {@link #listFiles(Location)}: lists the parent prefix
     * and filters the immediate children by the last-segment glob, mirroring HDFS
     * {@code globStatus} semantics for last-segment wildcards.  Directory marker blobs
     * (keys ending in {@code /}) and any keys whose path lies in a sub-directory of the
     * parent are skipped — Azure has no native delimiter listing exposed through the
     * current SPI, so the depth filter is enforced here.
     */
    private List<FileEntry> listFilesSingleLevelGlob(Location location) throws IOException {
        String uri = location.uri();
        int lastSlash = uri.lastIndexOf('/');
        String parentPrefix = uri.substring(0, lastSlash + 1);
        String basenameGlob = uri.substring(lastSlash + 1);
        Pattern matcher = Pattern.compile(globToRegex(basenameGlob));
        String parentKey = AzureUri.parse(parentPrefix).key();

        List<FileEntry> result = new ArrayList<>();
        String continuation = null;
        do {
            RemoteObjects page = objStorage.listObjects(parentPrefix, continuation);
            for (RemoteObject obj : page.getObjectList()) {
                String key = obj.getKey();
                if (key.endsWith(DIR_MARKER_SUFFIX)) {
                    continue;
                }
                if (!key.startsWith(parentKey)) {
                    continue;
                }
                String relative = key.substring(parentKey.length());
                if (relative.indexOf('/') >= 0) {
                    continue;
                }
                if (!matcher.matcher(relative).matches()) {
                    continue;
                }
                result.add(new FileEntry(
                        Location.of(parentPrefix + relative),
                        obj.getSize(), false, obj.getModificationTime(), List.of()));
            }
            continuation = page.isTruncated() ? page.getContinuationToken() : null;
        } while (continuation != null);
        return result;
    }

    @Override
    public FileIterator list(Location location) throws IOException {
        // Azure list-blobs is prefix-based, not directory-based: listing
        // "wasbs://c@a.host/foo" would also return blobs under "foo_bar/"
        // because they share the same string prefix. The FileSystem.list
        // contract specifies a directory, so enforce a trailing '/' to
        // constrain the prefix to a true directory boundary.
        String uri = location.uri();
        if (!uri.endsWith(DIR_MARKER_SUFFIX)) {
            uri = uri + DIR_MARKER_SUFFIX;
        }
        return new AzureFileIterator(uri);
    }

    @Override
    public DorisInputFile newInputFile(Location location) throws IOException {
        return new AzureInputFile(location);
    }

    @Override
    public DorisOutputFile newOutputFile(Location location) throws IOException {
        return new AzureOutputFile(location);
    }

    private static String rebuildUri(String prefix, String key) {
        // prefix is like "wasbs://container@account.blob.core.windows.net/path/",
        // key is the full blob name; reconstruct the full URI for the key.
        int schemeEnd = prefix.indexOf("://");
        if (schemeEnd < 0) {
            return prefix + key;
        }
        String scheme = prefix.substring(0, schemeEnd);
        String withoutScheme = prefix.substring(schemeEnd + 3);
        // For wasb/abfs: "container@account.host/path/"
        int firstSlash = withoutScheme.indexOf('/');
        if (firstSlash < 0) {
            return prefix + key;
        }
        String authority = withoutScheme.substring(0, firstSlash);
        return scheme + "://" + authority + "/" + key;
    }

    @Override
    public GlobListing globListWithLimit(Location path, String startAfter, long maxBytes,
            long maxFiles) throws IOException {
        String uri = path.uri();
        AzureUri parsed = AzureUri.parse(uri);
        String container = parsed.container();
        String keyPattern = parsed.key();
        // base = uri with the key portion stripped, preserving the original scheme/host syntax.
        String base = uri.substring(0, uri.length() - keyPattern.length());

        Pattern matcher = Pattern.compile(globToRegex(keyPattern));
        String listKeyPrefix = longestNonGlobPrefix(keyPattern);
        String listPrefixUri = base + listKeyPrefix;

        List<FileEntry> files = new ArrayList<>();
        long totalSize = 0L;
        String maxFile = "";
        String continuationToken = null;
        boolean reachLimit = false;

        outer:
        do {
            RemoteObjects page = objStorage.listObjects(listPrefixUri, continuationToken);
            for (RemoteObject obj : page.getObjectList()) {
                String key = obj.getKey();
                if (key.endsWith(DIR_MARKER_SUFFIX)) {
                    continue;
                }
                if (startAfter != null && !startAfter.isEmpty() && key.compareTo(startAfter) <= 0) {
                    continue;
                }
                if (!matcher.matcher(key).matches()) {
                    continue;
                }
                if ((maxFiles > 0 && files.size() >= maxFiles)
                        || (maxBytes > 0 && totalSize >= maxBytes)) {
                    maxFile = key;
                    reachLimit = true;
                    break outer;
                }
                files.add(new FileEntry(
                        Location.of(base + key),
                        obj.getSize(),
                        false,
                        obj.getModificationTime(),
                        null));
                totalSize += obj.getSize();
                maxFile = key;
            }
            continuationToken = page.isTruncated() ? page.getContinuationToken() : null;
        } while (continuationToken != null);

        if (!reachLimit && files.isEmpty()) {
            maxFile = "";
        }
        return new GlobListing(files, container, listKeyPrefix, maxFile);
    }

    /**
     * Returns true iff {@code path} contains a glob metacharacter.  Only {@code *} and
     * {@code ?} are recognised — literal {@code [} / {@code {} are valid characters in
     * Azure blob keys and must not be auto-routed through {@link #globListWithLimit}.
     */
    static boolean containsGlob(String path) {
        return path.indexOf('*') >= 0 || path.indexOf('?') >= 0;
    }

    /**
     * Returns true iff {@code pathStr} contains a glob (only {@code *} and {@code ?}
     * are recognised, matching {@link #containsGlob}) confined entirely to the last
     * path segment — i.e. the basename has at least one wildcard and the parent prefix
     * has none.  Cross-segment globs (wildcards in any non-last segment) return false
     * and the recursive {@link #globListWithLimit} branch is used instead.
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

    /**
     * Returns the longest key-prefix of {@code globPattern} that contains no glob
     * metacharacters ({@code * ? [ { \}). Used as the {@code prefix} parameter for the
     * Azure list-blobs call.
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
     * Translates a Java NIO glob pattern to a regex matched against raw blob keys.
     * Mirrors the JDK glob → regex conversion but operates on plain strings, so it
     * tolerates characters that the host OS path syntax would refuse.  Supports:
     * <ul>
     *   <li>{@code *} → any run of non-{@code /} characters;</li>
     *   <li>{@code **} → any run of characters including {@code /};</li>
     *   <li>{@code ?} → any single non-{@code /} character;</li>
     *   <li>{@code [...]} character class (with {@code !} for negation);</li>
     *   <li>{@code {a,b,c}} alternation;</li>
     *   <li>{@code \X} escapes the next character literally.</li>
     * </ul>
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

    /** Lazy-paginating FileIterator over Azure list results. */
    private class AzureFileIterator implements FileIterator {
        private final String prefix;
        private String continuationToken;
        private List<FileEntry> buffer = new ArrayList<>();
        private int bufferIdx = 0;
        private boolean done = false;

        AzureFileIterator(String prefix) {
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

        /**
         * Fetches the next listing page and refills {@link #buffer}.
         *
         * <p>Directory marker blobs (keys ending in {@code /}) are intentionally skipped:
         * the {@code FileSystem.list} contract is "iterate file entries", and surfacing
         * placeholder markers would force every caller to filter them. This matches the
         * skip-markers behaviour of {@link AzureFileSystem#listFilesSingleLevelGlob} and
         * {@link AzureFileSystem#globListWithLimit}.
         */
        private void fetchNextPage() throws IOException {
            RemoteObjects page = objStorage.listObjects(prefix, continuationToken);
            buffer = new ArrayList<>();
            bufferIdx = 0;
            for (RemoteObject obj : page.getObjectList()) {
                if (obj.getKey().endsWith(DIR_MARKER_SUFFIX)) {
                    continue;
                }
                Location loc = Location.of(rebuildUri(prefix, obj.getKey()));
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
                throw new NoSuchElementException("AzureFileIterator exhausted");
            }
            return buffer.get(bufferIdx++);
        }

        @Override
        public void close() throws IOException {
            // No-op: a listing page is fully drained into the in-memory `buffer` list and
            // the Azure SDK iterator is not retained between fetches, so there are no
            // network connections, file handles, or async resources to release here.
        }
    }

    /** Azure-backed DorisInputFile. */
    private class AzureInputFile implements DorisInputFile {
        private final Location location;

        AzureInputFile(Location location) {
            this.location = location;
        }

        @Override
        public Location location() {
            return location;
        }

        @Override
        public long length() throws IOException {
            return objStorage.headObject(location.uri()).getSize();
        }

        @Override
        public boolean exists() throws IOException {
            try {
                objStorage.headObject(location.uri());
                return true;
            } catch (IOException e) {
                if (isNotFoundError(e)) {
                    return false;
                }
                throw e;
            }
        }

        @Override
        public long lastModifiedTime() throws IOException {
            return ((AzureObjStorage) objStorage).headObjectLastModified(location.uri());
        }

        @Override
        public DorisInputStream newStream() throws IOException {
            long fileLength = length();
            return new AzureSeekableInputStream(location.uri(), (AzureObjStorage) objStorage, fileLength);
        }
    }

    /**
     * Seekable input stream for Azure blobs.
     * Uses Azure HTTP Range requests to seek without re-downloading the entire blob.
     */
    private static class AzureSeekableInputStream extends DorisInputStream {
        private final String remotePath;
        private final AzureObjStorage objStorage;
        private final long fileLength;
        private long position;
        private java.io.InputStream current;
        private boolean closed;

        AzureSeekableInputStream(String remotePath, AzureObjStorage objStorage, long fileLength) {
            this.remotePath = remotePath;
            this.objStorage = objStorage;
            this.fileLength = fileLength;
        }

        private void checkOpen() throws IOException {
            if (closed) {
                throw new IOException("Stream already closed: " + remotePath);
            }
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
                current = objStorage.openInputStreamAt(remotePath, position);
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

    /** Azure-backed DorisOutputFile (buffered in memory; uploads on close). */
    private class AzureOutputFile implements DorisOutputFile {
        private final Location location;

        AzureOutputFile(Location location) {
            this.location = location;
        }

        @Override
        public Location location() {
            return location;
        }

        @Override
        public OutputStream create() throws IOException {
            // Distinct from createOrOverwrite(): refuse if the destination already exists.
            // Race with a concurrent writer is still possible (the put happens on stream
            // close), but the head check turns the common case of a stale path collision
            // into a clear IOException instead of a silent overwrite.
            boolean exists;
            try {
                objStorage.headObject(location.uri());
                exists = true;
            } catch (IOException e) {
                if (!isNotFoundError(e)) {
                    throw e;
                }
                exists = false;
            }
            if (exists) {
                throw new IOException("File already exists: " + location);
            }
            return createOrOverwrite();
        }

        @Override
        public OutputStream createOrOverwrite() throws IOException {
            return new AzureOutputStream(location.uri(), (AzureObjStorage) objStorage);
        }
    }

    /**
     * Streaming OutputStream backed by Azure block-blob staged blocks.
     *
     * <p>Buffers up to {@link #PART_SIZE} bytes; when full, flushes the part via
     * {@link AzureObjStorage#uploadPart}. Small payloads (≤ {@code PART_SIZE}) are
     * uploaded as a single {@code putObject} on close to avoid the per-commit cost
     * of an empty multipart commit. {@link #flush()} is intentionally a no-op:
     * staging a block per call would multiply round-trips with no durability gain
     * (uncommitted blocks are not visible until {@link #close()}).
     */
    private static class AzureOutputStream extends OutputStream {
        static final int PART_SIZE = 8 * 1024 * 1024;

        private final String remotePath;
        private final AzureObjStorage storage;
        private final byte[] buffer = new byte[PART_SIZE];
        private final List<UploadPartResult> parts = new ArrayList<>();
        private int bufferPos;
        private long totalBytes;
        private int nextPartNumber = 1;
        private String uploadId;
        private boolean closed;

        AzureOutputStream(String remotePath, AzureObjStorage storage) {
            this.remotePath = remotePath;
            this.storage = storage;
        }

        @Override
        public void write(int b) throws IOException {
            checkNotClosed();
            if (bufferPos >= PART_SIZE) {
                flushPart();
            }
            buffer[bufferPos++] = (byte) b;
            totalBytes++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            checkNotClosed();
            int srcOff = off;
            int remaining = len;
            while (remaining > 0) {
                if (bufferPos >= PART_SIZE) {
                    flushPart();
                }
                int chunk = Math.min(remaining, PART_SIZE - bufferPos);
                System.arraycopy(b, srcOff, buffer, bufferPos, chunk);
                bufferPos += chunk;
                srcOff += chunk;
                remaining -= chunk;
                totalBytes += chunk;
            }
        }

        @Override
        public void flush() {
            // Intentional no-op; see class Javadoc.
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            if (uploadId == null) {
                // Total payload fits in one part (or is empty): single put avoids the
                // commit round-trip required by Azure's staged-block API.
                byte[] data = bufferPos == 0
                        ? new byte[0]
                        : Arrays.copyOfRange(buffer, 0, bufferPos);
                storage.putObject(remotePath,
                        RequestBody.of(new ByteArrayInputStream(data), data.length));
                return;
            }
            try {
                if (bufferPos > 0) {
                    UploadPartResult tail = storage.uploadPart(remotePath, uploadId,
                            nextPartNumber++,
                            RequestBody.of(new ByteArrayInputStream(buffer, 0, bufferPos), bufferPos));
                    parts.add(tail);
                    bufferPos = 0;
                }
                storage.completeMultipartUpload(remotePath, uploadId, parts);
            } catch (IOException e) {
                tryAbort(e);
                throw e;
            }
        }

        private void flushPart() throws IOException {
            if (uploadId == null) {
                uploadId = UUID.randomUUID().toString();
            }
            try {
                UploadPartResult result = storage.uploadPart(remotePath, uploadId,
                        nextPartNumber++,
                        RequestBody.of(new ByteArrayInputStream(buffer, 0, bufferPos), bufferPos));
                parts.add(result);
                bufferPos = 0;
            } catch (IOException e) {
                tryAbort(e);
                throw e;
            }
        }

        // Best-effort abort; only meaningful once at least one part is staged.
        private void tryAbort(IOException primary) {
            if (uploadId == null || parts.isEmpty()) {
                return;
            }
            try {
                storage.abortMultipartUpload(remotePath, uploadId);
            } catch (IOException ae) {
                primary.addSuppressed(ae);
            }
        }

        private void checkNotClosed() throws IOException {
            if (closed) {
                throw new IOException("Stream already closed: " + remotePath);
            }
        }
    }
}
