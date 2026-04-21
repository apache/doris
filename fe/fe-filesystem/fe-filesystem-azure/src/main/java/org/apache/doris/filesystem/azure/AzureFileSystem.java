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
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.spi.ObjFileSystem;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

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

    @Override
    protected boolean isNotFoundError(IOException e) {
        return e instanceof java.io.FileNotFoundException
                || (e.getMessage() != null && e.getMessage().contains("404"));
    }

    @Override
    public void mkdirs(Location location) throws IOException {
        // Azure is a flat namespace; create a zero-byte directory marker for compatibility.
        String path = location.uri().endsWith(DIR_MARKER_SUFFIX)
                ? location.uri()
                : location.uri() + DIR_MARKER_SUFFIX;
        objStorage.putObject(path, RequestBody.of(InputStream.nullInputStream(), 0));
    }

    @Override
    public void delete(Location location, boolean recursive) throws IOException {
        if (recursive) {
            String prefix = location.uri().endsWith(DIR_MARKER_SUFFIX)
                    ? location.uri() : location.uri() + DIR_MARKER_SUFFIX;
            deleteRecursive(prefix);
        }
        try {
            objStorage.deleteObject(location.uri());
        } catch (IOException e) {
            if (!isNotFoundError(e)) {
                throw e;
            }
        }
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
     * @throws IOException if the source appears to be a directory prefix
     */
    @Override
    public void rename(Location src, Location dst) throws IOException {
        if (src.uri().endsWith(DIR_MARKER_SUFFIX)) {
            throw new IOException(
                    "Renaming directories is not supported in Azure Blob Storage.");
        }
        objStorage.copyObject(src.uri(), dst.uri());
        objStorage.deleteObject(src.uri());
    }

    @Override
    public FileIterator list(Location location) throws IOException {
        return new AzureFileIterator(location.uri());
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

        private void fetchNextPage() throws IOException {
            RemoteObjects page = objStorage.listObjects(prefix, continuationToken);
            buffer = new ArrayList<>();
            bufferIdx = 0;
            for (RemoteObject obj : page.getObjectList()) {
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
            return buffer.get(bufferIdx++);
        }

        @Override
        public void close() throws IOException {
            // no-op
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
            return createOrOverwrite();
        }

        @Override
        public OutputStream createOrOverwrite() throws IOException {
            return new AzureOutputStream(location.uri(), (AzureObjStorage) objStorage);
        }
    }

    /** OutputStream that buffers writes in memory and uploads to Azure Blob on close. */
    private static class AzureOutputStream extends OutputStream {
        private final String remotePath;
        private final AzureObjStorage storage;
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private boolean closed = false;

        AzureOutputStream(String remotePath, AzureObjStorage storage) {
            this.remotePath = remotePath;
            this.storage = storage;
        }

        @Override
        public void write(int b) throws IOException {
            checkNotClosed();
            buffer.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            checkNotClosed();
            buffer.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            byte[] data = buffer.toByteArray();
            storage.putObject(remotePath, RequestBody.of(new ByteArrayInputStream(data), data.length));
        }

        private void checkNotClosed() throws IOException {
            if (closed) {
                throw new IOException("Stream already closed: " + remotePath);
            }
        }
    }
}
