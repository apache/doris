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

import org.apache.doris.filesystem.spi.DorisInputFile;
import org.apache.doris.filesystem.spi.DorisInputStream;
import org.apache.doris.filesystem.spi.DorisOutputFile;
import org.apache.doris.filesystem.spi.FileEntry;
import org.apache.doris.filesystem.spi.FileIterator;
import org.apache.doris.filesystem.spi.Location;
import org.apache.doris.filesystem.spi.ObjFileSystem;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;


 * S3-backed FileSystem implementation for the Doris FE filesystem SPI.
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
        return e instanceof java.io.FileNotFoundException
                || (e.getMessage() != null && e.getMessage().contains("404"));
    }

    @Override
    public void mkdirs(Location location) throws IOException {
        // S3 is flat; create a zero-byte directory marker for compatibility
        String path = location.uri().endsWith(DIR_MARKER_SUFFIX)
                ? location.uri()
                : location.uri() + DIR_MARKER_SUFFIX;
        objStorage.putObject(path,
                RequestBody.of(InputStream.nullInputStream(), 0));
    }

    @Override
    public void delete(Location location, boolean recursive) throws IOException {
        if (recursive) {
            // Delete all objects under this prefix
            String prefix = location.uri().endsWith(DIR_MARKER_SUFFIX)
                    ? location.uri() : location.uri() + DIR_MARKER_SUFFIX;
            deleteRecursive(prefix);
        }
        // Always attempt to delete the exact object too
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
                objStorage.deleteObject(reconstructUri(prefix, obj.getKey()));
            }
            continuationToken = batch.isTruncated() ? batch.getContinuationToken() : null;
        } while (continuationToken != null);
    }

    private static String reconstructUri(String prefix, String key) {
        // prefix is like "s3://bucket/path/", key is the full object key
        int schemeEnd = prefix.indexOf("://");
        if (schemeEnd >= 0) {
            String scheme = prefix.substring(0, schemeEnd);
            int slashAfterScheme = prefix.indexOf('/', schemeEnd + 3);
            if (slashAfterScheme >= 0) {
                String bucket = prefix.substring(schemeEnd + 3, slashAfterScheme);
                return scheme + "://" + bucket + "/" + key;
            }
        }
        return prefix + key;
    }

    @Override
    public void rename(Location src, Location dst) throws IOException {
        objStorage.copyObject(src.uri(), dst.uri());
        objStorage.deleteObject(src.uri());
    }

    @Override
    public FileIterator list(Location location) throws IOException {
        return new S3FileIterator(location.uri());
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
                Location loc = Location.of(reconstructUri(prefix, obj.getKey()));
                buffer.add(new FileEntry(loc, obj.getSize(), false, List.of()));
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

    /** S3-backed DorisInputFile. */
    private class S3InputFile implements DorisInputFile {
        private final Location location;

        S3InputFile(Location location) {
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
            return ((S3ObjStorage) objStorage).headObjectLastModified(location.uri());
        }

        @Override
        public DorisInputStream newStream() throws IOException {
            long fileLength = length();
            return new S3SeekableInputStream(location.uri(), (S3ObjStorage) objStorage, fileLength);
        }
    }

    /**
     * Seekable input stream for S3 objects.
     * Uses HTTP Range requests to seek without downloading the entire object.
     */
    private static class S3SeekableInputStream extends DorisInputStream {
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
            current = objStorage.openInputStreamAt(remotePath, position);
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
            return createOrOverwrite();
        }

        @Override
        public OutputStream createOrOverwrite() throws IOException {
            // Use a buffered in-memory stream; flush triggers PutObject on close
            return new S3OutputStream(location.uri(), (S3ObjStorage) objStorage);
        }
    }
}
