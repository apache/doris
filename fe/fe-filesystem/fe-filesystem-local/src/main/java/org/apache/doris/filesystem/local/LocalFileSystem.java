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

package org.apache.doris.filesystem.local;

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisInputStream;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Local filesystem implementation for unit testing only. Not for production use.
 */
public class LocalFileSystem implements FileSystem {

    private final Map<String, String> properties;

    public LocalFileSystem(Map<String, String> properties) {
        this.properties = properties;
    }

    private Path toPath(Location location) {
        String uri = location.toString();
        if (uri.startsWith("file:")) {
            return Paths.get(URI.create(uri));
        }
        if (uri.startsWith("local://")) {
            return Paths.get(uri.substring("local://".length()));
        }
        return Paths.get(uri);
    }

    @Override
    public boolean exists(Location location) throws IOException {
        return Files.exists(toPath(location));
    }

    @Override
    public void mkdirs(Location location) throws IOException {
        Files.createDirectories(toPath(location));
    }

    @Override
    public void delete(Location location, boolean recursive) throws IOException {
        Path path = toPath(location);
        if (recursive && Files.isDirectory(path)) {
            deleteRecursive(path);
        } else {
            Files.deleteIfExists(path);
        }
    }

    private void deleteRecursive(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                for (Path child : stream) {
                    deleteRecursive(child);
                }
            }
        }
        Files.deleteIfExists(path);
    }

    @Override
    public void rename(Location src, Location dst) throws IOException {
        Path dstPath = toPath(dst);
        Files.createDirectories(dstPath.getParent());
        Files.move(toPath(src), dstPath);
    }

    @Override
    public FileIterator list(Location location) throws IOException {
        Path dirPath = toPath(location);
        List<FileEntry> entries = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath)) {
            for (Path child : stream) {
                boolean isDir = Files.isDirectory(child);
                long length = isDir ? 0L : Files.size(child);
                // Java Path.toUri() appends a trailing slash for directories; strip it so callers
                // can rely on consistent URIs (e.g. glob pattern matching, locationName parsing).
                String uri = child.toUri().toString();
                if (isDir && uri.endsWith("/")) {
                    uri = uri.substring(0, uri.length() - 1);
                }
                entries.add(new FileEntry(Location.of(uri), length, isDir,
                        Files.getLastModifiedTime(child).toMillis(), null));
            }
        }
        Iterator<FileEntry> it = entries.iterator();
        return new FileIterator() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public FileEntry next() {
                return it.next();
            }

            @Override
            public void close() {
                // no-op
            }
        };
    }

    @Override
    public DorisInputFile newInputFile(Location location) throws IOException {
        Path path = toPath(location);
        return new DorisInputFile() {
            @Override
            public Location location() {
                return location;
            }

            @Override
            public long length() throws IOException {
                return Files.size(path);
            }

            @Override
            public boolean exists() throws IOException {
                return Files.exists(path);
            }

            @Override
            public long lastModifiedTime() throws IOException {
                return Files.getLastModifiedTime(path).toMillis();
            }

            @Override
            public DorisInputStream newStream() throws IOException {
                return new LocalSeekableInputStream(path);
            }
        };
    }

    /** Seekable stream backed by a local {@link RandomAccessFile}. For testing only. */
    private static class LocalSeekableInputStream extends DorisInputStream {
        private final RandomAccessFile raf;
        private boolean closed;

        LocalSeekableInputStream(Path path) throws IOException {
            this.raf = new RandomAccessFile(path.toFile(), "r");
        }

        @Override
        public long getPos() throws IOException {
            return raf.getFilePointer();
        }

        @Override
        public void seek(long pos) throws IOException {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            raf.seek(pos);
        }

        @Override
        public int read() throws IOException {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            return raf.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (closed) {
                throw new IOException("Stream is closed");
            }
            return raf.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                raf.close();
            }
        }
    }

    @Override
    public DorisOutputFile newOutputFile(Location location) throws IOException {
        Path path = toPath(location);
        return new DorisOutputFile() {
            @Override
            public Location location() {
                return location;
            }

            @Override
            public OutputStream create() throws IOException {
                if (Files.exists(path)) {
                    throw new IOException("File already exists: " + path);
                }
                Files.createDirectories(path.getParent());
                return Files.newOutputStream(path);
            }

            @Override
            public OutputStream createOrOverwrite() throws IOException {
                Files.createDirectories(path.getParent());
                return Files.newOutputStream(path,
                        java.nio.file.StandardOpenOption.CREATE,
                        java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);
            }
        };
    }

    @Override
    public void close() throws IOException {
        // no-op
    }
}
