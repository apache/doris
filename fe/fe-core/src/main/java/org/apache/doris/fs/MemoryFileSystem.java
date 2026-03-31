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

import org.apache.doris.fs.io.DorisInput;
import org.apache.doris.fs.io.DorisInputFile;
import org.apache.doris.fs.io.DorisInputStream;
import org.apache.doris.fs.io.DorisOutputFile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In-memory {@link FileSystem} implementation for unit testing.
 * <p>
 * File data is stored in a {@link ConcurrentHashMap}. Directories are implicit
 * (any Location whose path ends with "/" is treated as a directory).
 * Thread-safe for concurrent read/write operations.
 */
public class MemoryFileSystem implements FileSystem {

    // Maps location string → file bytes (null entry = directory marker)
    private final ConcurrentHashMap<String, byte[]> store = new ConcurrentHashMap<>();

    @Override
    public DorisInputFile newInputFile(Location location) {
        return newInputFile(location, -1L);
    }

    @Override
    public DorisInputFile newInputFile(Location location, long length) {
        return new MemoryInputFile(location, length);
    }

    @Override
    public DorisOutputFile newOutputFile(Location location) {
        return new MemoryOutputFile(location);
    }

    @Override
    public boolean exists(Location location) {
        String key = location.toString();
        if (store.containsKey(key)) {
            return true;
        }
        String prefix = key.endsWith("/") ? key : key + "/";
        return store.keySet().stream().anyMatch(k -> k.startsWith(prefix));
    }

    @Override
    public void deleteFile(Location location) throws IOException {
        String key = location.toString();
        if (store.remove(key) == null) {
            throw new IOException("File not found: " + location);
        }
    }

    @Override
    public void renameFile(Location source, Location target) throws IOException {
        byte[] data = store.remove(source.toString());
        if (data == null) {
            throw new IOException("Source not found: " + source);
        }
        store.put(target.toString(), data);
    }

    @Override
    public void deleteDirectory(Location location) throws IOException {
        String prefix = location.toString();
        String withSlash = prefix.endsWith("/") ? prefix : prefix + "/";
        store.keySet().removeIf(k -> k.equals(prefix) || k.startsWith(withSlash));
    }

    @Override
    public void createDirectory(Location location) {
        store.putIfAbsent(location.toString() + "/", new byte[0]);
    }

    @Override
    public void renameDirectory(Location source, Location target) throws IOException {
        String srcPrefix = source.toString();
        String dstPrefix = target.toString();
        Map<String, byte[]> toMove = store.entrySet().stream()
                .filter(e -> e.getKey().startsWith(srcPrefix))
                .collect(Collectors.toMap(
                        e -> dstPrefix + e.getKey().substring(srcPrefix.length()),
                        Map.Entry::getValue));
        if (toMove.isEmpty()) {
            throw new IOException("Source directory not found: " + source);
        }
        store.keySet().removeIf(k -> k.startsWith(srcPrefix));
        store.putAll(toMove);
    }

    @Override
    public FileIterator listFiles(Location location, boolean recursive) {
        String prefix = location.toString();
        String withSlash = prefix.endsWith("/") ? prefix : prefix + "/";
        List<FileEntry> entries = store.keySet().stream()
                .filter(k -> k.startsWith(withSlash) && !k.equals(withSlash))
                .filter(k -> {
                    if (recursive) {
                        return true;
                    }
                    String relative = k.substring(withSlash.length());
                    return !relative.contains("/") || relative.endsWith("/");
                })
                .map(k -> {
                    Location loc = Location.of(k);
                    byte[] data = store.get(k);
                    return FileEntry.builder(loc)
                            .directory(k.endsWith("/"))
                            .length(data == null ? 0 : data.length)
                            .build();
                })
                .collect(Collectors.toList());
        return FileIterator.ofList(entries);
    }

    @Override
    public Set<Location> listDirectories(Location location) {
        String prefix = location.toString();
        String withSlash = prefix.endsWith("/") ? prefix : prefix + "/";
        Set<Location> dirs = new HashSet<>();
        for (String key : store.keySet()) {
            if (!key.startsWith(withSlash)) {
                continue;
            }
            String relative = key.substring(withSlash.length());
            int slash = relative.indexOf('/');
            if (slash >= 0) {
                dirs.add(Location.of(withSlash + relative.substring(0, slash + 1)));
            }
        }
        return Collections.unmodifiableSet(dirs);
    }

    @Override
    public void close() {
        // No-op: in-memory, nothing to release
    }

    /** Writes data directly into this filesystem (test helper). */
    public void put(Location location, byte[] data) {
        store.put(location.toString(), data);
    }

    /** Reads raw bytes from this filesystem (test helper). */
    public byte[] get(Location location) {
        return store.get(location.toString());
    }

    private class MemoryInputFile implements DorisInputFile {
        private final Location location;
        private final long knownLength;

        MemoryInputFile(Location location, long knownLength) {
            this.location = location;
            this.knownLength = knownLength;
        }

        @Override
        public Location location() {
            return location;
        }

        @Override
        public long length() throws IOException {
            if (knownLength >= 0) {
                return knownLength;
            }
            byte[] data = store.get(location.toString());
            if (data == null) {
                throw new IOException("File not found: " + location);
            }
            return data.length;
        }

        @Override
        public long lastModifiedTime() {
            return 0L;
        }

        @Override
        public boolean exists() {
            return store.containsKey(location.toString());
        }

        @Override
        public DorisInput newInput() throws IOException {
            throw new UnsupportedOperationException("Use newStream() for MemoryFileSystem");
        }

        @Override
        public DorisInputStream newStream() throws IOException {
            byte[] data = store.get(location.toString());
            if (data == null) {
                throw new IOException("File not found: " + location);
            }
            return new MemorySeekableInputStream(data);
        }
    }

    /** In-memory seekable stream over a fixed byte array. For testing only. */
    private static class MemorySeekableInputStream extends DorisInputStream {
        private final byte[] data;
        private int position;
        private boolean closed;

        MemorySeekableInputStream(byte[] data) {
            this.data = data;
        }

        @Override
        public long getPosition() throws IOException {
            return position;
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos < 0 || pos > data.length) {
                throw new IOException("Seek out of range [0, " + data.length + "]: " + pos);
            }
            position = (int) pos;
        }

        @Override
        public int read() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }
            if (position >= data.length) {
                return -1;
            }
            return data[position++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }
            if (position >= data.length) {
                return -1;
            }
            int n = Math.min(len, data.length - position);
            System.arraycopy(data, position, b, off, n);
            position += n;
            return n;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private class MemoryOutputFile implements DorisOutputFile {
        private final Location location;

        MemoryOutputFile(Location location) {
            this.location = location;
        }

        @Override
        public Location location() {
            return location;
        }

        @Override
        public OutputStream create() throws IOException {
            if (store.containsKey(location.toString())) {
                throw new IOException("File already exists: " + location);
            }
            return buildStream();
        }

        @Override
        public OutputStream createOrOverwrite() {
            return buildStream();
        }

        private OutputStream buildStream() {
            return new ByteArrayOutputStream() {
                @Override
                public void close() throws IOException {
                    super.close();
                    store.put(location.toString(), toByteArray());
                }
            };
        }
    }
}
