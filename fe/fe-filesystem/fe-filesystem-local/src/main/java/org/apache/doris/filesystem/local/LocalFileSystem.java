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
import java.net.URISyntaxException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Local filesystem implementation for unit testing only. Not for production use.
 */
public class LocalFileSystem implements FileSystem {

    private static final LinkOption[] NO_FOLLOW = {LinkOption.NOFOLLOW_LINKS};

    private final Map<String, String> properties;

    public LocalFileSystem(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * Resolves a {@link Location} to a local {@link Path}. Accepts:
     * <ul>
     *   <li>hierarchical {@code file:} URIs (e.g. {@code file:///tmp/x}),</li>
     *   <li>{@code local://} URIs (the body is treated as an absolute path), and</li>
     *   <li>bare absolute paths starting with {@code /} (used by callers that pass raw
     *       filesystem paths through {@link Location#of(String)} — notably the FE Hive
     *       transaction code).</li>
     * </ul>
     * Anything else (e.g. {@code s3://}, {@code hdfs://}, opaque {@code file:foo}, or a
     * relative path) is rejected with {@link IOException} so URIs intended for a different
     * filesystem do not silently fall through to a local-path interpretation.
     */
    private Path toPath(Location location) throws IOException {
        String uri = location.uri();
        if (uri.startsWith("file:")) {
            try {
                URI parsed = new URI(uri);
                if (parsed.isOpaque() || parsed.getPath() == null) {
                    throw new IOException("Unsupported file URI for LocalFileSystem: " + uri);
                }
                return Paths.get(parsed);
            } catch (URISyntaxException | IllegalArgumentException e) {
                throw new IOException("Invalid URI for LocalFileSystem: " + uri, e);
            }
        }
        if (uri.startsWith("local://")) {
            String body = uri.substring("local://".length());
            // Normalize to a single absolute path; strip any leading slashes then prepend one.
            return Paths.get("/" + body.replaceFirst("^/+", ""));
        }
        if (uri.startsWith("/")) {
            return Paths.get(uri);
        }
        throw new IOException("Unsupported URI for LocalFileSystem: " + uri);
    }

    /** Creates parent directories if {@code path} has a parent. No-op for single-segment paths. */
    private static void mkParent(Path path) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
    }

    /**
     * Reads attributes without following symbolic links. Returns {@code null} if the path does
     * not exist; any other {@link IOException} (e.g. permission denied) is propagated.
     */
    private static BasicFileAttributes readAttrsOrNull(Path path) throws IOException {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, NO_FOLLOW);
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    @Override
    public boolean exists(Location location) throws IOException {
        return readAttrsOrNull(toPath(location)) != null;
    }

    @Override
    public void mkdirs(Location location) throws IOException {
        Files.createDirectories(toPath(location));
    }

    @Override
    public void delete(Location location, boolean recursive) throws IOException {
        Path path = toPath(location);
        BasicFileAttributes attrs = readAttrsOrNull(path);
        if (attrs == null) {
            return;
        }
        // Symlinks (incl. symlink-to-dir) are leaf entries here: only the link itself is removed.
        if (recursive && attrs.isDirectory() && !attrs.isSymbolicLink()) {
            deleteRecursive(path);
        } else {
            Files.deleteIfExists(path);
        }
    }

    private void deleteRecursive(Path root) throws IOException {
        // walkFileTree without FOLLOW_LINKS treats symlinks as leaves visited via visitFile.
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) {
                    throw exc;
                }
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @Override
    public void rename(Location src, Location dst) throws IOException {
        Path dstPath = toPath(dst);
        mkParent(dstPath);
        try {
            Files.move(toPath(src), dstPath, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("Atomic rename not supported across filesystems: "
                    + src.uri() + " -> " + dst.uri(), e);
        }
    }

    @Override
    public void renameDirectory(Location src, Location dst, Runnable whenSrcNotExists)
            throws IOException {
        Path dstPath = toPath(dst);
        mkParent(dstPath);
        try {
            Files.move(toPath(src), dstPath, StandardCopyOption.ATOMIC_MOVE);
        } catch (NoSuchFileException e) {
            whenSrcNotExists.run();
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("Atomic rename not supported across filesystems: "
                    + src.uri() + " -> " + dst.uri(), e);
        }
    }

    @Override
    public FileIterator list(Location location) throws IOException {
        Path dirPath = toPath(location);
        String scheme = location.scheme();
        DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath);
        Iterator<Path> raw = stream.iterator();
        return new FileIterator() {
            @Override
            public boolean hasNext() {
                return raw.hasNext();
            }

            @Override
            public FileEntry next() throws IOException {
                if (!raw.hasNext()) {
                    throw new NoSuchElementException();
                }
                Path child = raw.next();
                BasicFileAttributes attrs = Files.readAttributes(child,
                        BasicFileAttributes.class, NO_FOLLOW);
                // Symlinks (incl. symlink-to-dir) are reported as non-directories so default
                // recursive listing does not follow them and create infinite loops.
                boolean isDir = attrs.isDirectory() && !attrs.isSymbolicLink();
                long length = isDir ? 0L : attrs.size();
                long mtime = attrs.lastModifiedTime().toMillis();
                return new FileEntry(rebuildLocation(scheme, child), length, isDir, mtime, null);
            }

            @Override
            public void close() throws IOException {
                stream.close();
            }
        };
    }

    /**
     * Rebuilds a child {@link Location} preserving the caller-supplied scheme so that listings
     * round-trip through {@link LocalFileSystemProvider#supports} regardless of whether the
     * caller used {@code file://} or {@code local://}.
     */
    private static Location rebuildLocation(String parentScheme, Path child) {
        String absolute = child.toAbsolutePath().normalize().toString();
        String uri;
        if ("local".equals(parentScheme)) {
            uri = "local://" + absolute.replaceFirst("^/+", "");
        } else {
            // Use Path.toUri() for proper file:// percent-encoding, then strip the trailing
            // slash that toUri() appends for any path resolving to a directory (incl. symlinks
            // to directories) so callers get consistent URIs and so symlink entries — which we
            // surface as non-directories — round-trip cleanly.
            uri = child.toUri().toString();
            if (uri.endsWith("/")) {
                uri = uri.substring(0, uri.length() - 1);
            }
        }
        return Location.of(uri);
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
                return readAttrsOrNull(path) != null;
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
            if (closed) {
                throw new IOException("Stream is closed");
            }
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
                mkParent(path);
                // CREATE_NEW atomically throws FileAlreadyExistsException (subclass of IOException)
                // if the file already exists, so no separate exists() pre-check is needed.
                return Files.newOutputStream(path,
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE);
            }

            @Override
            public OutputStream createOrOverwrite() throws IOException {
                mkParent(path);
                return Files.newOutputStream(path,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING);
            }
        };
    }

    @Override
    public void close() throws IOException {
        // no-op
    }
}
