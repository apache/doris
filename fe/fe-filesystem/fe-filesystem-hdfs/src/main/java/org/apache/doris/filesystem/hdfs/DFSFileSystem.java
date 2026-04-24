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

package org.apache.doris.filesystem.hdfs;

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.spi.HadoopAuthenticator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SPI FileSystem implementation for HDFS, ViewFS, JFS, OFS, and OSSHdfs.
 * Has zero dependency on fe-core; accepts {@code Map<String, String>} configuration.
 */
public class DFSFileSystem implements org.apache.doris.filesystem.FileSystem {

    private static final Logger LOG = LogManager.getLogger(DFSFileSystem.class);

    private final Map<String, String> properties;
    private final Configuration conf;
    private final HadoopAuthenticator authenticator;
    // Keyed by URI authority (host:port or "" for default-FS paths) so that a
    // single DFSFileSystem instance can serve paths on multiple HDFS authorities
    // without the "Wrong FS" error that occurred when a singleton cached the FS
    // for the first authority and then reused it for a different authority.
    private final ConcurrentHashMap<String, org.apache.hadoop.fs.FileSystem> fsByAuthority =
            new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public DFSFileSystem(Map<String, String> properties) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
        this.conf = HdfsConfigBuilder.build(properties);
        if (HdfsConfigBuilder.isKerberosEnabled(properties)) {
            this.authenticator = new KerberosHadoopAuthenticator(
                    properties.get(HdfsConfigBuilder.KEY_PRINCIPAL),
                    properties.get(HdfsConfigBuilder.KEY_KEYTAB),
                    conf);
        } else {
            this.authenticator = new SimpleHadoopAuthenticator(
                    properties.get("hadoop.username"));
        }
    }

    private org.apache.hadoop.fs.FileSystem getHadoopFs(Path path) throws IOException {
        if (closed.get()) {
            throw new IOException("DFSFileSystem is closed.");
        }
        String authority = path.toUri().getAuthority();
        String key = authority != null ? authority : "";
        org.apache.hadoop.fs.FileSystem fs = fsByAuthority.get(key);
        if (fs != null) {
            return fs;
        }
        synchronized (this) {
            if (closed.get()) {
                throw new IOException("DFSFileSystem is closed.");
            }
            fs = fsByAuthority.get(key);
            if (fs == null) {
                // Switch the thread context classloader to the plugin classloader before
                // calling FileSystem.get(). Hadoop's ServiceLoader.load(FileSystem.class)
                // uses the context classloader to discover FileSystem implementations. If
                // left as the FE parent classloader, hive-exec.jar (on the parent classpath)
                // injects NullScanFileSystem which is not a subtype of the plugin's
                // FileSystem class, causing a ServiceConfigurationError that prevents HDFS
                // from being registered and leads to "No FileSystem for scheme 'hdfs'".
                // For other schemes (jfs, ofs, oss, etc.) the parent FE classloader must
                // remain accessible so third-party drivers can be found.
                ClassLoader pluginCL = DFSFileSystem.class.getClassLoader();
                Thread currentThread = Thread.currentThread();
                ClassLoader previousCCL = currentThread.getContextClassLoader();
                String scheme = path.toUri().getScheme();
                boolean needPluginCL = "hdfs".equalsIgnoreCase(scheme)
                        || "viewfs".equalsIgnoreCase(scheme);
                currentThread.setContextClassLoader(needPluginCL ? pluginCL : previousCCL);
                try {
                    fs = authenticator.doAs(
                            () -> org.apache.hadoop.fs.FileSystem.get(path.toUri(), conf));
                    fsByAuthority.put(key, fs);
                } finally {
                    currentThread.setContextClassLoader(previousCCL);
                }
            }
        }
        return fs;
    }

    /** Package-private accessor for HdfsInputFile and HdfsOutputFile. */
    org.apache.hadoop.fs.FileSystem requireFs(Path path) throws IOException {
        return getHadoopFs(path);
    }

    @Override
    public boolean exists(Location location) throws IOException {
        Path path = new Path(location.toString());
        return authenticator.doAs(() -> getHadoopFs(path).exists(path));
    }

    @Override
    public void mkdirs(Location location) throws IOException {
        Path path = new Path(location.toString());
        authenticator.doAs(() -> {
            org.apache.hadoop.fs.FileSystem hfs = getHadoopFs(path);
            if (!hfs.mkdirs(path)) {
                // Hadoop returns false (without throwing) when the path already exists
                // as a regular file, or when some other non-exceptional failure occurs.
                // Re-stat the path: if it's already a directory treat as idempotent
                // success; otherwise surface the failure as IOException.
                try {
                    FileStatus st = hfs.getFileStatus(path);
                    if (!st.isDirectory()) {
                        throw new IOException(
                                "HDFS mkdirs failed: path exists and is not a directory: " + path);
                    }
                } catch (FileNotFoundException e) {
                    throw new IOException("HDFS mkdirs returned false for " + path, e);
                }
            }
            return null;
        });
    }

    @Override
    public void delete(Location location, boolean recursive) throws IOException {
        Path path = new Path(location.toString());
        authenticator.doAs(() -> {
            org.apache.hadoop.fs.FileSystem hfs = getHadoopFs(path);
            boolean ok = hfs.delete(path, recursive);
            if (!ok && hfs.exists(path)) {
                // Preserve idempotent "target already missing" behaviour (common Hadoop
                // convention) but surface real failures such as immutable snapshots or
                // viewfs read-only mounts.
                throw new IOException("HDFS delete returned false but path still exists: " + path);
            }
            return null;
        });
    }

    @Override
    public void rename(Location src, Location dst) throws IOException {
        Path srcPath = new Path(src.toString());
        Path dstPath = new Path(dst.toString());
        authenticator.doAs(() -> {
            org.apache.hadoop.fs.FileSystem hfs = getHadoopFs(srcPath);
            boolean ok = hfs.rename(srcPath, dstPath);
            if (!ok) {
                boolean srcExists = hfs.exists(srcPath);
                boolean dstExists = hfs.exists(dstPath);
                throw new IOException("HDFS rename failed: " + src + " -> " + dst
                        + " (srcExists=" + srcExists + ", dstExists=" + dstExists + ")");
            }
            return null;
        });
    }

    /**
     * Renames a directory on HDFS, creating any missing parent of {@code dst} first.
     *
     * <p>This operation is <em>not atomic</em>; it performs a parent-mkdirs followed by a
     * rename. Without the parent-mkdirs the HDFS rename returns {@code false} when the
     * intermediate partition directory (e.g. {@code pt1=wuu/}) does not yet exist, causing
     * silent data loss in the Hive write path.
     *
     * <p>Both source and destination must live on the same HDFS authority; cross-authority
     * renames fail fast to avoid Hadoop's confusing {@code Wrong FS} mid-sequence partial
     * state. When {@code src} does not exist, {@code whenSrcNotExists} is invoked and the
     * method returns without further action.
     */
    @Override
    public void renameDirectory(Location src, Location dst, Runnable whenSrcNotExists)
            throws IOException {
        if (!exists(src)) {
            whenSrcNotExists.run();
            return;
        }
        Path srcPath = new Path(src.toString());
        Path dstPath = new Path(dst.toString());
        String srcAuth = srcPath.toUri().getAuthority();
        String dstAuth = dstPath.toUri().getAuthority();
        if (!Objects.equals(srcAuth, dstAuth)) {
            throw new IOException("renameDirectory across authorities is not supported: src="
                    + src + " dst=" + dst);
        }
        Path dstParent = dstPath.getParent();
        org.apache.hadoop.fs.FileSystem hadoopFs = getHadoopFs(srcPath);
        authenticator.doAs(() -> {
            if (dstParent != null && !hadoopFs.mkdirs(dstParent)) {
                // mkdirs is idempotent; a false return means either the path exists
                // already or a real failure. Re-stat to distinguish.
                try {
                    FileStatus st = hadoopFs.getFileStatus(dstParent);
                    if (!st.isDirectory()) {
                        throw new IOException("renameDirectory parent exists but is not a "
                                + "directory: " + dstParent);
                    }
                } catch (FileNotFoundException e) {
                    throw new IOException("renameDirectory parent mkdirs returned false for "
                            + dstParent, e);
                }
            }
            boolean ok = hadoopFs.rename(srcPath, dstPath);
            if (!ok) {
                boolean srcExists = hadoopFs.exists(srcPath);
                boolean dstExists = hadoopFs.exists(dstPath);
                throw new IOException("HDFS renameDirectory failed: " + src + " -> " + dst
                        + " (srcExists=" + srcExists + ", dstExists=" + dstExists + ")");
            }
            return null;
        });
    }

    /**
     * Lists non-directory entries directly under {@code location}. When {@code location}
     * contains glob meta-characters, glob-matched <em>directory</em> entries are filtered
     * out (consistent with the contract's "non-directory entries" wording); callers that
     * need to discover partition directories matched by a glob must use
     * {@link #listFilesRecursive(Location)} or {@link #list(Location)} instead. As a
     * consequence, a glob that matches only directories yields an empty list.
     */
    @Override
    public List<FileEntry> listFiles(Location location) throws IOException {
        String locationStr = location.toString();
        // BrokerLoadPendingTask passes paths with glob characters (e.g. _*).
        // Hadoop's listStatusIterator does not expand globs; use globStatus instead.
        if (containsGlob(locationStr)) {
            Path path = new Path(locationStr);
            org.apache.hadoop.fs.FileSystem hfs = getHadoopFs(path);
            FileStatus[] statuses = authenticator.doAs(() -> hfs.globStatus(path));
            if (statuses == null) {
                throw new FileNotFoundException("Path does not exist: " + location);
            }
            List<FileEntry> result = new ArrayList<>();
            for (FileStatus s : statuses) {
                if (s.isDirectory()) {
                    continue;
                }
                result.add(new FileEntry(Location.of(s.getPath().toString()),
                        s.getLen(), false, s.getModificationTime(), null));
            }
            return result;
        }
        return org.apache.doris.filesystem.FileSystem.super.listFiles(location);
    }

    private static boolean containsGlob(String path) {
        return path.contains("*") || path.contains("?") || path.contains("[");
    }

    @Override
    public FileIterator list(Location location) throws IOException {
        Path path = new Path(location.toString());
        org.apache.hadoop.fs.RemoteIterator<FileStatus> it =
                authenticator.doAs(() -> getHadoopFs(path).listStatusIterator(path));
        return new HdfsFileIterator(it, authenticator);
    }

    @Override
    public DorisInputFile newInputFile(Location location) throws IOException {
        Path path = new Path(location.toString());
        return new HdfsInputFile(path, authenticator, this);
    }

    @Override
    public DorisInputFile newInputFile(Location location, long length) throws IOException {
        Path path = new Path(location.toString());
        return new HdfsInputFile(path, authenticator, this, length);
    }

    @Override
    public DorisOutputFile newOutputFile(Location location) throws IOException {
        Path path = new Path(location.toString());
        return new HdfsOutputFile(path, authenticator, this);
    }

    @Override
    public GlobListing globListWithLimit(Location path, String startAfter, long maxBytes,
            long maxFiles) throws IOException {
        Path hadoopPath = new Path(path.toString());
        org.apache.hadoop.fs.FileSystem hfs = getHadoopFs(hadoopPath);
        boolean isGlob = containsGlob(path.toString());
        FileStatus[] statuses = authenticator.doAs(() ->
                isGlob ? hfs.globStatus(hadoopPath) : hfs.listStatus(hadoopPath));
        if (statuses == null) {
            throw new FileNotFoundException("Path does not exist: " + path);
        }
        List<FileEntry> files = new ArrayList<>();
        long totalBytes = 0;
        boolean hasStartAfter = startAfter != null && !startAfter.isEmpty();
        // maxFile is a pagination cursor, per FileSystem#globListWithLimit contract:
        //   - if a page-limit (maxFiles / maxBytes) was hit AND another matching key
        //     exists strictly past the page, maxFile is that next matching key;
        //   - otherwise maxFile is the last matching key on the returned page, or ""
        //     when nothing matched at all.
        String maxFile = "";
        boolean limitHit = false;
        for (FileStatus status : statuses) {
            if (status.isDirectory()) {
                continue;
            }
            String filePath = status.getPath().toString();
            // Apply startAfter filter BEFORE the page-limit check, so skipped entries
            // do not consume the page budget.
            if (hasStartAfter && filePath.compareTo(startAfter) <= 0) {
                continue;
            }
            if (!limitHit && ((maxFiles > 0 && files.size() >= maxFiles)
                    || (maxBytes > 0 && totalBytes >= maxBytes))) {
                limitHit = true;
            }
            if (limitHit) {
                // Page limit already reached: this entry is the next matching key past
                // the page — record as the pagination cursor and stop scanning.
                maxFile = Location.of(filePath).uri();
                break;
            }
            // Use Path.toString() (not toUri().toString()) to avoid double URL-encoding.
            // Hadoop may return paths with percent-encoded characters (e.g., %3A for colons
            // in Hive timestamp partition values). toUri().toString() would re-encode the %
            // to %25, producing broken paths like pt7=2024-04-09%2012%253A34%253A56.
            FileEntry entry = new FileEntry(Location.of(filePath),
                    status.getLen(), false, status.getModificationTime(), null);
            files.add(entry);
            totalBytes += status.getLen();
            maxFile = entry.location().uri();
        }
        return new GlobListing(files, "", path.toString(), maxFile);
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            for (org.apache.hadoop.fs.FileSystem fs : fsByAuthority.values()) {
                fs.close();
            }
            fsByAuthority.clear();
        }
    }
}
