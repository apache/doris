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
            getHadoopFs(path).mkdirs(path);
            return null;
        });
    }

    @Override
    public void delete(Location location, boolean recursive) throws IOException {
        Path path = new Path(location.toString());
        authenticator.doAs(() -> {
            getHadoopFs(path).delete(path, recursive);
            return null;
        });
    }

    @Override
    public void rename(Location src, Location dst) throws IOException {
        Path srcPath = new Path(src.toString());
        Path dstPath = new Path(dst.toString());
        boolean success = authenticator.doAs(() ->
                getHadoopFs(srcPath).rename(srcPath, dstPath));
        if (!success) {
            throw new IOException("HDFS rename failed: " + src + " -> " + dst);
        }
    }

    /**
     * Renames a directory atomically on HDFS, creating the parent of {@code dst} if it
     * does not exist — matching the behaviour of the legacy {@code RemoteFileSystem.renameDir}.
     * Without this parent-mkdir the HDFS rename returns {@code false} when the intermediate
     * partition directory (e.g. {@code pt1=wuu/}) does not yet exist, causing silent data loss
     * in the Hive write path.
     */
    @Override
    public void renameDirectory(Location src, Location dst, Runnable whenSrcNotExists)
            throws IOException {
        if (!exists(src)) {
            whenSrcNotExists.run();
            return;
        }
        Path dstPath = new Path(dst.toString());
        Path dstParent = dstPath.getParent();
        org.apache.hadoop.fs.FileSystem hadoopFs = getHadoopFs(dstPath);
        if (dstParent != null && !authenticator.doAs(() -> hadoopFs.exists(dstParent))) {
            authenticator.doAs(() -> {
                hadoopFs.mkdirs(dstParent);
                return null;
            });
        }
        rename(src, dst);
    }

    @Override
    public List<FileEntry> listFiles(Location location) throws IOException {
        String locationStr = location.toString();
        // BrokerLoadPendingTask passes paths with glob characters (e.g. _*).
        // Hadoop's listStatusIterator does not expand globs; use globStatus instead.
        if (containsGlob(locationStr)) {
            Path path = new Path(locationStr);
            FileStatus[] statuses = authenticator.doAs(() -> getHadoopFs(path).globStatus(path));
            if (statuses == null) {
                throw new FileNotFoundException("Path does not exist: " + location);
            }
            List<FileEntry> result = new ArrayList<>();
            for (FileStatus s : statuses) {
                if (!s.isDirectory()) {
                    result.add(new FileEntry(Location.of(s.getPath().toString()),
                            s.getLen(), false, s.getModificationTime(), null));
                }
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
    public DorisOutputFile newOutputFile(Location location) throws IOException {
        Path path = new Path(location.toString());
        return new HdfsOutputFile(path, authenticator, this);
    }

    @Override
    public GlobListing globListWithLimit(Location path, String startAfter, long maxBytes,
            long maxFiles) throws IOException {
        Path hadoopPath = new Path(path.toString());
        FileStatus[] statuses = authenticator.doAs(() -> getHadoopFs(hadoopPath).globStatus(hadoopPath));
        if (statuses == null) {
            throw new FileNotFoundException("Path does not exist: " + path);
        }
        List<FileEntry> files = new ArrayList<>();
        long totalBytes = 0;
        boolean hasStartAfter = startAfter != null && !startAfter.isEmpty();
        for (FileStatus status : statuses) {
            if ((maxFiles > 0 && files.size() >= maxFiles) || (maxBytes > 0 && totalBytes >= maxBytes)) {
                break;
            }
            if (!status.isDirectory()) {
                String filePath = status.getPath().toString();
                if (hasStartAfter && filePath.compareTo(startAfter) <= 0) {
                    continue;
                }
                // Use Path.toString() (not toUri().toString()) to avoid double URL-encoding.
                // Hadoop may return paths with percent-encoded characters (e.g., %3A for colons
                // in Hive timestamp partition values). toUri().toString() would re-encode the %
                // to %25, producing broken paths like pt7=2024-04-09%2012%253A34%253A56.
                files.add(new FileEntry(Location.of(filePath),
                        status.getLen(), false, status.getModificationTime(), null));
                totalBytes += status.getLen();
            }
        }
        String maxFile = files.isEmpty() ? "" : files.get(files.size() - 1).location().uri();
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
