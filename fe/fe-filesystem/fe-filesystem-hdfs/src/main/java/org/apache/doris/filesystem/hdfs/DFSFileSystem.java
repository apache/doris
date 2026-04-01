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
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.spi.HadoopAuthenticator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
    private volatile org.apache.hadoop.fs.FileSystem hadoopFs;
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
            this.authenticator = new SimpleHadoopAuthenticator();
        }
    }

    private org.apache.hadoop.fs.FileSystem getHadoopFs(Path path) throws IOException {
        if (closed.get()) {
            throw new IOException("DFSFileSystem is closed.");
        }
        if (hadoopFs == null) {
            synchronized (this) {
                if (closed.get()) {
                    throw new IOException("DFSFileSystem is closed.");
                }
                if (hadoopFs == null) {
                    hadoopFs = authenticator.doAs(
                            () -> org.apache.hadoop.fs.FileSystem.get(path.toUri(), conf));
                }
            }
        }
        return hadoopFs;
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
        authenticator.doAs(() -> {
            getHadoopFs(srcPath).rename(srcPath, dstPath);
            return null;
        });
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
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (hadoopFs != null) {
                hadoopFs.close();
            }
        }
    }
}
