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

package org.apache.doris.datasource.iceberg.io;

import org.apache.doris.qe.BDPAuthContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

public class AuthHadoopFileIO implements FileIO, HadoopConfigurable, SupportsPrefixOperations {
    private static final Logger LOG = LoggerFactory.getLogger(AuthHadoopFileIO.class);

    private SerializableSupplier<Configuration> hadoopConf;

    /**
     * Constructor used for dynamic FileIO loading.
     *
     * <p>{@link Configuration Hadoop configuration} must be set through {@link
     * org.apache.iceberg.hadoop.HadoopFileIO#setConf(Configuration)}
     */
    public AuthHadoopFileIO() {}

    public AuthHadoopFileIO(Configuration hadoopConf) {
        this(new SerializableConfiguration(hadoopConf)::get);
    }

    public AuthHadoopFileIO(SerializableSupplier<Configuration> hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    public Configuration conf() {
        return hadoopConf.get();
    }

    private FileSystem getAuthFileSystem(Path path) {
        Preconditions.checkNotNull(BDPAuthContext.get(), "bdp auth info cannot be null");
        BDPAuthContext bdpAuthContext = BDPAuthContext.get();
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(bdpAuthContext.getHadoopUserName(),
                null, bdpAuthContext.getUserToken());
        FileSystem fs = ugi.doAs((PrivilegedAction<FileSystem>) () -> {
            try {
                Configuration conf = new Configuration();
                conf.set("BEE_USER", bdpAuthContext.getErp());
                conf.set("BEE_SOURCE", bdpAuthContext.getSource());
                conf.set("BEE_COMPUTE", "Doris");
                return path.getFileSystem(conf);
            } catch (IOException ioException) {
                LOG.warn("access hdfs failed", ioException);
            }
            return null;
        });
        return fs;
    }

    @Override
    public InputFile newInputFile(String path) {
        Path hadoopPath = new Path(path);
        FileSystem fs = getAuthFileSystem(hadoopPath);
        return HadoopInputFile.fromPath(hadoopPath, fs);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        Path hadoopPath = new Path(path);
        FileSystem fs = getAuthFileSystem(hadoopPath);
        return HadoopInputFile.fromPath(hadoopPath, length, fs);
    }

    @Override
    public OutputFile newOutputFile(String path) {
        Path hadoopPath = new Path(path);
        FileSystem fs = getAuthFileSystem(hadoopPath);
        return HadoopOutputFile.fromPath(hadoopPath, fs);
    }

    @Override
    public void deleteFile(String path) {
        Path toDelete = new Path(path);
        FileSystem fs = Util.getFs(toDelete, hadoopConf.get());
        try {
            fs.delete(toDelete, false /* not recursive */);
        } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to delete file: %s", path);
        }
    }

    @Override
    public Map<String, String> properties() {
        return ImmutableMap.of();
    }

    @Override
    public void setConf(Configuration conf) {
        this.hadoopConf = new SerializableConfiguration(conf)::get;
    }

    @Override
    public Configuration getConf() {
        return hadoopConf.get();
    }

    @Override
    public void serializeConfWith(Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
        this.hadoopConf = confSerializer.apply(getConf());
    }

    @Override
    public Iterable<FileInfo> listPrefix(String prefix) {
        Path prefixToList = new Path(prefix);
        FileSystem fs = getAuthFileSystem(prefixToList);
        return () -> {
            try {
                return Streams.stream(
                        new AdaptingIterator<>(fs.listFiles(prefixToList, true /* recursive */)))
                    .map(
                        fileStatus ->
                            new FileInfo(
                                fileStatus.getPath().toString(),
                                fileStatus.getLen(),
                                fileStatus.getModificationTime()))
                    .iterator();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    @Override
    public void deletePrefix(String prefix) {
        Path prefixToDelete = new Path(prefix);
        FileSystem fs = getAuthFileSystem(prefixToDelete);
        try {
            fs.delete(prefixToDelete, true /* recursive */);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * This class is a simple adaptor to allow for using Hadoop's RemoteIterator as an Iterator.
     *
     * @param <E> element type
     */
    private static class AdaptingIterator<E> implements Iterator<E>, RemoteIterator<E> {
        private final RemoteIterator<E> delegate;

        AdaptingIterator(RemoteIterator<E> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            try {
                return delegate.hasNext();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public E next() {
            try {
                return delegate.next();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
