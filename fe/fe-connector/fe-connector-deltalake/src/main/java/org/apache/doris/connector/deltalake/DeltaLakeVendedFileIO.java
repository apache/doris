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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorContext;

import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.SeekableInputStream;
import io.delta.kernel.defaults.engine.hadoopio.HadoopSeekableInputStream;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

/** Hadoop loads S3A providers in the FE class loader. Give its built-in session provider a UC READ grant. */
final class DeltaLakeVendedFileIO extends DeltaLakeFileIO {
    interface GrantLoader {
        Grant load() throws IOException;
    }

    static final class Grant {
        private final String tableId;
        private final Map<String, String> aws;
        private final long refreshAt;

        Grant(String tableId, Map<String, String> aws, long expiry) {
            this.tableId = tableId;
            this.aws = aws;
            long now = System.currentTimeMillis();
            // The open-source static-session vendor may omit expiry. Reauthorize periodically in that case.
            refreshAt = expiry == 0 ? now + 60_000 : expiry - Math.min(60_000, (expiry - now) / 10);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Grant)) {
                return false;
            }
            Grant other = (Grant) o;
            return tableId.equals(other.tableId) && aws.equals(other.aws);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, aws);
        }
    }

    static final class Clients implements AutoCloseable {
        private static final int MAX_CLIENTS_PER_CATALOG = 64;
        private static final int MAX_CLIENTS_PER_GRANT = 8;
        private static final long BORROW_TIMEOUT_MILLIS = 30_000L;
        private final Configuration base;
        private final ConnectorContext context;
        private final GenericKeyedObjectPool<Grant, DeltaLakeFileIO> pool;

        Clients(Configuration storage, ConnectorContext context) {
            // S3A gives bucket-specific keys and credential stores precedence over ordinary properties.
            // A UC grant must be the sole identity, even when this catalog also defines storage credentials.
            base = new Configuration(false);
            base.setClassLoader(storage.getClassLoader());
            base.addResource("core-default.xml");
            for (String key : List.of("fs.s3a.endpoint", "fs.s3a.endpoint.region",
                    "fs.s3a.path.style.access", "fs.s3a.connection.ssl.enabled")) {
                String value = storage.get(key);
                if (value != null) {
                    base.set(key, value);
                }
            }
            this.context = context;
            GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
            config.setJmxEnabled(false);
            // A stream holds its client until close. Bound both concurrent readers of one grant
            // and the aggregate across tables and credential rotations in this catalog.
            config.setMaxTotal(MAX_CLIENTS_PER_CATALOG);
            config.setMaxTotalPerKey(MAX_CLIENTS_PER_GRANT);
            config.setMaxIdlePerKey(2);
            config.setBlockWhenExhausted(true);
            config.setMaxWaitMillis(BORROW_TIMEOUT_MILLIS);
            config.setMinEvictableIdleTimeMillis(60_000);
            config.setTimeBetweenEvictionRunsMillis(60_000);
            config.setNumTestsPerEvictionRun(-1);
            pool = new GenericKeyedObjectPool<>(new BaseKeyedPooledObjectFactory<Grant, DeltaLakeFileIO>() {
                @Override
                public DeltaLakeFileIO create(Grant key) {
                    Configuration conf = new Configuration(base);
                    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                    conf.set("fs.s3a.aws.credentials.provider",
                            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
                    conf.set("fs.s3a.access.key", key.aws.get("AWS_ACCESS_KEY"));
                    conf.set("fs.s3a.secret.key", key.aws.get("AWS_SECRET_KEY"));
                    conf.set("fs.s3a.session.token", key.aws.get("AWS_TOKEN"));
                    return new DeltaLakeFileIO(conf, context);
                }

                @Override
                public PooledObject<DeltaLakeFileIO> wrap(DeltaLakeFileIO io) {
                    return new DefaultPooledObject<>(io);
                }

                @Override
                public void destroyObject(Grant key, PooledObject<DeltaLakeFileIO> item) throws IOException {
                    item.getObject().close();
                }
            }, config);
        }

        DeltaLakeFileIO borrow(Grant grant) throws IOException {
            try {
                return pool.borrowObject(grant);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted opening UC table storage", e);
            } catch (NoSuchElementException e) {
                throw new IOException("UC table storage client pool exhausted (borrow timeout "
                        + pool.getMaxWaitMillis() + " ms)", e);
            } catch (Exception e) {
                throw new IOException("Failed to open UC table storage", e);
            }
        }

        @Override
        public void close() {
            pool.close();
        }
    }

    private final Clients clients;
    private final GrantLoader loader;
    private Grant current;

    DeltaLakeVendedFileIO(Clients clients, Grant initial, GrantLoader loader) {
        super(clients.base, clients.context);
        this.clients = clients;
        this.loader = loader;
        current = initial;
    }

    private synchronized Grant grant() throws IOException {
        if (current == null || System.currentTimeMillis() >= current.refreshAt) {
            current = loader.load();
        }
        return current;
    }

    @FunctionalInterface
    private interface Read<T> {
        T apply(DeltaLakeFileIO io) throws IOException;
    }

    private <T> T read(Read<T> action) throws IOException {
        Grant key = grant();
        DeltaLakeFileIO io = clients.borrow(key);
        try {
            return action.apply(io);
        } finally {
            clients.pool.returnObject(key, io);
        }
    }

    @Override
    public CloseableIterator<FileStatus> listFrom(String path) throws IOException {
        // DeltaLakeFileIO materializes the directory entries before returning the iterator.
        return read(io -> io.listFrom(path));
    }

    @Override
    public FileStatus getFileStatus(String path) throws IOException {
        return read(io -> io.getFileStatus(path));
    }

    @Override
    public String resolvePath(String path) throws IOException {
        return read(io -> io.resolvePath(path));
    }

    @Override
    public InputFile newInputFile(String path, long fileSize) {
        return new InputFile() {
            @Override
            public long length() throws IOException {
                return fileSize >= 0 ? fileSize : getFileStatus(path).getSize();
            }

            @Override
            public String path() {
                return path;
            }

            @Override
            public SeekableInputStream newStream() throws IOException {
                Grant key = grant();
                DeltaLakeFileIO io = clients.borrow(key);
                Path file = new Path(path);
                FSDataInputStream stream;
                try {
                    stream = io.fileSystem(file).open(file);
                } catch (IOException | RuntimeException | Error e) {
                    clients.pool.returnObject(key, io);
                    throw e;
                }
                return new HadoopSeekableInputStream(stream) {
                    private boolean closed;

                    @Override
                    public void close() throws IOException {
                        if (!closed) {
                            closed = true;
                            try {
                                super.close();
                            } finally {
                                clients.pool.returnObject(key, io);
                            }
                        }
                    }
                };
            }
        };
    }
}
