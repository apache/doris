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

package org.apache.doris.datasource.iceberg.action;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestListFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.LoggingMetricsReporter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/** A borrowed, local table view for the SDK's shared partition-statistics worker pool. */
final class IcebergPartitionStatsTable extends BaseTable {
    private final Table delegate;
    private final ExecutionAuthenticator authenticator;
    private final FileIO authenticatedIo;
    private final Map<Long, Snapshot> snapshotsById;

    IcebergPartitionStatsTable(Table table, ExecutionAuthenticator authenticator) {
        super(((HasTableOperations) table).operations(), table.name(),
                table instanceof BaseTable ? ((BaseTable) table).reporter() : LoggingMetricsReporter.instance());
        this.delegate = table;
        this.authenticator = Objects.requireNonNull(authenticator, "authenticator is null");
        this.authenticatedIo = new AuthenticatedFileIO(table.io());
        // Materialize lazy snapshot metadata under authentication once. The SDK looks up a
        // snapshot for every manifest entry, so workers must only read this immutable index.
        this.snapshotsById = unchecked(() -> {
            Map<Long, Snapshot> snapshots = new HashMap<>();
            for (Snapshot snapshot : table.snapshots()) {
                snapshots.put(snapshot.snapshotId(), snapshot);
            }
            return Collections.unmodifiableMap(snapshots);
        });
    }

    @Override
    public FileIO io() {
        return authenticatedIo;
    }

    @Override
    public Map<Integer, PartitionSpec> specs() {
        return unchecked(delegate::specs);
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return snapshotsById.get(snapshotId);
    }

    private <T> T callIo(Callable<T> task) throws IOException {
        try {
            return authenticator.execute(task);
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during authenticated Iceberg statistics I/O", e);
        } catch (Exception e) {
            throw new IOException("Authenticated Iceberg statistics I/O failed", e);
        }
    }

    private <T> T unchecked(Callable<T> task) {
        try {
            return callIo(task);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void runIo(IoRunnable task) throws IOException {
        callIo(() -> {
            task.run();
            return null;
        });
    }

    private void closeStream(IoRunnable close) throws IOException {
        AtomicBoolean entered = new AtomicBoolean();
        try {
            runIo(() -> {
                entered.set(true);
                close.run();
            });
        } catch (IOException | RuntimeException failure) {
            if (!entered.get()) {
                // Authentication failure must not prevent releasing an already-owned stream.
                try {
                    close.run();
                } catch (IOException | RuntimeException cleanupFailure) {
                    if (cleanupFailure != failure) {
                        failure.addSuppressed(cleanupFailure);
                    }
                }
            }
            throw failure;
        }
    }

    @FunctionalInterface
    private interface IoRunnable {
        void run() throws IOException;
    }

    private final class AuthenticatedFileIO implements FileIO {
        private final FileIO fileIo;

        private AuthenticatedFileIO(FileIO fileIo) {
            this.fileIo = Objects.requireNonNull(fileIo, "fileIo is null");
        }

        @Override
        public InputFile newInputFile(String path) {
            return wrapInputFile(unchecked(() -> fileIo.newInputFile(path)));
        }

        @Override
        public InputFile newInputFile(String path, long length) {
            return wrapInputFile(unchecked(() -> fileIo.newInputFile(path, length)));
        }

        @Override
        public InputFile newInputFile(DataFile file) {
            return wrapInputFile(unchecked(() -> fileIo.newInputFile(file)));
        }

        @Override
        public InputFile newInputFile(DeleteFile file) {
            return wrapInputFile(unchecked(() -> fileIo.newInputFile(file)));
        }

        @Override
        public InputFile newInputFile(ManifestFile file) {
            return wrapInputFile(unchecked(() -> fileIo.newInputFile(file)));
        }

        @Override
        public InputFile newInputFile(ManifestListFile file) {
            return wrapInputFile(unchecked(() -> fileIo.newInputFile(file)));
        }

        @Override
        public OutputFile newOutputFile(String path) {
            return wrapOutputFile(unchecked(() -> fileIo.newOutputFile(path)));
        }

        @Override
        public void deleteFile(String path) {
            unchecked(() -> {
                fileIo.deleteFile(path);
                return null;
            });
        }

        @Override
        public Map<String, String> properties() {
            return unchecked(fileIo::properties);
        }

        // This view borrows the catalog's initialized FileIO; it must not close or reinitialize it.
    }

    private InputFile wrapInputFile(InputFile file) {
        return new InputFile() {
            @Override
            public long getLength() {
                return unchecked(file::getLength);
            }

            @Override
            public SeekableInputStream newStream() {
                return wrapInputStream(unchecked(file::newStream));
            }

            @Override
            public String location() {
                return unchecked(file::location);
            }

            @Override
            public boolean exists() {
                return unchecked(file::exists);
            }
        };
    }

    private SeekableInputStream wrapInputStream(SeekableInputStream stream) {
        return new SeekableInputStream() {
            @Override
            public long getPos() throws IOException {
                return callIo(stream::getPos);
            }

            @Override
            public void seek(long position) throws IOException {
                runIo(() -> stream.seek(position));
            }

            @Override
            public int read() throws IOException {
                return callIo(stream::read);
            }

            @Override
            public int read(byte[] bytes, int offset, int length) throws IOException {
                return callIo(() -> stream.read(bytes, offset, length));
            }

            @Override
            public long skip(long length) throws IOException {
                return callIo(() -> stream.skip(length));
            }

            @Override
            public int available() throws IOException {
                return callIo(stream::available);
            }

            @Override
            public void close() throws IOException {
                closeStream(stream::close);
            }
        };
    }

    private OutputFile wrapOutputFile(OutputFile file) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create() {
                return wrapOutputStream(unchecked(file::create));
            }

            @Override
            public PositionOutputStream createOrOverwrite() {
                return wrapOutputStream(unchecked(file::createOrOverwrite));
            }

            @Override
            public String location() {
                return unchecked(file::location);
            }

            @Override
            public InputFile toInputFile() {
                return wrapInputFile(unchecked(file::toInputFile));
            }
        };
    }

    private PositionOutputStream wrapOutputStream(PositionOutputStream stream) {
        return new PositionOutputStream() {
            @Override
            public long getPos() throws IOException {
                return callIo(stream::getPos);
            }

            @Override
            public long storedLength() throws IOException {
                return callIo(stream::storedLength);
            }

            @Override
            public void write(int value) throws IOException {
                runIo(() -> stream.write(value));
            }

            @Override
            public void write(byte[] bytes, int offset, int length) throws IOException {
                runIo(() -> stream.write(bytes, offset, length));
            }

            @Override
            public void flush() throws IOException {
                runIo(stream::flush);
            }

            @Override
            public void close() throws IOException {
                closeStream(stream::close);
            }
        };
    }
}
