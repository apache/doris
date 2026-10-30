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

import org.apache.doris.connector.spi.DorisConnectorException;

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.utils.CloseableIterator;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Consumer;

/** Consumers must copy values they retain: Kernel rows belong to the current batch. */
final class DeltaLakeScanFiles {
    private DeltaLakeScanFiles() {
    }

    static void forEach(Snapshot snapshot, Engine engine, boolean includeStats, Consumer<Row> consumer)
            throws IOException {
        forEach(snapshot, engine, includeStats, Optional.empty(), consumer);
    }

    static void forEach(Snapshot snapshot, Engine engine, boolean includeStats, Optional<Predicate> filter,
            Consumer<Row> consumer) throws IOException {
        try (CloseableIterator<Row> files = open(snapshot, engine, includeStats, filter)) {
            while (files.hasNext()) {
                consumer.accept(files.next());
            }
        }
    }

    static CloseableIterator<Row> open(Snapshot snapshot, Engine engine, boolean includeStats,
            Optional<Predicate> filter) {
        return new Files(() -> {
            ScanBuilder builder = snapshot.getScanBuilder();
            filter.ifPresent(builder::withFilter);
            return builder.build().getScanFiles(engine, includeStats);
        });
    }

    interface Opener {
        CloseableIterator<FilteredColumnarBatch> open() throws IOException;
    }

    static final class Files implements CloseableIterator<Row> {
        private final Opener opener;
        private CloseableIterator<FilteredColumnarBatch> batches;
        private CloseableIterator<Row> rows;
        private boolean closed;

        Files(Opener opener) {
            this.opener = opener;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            try {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedIOException("Delta file planning interrupted");
                }
                if (batches == null) {
                    batches = opener.open();
                }
                while (rows == null || !rows.hasNext()) {
                    closeRows();
                    if (!batches.hasNext()) {
                        close();
                        return false;
                    }
                    rows = batches.next().getRows();
                }
                return true;
            } catch (IOException e) {
                closeAfterFailure(e);
                throw new DorisConnectorException("Failed to iterate Delta scan files", e);
            } catch (RuntimeException | Error e) {
                closeAfterFailure(e);
                throw e;
            }
        }

        @Override
        public Row next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                return rows.next();
            } catch (RuntimeException | Error e) {
                closeAfterFailure(e);
                throw e;
            }
        }

        private void closeRows() throws IOException {
            CloseableIterator<Row> previous = rows;
            rows = null;
            if (previous != null) {
                previous.close();
            }
        }

        private void closeAfterFailure(Throwable error) {
            try {
                close();
            } catch (IOException | RuntimeException e) {
                error.addSuppressed(e);
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            try (CloseableIterator<FilteredColumnarBatch> owned = batches) {
                closeRows();
            }
        }
    }
}
