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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class DeltaLakeScanFilesTest {
    @Test
    void closesBothLevelsAtEofAndDoesNotOpenAnUnusedSource() throws Exception {
        AtomicInteger opened = new AtomicInteger();
        DeltaLakeScanFiles.Files unused = new DeltaLakeScanFiles.Files(() -> {
            opened.incrementAndGet();
            return Assertions.fail("unused source opened");
        });
        unused.close();
        Assertions.assertFalse(unused.hasNext());
        Assertions.assertEquals(0, opened.get());
        AtomicInteger rowsClosed = new AtomicInteger();
        AtomicInteger batchesClosed = new AtomicInteger();
        try (DeltaLakeScanFiles.Files files = source(rowsClosed, batchesClosed, new AtomicBoolean())) {
            Assertions.assertTrue(files.hasNext());
            Assertions.assertNotNull(files.next());
            Assertions.assertFalse(files.hasNext());
            Assertions.assertEquals(1, rowsClosed.get());
            Assertions.assertEquals(1, batchesClosed.get());
        }
        Assertions.assertEquals(1, rowsClosed.get());
        Assertions.assertEquals(1, batchesClosed.get());
    }

    @Test
    void closesBothLevelsOnEarlyCloseAndIteratorFailure() throws Exception {
        AtomicInteger rowsClosed = new AtomicInteger();
        AtomicInteger batchesClosed = new AtomicInteger();
        AtomicBoolean fail = new AtomicBoolean();
        try (DeltaLakeScanFiles.Files files = source(rowsClosed, batchesClosed, fail)) {
            Assertions.assertTrue(files.hasNext());
            fail.set(true);
            Assertions.assertThrows(IllegalStateException.class, files::hasNext);
            Assertions.assertEquals(1, rowsClosed.get());
            Assertions.assertEquals(1, batchesClosed.get());
        }
        fail.set(false);
        try (DeltaLakeScanFiles.Files files = source(rowsClosed, batchesClosed, fail)) {
            Assertions.assertTrue(files.hasNext());
        }
        Assertions.assertEquals(2, rowsClosed.get());
        Assertions.assertEquals(2, batchesClosed.get());
    }

    @Test
    void cancellationClosesIteratorsAndPreservesTheInterrupt() throws Exception {
        AtomicInteger rowsClosed = new AtomicInteger();
        AtomicInteger batchesClosed = new AtomicInteger();
        try (DeltaLakeScanFiles.Files files = source(rowsClosed, batchesClosed, new AtomicBoolean())) {
            Assertions.assertTrue(files.hasNext());
            Thread.currentThread().interrupt();
            Assertions.assertThrows(DorisConnectorException.class, files::hasNext);
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
            Assertions.assertEquals(1, rowsClosed.get());
            Assertions.assertEquals(1, batchesClosed.get());
        } finally {
            Thread.interrupted();
        }
    }

    private DeltaLakeScanFiles.Files source(AtomicInteger rowsClosed, AtomicInteger batchesClosed, AtomicBoolean fail) {
        ColumnarBatch data = new ColumnarBatch() {
            @Override
            public StructType getSchema() {
                return new StructType();
            }

            @Override
            public ColumnVector getColumnVector(int ordinal) {
                throw new IndexOutOfBoundsException();
            }

            @Override
            public int getSize() {
                return 1;
            }
        };
        FilteredColumnarBatch batch = new FilteredColumnarBatch(data, Optional.empty()) {
            @Override
            public CloseableIterator<Row> getRows() {
                return tracked(data.getRows(), rowsClosed, fail);
            }
        };
        return new DeltaLakeScanFiles.Files(() -> tracked(List.of(batch).iterator(), batchesClosed,
                new AtomicBoolean()));
    }

    private static <T> CloseableIterator<T> tracked(Iterator<T> delegate, AtomicInteger closed, AtomicBoolean fail) {
        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                if (fail.get()) {
                    throw new IllegalStateException("injected iterator failure");
                }
                return delegate.hasNext();
            }

            @Override
            public T next() {
                return delegate.next();
            }

            @Override
            public void close() throws IOException {
                closed.incrementAndGet();
                if (delegate instanceof CloseableIterator) {
                    ((CloseableIterator<?>) delegate).close();
                }
            }
        };
    }
}
