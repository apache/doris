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

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.function.Consumer;

/** Consumers must copy values they retain: Kernel rows belong to the current batch. */
final class DeltaLakeScanFiles {
    private DeltaLakeScanFiles() {
    }

    static void forEach(Snapshot snapshot, Engine engine, boolean includeStats, Consumer<Row> consumer)
            throws IOException {
        try (CloseableIterator<FilteredColumnarBatch> batches =
                snapshot.getScanBuilder().build().getScanFiles(engine, includeStats)) {
            while (batches.hasNext()) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedIOException("Delta file planning interrupted");
                }
                try (CloseableIterator<Row> files = batches.next().getRows()) {
                    while (files.hasNext()) {
                        consumer.accept(files.next());
                    }
                }
            }
        }
    }
}
