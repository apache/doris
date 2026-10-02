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

package org.apache.doris.connector.delta;


import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

/** FE-owned Delta transaction state for one append or full-table overwrite. */
final class DeltaInsertHandle {

    private final DeltaKernelWriter writer;
    private final Transaction transaction;
    private final Row transactionState;
    private final AutoCloseable resource;
    private final boolean overwrite;
    private final List<DeltaRemoveFile> overwriteRemoves;
    private final OptionalLong originalRowCount;
    private final AtomicBoolean resourceClosed = new AtomicBoolean();

    DeltaInsertHandle(DeltaKernelWriter writer, Transaction transaction, Row transactionState) {
        this(writer, transaction, transactionState, null);
    }

    DeltaInsertHandle(DeltaKernelWriter writer, Transaction transaction, Row transactionState,
            AutoCloseable resource) {
        this(writer, transaction, transactionState, resource, false, List.of(),
                OptionalLong.empty());
    }

    DeltaInsertHandle(DeltaKernelWriter writer, Transaction transaction, Row transactionState,
            AutoCloseable resource, boolean overwrite,
            List<DeltaRemoveFile> overwriteRemoves, OptionalLong originalRowCount) {
        this.writer = writer;
        this.transaction = transaction;
        this.transactionState = transactionState;
        this.resource = resource;
        this.overwrite = overwrite;
        this.overwriteRemoves = List.copyOf(overwriteRemoves);
        this.originalRowCount = originalRowCount;
    }

    DeltaKernelWriter getWriter() {
        return writer;
    }

    Transaction getTransaction() {
        return transaction;
    }

    Row getTransactionState() {
        return transactionState;
    }

    boolean isOverwrite() {
        return overwrite;
    }

    List<DeltaRemoveFile> getOverwriteRemoves() {
        return overwriteRemoves;
    }

    public OptionalLong getOriginalRowCount() {
        return originalRowCount;
    }

    void closeResource() throws Exception {
        if (resource != null && resourceClosed.compareAndSet(false, true)) {
            resource.close();
        }
    }
}
