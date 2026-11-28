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

package org.apache.doris.cdcclient.common;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/** Keeps a reader reservation until the consumer has finished using its returned records. */
public final class ReaderBatch<T> implements Iterator<T>, AutoCloseable {
    private volatile Iterator<T> records;
    private final Runnable onClose;

    ReaderBatch(Iterator<T> records, Runnable onClose) {
        this.records = Objects.requireNonNull(records);
        this.onClose = onClose;
    }

    public static <T> ReaderBatch<T> wrap(Iterator<T> records) {
        if (records instanceof ReaderBatch) {
            return (ReaderBatch<T>) records;
        }
        return new ReaderBatch<>(records, () -> {});
    }

    public boolean isClosed() {
        return records == null;
    }

    @Override
    public boolean hasNext() {
        return records != null && records.hasNext();
    }

    @Override
    public T next() {
        if (records == null) {
            throw new NoSuchElementException("Reader batch is closed");
        }
        return records.next();
    }

    @Override
    public synchronized void close() {
        if (records != null) {
            records = null;
            onClose.run();
        }
    }
}
