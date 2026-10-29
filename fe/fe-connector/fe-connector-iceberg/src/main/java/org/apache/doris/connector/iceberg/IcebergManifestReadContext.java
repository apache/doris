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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.iceberg.ManifestFile;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

/** Query-wide admission and lifetime for both data and delete manifest reads. */
final class IcebergManifestReadContext {
    private final ConnectorContext context;
    private final Semaphore permits;
    private final BooleanSupplier closed;
    private final Supplier<AutoCloseable> retainOwner;

    IcebergManifestReadContext(ConnectorContext context, Semaphore permits, BooleanSupplier closed,
            Supplier<AutoCloseable> retainOwner) {
        this.context = context;
        this.permits = permits;
        this.closed = closed;
        this.retainOwner = retainOwner;
    }

    <T> T read(ManifestFile manifest, Function<ManifestFile, T> action) throws InterruptedException {
        return read(manifest.path(), () -> action.apply(manifest));
    }

    <T> T read(String description, Supplier<T> action) throws InterruptedException {
        checkOpen();
        // A second scan may be waiting while another scan owns all read permits. Check cancellation
        // periodically, without interrupting an elected cache load that other queries still need.
        while (!permits.tryAcquire(100, TimeUnit.MILLISECONDS)) {
            checkOpen();
        }
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            checkOpen();
            Thread.currentThread().setContextClassLoader(IcebergManifestReadContext.class.getClassLoader());
            // Cancellation of the task Future can precede the end of remote I/O.
            try (AutoCloseable owner = retainOwner.get()) {
                return context == null ? action.get()
                        : context.executeAuthenticated(action::get);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Failed to load Iceberg manifest " + description, e);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
            permits.release();
        }
    }

    private void checkOpen() {
        if (closed.getAsBoolean()) {
            throw new CancellationException("Iceberg manifest source is closed");
        }
    }
}
