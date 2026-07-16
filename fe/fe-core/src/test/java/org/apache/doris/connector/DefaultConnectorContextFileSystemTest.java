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

package org.apache.doris.connector;

import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.fs.SpiSwitchingFileSystem;
import org.apache.doris.kerberos.ExecutionAuthenticator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * HIVEFS-3: pins {@link DefaultConnectorContext#getFileSystem} — the engine-owned, per-catalog FileSystem is
 * built lazily, cached (one instance reused across scans), returns {@code null} when the catalog has no
 * storage, and is closed exactly once on teardown. The connector read/write paths borrow this FS and must
 * not close it, so the engine owning close is load-bearing.
 */
public class DefaultConnectorContextFileSystemTest {

    private static final Supplier<ExecutionAuthenticator> NOOP_AUTH = () -> new ExecutionAuthenticator() {};

    /** Records close() so the test can assert the engine forwards teardown to the cached filesystem. */
    private static final class RecordingFileSystem extends SpiSwitchingFileSystem {
        private int closeCount;

        private RecordingFileSystem() {
            super((FileSystem) null); // test-delegate ctor; no path is ever routed through it
        }

        @Override
        public void close() {
            closeCount++;
        }
    }

    /** Context that intercepts the FS build with a recording fake (no real storage/FS wiring needed). */
    private static final class RecordingContext extends DefaultConnectorContext {
        private final RecordingFileSystem fs = new RecordingFileSystem();
        private int buildCount;

        private RecordingContext(Supplier<Map<StorageProperties.Type, StorageProperties>> storageSupplier) {
            super("c", 1L, NOOP_AUTH, storageSupplier);
        }

        @Override
        FileSystem buildCatalogFileSystem(Map<StorageProperties.Type, StorageProperties> storageProps) {
            buildCount++;
            return fs;
        }
    }

    private static Map<StorageProperties.Type, StorageProperties> nonEmptyStorage() {
        // The value is irrelevant — RecordingContext overrides the actual FS build; only non-emptiness matters,
        // because getFileSystem returns null on an empty storage map.
        return Collections.singletonMap(StorageProperties.Type.HDFS, (StorageProperties) null);
    }

    @Test
    public void returnsNullWhenCatalogHasNoStorage() {
        // 2-arg ctor -> empty storage map -> no engine-managed filesystem (parity with
        // getBackendStorageProperties). MUTATION: building an FS over the empty map -> non-null -> red.
        Assertions.assertNull(new DefaultConnectorContext("c", 1L).getFileSystem(null));
    }

    @Test
    public void lazilyBuildsAndReusesSingleInstance() {
        RecordingContext ctx = new RecordingContext(DefaultConnectorContextFileSystemTest::nonEmptyStorage);
        FileSystem first = ctx.getFileSystem(null);
        FileSystem second = ctx.getFileSystem(null);
        Assertions.assertNotNull(first);
        Assertions.assertSame(first, second, "getFileSystem must cache and reuse one instance");
        Assertions.assertEquals(1, ctx.buildCount, "the catalog filesystem must be built exactly once");
    }

    @Test
    public void closeForwardsToCachedFileSystemAndIsIdempotent() throws Exception {
        RecordingContext ctx = new RecordingContext(DefaultConnectorContextFileSystemTest::nonEmptyStorage);
        ctx.getFileSystem(null); // build + cache
        ctx.close();
        ctx.close(); // idempotent
        Assertions.assertEquals(1, ctx.fs.closeCount, "close must forward to the cached FS exactly once");
        // After teardown the context yields no filesystem and does not rebuild.
        Assertions.assertNull(ctx.getFileSystem(null));
        Assertions.assertEquals(1, ctx.buildCount, "getFileSystem must not rebuild after close");
    }

    @Test
    public void closeWithoutGetFileSystemIsNoop() throws Exception {
        RecordingContext ctx = new RecordingContext(DefaultConnectorContextFileSystemTest::nonEmptyStorage);
        ctx.close(); // never built a filesystem
        Assertions.assertEquals(0, ctx.fs.closeCount, "close must not touch a filesystem that was never built");
    }
}
