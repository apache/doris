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

package org.apache.doris.fs;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.filesystem.FileSystem;

import com.google.common.collect.Maps;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link SpiSwitchingFileSystem}.
 *
 * <p>Covers the H2 fix: {@link SpiSwitchingFileSystem#forPath(String)} must propagate
 * {@link IOException} as {@code IOException}, not wrapped in {@code RuntimeException}.
 */
public class SpiSwitchingFileSystemTest {

    @Mocked
    private FileSystem mockDelegate;

    /**
     * H2: When {@link FileSystemFactory#getFileSystem(StorageProperties)} throws {@link IOException},
     * {@link SpiSwitchingFileSystem#forPath(String)} must rethrow it as {@link IOException},
     * not as {@link RuntimeException}.
     *
     * <p>Before the fix, the lambda inside {@code ConcurrentHashMap.computeIfAbsent()} wrapped the
     * {@link IOException} in a plain {@code RuntimeException}, making the method's
     * {@code throws IOException} declaration misleading — callers could not catch the real cause.
     * The fix uses {@link java.io.UncheckedIOException} as the carrier and unwraps it in an outer catch.
     */
    @Test
    public void testForPathPropagatesIOExceptionNotRuntimeException() {
        // Mock LocationPath.of() so it returns a LocationPath with a non-null StorageProperties,
        // allowing execution to reach FileSystemFactory.getFileSystem().
        // JMockit @Mock does not use the 'static' keyword — it mirrors the target signature without it.
        BrokerProperties bp = BrokerProperties.of("test-broker", Maps.newHashMap());
        new MockUp<LocationPath>() {
            @Mock
            public LocationPath of(String location,
                    Map<StorageProperties.Type, StorageProperties> storagePropertiesMap) {
                return LocationPath.ofDirect(location, "broker", "broker://", bp);
            }
        };

        // Mock FileSystemFactory to throw IOException — this is the scenario under test.
        IOException rootCause = new IOException("simulated connection failure");
        new MockUp<FileSystemFactory>() {
            @Mock
            public FileSystem getFileSystem(StorageProperties storageProperties) throws IOException {
                throw rootCause;
            }
        };

        SpiSwitchingFileSystem spiFs = new SpiSwitchingFileSystem(Collections.emptyMap());
        try {
            spiFs.forPath("broker://host/path");
            Assert.fail("Expected IOException but no exception was thrown");
        } catch (IOException e) {
            // Correct: IOException propagated as-is.
            Assert.assertSame("forPath() must rethrow the original IOException", rootCause, e);
        } catch (RuntimeException e) {
            Assert.fail("forPath() wrapped IOException in RuntimeException: " + e.getClass().getName());
        }
    }

    /**
     * H2: When the test-delegate constructor is used, {@link SpiSwitchingFileSystem#forPath(String)}
     * bypasses the cache and returns the delegate directly.
     */
    @Test
    public void testForPathWithTestDelegateBypasses() throws IOException {
        SpiSwitchingFileSystem spiFs = new SpiSwitchingFileSystem(mockDelegate);
        FileSystem result = spiFs.forPath("s3://bucket/key");
        Assert.assertSame("Test-delegate constructor must return the injected delegate", mockDelegate, result);
    }

    // -----------------------------------------------------------------------
    // M1: close() tests
    // -----------------------------------------------------------------------

    /**
     * M1: close() on an empty cache must not throw.
     */
    @Test
    public void testCloseWithEmptyCache() throws IOException {
        SpiSwitchingFileSystem spiFs = new SpiSwitchingFileSystem(Collections.emptyMap());
        // Must not throw any exception
        spiFs.close();
    }

    /**
     * M1: close() must invoke close() on every FileSystem cached in the map.
     */
    @Test
    public void testCloseWithCachedFileSystems() throws Exception {
        SpiSwitchingFileSystem spiFs = new SpiSwitchingFileSystem(Collections.emptyMap());

        AtomicInteger closeCount = new AtomicInteger(0);
        FileSystem countingFs = new StubFileSystem() {
            @Override
            public void close() throws IOException {
                closeCount.incrementAndGet();
            }
        };

        injectIntoCache(spiFs, BrokerProperties.of("broker1", new HashMap<>()), countingFs);
        injectIntoCache(spiFs, BrokerProperties.of("broker2", new HashMap<>()), countingFs);

        spiFs.close();

        Assert.assertEquals("close() must have been called once per cached FileSystem", 2, closeCount.get());
    }

    /**
     * M1: A second call to close() must be a no-op — cached FileSystems are not closed again.
     */
    @Test
    public void testCloseIsIdempotent() throws Exception {
        SpiSwitchingFileSystem spiFs = new SpiSwitchingFileSystem(Collections.emptyMap());

        AtomicInteger closeCount = new AtomicInteger(0);
        FileSystem countingFs = new StubFileSystem() {
            @Override
            public void close() throws IOException {
                closeCount.incrementAndGet();
            }
        };

        injectIntoCache(spiFs, BrokerProperties.of("broker1", new HashMap<>()), countingFs);

        spiFs.close(); // first close — should close the cached FS
        spiFs.close(); // second close — must be a no-op

        Assert.assertEquals("second close() must not close FileSystems again", 1, closeCount.get());
    }

    /**
     * M1: When multiple cached FileSystems throw IOException on close(), the first exception
     * is propagated and the remaining are attached as suppressed exceptions.
     */
    @Test
    public void testCloseCollectsExceptions() throws Exception {
        SpiSwitchingFileSystem spiFs = new SpiSwitchingFileSystem(Collections.emptyMap());

        IOException ex1 = new IOException("first failure");
        IOException ex2 = new IOException("second failure");

        FileSystem failingFs1 = new StubFileSystem() {
            @Override
            public void close() throws IOException {
                throw ex1;
            }
        };
        FileSystem failingFs2 = new StubFileSystem() {
            @Override
            public void close() throws IOException {
                throw ex2;
            }
        };

        // ConcurrentHashMap insertion order is unpredictable; use distinct keys.
        injectIntoCache(spiFs, BrokerProperties.of("broker1", new HashMap<>()), failingFs1);
        injectIntoCache(spiFs, BrokerProperties.of("broker2", new HashMap<>()), failingFs2);

        try {
            spiFs.close();
            Assert.fail("Expected IOException from close()");
        } catch (IOException thrown) {
            // The thrown exception must be one of the two, and the other must be suppressed.
            boolean firstIsThrown = thrown == ex1;
            boolean secondIsThrown = thrown == ex2;
            Assert.assertTrue("Thrown exception must be one of the two IOExceptions",
                    firstIsThrown || secondIsThrown);
            Assert.assertEquals("Exactly one exception must be suppressed", 1,
                    thrown.getSuppressed().length);
            Throwable suppressed = thrown.getSuppressed()[0];
            if (firstIsThrown) {
                Assert.assertSame("Second exception must be suppressed", ex2, suppressed);
            } else {
                Assert.assertSame("First exception must be suppressed", ex1, suppressed);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Injects a FileSystem directly into the {@code cache} field of a {@link SpiSwitchingFileSystem}
     * using reflection (the cache is not accessible from tests).
     */
    @SuppressWarnings("unchecked")
    private static void injectIntoCache(SpiSwitchingFileSystem spiFs,
            StorageProperties key, FileSystem value) throws Exception {
        Field cacheField = SpiSwitchingFileSystem.class.getDeclaredField("cache");
        cacheField.setAccessible(true);
        Map<StorageProperties, FileSystem> cache =
                (ConcurrentHashMap<StorageProperties, FileSystem>) cacheField.get(spiFs);
        cache.put(key, value);
    }

    /**
     * Minimal no-op stub that satisfies the {@link FileSystem} interface.
     * All methods throw {@link UnsupportedOperationException} except close(), which is a no-op.
     * Tests override the methods they need.
     */
    private abstract static class StubFileSystem implements FileSystem {

        @Override
        public boolean exists(org.apache.doris.filesystem.Location location) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void mkdirs(org.apache.doris.filesystem.Location location) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(org.apache.doris.filesystem.Location location, boolean recursive)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rename(org.apache.doris.filesystem.Location src,
                org.apache.doris.filesystem.Location dst) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.apache.doris.filesystem.FileIterator list(
                org.apache.doris.filesystem.Location location) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.apache.doris.filesystem.DorisInputFile newInputFile(
                org.apache.doris.filesystem.Location location) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public org.apache.doris.filesystem.DorisOutputFile newOutputFile(
                org.apache.doris.filesystem.Location location) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            // no-op by default; tests override as needed
        }
    }
}
