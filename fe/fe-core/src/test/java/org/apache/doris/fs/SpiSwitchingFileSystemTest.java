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
import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisInputStream;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link SpiSwitchingFileSystem}.
 *
 * <p>Covers the H2 fix: {@link SpiSwitchingFileSystem#forPath(String)} must propagate
 * {@link IOException} as {@code IOException}, not wrapped in {@code RuntimeException}.
 */
public class SpiSwitchingFileSystemTest {

    private final FileSystem mockDelegate = Mockito.mock(FileSystem.class);

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
        BrokerProperties bp = BrokerProperties.of("test-broker", Maps.newHashMap());
        IOException rootCause = new IOException("simulated connection failure");

        try (MockedStatic<LocationPath> mockedLocationPath = Mockito.mockStatic(LocationPath.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<FileSystemFactory> mockedFactory =
                        Mockito.mockStatic(FileSystemFactory.class)) {
            // Mock LocationPath.of() so it returns a LocationPath with a non-null StorageProperties,
            // allowing execution to reach FileSystemFactory.getFileSystem().
            mockedLocationPath.when(() -> LocationPath.of(Mockito.anyString(), Mockito.anyMap()))
                    .thenAnswer(inv -> {
                        String location = inv.getArgument(0);
                        return LocationPath.ofDirect(location, "broker", "broker://", bp);
                    });

            // Mock FileSystemFactory to throw IOException — this is the scenario under test.
            mockedFactory.when(() -> FileSystemFactory.getFileSystem(
                            Mockito.any(StorageProperties.class)))
                    .thenThrow(rootCause);

            SpiSwitchingFileSystem spiFs = new SpiSwitchingFileSystem(Collections.emptyMap());
            try {
                spiFs.forPath("broker://host/path");
                Assert.fail("Expected IOException but no exception was thrown");
            } catch (IOException e) {
                // Correct: IOException propagated as-is.
                Assert.assertSame("forPath() must rethrow the original IOException", rootCause, e);
            } catch (RuntimeException e) {
                Assert.fail("forPath() wrapped IOException in RuntimeException: "
                        + e.getClass().getName());
            }
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

        // Use distinct origProps so the two BrokerProperties have different
        // equals()/hashCode() — ConnectionProperties.equals() is based on origProps.
        Map<String, String> props1 = new HashMap<>();
        props1.put("broker.name", "broker1");
        Map<String, String> props2 = new HashMap<>();
        props2.put("broker.name", "broker2");
        injectIntoCache(spiFs, BrokerProperties.of("broker1", props1), countingFs);
        injectIntoCache(spiFs, BrokerProperties.of("broker2", props2), countingFs);

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

        // Use distinct origProps so the two BrokerProperties have different
        // equals()/hashCode() — ConnectionProperties.equals() is based on origProps.
        Map<String, String> props1 = new HashMap<>();
        props1.put("broker.name", "broker1");
        Map<String, String> props2 = new HashMap<>();
        props2.put("broker.name", "broker2");
        injectIntoCache(spiFs, BrokerProperties.of("broker1", props1), failingFs1);
        injectIntoCache(spiFs, BrokerProperties.of("broker2", props2), failingFs2);

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
    // Legacy cross-scheme fallback translation
    // -----------------------------------------------------------------------

    /**
     * When the legacy fallback routes a {@code cos://} path to an S3-typed storage
     * (LocationPath.findStorageProperties step 3), the delegate filesystem must receive
     * the storage's native {@code s3://} scheme (its URI parser whitelists only its own
     * schemes), while every path returned to the caller must keep the original
     * {@code cos://} scheme so comparisons against HMS/Iceberg metadata still hold.
     */
    @Test
    public void testCompatFallbackTranslatesUrisBothWays() throws Exception {
        StorageProperties s3Props = Mockito.mock(StorageProperties.class);
        Mockito.when(s3Props.getType()).thenReturn(StorageProperties.Type.S3);
        CapturingFileSystem fake = new CapturingFileSystem();

        try (MockedStatic<LocationPath> mockedLocationPath =
                Mockito.mockStatic(LocationPath.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<FileSystemFactory> mockedFactory =
                        Mockito.mockStatic(FileSystemFactory.class)) {
            // Simulate the fallback: cos:// path served by an S3-typed storage whose
            // validateAndNormalizeUri rewrote the scheme prefix to s3://.
            mockedLocationPath.when(() -> LocationPath.of(Mockito.anyString(), Mockito.anyMap()))
                    .thenAnswer(inv -> {
                        String location = inv.getArgument(0);
                        String normalized = "s3://" + location.substring("cos://".length());
                        return LocationPath.ofDirect(normalized, "cos", "s3://bucket", s3Props);
                    });
            mockedFactory.when(() -> FileSystemFactory.getFileSystem(
                            Mockito.any(StorageProperties.class)))
                    .thenReturn(fake);

            SpiSwitchingFileSystem spiFs = new SpiSwitchingFileSystem(Collections.emptyMap());

            // Inbound: single-location operations are translated to the native scheme.
            spiFs.exists(Location.of("cos://bucket/dir/file1"));
            Assert.assertEquals("s3://bucket/dir/file1", fake.lastUri);

            // Inbound: rename translates both endpoints.
            spiFs.rename(Location.of("cos://bucket/dir/a"), Location.of("cos://bucket/dir/b"));
            Assert.assertEquals("s3://bucket/dir/a", fake.lastRenameSrc);
            Assert.assertEquals("s3://bucket/dir/b", fake.lastRenameDst);

            // Outbound: listing results are translated back to the caller's scheme.
            fake.entries = List.of(
                    new FileEntry(Location.of("s3://bucket/dir/f1"), 1, false, 0, null),
                    new FileEntry(Location.of("s3://bucket/dir/sub/"), 0, true, 0, null));
            List<FileEntry> files = spiFs.listFiles(Location.of("cos://bucket/dir"));
            Assert.assertEquals("s3://bucket/dir", fake.lastUri);
            Assert.assertEquals(1, files.size());
            Assert.assertEquals("cos://bucket/dir/f1", files.get(0).location().uri());

            Set<String> dirs = spiFs.listDirectories(Location.of("cos://bucket/dir"));
            Assert.assertEquals(Collections.singleton("cos://bucket/dir/sub/"), dirs);

            // Outbound: newInputFile reports the caller's location, while the delegate
            // was opened with the native scheme.
            DorisInputFile inputFile = spiFs.newInputFile(Location.of("cos://bucket/dir/f1"));
            Assert.assertEquals("s3://bucket/dir/f1", fake.lastUri);
            Assert.assertEquals("cos://bucket/dir/f1", inputFile.location().uri());
        }
    }

    /**
     * A direct type match (cos:// path on a COS-typed storage) must NOT be translated,
     * even though normalization rewrites the scheme — the delegate natively accepts the
     * caller's scheme and must keep seeing it (today's behavior, byte for byte).
     */
    @Test
    public void testDirectMatchSkipsTranslation() throws Exception {
        StorageProperties cosProps = Mockito.mock(StorageProperties.class);
        Mockito.when(cosProps.getType()).thenReturn(StorageProperties.Type.COS);
        CapturingFileSystem fake = new CapturingFileSystem();

        try (MockedStatic<LocationPath> mockedLocationPath =
                Mockito.mockStatic(LocationPath.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<FileSystemFactory> mockedFactory =
                        Mockito.mockStatic(FileSystemFactory.class)) {
            mockedLocationPath.when(() -> LocationPath.of(Mockito.anyString(), Mockito.anyMap()))
                    .thenAnswer(inv -> {
                        String location = inv.getArgument(0);
                        String normalized = "s3://" + location.substring("cos://".length());
                        return LocationPath.ofDirect(normalized, "cos", "s3://bucket", cosProps);
                    });
            mockedFactory.when(() -> FileSystemFactory.getFileSystem(
                            Mockito.any(StorageProperties.class)))
                    .thenReturn(fake);

            SpiSwitchingFileSystem spiFs = new SpiSwitchingFileSystem(Collections.emptyMap());
            spiFs.exists(Location.of("cos://bucket/dir/file1"));
            Assert.assertEquals("cos://bucket/dir/file1", fake.lastUri);
        }
    }

    /**
     * A cross-type fallback whose normalization does not change the scheme (e.g. an
     * s3:// path served by a MinIO-typed storage) must not be translated either.
     */
    @Test
    public void testSameSchemeFallbackSkipsTranslation() throws Exception {
        StorageProperties minioProps = Mockito.mock(StorageProperties.class);
        Mockito.when(minioProps.getType()).thenReturn(StorageProperties.Type.MINIO);
        CapturingFileSystem fake = new CapturingFileSystem();

        try (MockedStatic<LocationPath> mockedLocationPath =
                Mockito.mockStatic(LocationPath.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<FileSystemFactory> mockedFactory =
                        Mockito.mockStatic(FileSystemFactory.class)) {
            mockedLocationPath.when(() -> LocationPath.of(Mockito.anyString(), Mockito.anyMap()))
                    .thenAnswer(inv -> {
                        String location = inv.getArgument(0);
                        return LocationPath.ofDirect(location, "s3", "s3://bucket", minioProps);
                    });
            mockedFactory.when(() -> FileSystemFactory.getFileSystem(
                            Mockito.any(StorageProperties.class)))
                    .thenReturn(fake);

            SpiSwitchingFileSystem spiFs = new SpiSwitchingFileSystem(Collections.emptyMap());
            spiFs.exists(Location.of("s3://bucket/key"));
            Assert.assertEquals("s3://bucket/key", fake.lastUri);
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
     * Records the URIs the delegate actually receives and serves canned listing entries,
     * so tests can assert on both directions of the scheme translation.
     */
    private static class CapturingFileSystem extends StubFileSystem {
        String lastUri;
        String lastRenameSrc;
        String lastRenameDst;
        List<FileEntry> entries = List.of();

        @Override
        public boolean exists(Location location) throws IOException {
            lastUri = location.uri();
            return true;
        }

        @Override
        public void rename(Location src, Location dst) throws IOException {
            lastRenameSrc = src.uri();
            lastRenameDst = dst.uri();
        }

        @Override
        public FileIterator list(Location location) throws IOException {
            lastUri = location.uri();
            Iterator<FileEntry> it = entries.iterator();
            return new FileIterator() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public FileEntry next() {
                    return it.next();
                }

                @Override
                public void close() {
                    // nothing to release
                }
            };
        }

        @Override
        public DorisInputFile newInputFile(Location location) throws IOException {
            lastUri = location.uri();
            return new DorisInputFile() {
                @Override
                public Location location() {
                    return location;
                }

                @Override
                public long length() {
                    return 0;
                }

                @Override
                public DorisInputStream newStream() {
                    throw new UnsupportedOperationException();
                }
            };
        }
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
