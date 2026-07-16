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

import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.property.storage.BrokerProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.filesystem.FileEntry;
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

    @Test
    public void testNormalizeOssBucketEndpointPathBeforeDelegating() throws IOException {
        RecordingFileSystem delegate = new RecordingFileSystem();
        SpiSwitchingFileSystem spiFs = new RecordingSwitchingFileSystem(delegate, createOssStorageProperties());

        Location source = Location.of("oss://my-bucket.oss-cn-beijing-internal.aliyuncs.com/path/to/source");
        Location dest = Location.of("oss://my-bucket.oss-cn-beijing-internal.aliyuncs.com/path/to/dest");

        spiFs.delete(source, true);
        Assert.assertEquals("s3://my-bucket/path/to/source", delegate.location.uri());
        Assert.assertEquals("s3://my-bucket/path/to/source", delegate.selectedLocation);
        Assert.assertEquals(StorageProperties.Type.OSS, delegate.selectedStorageType);

        spiFs.listFiles(source);
        Assert.assertEquals("s3://my-bucket/path/to/source", delegate.location.uri());
        Assert.assertEquals("s3://my-bucket/path/to/source", delegate.selectedLocation);
        Assert.assertEquals(StorageProperties.Type.OSS, delegate.selectedStorageType);

        spiFs.rename(source, dest);
        Assert.assertEquals("s3://my-bucket/path/to/source", delegate.location.uri());
        Assert.assertEquals("s3://my-bucket/path/to/dest", delegate.destLocation.uri());
        Assert.assertEquals("s3://my-bucket/path/to/source", delegate.selectedLocation);
        Assert.assertEquals(StorageProperties.Type.OSS, delegate.selectedStorageType);

        spiFs.newOutputFile(dest);
        Assert.assertEquals("s3://my-bucket/path/to/dest", delegate.location.uri());
        Assert.assertEquals("s3://my-bucket/path/to/dest", delegate.selectedLocation);
        Assert.assertEquals(StorageProperties.Type.OSS, delegate.selectedStorageType);
    }

    @Test
    public void testOssBucketEndpointPathSelectsOssWhenS3IsAlsoConfigured()
            throws IOException, UserException {
        RecordingFileSystem delegate = new RecordingFileSystem();
        SpiSwitchingFileSystem spiFs = new RecordingSwitchingFileSystem(
                delegate, createOssAndS3StorageProperties());

        Location source = Location.of("oss://my-bucket.oss-cn-beijing-internal.aliyuncs.com/path/to/source");

        spiFs.delete(source, true);

        Assert.assertEquals("s3://my-bucket/path/to/source", delegate.location.uri());
        Assert.assertEquals("s3://my-bucket/path/to/source", delegate.selectedLocation);
        Assert.assertEquals(StorageProperties.Type.OSS, delegate.selectedStorageType);
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

    private static Map<StorageProperties.Type, StorageProperties> createOssStorageProperties() {
        Map<String, String> origProps = new HashMap<>();
        origProps.put("oss.endpoint", "oss-cn-beijing-internal.aliyuncs.com");
        origProps.put("oss.access_key", "ak");
        origProps.put("oss.secret_key", "sk");
        origProps.put(StorageProperties.FS_OSS_SUPPORT, "true");

        Map<StorageProperties.Type, StorageProperties> storageProperties = new HashMap<>();
        storageProperties.put(StorageProperties.Type.OSS, StorageProperties.createPrimary(origProps));
        return storageProperties;
    }

    private static Map<StorageProperties.Type, StorageProperties> createOssAndS3StorageProperties()
            throws UserException {
        Map<String, String> origProps = new HashMap<>();
        origProps.put(StorageProperties.FS_OSS_SUPPORT, "true");
        origProps.put("oss.endpoint", "oss-cn-beijing-internal.aliyuncs.com");
        origProps.put("oss.access_key", "oss-ak");
        origProps.put("oss.secret_key", "oss-sk");
        origProps.put(StorageProperties.FS_S3_SUPPORT, "true");
        origProps.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        origProps.put("s3.access_key", "s3-ak");
        origProps.put("s3.secret_key", "s3-sk");
        origProps.put("s3.region", "us-east-1");

        Map<StorageProperties.Type, StorageProperties> storageProperties = new HashMap<>();
        for (StorageProperties storageProperty : StorageProperties.createAll(origProps)) {
            storageProperties.put(storageProperty.getType(), storageProperty);
        }
        return storageProperties;
    }

    private static class RecordingSwitchingFileSystem extends SpiSwitchingFileSystem {
        private final RecordingFileSystem delegate;

        RecordingSwitchingFileSystem(RecordingFileSystem delegate,
                Map<StorageProperties.Type, StorageProperties> storagePropertiesMap) {
            super(storagePropertiesMap);
            this.delegate = delegate;
        }

        @Override
        FileSystem forLocationPath(LocationPath locationPath, String originalUri) {
            delegate.selectedLocation = locationPath.getNormalizedLocation();
            delegate.selectedStorageType = locationPath.getStorageProperties().getType();
            return delegate;
        }
    }

    private static class RecordingFileSystem extends StubFileSystem {
        private String selectedLocation;
        private StorageProperties.Type selectedStorageType;
        private Location location;
        private Location destLocation;

        @Override
        public void delete(Location location, boolean recursive) throws IOException {
            this.location = location;
        }

        @Override
        public void rename(Location src, Location dst) throws IOException {
            this.location = src;
            this.destLocation = dst;
        }

        @Override
        public List<FileEntry> listFiles(Location dir) throws IOException {
            this.location = dir;
            return Collections.emptyList();
        }

        @Override
        public org.apache.doris.filesystem.DorisOutputFile newOutputFile(Location location) throws IOException {
            this.location = location;
            return null;
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
        public Set<String> listDirectories(org.apache.doris.filesystem.Location dir) throws IOException {
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
