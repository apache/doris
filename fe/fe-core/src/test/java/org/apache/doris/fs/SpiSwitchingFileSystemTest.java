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
import java.util.Collections;
import java.util.Map;

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
}
