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

package org.apache.doris.connector.paimon;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.paimon.fs.UnsupportedSchemeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Map;

class PaimonCatalogCreationExceptionTest {

    private static final String PREFIX = "Failed to create Paimon catalog with filesystem metastore "
            + "(flavor=filesystem)";
    private static final String RESOLUTION_ERROR = "Could not find a file io implementation for scheme 'obs' "
            + "in the classpath. Hadoop FileSystem also cannot access this path 'obs://bucket/warehouse'.";

    @Test
    void obsAccessTimeoutIsVisibleThroughRealFileIoFallback() throws Exception {
        Map<String, String> properties = Map.of(
                "paimon.catalog.type", "filesystem",
                "warehouse", "obs://bucket/warehouse",
                "fs.obs.impl", FailingObsFileSystem.class.getName(),
                "fs.obs.impl.disable.cache", "true");
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (PaimonConnector connector = new PaimonConnector(properties, new RecordingConnectorContext())) {
            RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                    () -> connector.getMetadata(null));

            Assertions.assertTrue(failure.getMessage().startsWith(PREFIX), failure.getMessage());
            Assertions.assertTrue(failure.getMessage().contains("SocketTimeoutException: OBS connect timed out"),
                    failure.getMessage());
            Assertions.assertTrue(failure.getMessage().contains("obs://bucket/warehouse"), failure.getMessage());
            Assertions.assertEquals("SocketTimeoutException: OBS connect timed out",
                    ExceptionUtils.getRootCauseMessage(failure));
            Assertions.assertEquals(1, failure.getSuppressed().length);
            Assertions.assertInstanceOf(UnsupportedSchemeException.class,
                    ExceptionUtils.getRootCause(failure.getSuppressed()[0]));
            Assertions.assertSame(previous, Thread.currentThread().getContextClassLoader());
        }
    }

    @Test
    void nestedFileIoFailureExposesAllAccessAttemptsAndRetainsOriginalStack() {
        SocketTimeoutException timeout = new SocketTimeoutException("OBS connect timed out");
        IOException access = new IOException("Failed to access obs://bucket/warehouse", timeout);
        UnknownHostException unknownHost = new UnknownHostException("obs.example.invalid");
        UnsupportedSchemeException resolution = new UnsupportedSchemeException(RESOLUTION_ERROR);
        resolution.addSuppressed(access);
        resolution.addSuppressed(unknownHost);
        RuntimeException original = new RuntimeException("Catalog factory failed", resolution);

        RuntimeException failure = PaimonExceptionUtils.catalogCreationFailure(PREFIX, original);

        Assertions.assertTrue(failure.getMessage().contains("SocketTimeoutException: OBS connect timed out"));
        Assertions.assertTrue(failure.getMessage().contains("UnknownHostException: obs.example.invalid"));
        Assertions.assertTrue(failure.getMessage().contains(RESOLUTION_ERROR));
        Assertions.assertSame(access, failure.getCause());
        Assertions.assertSame(original, failure.getSuppressed()[0]);
        Assertions.assertEquals("SocketTimeoutException: OBS connect timed out",
                ExceptionUtils.getRootCauseMessage(failure));
        StringWriter stack = new StringWriter();
        failure.printStackTrace(new PrintWriter(stack));
        Assertions.assertTrue(stack.toString().contains("Catalog factory failed"));
        Assertions.assertTrue(stack.toString().contains("UnknownHostException: obs.example.invalid"));
    }

    @Test
    void accessFailureWithoutMessageStillShowsItsType() {
        UnsupportedSchemeException resolution = new UnsupportedSchemeException(RESOLUTION_ERROR);
        resolution.addSuppressed(new IOException(new SocketTimeoutException()));

        RuntimeException failure = PaimonExceptionUtils.catalogCreationFailure(PREFIX, resolution);

        Assertions.assertTrue(failure.getMessage().contains("SocketTimeoutException"), failure.getMessage());
    }

    @Test
    void unsupportedSchemeWithoutAccessFailuresPreservesOriginalError() {
        UnsupportedSchemeException original = new UnsupportedSchemeException(RESOLUTION_ERROR);

        RuntimeException failure = PaimonExceptionUtils.catalogCreationFailure(PREFIX, original);

        Assertions.assertEquals(PREFIX + ": " + RESOLUTION_ERROR, failure.getMessage());
        Assertions.assertSame(original, failure.getCause());
        Assertions.assertEquals(0, failure.getSuppressed().length);
    }

    @Test
    void missingHadoopImplementationStillReportsUnsupportedFileSystem() {
        UnsupportedFileSystemException missing = new UnsupportedFileSystemException("No FileSystem for scheme obs");
        UnsupportedSchemeException resolution = new UnsupportedSchemeException(RESOLUTION_ERROR);
        resolution.addSuppressed(missing);

        RuntimeException failure = PaimonExceptionUtils.catalogCreationFailure(PREFIX, resolution);

        Assertions.assertTrue(failure.getMessage().contains(
                "UnsupportedFileSystemException: No FileSystem for scheme obs"));
        Assertions.assertSame(missing, failure.getCause());
        Assertions.assertSame(resolution, failure.getSuppressed()[0]);
    }

    @Test
    void unrelatedSuppressedExceptionDoesNotReplaceCatalogFailure() {
        IllegalArgumentException original = new IllegalArgumentException("Invalid catalog option");
        original.addSuppressed(new IOException("close failed"));

        RuntimeException failure = PaimonExceptionUtils.catalogCreationFailure(PREFIX, original);

        Assertions.assertEquals(PREFIX + ": Invalid catalog option", failure.getMessage());
        Assertions.assertSame(original, failure.getCause());
    }

    /** Exercises Paimon's Hadoop fallback without connecting to an OBS service. */
    public static class FailingObsFileSystem extends RawLocalFileSystem {
        @Override
        public URI getUri() {
            return URI.create("obs://bucket");
        }

        @Override
        public boolean exists(Path path) throws IOException {
            throw new IOException("Failed to access " + path, new SocketTimeoutException("OBS connect timed out"));
        }
    }
}
