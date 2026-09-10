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

package org.apache.doris.connector.spi;

import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class ConnectorStorageAccessResolverTest {

    @Test
    public void providerNamesAreAnIndependentSnapshot() {
        Set<String> providers = new LinkedHashSet<>(Set.of("Azure", "hdfs"));
        ConnectorStorageAccessResolver resolver = new ConnectorStorageAccessResolver(providers, rawUri -> {
            throw new AssertionError("Provider lookup must not resolve a location");
        });

        providers.clear();
        providers.add("s3");

        Assertions.assertTrue(resolver.hasProvider("Azure"));
        Assertions.assertTrue(resolver.hasProvider("hdfs"));
        Assertions.assertFalse(resolver.hasProvider("s3"));
    }

    @Test
    public void providerLookupIsCaseInsensitive() {
        ConnectorStorageAccessResolver resolver = new ConnectorStorageAccessResolver(
                Set.of("AzUrE", "HDFS", "s3"), rawUri -> {
                    throw new AssertionError("Provider lookup must not resolve a location");
                });

        for (String provider : List.of("azure", "AZURE", "aZuRe", "hdfs", "HdFs", "S3")) {
            Assertions.assertTrue(resolver.hasProvider(provider));
        }
        Assertions.assertFalse(resolver.hasProvider("oss"));
    }

    @Test
    public void providerLookupDoesNotExecuteTheUriResolver() {
        AtomicInteger calls = new AtomicInteger();
        for (Set<String> providers : List.of(Set.of("azure"), Set.<String>of())) {
            ConnectorStorageAccessResolver resolver = new ConnectorStorageAccessResolver(providers, rawUri -> {
                calls.incrementAndGet();
                throw new IllegalStateException("Resolution requires an actual data-file location");
            });

            Assertions.assertEquals(providers.contains("azure"), resolver.hasProvider("AZURE"));
            Assertions.assertFalse(resolver.hasProvider("s3"));
        }
        Assertions.assertEquals(0, calls.get());
    }

    @Test
    public void applyDelegatesTheUnchangedUriAndReturnsTheSameAccess() {
        String uri = "abfss://container@account.dfs.core.windows.net/data/a%2Fb.parquet";
        ConnectorStorageAccess access = new ConnectorStorageAccess("azure", uri,
                BackendStorageKind.NATIVE, "FILE_S3", Map.of("provider", "azure"));
        AtomicReference<String> resolvedUri = new AtomicReference<>();
        Function<String, ConnectorStorageAccess> resolver = new ConnectorStorageAccessResolver(
                Set.of("azure"), rawUri -> {
                    resolvedUri.set(rawUri);
                    return access;
                });

        Assertions.assertSame(access, resolver.apply(uri));
        Assertions.assertEquals(uri, resolvedUri.get());
    }

    @Test
    public void applyPropagatesTheResolutionFailure() {
        IllegalStateException failure = new IllegalStateException("No matching storage binding");
        ConnectorStorageAccessResolver resolver = new ConnectorStorageAccessResolver(Set.of(), rawUri -> {
            throw failure;
        });

        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                () -> resolver.apply("s3://metadata-only-bucket/table")));
    }

    @Test
    public void oldConstructorRejectsPrefixMatchingWithoutResolvingCredentials() {
        ConnectorStorageAccessResolver resolver = new ConnectorStorageAccessResolver(Set.of("azure"), rawUri -> {
            throw new AssertionError("Prefix matching must not resolve credentials");
        });

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> resolver.matchesLocationPrefix("abfss://container@account.dfs.core.windows.net/data/file",
                        "abfss://container@account.dfs.core.windows.net/data/"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void prefixMatchingPreservesRawArgumentsAndDoesNotResolveCredentials(boolean matches) {
        String location = "ABFSS://container@account.dfs.core.windows.net/data/a%2Fb//./../http://example+file";
        String prefix = "abfss://container@account.dfs.core.windows.net/data/a%2Fb/";
        AtomicReference<List<String>> arguments = new AtomicReference<>();
        ConnectorStorageAccessResolver resolver = new ConnectorStorageAccessResolver(Set.of("azure"), rawUri -> {
            throw new AssertionError("Prefix matching must not resolve credentials");
        }, (rawLocation, rawPrefix) -> {
            arguments.set(List.of(rawLocation, rawPrefix));
            return matches;
        });

        Assertions.assertEquals(matches, resolver.matchesLocationPrefix(location, prefix));
        Assertions.assertEquals(List.of(location, prefix), arguments.get());
    }

    @Test
    public void prefixMatchingPropagatesTheProviderFailure() {
        IllegalArgumentException failure = new IllegalArgumentException("Invalid location prefix");
        ConnectorStorageAccessResolver resolver = new ConnectorStorageAccessResolver(Set.of("azure"), rawUri -> {
            throw new AssertionError("Prefix matching must not resolve credentials");
        }, (rawLocation, rawPrefix) -> {
            throw failure;
        });

        Assertions.assertSame(failure, Assertions.assertThrows(IllegalArgumentException.class,
                () -> resolver.matchesLocationPrefix("abfss://container@account.dfs.core.windows.net/data/file",
                        "invalid-prefix")));
    }

    @Test
    public void providerLookupAndResolutionDoNotInvokeThePrefixMatcher() {
        String uri = "s3://bucket/data/file";
        ConnectorStorageAccess access = new ConnectorStorageAccess("s3", uri,
                BackendStorageKind.NATIVE, "FILE_S3", Map.of("provider", "s3"));
        ConnectorStorageAccessResolver resolver = new ConnectorStorageAccessResolver(Set.of("s3"), rawUri -> access,
                (rawLocation, rawPrefix) -> {
                    throw new AssertionError("Only explicit prefix checks may invoke the matcher");
                });

        Assertions.assertTrue(resolver.hasProvider("S3"));
        Assertions.assertSame(access, resolver.apply(uri));
    }

    @Test
    public void defaultStoragePrefixMatchingKeepsRawPathBoundaries() {
        StorageProperties properties = new DefaultStorageProperties();

        Assertions.assertTrue(properties.matchesLocationPrefix("s3://bucket/data/file", "s3://bucket/data/file"));
        Assertions.assertTrue(properties.matchesLocationPrefix("s3://bucket/data/file", "s3://bucket/data"));
        Assertions.assertFalse(properties.matchesLocationPrefix("s3://bucket/database/file", "s3://bucket/data"));
        Assertions.assertFalse(properties.matchesLocationPrefix("s3://other/data/file", "s3://bucket/data"));
        Assertions.assertFalse(properties.matchesLocationPrefix("s3://bucket/data", "s3://bucket/data/"));
        Assertions.assertTrue(properties.matchesLocationPrefix("s3://bucket/data/../file", "s3://bucket/data/"));
    }

    private static final class DefaultStorageProperties implements StorageProperties {
        @Override
        public String providerName() {
            return "S3";
        }

        @Override
        public StorageKind kind() {
            return StorageKind.OBJECT_STORAGE;
        }

        @Override
        public FileSystemType type() {
            return FileSystemType.S3;
        }

        @Override
        public Map<String, String> rawProperties() {
            return Map.of();
        }

        @Override
        public Map<String, String> matchedProperties() {
            return Map.of();
        }

        @Override
        public void validateForAccess() {
            throw new AssertionError("Default prefix matching must not access credentials");
        }
    }
}
