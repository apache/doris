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

package org.apache.doris.datasource.lance;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.TableVersionNotFoundException;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DescribeTableVersionRequest;
import org.lance.namespace.model.DescribeTableVersionResponse;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The namespace the Lance SDK opens a managed table through. The SDK calls it from Java for its
 * own describe and back through JNI afterwards; both are plain method calls here, so none of this
 * needs the native library.
 */
public class LanceSdkNamespaceTest {
    private static final String S3_URI = "s3://bucket/table.lance";

    /** A catalog namespace that describes one table with the options it currently vends. */
    static final class StubNamespace implements LanceNamespace {
        private final String location;
        private volatile Map<String, String> vended;
        private volatile RuntimeException versionFailure;

        StubNamespace(String location, Map<String, String> vended) {
            this.location = location;
            this.vended = vended;
        }

        @Override
        public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {
        }

        @Override
        public String namespaceId() {
            return "RestNamespace { endpoint: \"http://catalog\" }";
        }

        @Override
        public DescribeTableResponse describeTable(DescribeTableRequest request) {
            return new DescribeTableResponse().location(location).managedVersioning(true)
                    .storageOptions(new HashMap<>(vended));
        }

        @Override
        public DescribeTableVersionResponse describeTableVersion(DescribeTableVersionRequest request) {
            throw versionFailure;
        }
    }

    private static Map<String, String> options(String... keysAndValues) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            result.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return result;
    }

    private static Map<String, String> handed() {
        return options("aws_endpoint", "https://storage-a", "aws_region", "us-east-1",
                "aws_access_key_id", "ak-1", "aws_secret_access_key", "sk-1");
    }

    /** The id the SDK keys the store cache by after opening with {@code handed} and a describe vending {@code vended}. */
    private static String openedId(Map<String, String> handed, Map<String, String> vended) {
        LanceSdkNamespace sdkNamespace = new LanceSdkNamespace(new StubNamespace(S3_URI, vended), handed);
        sdkNamespace.describeTable(new DescribeTableRequest());
        return sdkNamespace.namespaceId();
    }

    /**
     * The SDK opens with what it is handed plus what its describe vends. Vended in the canonical
     * spelling, the endpoint replaces the one it was handed; under an alias it would sit beside the
     * canonical key or, if that were missing, let Lance add AWS_ENDPOINT from the FE environment.
     */
    @Test
    public void testDescribeVendsTheCanonicalSpelling() {
        LanceSdkNamespace sdkNamespace = new LanceSdkNamespace(new StubNamespace(S3_URI,
                options("endpoint", "https://storage-b", "access_key_id", "ak-2", "custom_option", "x")), handed());

        DescribeTableResponse response = sdkNamespace.describeTable(new DescribeTableRequest());

        Assertions.assertEquals(options("aws_endpoint", "https://storage-b", "aws_access_key_id", "ak-2",
                "custom_option", "x"), response.getStorageOptions());
        Assertions.assertEquals(S3_URI, response.getLocation());
        Assertions.assertTrue(response.getManagedVersioning());
    }

    /**
     * Two reads of one table share a store only if the store would be built the same: another
     * endpoint, region or addressing style keeps them apart.
     */
    @Test
    public void testStoreIdentityFollowsWhereTheStoreConnects() {
        String base = openedId(handed(), Collections.emptyMap());
        Assertions.assertEquals(base, openedId(handed(), Collections.emptyMap()));

        Assertions.assertNotEquals(base, openedId(handed(), options("endpoint", "https://storage-b")));
        Assertions.assertNotEquals(base, openedId(handed(), options("region", "eu-west-1")));
        Assertions.assertNotEquals(base, openedId(handed(), options("virtual_hosted_style_request", "false")));
        Map<String, String> otherCatalogEndpoint = handed();
        otherCatalogEndpoint.put("aws_endpoint", "https://storage-b");
        Assertions.assertNotEquals(base, openedId(otherCatalogEndpoint, Collections.emptyMap()));

        // The catalog namespace stays recognizable; the options only appear as a digest.
        Assertions.assertTrue(base.contains("RestNamespace"), base);
        Assertions.assertFalse(base.contains("storage-a"), base);
    }

    /**
     * Credentials with an expiry do not keep two reads apart: the store refreshes them from the
     * namespace, which may vend new ones on every describe. Without an expiry the store keeps the
     * credentials it was built with, so other credentials get another store.
     */
    @Test
    public void testStoreIdentityCountsCredentialsTheStoreDoesNotRefresh() {
        String expiring = openedId(handed(), options("access_key_id", "ak-2", "secret_access_key", "sk-2",
                "session_token", "t-2", "expires_at_millis", "1"));
        Assertions.assertEquals(expiring, openedId(handed(), options("access_key_id", "ak-3",
                "secret_access_key", "sk-3", "session_token", "t-3", "expires_at_millis", "2")));

        String fixed = openedId(handed(), options("access_key_id", "ak-2", "secret_access_key", "sk-2"));
        Assertions.assertEquals(fixed,
                openedId(handed(), options("access_key_id", "ak-2", "secret_access_key", "sk-2")));
        Assertions.assertNotEquals(fixed, openedId(handed(), options("access_key_id", "ak-3")));
        Assertions.assertNotEquals(fixed,
                openedId(handed(), options("access_key_id", "ak-2", "secret_access_key", "sk-3")));
        Assertions.assertNotEquals(fixed, openedId(handed(), Collections.emptyMap()));
        Assertions.assertFalse(fixed.contains("ak-2"), fixed);
    }

    /**
     * The id is fixed by the SDK's describe while it opens the dataset. Later describes refresh
     * credentials for a store already built, so they leave it alone whatever they vend.
     */
    @Test
    public void testIdComesFromTheOpeningDescribe() {
        StubNamespace catalog = new StubNamespace(S3_URI, Collections.emptyMap());
        LanceSdkNamespace sdkNamespace = new LanceSdkNamespace(catalog, handed());
        Assertions.assertThrows(IllegalStateException.class, sdkNamespace::namespaceId);

        sdkNamespace.describeTable(new DescribeTableRequest());
        String id = sdkNamespace.namespaceId();
        catalog.vended = options("endpoint", "https://storage-b");
        Assertions.assertEquals("https://storage-b",
                sdkNamespace.describeTable(new DescribeTableRequest()).getStorageOptions().get("aws_endpoint"));
        Assertions.assertEquals(id, sdkNamespace.namespaceId());
    }

    /**
     * The SDK reports an exception a JNI callback threw only as "Java exception was thrown"; the
     * namespace hands out the exception itself instead, once, and leaves other SDK errors alone.
     */
    @Test
    public void testCallbackFailureIsHandedOutInPlaceOfTheSdkReport() {
        StubNamespace catalog = new StubNamespace(S3_URI, Collections.emptyMap());
        LanceSdkNamespace sdkNamespace = new LanceSdkNamespace(catalog, handed());
        TableVersionNotFoundException missing = new TableVersionNotFoundException("Table version 9 not found");
        catalog.versionFailure = missing;
        Assertions.assertThrows(TableVersionNotFoundException.class,
                () -> sdkNamespace.describeTableVersion(new DescribeTableVersionRequest()));

        IOException unrelated = new IOException("Not found: bucket/table.lance/_versions/9.manifest");
        Assertions.assertSame(unrelated, sdkNamespace.unwrapCallbackFailure(unrelated));
        IOException sdkReport = new IOException(
                "LanceError(IO): Failed to call describeTableVersion: Java exception was thrown");
        Exception unwrapped = sdkNamespace.unwrapCallbackFailure(sdkReport);
        Assertions.assertSame(missing, unwrapped);
        Assertions.assertSame(sdkReport, unwrapped.getSuppressed()[0]);
        Assertions.assertSame(sdkReport, sdkNamespace.unwrapCallbackFailure(sdkReport));
    }
}
