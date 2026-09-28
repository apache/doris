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
import org.lance.namespace.model.ListTableVersionsRequest;
import org.lance.namespace.model.ListTableVersionsResponse;
import org.lance.namespace.model.TableVersion;

import java.io.IOException;
import java.util.Arrays;
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
        /** The manifest path it records for every version. */
        private volatile String manifestPath;

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
            if (versionFailure != null) {
                throw versionFailure;
            }
            return new DescribeTableVersionResponse().version(
                    new TableVersion().version(request.getVersion()).manifestPath(manifestPath));
        }

        @Override
        public ListTableVersionsResponse listTableVersions(ListTableVersionsRequest request) {
            return new ListTableVersionsResponse().versions(Arrays.asList(
                    new TableVersion().version(2L).manifestPath(null),
                    new TableVersion().version(3L).manifestPath(manifestPath)));
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
        return openedId(S3_URI, handed, vended);
    }

    private static String openedId(String location, Map<String, String> handed, Map<String, String> vended) {
        LanceSdkNamespace sdkNamespace = new LanceSdkNamespace(new StubNamespace(location, vended), handed);
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
     * A namespace may vend credentials that only cover the table's own prefix, so another location
     * gets another store even with the same options. The location's query, which may carry
     * credentials, and a trailing slash do not count.
     */
    @Test
    public void testStoreIdentityCountsTheLocation() {
        Map<String, String> expiring = options("access_key_id", "ak-2", "expires_at_millis", "1");
        String base = openedId(S3_URI, handed(), expiring);
        Assertions.assertNotEquals(base, openedId("s3://bucket/other.lance", handed(), expiring));
        Assertions.assertEquals(base, openedId(S3_URI + "/", handed(), expiring));
        Assertions.assertEquals(openedId(S3_URI + "?sig=a", handed(), expiring),
                openedId(S3_URI + "?sig=b", handed(), expiring));
    }

    /** lance-io only refreshes by an expiry that parses as an unsigned 64-bit integer. */
    @Test
    public void testStoreIdentityCountsCredentialsWithAnExpiryLanceCannotRead() {
        for (String expiry : Arrays.asList("-1", "", "1790000000000.0")) {
            Assertions.assertNotEquals(
                    openedId(handed(), options("access_key_id", "ak-2", "expires_at_millis", expiry)),
                    openedId(handed(), options("access_key_id", "ak-3", "expires_at_millis", expiry)), expiry);
        }
    }

    /**
     * The path lance-io addresses a location by: after the bucket, percent-decoded for a URL, as
     * is without a scheme.
     */
    @Test
    public void testObjectStorePathFollowsLance() {
        Assertions.assertEquals("sales data/t.lance",
                LanceSdkNamespace.objectStorePath("s3://bucket/sales data/t.lance"));
        Assertions.assertEquals("sales data/t.lance",
                LanceSdkNamespace.objectStorePath("s3://bucket/sales%20data/t.lance"));
        Assertions.assertEquals("a|b/t.lance", LanceSdkNamespace.objectStorePath("s3://bucket/a|b/t.lance/"));
        Assertions.assertEquals("", LanceSdkNamespace.objectStorePath("s3://bucket"));
        Assertions.assertEquals("", LanceSdkNamespace.objectStorePath("s3://bucket/"));
        Assertions.assertEquals("t.lance",
                LanceSdkNamespace.objectStorePath("s3+ddb://bucket/t.lance?ddbTableName=x"));
        Assertions.assertEquals("p/t.lance",
                LanceSdkNamespace.objectStorePath("abfss://container@account.dfs.core.windows.net/p/t.lance"));
        Assertions.assertEquals("tmp/a b/t.lance", LanceSdkNamespace.objectStorePath("file:///tmp/a%20b/t.lance"));
        Assertions.assertEquals("tmp/t.lance", LanceSdkNamespace.objectStorePath("file:/tmp/t.lance"));
        Assertions.assertEquals("tmp/a%20b/t.lance", LanceSdkNamespace.objectStorePath("/tmp/a%20b/t.lance"));
        Assertions.assertEquals("t%zz.lance", LanceSdkNamespace.objectStorePath("s3://bucket/t%zz.lance"));
    }

    /**
     * lance-io builds an OpenDAL store ({@code use_opendal}) from the options once, without the
     * credential provider, so its credentials count even when they carry an expiry.
     */
    @Test
    public void testStoreIdentityCountsCredentialsOfAnOpenDalStore() {
        Assertions.assertNotEquals(
                openedId(handed(), options("access_key_id", "ak-2", "expires_at_millis", "1", "use_opendal", "Yes")),
                openedId(handed(), options("access_key_id", "ak-3", "expires_at_millis", "1", "use_opendal", "Yes")));
        Assertions.assertEquals(
                openedId(handed(), options("access_key_id", "ak-2", "expires_at_millis", "1", "use_opendal", "false")),
                openedId(handed(), options("access_key_id", "ak-3", "expires_at_millis", "1", "use_opendal", "false")));
    }

    /**
     * The BE opens a version at its canonical path under the chain's {@code _versions/}. Lance
     * opens a finalized manifest where the namespace records it, and copies a staged one there
     * first, so a finalized manifest anywhere else fails the read before Lance opens it.
     */
    @Test
    public void testFinalizedManifestsMustBeAtTheirCanonicalPath() {
        // Version 3 in V2 naming is u64::MAX - 3.
        String canonical = "table.lance/_versions/18446744073709551612.manifest";
        Assertions.assertEquals(canonical, describedManifest(canonical, null));
        Assertions.assertEquals("/" + canonical, describedManifest("/" + canonical, null));
        // The V1 naming scheme.
        String v1 = "table.lance/_versions/3.manifest";
        Assertions.assertEquals(v1, describedManifest(v1, null));
        Assertions.assertEquals("table.lance/tree/dev/_versions/18446744073709551612.manifest",
                describedManifest("table.lance/tree/dev/_versions/18446744073709551612.manifest", "dev"));
        // Staged: Lance finalizes it to the canonical path before reading.
        Assertions.assertEquals(canonical + "-2f6a1c0e", describedManifest(canonical + "-2f6a1c0e", null));

        for (String elsewhere : Arrays.asList("table.lance/_versions/custom.manifest",
                "other.lance/_versions/18446744073709551612.manifest",
                "table.lance/_versions/18446744073709551613.manifest")) {
            IllegalStateException rejected = Assertions.assertThrows(IllegalStateException.class,
                    () -> describedManifest(elsewhere, null), elsewhere);
            Assertions.assertTrue(rejected.getMessage().contains("'" + canonical + "'"), rejected.getMessage());
        }
        // Lance parses the recorded path first, which drops a trailing slash.
        Assertions.assertThrows(IllegalStateException.class,
                () -> describedManifest("table.lance/_versions/custom.manifest/", null));
        // A table at the root of its bucket, and one whose location Java's URI cannot parse.
        Assertions.assertEquals("_versions/18446744073709551612.manifest",
                describedManifest("s3://bucket", "_versions/18446744073709551612.manifest", null));
        Assertions.assertEquals("sales data/t.lance/_versions/3.manifest",
                describedManifest("s3://bucket/sales data/t.lance", "sales data/t.lance/_versions/3.manifest", null));

        // A branch version at main's path, and a main version at a branch's.
        Assertions.assertThrows(IllegalStateException.class, () -> describedManifest(canonical, "dev"));
        Assertions.assertThrows(IllegalStateException.class,
                () -> describedManifest("table.lance/tree/dev/_versions/18446744073709551612.manifest", null));

        // Lance takes a chain's head from the list, so its entries are checked too.
        StubNamespace catalog = new StubNamespace(S3_URI, Collections.emptyMap());
        LanceSdkNamespace sdkNamespace = new LanceSdkNamespace(catalog, handed());
        sdkNamespace.describeTable(new DescribeTableRequest());
        catalog.manifestPath = canonical;
        Assertions.assertEquals(2, sdkNamespace.listTableVersions(new ListTableVersionsRequest()).getVersions().size());
        catalog.manifestPath = "table.lance/_versions/custom.manifest";
        Assertions.assertThrows(IllegalStateException.class,
                () -> sdkNamespace.listTableVersions(new ListTableVersionsRequest()));
    }

    /**
     * The manifest path the SDK gets for version 3 of {@code branch} when the namespace records
     * {@code recorded}.
     */
    private static String describedManifest(String recorded, String branch) {
        return describedManifest(S3_URI, recorded, branch);
    }

    private static String describedManifest(String location, String recorded, String branch) {
        StubNamespace catalog = new StubNamespace(location, Collections.emptyMap());
        catalog.manifestPath = recorded;
        LanceSdkNamespace sdkNamespace = new LanceSdkNamespace(catalog, handed());
        sdkNamespace.describeTable(new DescribeTableRequest());
        return sdkNamespace.describeTableVersion(new DescribeTableVersionRequest().version(3L).branch(branch))
                .getVersion().getManifestPath();
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
