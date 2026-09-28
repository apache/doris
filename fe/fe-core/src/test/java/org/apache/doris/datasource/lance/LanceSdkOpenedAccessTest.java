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

import org.apache.doris.datasource.lance.metadata.LanceTableAccess;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.namespace.model.DescribeTableRequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The access a managed read hands the BE after the Lance SDK opened the table with the result of
 * its own DescribeTable. The SDK opens with the options it was handed and that describe's vended
 * options put on top ({@code OpenDatasetBuilder.buildFromNamespaceClient} in Lance 12), and
 * describes the table through {@link LanceSdkNamespace}. {@link #sdkOpen} reproduces both, so none
 * of this needs the native library.
 */
public class LanceSdkOpenedAccessTest {
    private static final String S3_URI = "s3://bucket/table.lance";
    private static final List<String> TABLE_ID = Collections.singletonList("table");

    private static LanceNamespaceClient client(Map<String, String> catalogProperties) {
        List<StorageProperties> storageProperties;
        try {
            storageProperties = catalogProperties.isEmpty()
                    ? Collections.emptyList() : StorageProperties.createAll(catalogProperties);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return new LanceNamespaceClient(null, LanceExternalCatalog.LANCE_REST, "default", Collections.emptyList(),
                storageProperties);
    }

    private static Map<String, String> minioCatalog() {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.endpoint", "http://minio:9000");
        properties.put("s3.region", "us-east-1");
        properties.put("s3.access_key", "ak");
        properties.put("s3.secret_key", "sk");
        return properties;
    }

    private static Map<String, String> options(String... keysAndValues) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            result.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return result;
    }

    /** The options the SDK opens with when handed {@code access} and its describe vends {@code vended}. */
    private static Map<String, String> sdkOpen(LanceTableAccess access, Map<String, String> vended) {
        return sdkOpen(access, S3_URI, vended);
    }

    private static Map<String, String> sdkOpen(LanceTableAccess access, String location, Map<String, String> vended) {
        LanceSdkNamespace sdkNamespace = new LanceSdkNamespace(
                new LanceSdkNamespaceTest.StubNamespace(location, vended), access.getStorageOptions());
        Map<String, String> result = new HashMap<>(access.getStorageOptions());
        result.putAll(sdkNamespace.describeTable(new DescribeTableRequest().id(TABLE_ID)).getStorageOptions());
        return result;
    }

    @Test
    public void testUnchangedDescribeKeepsTheResolvedAccess() {
        LanceNamespaceClient client = client(minioCatalog());
        LanceTableAccess access = client.managedAccess(S3_URI,
                options("aws_access_key_id", "vak", "aws_secret_access_key", "vsk"), TABLE_ID);
        Map<String, String> opened = sdkOpen(access,
                options("aws_access_key_id", "vak", "aws_secret_access_key", "vsk"));
        Assertions.assertSame(access, client.accessOpenedBySdk(access, S3_URI, opened));
        Assertions.assertTrue(LanceNamespaceClient.opensAs(access, S3_URI, opened));
        // A describe that vends nothing, or drops a key, leaves the SDK with what it was handed.
        for (Map<String, String> vended : Arrays.<Map<String, String>>asList(Collections.emptyMap(),
                options("aws_access_key_id", "vak"))) {
            Map<String, String> unchanged = sdkOpen(access, vended);
            Assertions.assertSame(access, client.accessOpenedBySdk(access, S3_URI + "/", unchanged));
            Assertions.assertTrue(LanceNamespaceClient.opensAs(access, S3_URI + "/", unchanged));
        }
        // Without catalog or vended options the SDK reports no initial options at all.
        LanceNamespaceClient bare = client(Collections.emptyMap());
        LanceTableAccess bareAccess = bare.managedAccess(S3_URI, null, TABLE_ID);
        Assertions.assertSame(bareAccess, bare.accessOpenedBySdk(bareAccess, S3_URI, null));
        Assertions.assertTrue(LanceNamespaceClient.opensAs(bareAccess, S3_URI, null));
    }

    @Test
    public void testSameUriEndpointChangeReachesTheBe() {
        LanceNamespaceClient client = client(minioCatalog());
        LanceTableAccess access = client.managedAccess(S3_URI,
                options("aws_endpoint", "https://storage-a", "aws_session_token", "t1"), TABLE_ID);
        Assertions.assertEquals("https://storage-a", access.getStorageOptions().get("aws_endpoint"));
        Map<String, String> opened = sdkOpen(access,
                options("aws_endpoint", "https://storage-b", "aws_session_token", "t2"));
        LanceTableAccess forBe = client.accessOpenedBySdk(access, S3_URI, opened);
        Assertions.assertEquals("https://storage-b", forBe.getStorageOptions().get("aws_endpoint"));
        Assertions.assertEquals("t2", forBe.getStorageOptions().get("aws_session_token"));
        Assertions.assertEquals(S3_URI, forBe.getDatasetUri());
        Assertions.assertEquals(TABLE_ID, forBe.getNamespaceTableId());
        // Same spellings, new values: what the SDK opened with is exactly the new access's options.
        Assertions.assertTrue(LanceNamespaceClient.opensAs(forBe, S3_URI, opened));
    }

    /**
     * A managed read pinned to the namespace's newest version lists it again when the SDK opened
     * another store: a new endpoint, or another location, even under the same URI. New credentials
     * alone, which a namespace may vend on every describe, or a new query on the location do not
     * count, so they cost ordinary reads no extra list.
     */
    @Test
    public void testOnlyAnotherStoreOrLocationCountsAsAMove() {
        LanceNamespaceClient client = client(minioCatalog());
        LanceTableAccess access = client.managedAccess(S3_URI, options("aws_endpoint", "https://storage-a",
                "aws_access_key_id", "vak-1", "aws_session_token", "t1", "expires_at_millis", "1"), TABLE_ID);
        LanceTableAccess rotated = client.accessOpenedBySdk(access, S3_URI, sdkOpen(access, options(
                "aws_endpoint", "https://storage-a", "aws_access_key_id", "vak-2", "aws_session_token", "t2",
                "expires_at_millis", "2")));
        Assertions.assertTrue(LanceCatalogClient.sameStore(access, rotated));
        Assertions.assertTrue(LanceCatalogClient.sameStore(access,
                client.managedAccess(S3_URI + "?sig=x", access.getStorageOptions(), TABLE_ID)));

        LanceTableAccess otherEndpoint = client.accessOpenedBySdk(access, S3_URI, sdkOpen(access,
                options("aws_endpoint", "https://storage-b", "aws_session_token", "t2")));
        Assertions.assertFalse(LanceCatalogClient.sameStore(access, otherEndpoint));
        String elsewhere = "s3://bucket/moved.lance";
        LanceTableAccess otherLocation = client.accessOpenedBySdk(access, elsewhere,
                sdkOpen(access, elsewhere, Collections.emptyMap()));
        Assertions.assertFalse(LanceCatalogClient.sameStore(access, otherLocation));
    }

    /**
     * A describe that vends an option under an alias reaches the SDK in the canonical spelling,
     * replacing the catalog's value: with both spellings, object_store would keep whichever its map
     * yields last, and Lance would fill the canonical key from the FE environment were it missing.
     */
    @Test
    public void testAliasFromTheSdkDescribeReplacesTheCanonicalKey() {
        LanceNamespaceClient client = client(minioCatalog());
        LanceTableAccess access = client.managedAccess(S3_URI, options("endpoint", "https://storage-a"), TABLE_ID);
        Assertions.assertEquals("https://storage-a", access.getStorageOptions().get("aws_endpoint"));
        Assertions.assertFalse(access.getStorageOptions().containsKey("endpoint"));
        Map<String, String> opened = sdkOpen(access, options("endpoint", "https://storage-b"));
        Assertions.assertEquals("https://storage-b", opened.get("aws_endpoint"));
        Assertions.assertFalse(opened.containsKey("endpoint"));
        LanceTableAccess rebuilt = client.accessOpenedBySdk(access, S3_URI, opened);
        Assertions.assertEquals("https://storage-b", rebuilt.getStorageOptions().get("aws_endpoint"));
        Assertions.assertTrue(LanceNamespaceClient.opensAs(rebuilt, S3_URI, opened));
    }

    @Test
    public void testInferredOptionNeedsAReopen() {
        // An endpoint moving off plain HTTP drops the allow_http the old one implied.
        LanceNamespaceClient bare = client(Collections.emptyMap());
        LanceTableAccess http = bare.managedAccess(S3_URI, options("aws_endpoint", "http://storage-a"), TABLE_ID);
        Assertions.assertEquals("true", http.getStorageOptions().get("allow_http"));
        Map<String, String> https = sdkOpen(http, options("aws_endpoint", "https://storage-b"));
        LanceTableAccess rebuiltHttps = bare.accessOpenedBySdk(http, S3_URI, https);
        Assertions.assertNull(rebuiltHttps.getStorageOptions().get("allow_http"));
        Assertions.assertFalse(LanceNamespaceClient.opensAs(rebuiltHttps, S3_URI, https));
    }

    @Test
    public void testRelocationIsReadAtTheNewLocation() {
        LanceNamespaceClient client = client(minioCatalog());
        LanceTableAccess access = client.managedAccess(S3_URI, Collections.emptyMap(), TABLE_ID);
        // Same store: the options carry over and the SDK already opened with them.
        String moved = "s3://bucket/moved.lance";
        Map<String, String> opened = sdkOpen(access, Collections.emptyMap());
        LanceTableAccess relocated = client.accessOpenedBySdk(access, moved, opened);
        Assertions.assertEquals(moved, relocated.getDatasetUri());
        Assertions.assertTrue(LanceNamespaceClient.opensAs(relocated, moved, opened));
        // Another store: the SDK opened it with S3 options, so it has to be opened again with the
        // options built for the new location.
        String oss = "oss://bucket/table.lance";
        Map<String, String> ossOpened = sdkOpen(access, oss, options("oss_access_key_id", "oak"));
        LanceTableAccess onOss = client.accessOpenedBySdk(access, oss, ossOpened);
        Assertions.assertEquals(oss, onOss.getDatasetUri());
        Assertions.assertFalse(LanceNamespaceClient.opensAs(onOss, oss, ossOpened));
        Map<String, String> ossReopened = sdkOpen(onOss, oss, options("oss_access_key_id", "oak"));
        Assertions.assertSame(onOss, client.accessOpenedBySdk(onOss, oss, ossReopened));
        Assertions.assertTrue(LanceNamespaceClient.opensAs(onOss, oss, ossReopened));
    }
}
