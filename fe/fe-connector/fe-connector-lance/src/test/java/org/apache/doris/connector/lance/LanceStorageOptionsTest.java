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

package org.apache.doris.connector.lance;

import org.apache.doris.filesystem.properties.StorageProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/** How the catalog's storage keys become the options both FE and BE open a dataset with. */
class LanceStorageOptionsTest {

    private static final String S3_URI = "s3://warehouse/lance/all_types.lance";

    private static FakeS3CompatibleStorageProperties minio() {
        return new FakeS3CompatibleStorageProperties("S3").endpoint("http://minio:9000").region("us-east-1")
                .accessKey("ak").secretKey("sk").usePathStyle("true");
    }

    @Test
    void catalogPropertiesMapToTheCanonicalS3Spelling() {
        Map<String, String> options = LanceStorageOptions.fromDorisStorageProperties(S3_URI,
                Collections.singletonList(minio().sessionToken("token")));
        Assertions.assertEquals("ak", options.get("aws_access_key_id"));
        Assertions.assertEquals("sk", options.get("aws_secret_access_key"));
        Assertions.assertEquals("token", options.get("aws_session_token"));
        Assertions.assertEquals("http://minio:9000", options.get("aws_endpoint"));
        Assertions.assertEquals("us-east-1", options.get("aws_region"));
        Assertions.assertEquals("false", options.get("aws_virtual_hosted_style_request"));
        // Lance refuses a plain-HTTP endpoint without this.
        Assertions.assertEquals("true", options.get("allow_http"));
        Assertions.assertEquals(
                new TreeSet<>(Arrays.asList("aws_access_key_id", "aws_secret_access_key",
                        "aws_session_token", "aws_endpoint", "aws_region",
                        "aws_virtual_hosted_style_request", "allow_http")),
                new TreeSet<>(options.keySet()));
    }

    /** Doris allows anonymous access; the credential keys are then absent rather than empty. */
    @Test
    void anonymousAccessEmitsNoCredentials() {
        Map<String, String> options = LanceStorageOptions.fromDorisStorageProperties(S3_URI,
                Collections.singletonList(new FakeS3CompatibleStorageProperties("S3")
                        .endpoint("https://s3.amazonaws.com").region("us-east-1")));
        Assertions.assertNull(options.get("aws_access_key_id"));
        Assertions.assertNull(options.get("aws_secret_access_key"));
        Assertions.assertNull(options.get("aws_session_token"));
        // allow_http only makes sense for a plain-HTTP endpoint.
        Assertions.assertNull(options.get("allow_http"));
        Assertions.assertNull(options.get("aws_virtual_hosted_style_request"));
        Assertions.assertEquals("https://s3.amazonaws.com", options.get("aws_endpoint"));
    }

    /** Naming a dialect is a choice; the generic S3 binding is also what a heuristic match lands on. */
    @Test
    void concreteDialectWinsOverTheGenericS3Binding() {
        List<StorageProperties> storage = Arrays.asList(
                new FakeS3CompatibleStorageProperties("S3").endpoint("http://generic:9000").accessKey("g")
                        .secretKey("g"),
                new FakeS3CompatibleStorageProperties("MINIO").endpoint("http://minio:9000").accessKey("m")
                        .secretKey("m"));
        Map<String, String> options = LanceStorageOptions.fromDorisStorageProperties(S3_URI, storage);
        Assertions.assertEquals("http://minio:9000", options.get("aws_endpoint"));
        Assertions.assertEquals("m", options.get("aws_access_key_id"));
    }

    /** The provider follows the dataset URL: a local dataset gets none of the S3 options. */
    @Test
    void onlyAnS3DatasetReadsTheS3Binding() {
        Assertions.assertTrue(LanceStorageOptions.fromDorisStorageProperties("/tmp/lance/all_types.lance",
                Collections.singletonList(minio())).isEmpty());
        Assertions.assertTrue(LanceStorageOptions.fromDorisStorageProperties("file:///tmp/lance/t.lance",
                Collections.singletonList(minio())).isEmpty());
        Assertions.assertFalse(LanceStorageOptions.fromDorisStorageProperties("s3+ddb://bucket/t.lance",
                Collections.singletonList(minio())).isEmpty());
    }

    /** An explicit allow_http, from either side, is never overridden by the inferred default. */
    @Test
    void explicitAllowHttpWins() {
        Map<String, String> vended = new HashMap<>();
        vended.put("allow_http", "false");
        Map<String, String> options = LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                Collections.singletonList(minio()), vended);
        Assertions.assertEquals("false", options.get("allow_http"));
    }

    /** What a namespace vends for a table wins, and lands on the spelling the catalog side uses. */
    @Test
    void vendedOptionsWinAndAreNormalizedOntoTheCanonicalSpelling() {
        Map<String, String> vended = new HashMap<>();
        vended.put("access_key_id", "vended-ak");
        vended.put("AWS_SECRET_ACCESS_KEY", "vended-sk");
        vended.put("token", "vended-token");
        vended.put("custom_option", "kept");
        Map<String, String> options = LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                Collections.singletonList(minio()), vended);
        Assertions.assertEquals("vended-ak", options.get("aws_access_key_id"));
        Assertions.assertEquals("vended-sk", options.get("aws_secret_access_key"));
        Assertions.assertEquals("vended-token", options.get("aws_session_token"));
        Assertions.assertEquals("kept", options.get("custom_option"));
        Assertions.assertFalse(options.containsKey("access_key_id"));
        Assertions.assertFalse(options.containsKey("token"));
    }

    @Test
    void conflictingVendedAliasesAndNulsAreRejected() {
        Map<String, String> conflicting = new HashMap<>();
        conflicting.put("access_key_id", "one");
        conflicting.put("aws_access_key_id", "two");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                        Collections.singletonList(minio()), conflicting));

        Map<String, String> withNul = new HashMap<>();
        withNul.put("aws_region", "us-east-1" + (char) 0);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.fromDorisAndVendedStorageOptions(S3_URI,
                        Collections.singletonList(minio()), withNul));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.fromDorisStorageProperties(S3_URI,
                        Collections.singletonList(minio().secretKey("s" + (char) 0 + "k"))));
    }
}
