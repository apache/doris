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

import org.apache.doris.datasource.property.storage.StorageProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class LanceStorageOptionsTest {

    private static final String S3_URI = "s3://warehouse/table.lance";

    /**
     * Parsed by Doris exactly as a real catalog would, so these fixtures also have to satisfy its
     * rules: endpoint and region are required, and the access key and secret key must be set
     * together or not at all.
     */
    private static List<StorageProperties> createAll(Map<String, String> properties) {
        try {
            return StorageProperties.createAll(properties);
        } catch (Exception e) {
            throw new IllegalStateException("failed to parse test storage properties", e);
        }
    }

    private static Map<String, String> minioProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.endpoint", "http://minio:9000");
        properties.put("s3.region", "us-east-1");
        properties.put("s3.access_key", "ak");
        properties.put("s3.secret_key", "sk");
        properties.put("use_path_style", "true");
        return properties;
    }

    private static List<StorageProperties> minioCatalog() {
        return createAll(minioProperties());
    }

    @Test
    public void testCatalogPropertiesMapToTheCanonicalS3Spelling() {
        Map<String, String> properties = minioProperties();
        properties.put("s3.session_token", "token");

        Map<String, String> options =
                LanceStorageOptions.forDataset(S3_URI, createAll(properties), null);
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
    public void testAnonymousAccessEmitsNoCredentials() {
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.endpoint", "https://s3.amazonaws.com");
        properties.put("s3.region", "us-east-1");

        Map<String, String> options =
                LanceStorageOptions.forDataset(S3_URI, createAll(properties), null);
        Assertions.assertNull(options.get("aws_access_key_id"));
        Assertions.assertNull(options.get("aws_secret_access_key"));
        Assertions.assertNull(options.get("aws_session_token"));
        // allow_http only makes sense for a plain-HTTP endpoint.
        Assertions.assertNull(options.get("allow_http"));
        Assertions.assertEquals("https://s3.amazonaws.com", options.get("aws_endpoint"));
    }

    /**
     * The case this exists for: a catalog with static credentials whose namespace also vends them,
     * spelled the way real servers spell them. Both must land on one key with the namespace's
     * value, or object_store folds them onto one config key and keeps whichever its HashMap yields
     * last - independently in the FE and in the BE.
     */
    @Test
    public void testVendedCredentialsSupersedeTheCatalogsOnS3() {
        Map<String, String> vended = new HashMap<>();
        vended.put("access_key_id", "vended-ak");
        vended.put("secret_access_key", "vended-sk");
        vended.put("session_token", "vended-token");
        vended.put("region", "eu-west-1");
        vended.put("virtual_hosted_style_request", "true");

        Map<String, String> merged =
                LanceStorageOptions.forDataset(S3_URI, minioCatalog(), vended);

        Assertions.assertEquals("vended-ak", merged.get("aws_access_key_id"));
        Assertions.assertEquals("vended-sk", merged.get("aws_secret_access_key"));
        Assertions.assertEquals("vended-token", merged.get("aws_session_token"));
        Assertions.assertEquals("eu-west-1", merged.get("aws_region"));
        Assertions.assertEquals("true", merged.get("aws_virtual_hosted_style_request"));
        // The catalog's endpoint is untouched, and no unprefixed twin survives anywhere.
        Assertions.assertEquals("http://minio:9000", merged.get("aws_endpoint"));
        for (String alias : vended.keySet()) {
            Assertions.assertNull(merged.get(alias), alias + " must not survive beside its twin");
        }
    }

    /** Every accepted spelling has to collapse, or the race just moves to the ones missed. */
    @Test
    public void testEveryS3AliasCollapsesOntoOneEntry() {
        for (String alias : new String[] {"endpoint", "endpoint_url", "aws_endpoint",
                "aws_endpoint_url", "ENDPOINT", "AWS_Endpoint_Url"}) {
            Map<String, String> vended = new HashMap<>();
            vended.put(alias, "http://127.0.0.1:9000");

            Map<String, String> merged =
                    LanceStorageOptions.forDataset(S3_URI, minioCatalog(), vended);
            long endpoints = merged.keySet().stream().filter(k -> k.contains("endpoint")).count();
            Assertions.assertEquals(1, endpoints, alias + " left a competing entry");
            Assertions.assertEquals("http://127.0.0.1:9000", merged.get("aws_endpoint"),
                    alias + " did not win");
        }
    }

    /**
     * {@code token} means an S3 session token to object_store's S3 parser and a bearer token to
     * its Azure one. Knowing the provider is what makes it resolvable at all.
     */
    @Test
    public void testAmbiguousTokenResolvesOnceTheProviderIsKnown() {
        Map<String, String> properties = minioProperties();
        properties.put("s3.session_token", "static-token");
        List<StorageProperties> catalog = createAll(properties);

        Map<String, String> vended = new HashMap<>();
        vended.put("token", "vended-token");

        Map<String, String> onS3 = LanceStorageOptions.forDataset(S3_URI, catalog, vended);
        Assertions.assertEquals("vended-token", onS3.get("aws_session_token"));
        Assertions.assertNull(onS3.get("token"));

        Map<String, String> onAzure = LanceStorageOptions.forDataset(
                "az://container/table.lance", catalog, vended);
        Assertions.assertEquals("vended-token", onAzure.get("token"));
        Assertions.assertNull(onAzure.get("aws_session_token"));
    }

    /**
     * The regression that motivated all of this: object_store's Azure parser reads
     * {@code endpoint} but not {@code aws_endpoint}, and Lance's OSS provider requires
     * {@code endpoint} and reads {@code access_key_id}. Rewriting those onto the S3 spellings
     * leaves the dataset unreachable, so a non-S3 dataset must come through untouched.
     */
    @Test
    public void testNonS3DatasetsAreNeverRewritten() {
        Map<String, String> vended = new HashMap<>();
        vended.put("endpoint", "http://azurite:10000");
        vended.put("access_key_id", "vended-ak");
        vended.put("secret_access_key", "vended-sk");

        for (String uri : new String[] {"az://container/table.lance", "abfss://fs@acct/table",
                "oss://bucket/table.lance", "gs://bucket/table.lance", "cos://bucket/table",
                "file:///tmp/table.lance"}) {
            Map<String, String> merged =
                    LanceStorageOptions.forDataset(uri, minioCatalog(), vended);
            Assertions.assertEquals(vended, merged,
                    uri + " must reach Lance exactly as the namespace wrote it");
        }
    }

    /**
     * A filesystem catalog routes on its warehouse URL, so a local warehouse must not be handed
     * S3 options - and a REST namespace, which is reached over HTTP and reads no storage of its
     * own, reports no URL at all and gets nothing.
     */
    @Test
    public void testCatalogWithoutAnS3UrlGetsNoS3Options() {
        for (String uri : new String[] {"file:///warehouse/lance", "", null}) {
            Assertions.assertTrue(
                    LanceStorageOptions.forDataset(uri, minioCatalog(), null).isEmpty(),
                    "expected no options for " + uri);
        }
    }

    /** Doris has no Lance vocabulary for a non-S3 provider yet, so it contributes none. */
    @Test
    public void testCatalogPropertiesAreNotAppliedToANonS3Dataset() {
        Map<String, String> merged = LanceStorageOptions.forDataset(
                "oss://bucket/table.lance", minioCatalog(), null);
        Assertions.assertTrue(merged.isEmpty(), "S3 credentials must not leak onto another provider");
    }

    /**
     * The list is not user-ordered - it follows {@code StorageProperties.PROVIDERS} and leads with
     * a default HDFS entry - so the S3-compatible one has to be found by type, not by index.
     */
    @Test
    public void testSelectionSkipsNonS3CompatibleEntries() {
        List<StorageProperties> catalog = minioCatalog();
        Assertions.assertTrue(catalog.size() > 1,
                "expected Doris to add its default non-S3 entry ahead of the S3 one");
        Assertions.assertEquals("ak",
                LanceStorageOptions.forDataset(S3_URI, catalog, null).get("aws_access_key_id"));
    }

    @Test
    public void testUnknownVendedOptionsArePassedThroughOnS3Too() {
        Map<String, String> vended = new HashMap<>();
        // Lance reads this on its namespace-backed refresh path; Doris assigns it no meaning.
        vended.put("expires_at_millis", "1760000000000");
        vended.put("azure_storage_sas_token", "sas");
        // Empty is expressible as a C string, so it is carried rather than second-guessed.
        vended.put("deliberately_empty", "");

        Map<String, String> merged =
                LanceStorageOptions.forDataset(S3_URI, Collections.emptyList(), vended);
        Assertions.assertEquals(vended, merged);
    }

    @Test
    public void testAbsentVendedOptionsLeaveCatalogOptionsIntact() {
        Map<String, String> catalogOptions =
                LanceStorageOptions.forDataset(S3_URI, minioCatalog(), null);
        Assertions.assertEquals(catalogOptions,
                LanceStorageOptions.forDataset(S3_URI, minioCatalog(), null));
        Assertions.assertEquals(catalogOptions,
                LanceStorageOptions.forDataset(S3_URI, minioCatalog(), new HashMap<>()));
    }

    /** A namespace contradicting itself is not something to resolve by coin toss. */
    @Test
    public void testConflictingSpellingsOfOneOptionAreRejected() {
        Map<String, String> vended = new HashMap<>();
        vended.put("access_key_id", "one");
        vended.put("aws_access_key_id", "another");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.forDataset(S3_URI, minioCatalog(), vended));

        // Agreeing on the value is not a conflict.
        Map<String, String> agreeing = new HashMap<>();
        agreeing.put("access_key_id", "same");
        agreeing.put("aws_access_key_id", "same");
        Assertions.assertEquals("same", LanceStorageOptions
                .forDataset(S3_URI, minioCatalog(), agreeing).get("aws_access_key_id"));
    }

    /**
     * lance-c reads these as C strings, so a NUL truncates the option there while the FE keeps
     * reading the whole key. Dropping it would only move the divergence, so it has to fail.
     */
    @Test
    public void testOptionsThatCannotReachTheBackendAreRejected() {
        Map<String, String> withNulKey = new HashMap<>();
        withNulKey.put("bucket\0ignored", "other-bucket");
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceStorageOptions
                .forDataset(S3_URI, Collections.emptyList(), withNulKey));

        Map<String, String> withNulValue = new HashMap<>();
        withNulValue.put("aws_region", "us-east-1\0ignored");
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceStorageOptions
                .forDataset(S3_URI, Collections.emptyList(), withNulValue));

        Map<String, String> withNullValue = new HashMap<>();
        withNullValue.put("aws_region", null);
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceStorageOptions
                .forDataset(S3_URI, Collections.emptyList(), withNullValue));

        Map<String, String> withNullKey = new HashMap<>();
        withNullKey.put(null, "value");
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceStorageOptions
                .forDataset(S3_URI, Collections.emptyList(), withNullKey));
    }
}
