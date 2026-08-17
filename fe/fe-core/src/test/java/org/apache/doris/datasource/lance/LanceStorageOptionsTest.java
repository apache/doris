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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LanceStorageOptionsTest {

    private static Map<String, String> minioCatalogProperties() {
        Map<String, String> backendProperties = new HashMap<>();
        backendProperties.put("AWS_ACCESS_KEY", "ak");
        backendProperties.put("AWS_SECRET_KEY", "sk");
        backendProperties.put("AWS_ENDPOINT", "http://minio:9000");
        backendProperties.put("AWS_REGION", "us-east-1");
        backendProperties.put("use_path_style", "true");
        return backendProperties;
    }

    @Test
    public void testMinioStorageOptionMapping() {
        Map<String, String> backendProperties = minioCatalogProperties();
        backendProperties.put("AWS_TOKEN", "token");

        Map<String, String> options = LanceStorageOptions.toLanceOptions(backendProperties);
        Assertions.assertEquals("ak", options.get("access_key_id"));
        Assertions.assertEquals("sk", options.get("secret_access_key"));
        Assertions.assertEquals("token", options.get("session_token"));
        Assertions.assertEquals("http://minio:9000", options.get("endpoint"));
        Assertions.assertEquals("us-east-1", options.get("region"));
        Assertions.assertEquals("true", options.get("allow_http"));
        Assertions.assertEquals("false", options.get("virtual_hosted_style_request"));
    }

    /**
     * Lance reaches object storage through two backends. Only the unprefixed spellings are accepted
     * by both, so emitting the prefixed ones would drop every credential on the backend that does
     * not normalize aliases.
     */
    @Test
    public void testEmittedOptionsUseTheSpellingBothLanceBackendsAccept() {
        Map<String, String> options = LanceStorageOptions.toLanceOptions(minioCatalogProperties());
        Assertions.assertNull(options.get("aws_access_key_id"));
        Assertions.assertNull(options.get("aws_secret_access_key"));
        Assertions.assertNull(options.get("aws_endpoint"));
        Assertions.assertNull(options.get("aws_region"));
    }

    @Test
    public void testOptionalPropertiesAreOmittedRatherThanEmpty() {
        Map<String, String> backendProperties = new HashMap<>();
        backendProperties.put("AWS_ACCESS_KEY", "ak");
        backendProperties.put("AWS_SECRET_KEY", "");
        backendProperties.put("AWS_ENDPOINT", "https://s3.amazonaws.com");

        Map<String, String> options = LanceStorageOptions.toLanceOptions(backendProperties);
        Assertions.assertEquals("ak", options.get("access_key_id"));
        Assertions.assertNull(options.get("secret_access_key"));
        Assertions.assertNull(options.get("session_token"));
        // allow_http only makes sense for a plain-HTTP endpoint.
        Assertions.assertNull(options.get("allow_http"));
        Assertions.assertNull(options.get("virtual_hosted_style_request"));
    }

    /**
     * object_store folds an alias and its canonical name onto one config key and keeps just one of
     * the values, picked by hash order. Letting both spellings through would therefore leave the
     * effective credentials and addressing style up to chance, so a vended alias has to replace the
     * catalog's entry rather than sit beside it.
     */
    @Test
    public void testVendedPrefixedAliasReplacesTheCatalogEntry() {
        Map<String, String> vended = new HashMap<>();
        vended.put("aws_access_key_id", "vended-ak");
        vended.put("aws_secret_access_key", "vended-sk");
        vended.put("aws_endpoint", "http://127.0.0.1:9000");
        vended.put("aws_region", "eu-west-1");
        vended.put("aws_virtual_hosted_style_request", "true");
        vended.put("aws_session_token", "vended-token");

        Map<String, String> merged = LanceStorageOptions.mergeVended(
                LanceStorageOptions.toLanceOptions(minioCatalogProperties()), vended);

        Assertions.assertEquals("vended-ak", merged.get("access_key_id"));
        Assertions.assertEquals("vended-sk", merged.get("secret_access_key"));
        Assertions.assertEquals("http://127.0.0.1:9000", merged.get("endpoint"));
        Assertions.assertEquals("eu-west-1", merged.get("region"));
        Assertions.assertEquals("true", merged.get("virtual_hosted_style_request"));
        Assertions.assertEquals("vended-token", merged.get("session_token"));

        // No prefixed duplicate may survive alongside the value it replaced.
        for (String alias : new String[] {"aws_access_key_id", "aws_secret_access_key",
                "aws_endpoint", "aws_region", "aws_virtual_hosted_style_request",
                "aws_session_token"}) {
            Assertions.assertNull(merged.get(alias), alias + " must not survive the merge");
        }
    }

    /**
     * object_store accepts four spellings of the endpoint and four of the session token. Any one
     * this class fails to recognize reintroduces the race, so the whole equivalence class has to
     * collapse onto a single entry.
     */
    @Test
    public void testEveryAcceptedAliasCollapsesOntoOneEntry() {
        for (String alias : new String[] {"endpoint", "endpoint_url", "aws_endpoint",
                "aws_endpoint_url", "ENDPOINT", "AWS_Endpoint_Url"}) {
            Map<String, String> vended = new HashMap<>();
            vended.put(alias, "http://127.0.0.1:9000");

            Map<String, String> merged = LanceStorageOptions.mergeVended(
                    LanceStorageOptions.toLanceOptions(minioCatalogProperties()), vended);

            long endpoints = merged.entrySet().stream()
                    .filter(e -> e.getKey().toLowerCase(java.util.Locale.ROOT).contains("endpoint"))
                    .count();
            Assertions.assertEquals(1, endpoints, "alias " + alias + " left a competing entry");
            Assertions.assertEquals("http://127.0.0.1:9000", merged.get("endpoint"),
                    "alias " + alias + " did not win");
        }
    }

    /**
     * {@code token} is an S3 session token to object_store but a bearer token to its Azure parser,
     * so it keeps the namespace's spelling - it still has to displace the catalog's entry though.
     */
    @Test
    public void testAmbiguousAliasSupersedesWithoutBeingRenamed() {
        Map<String, String> catalogProperties = minioCatalogProperties();
        catalogProperties.put("AWS_TOKEN", "static-token");

        Map<String, String> vended = new HashMap<>();
        vended.put("token", "vended-token");

        Map<String, String> merged = LanceStorageOptions.mergeVended(
                LanceStorageOptions.toLanceOptions(catalogProperties), vended);
        Assertions.assertEquals("vended-token", merged.get("token"));
        Assertions.assertNull(merged.get("session_token"));
    }

    @Test
    public void testVendedUnprefixedOptionsReplaceTheCatalogEntry() {
        Map<String, String> vended = new HashMap<>();
        vended.put("access_key_id", "vended-ak");
        vended.put("endpoint", "http://127.0.0.1:9000");

        Map<String, String> merged = LanceStorageOptions.mergeVended(
                LanceStorageOptions.toLanceOptions(minioCatalogProperties()), vended);
        Assertions.assertEquals("vended-ak", merged.get("access_key_id"));
        Assertions.assertEquals("http://127.0.0.1:9000", merged.get("endpoint"));
        Assertions.assertEquals("sk", merged.get("secret_access_key"));
    }

    /**
     * A namespace can supply the only endpoint there is, or replace the catalog's, so whether plain
     * HTTP is allowed has to follow the endpoint that ends up in use.
     */
    @Test
    public void testAllowHttpFollowsTheEndpointActuallyUsed() {
        Map<String, String> catalogWithoutEndpoint = new HashMap<>();
        catalogWithoutEndpoint.put("AWS_ACCESS_KEY", "ak");

        Map<String, String> vended = new HashMap<>();
        vended.put("endpoint", "http://127.0.0.1:9000");

        Map<String, String> merged = LanceStorageOptions.mergeVended(
                LanceStorageOptions.toLanceOptions(catalogWithoutEndpoint), vended);
        Assertions.assertEquals("true", merged.get("allow_http"));

        // An explicitly vended value is respected rather than re-derived.
        Map<String, String> vendedWithFlag = new HashMap<>();
        vendedWithFlag.put("endpoint", "http://127.0.0.1:9000");
        vendedWithFlag.put("allow_http", "false");
        Assertions.assertEquals("false", LanceStorageOptions.mergeVended(
                Collections.emptyMap(), vendedWithFlag).get("allow_http"));
    }

    /**
     * The catalog's plain-HTTP endpoint derives allow_http. Replacing it with an HTTPS endpoint has
     * to retract that, or the merged options keep permitting plain HTTP for an endpoint that never
     * asked for it.
     */
    @Test
    public void testAllowHttpIsRetractedWhenTheEndpointBecomesHttps() {
        Map<String, String> catalogOptions =
                LanceStorageOptions.toLanceOptions(minioCatalogProperties());
        Assertions.assertEquals("true", catalogOptions.get("allow_http"));

        Map<String, String> vended = new HashMap<>();
        vended.put("endpoint", "https://s3.amazonaws.com");

        Map<String, String> merged = LanceStorageOptions.mergeVended(catalogOptions, vended);
        Assertions.assertEquals("https://s3.amazonaws.com", merged.get("endpoint"));
        Assertions.assertNull(merged.get("allow_http"));
    }

    /**
     * Lance recognizes this key on its namespace-backed refresh path. lance-c opens datasets with
     * static options, so it has no effect on the BE today, but the option map is meant to reach
     * Lance as the namespace wrote it.
     */
    @Test
    public void testUnrecognizedVendedOptionsArePassedThrough() {
        Map<String, String> vended = new HashMap<>();
        vended.put("access_key_id", "vended-ak");
        vended.put("expires_at_millis", "1760000000000");
        vended.put("refresh_offset_millis", "60000");

        Map<String, String> merged = LanceStorageOptions.mergeVended(Collections.emptyMap(), vended);
        Assertions.assertEquals("1760000000000", merged.get("expires_at_millis"));
        Assertions.assertEquals("60000", merged.get("refresh_offset_millis"));
    }

    /** Options for other providers must survive too, or the catalog only ever works on S3. */
    @Test
    public void testNonS3VendedOptionsArePassedThrough() {
        Map<String, String> vended = new HashMap<>();
        vended.put("azure_storage_sas_token", "sas");
        vended.put("google_storage_token", "gcp-token");

        Map<String, String> merged = LanceStorageOptions.mergeVended(Collections.emptyMap(), vended);
        Assertions.assertEquals("sas", merged.get("azure_storage_sas_token"));
        Assertions.assertEquals("gcp-token", merged.get("google_storage_token"));
    }

    @Test
    public void testVendedOptionsCannotRedirectWhichDataIsRead() {
        Map<String, String> vended = new HashMap<>();
        vended.put("access_key_id", "vended-ak");
        vended.put("bucket", "attacker-bucket");
        vended.put("aws_bucket_name", "attacker-bucket");
        vended.put("ROOT", "/elsewhere");

        Map<String, String> merged = LanceStorageOptions.mergeVended(Collections.emptyMap(), vended);
        Assertions.assertEquals("vended-ak", merged.get("access_key_id"));
        Assertions.assertNull(merged.get("bucket"));
        Assertions.assertNull(merged.get("aws_bucket_name"));
        Assertions.assertNull(merged.get("ROOT"));
    }

    @Test
    public void testAbsentVendedOptionsLeaveCatalogOptionsIntact() {
        Map<String, String> catalogOptions = LanceStorageOptions.toLanceOptions(minioCatalogProperties());
        Assertions.assertEquals(catalogOptions,
                LanceStorageOptions.mergeVended(catalogOptions, null));
        Assertions.assertEquals(catalogOptions,
                LanceStorageOptions.mergeVended(catalogOptions, new HashMap<>()));

        Map<String, String> vended = new HashMap<>();
        vended.put("access_key_id", "");
        vended.put(null, "ignored");
        Assertions.assertEquals(catalogOptions,
                LanceStorageOptions.mergeVended(catalogOptions, vended));
    }
}
