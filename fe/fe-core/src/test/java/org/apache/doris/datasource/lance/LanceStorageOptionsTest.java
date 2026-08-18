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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

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
        Assertions.assertEquals("ak", options.get("aws_access_key_id"));
        Assertions.assertEquals("sk", options.get("aws_secret_access_key"));
        Assertions.assertEquals("token", options.get("aws_session_token"));
        Assertions.assertEquals("http://minio:9000", options.get("aws_endpoint"));
        // object_store prefers its S3-specific endpoint key, so both carry the same value.
        Assertions.assertEquals("http://minio:9000", options.get("aws_endpoint_url_s3"));
        Assertions.assertEquals("us-east-1", options.get("aws_region"));
        Assertions.assertEquals("true", options.get("allow_http"));
        Assertions.assertEquals("false", options.get("aws_virtual_hosted_style_request"));
    }

    /**
     * Lance folds the process environment into the options before it builds a store, and skips an
     * environment value only when the map already holds the spelling object_store reports as
     * canonical. Emitting any other accepted alias lets AWS_* variables in alongside the catalog's
     * own values, where object_store resolves both to one config key and keeps whichever its
     * HashMap happens to yield last.
     */
    @Test
    public void testEmittedOptionsUseTheSpellingThatSuppressesTheEnvironment() {
        Map<String, String> backendProperties = minioCatalogProperties();
        backendProperties.put("AWS_TOKEN", "token");

        Map<String, String> options = LanceStorageOptions.toLanceOptions(backendProperties);
        // Exactly the keys AmazonS3ConfigKey::as_ref() reports; allow_http is canonical unprefixed
        // because object_store carries it as a shared client option rather than an S3 one.
        Assertions.assertEquals(
                new TreeSet<>(Arrays.asList("aws_access_key_id", "aws_secret_access_key",
                        "aws_session_token", "aws_endpoint", "aws_endpoint_url_s3", "aws_region",
                        "aws_virtual_hosted_style_request", "allow_http")),
                new TreeSet<>(options.keySet()));
    }

    @Test
    public void testOptionalPropertiesAreOmittedRatherThanEmpty() {
        Map<String, String> backendProperties = new HashMap<>();
        backendProperties.put("AWS_ACCESS_KEY", "ak");
        backendProperties.put("AWS_SECRET_KEY", "");
        backendProperties.put("AWS_ENDPOINT", "https://s3.amazonaws.com");

        Map<String, String> options = LanceStorageOptions.toLanceOptions(backendProperties);
        Assertions.assertEquals("ak", options.get("aws_access_key_id"));
        Assertions.assertNull(options.get("aws_secret_access_key"));
        Assertions.assertNull(options.get("aws_session_token"));
        // allow_http only makes sense for a plain-HTTP endpoint.
        Assertions.assertNull(options.get("allow_http"));
        Assertions.assertNull(options.get("aws_virtual_hosted_style_request"));
    }

    /**
     * object_store folds an alias and its canonical name onto one config key and keeps just one of
     * the values, picked by hash order. Letting both spellings through would therefore leave the
     * effective credentials and addressing style up to chance, so a vended alias has to replace the
     * catalog's entry rather than sit beside it.
     */
    @Test
    public void testVendedUnprefixedAliasReplacesTheCatalogEntry() {
        Map<String, String> vended = new HashMap<>();
        vended.put("access_key_id", "vended-ak");
        vended.put("secret_access_key", "vended-sk");
        vended.put("endpoint", "http://127.0.0.1:9000");
        vended.put("region", "eu-west-1");
        vended.put("virtual_hosted_style_request", "true");
        vended.put("session_token", "vended-token");

        Map<String, String> merged = LanceStorageOptions.mergeVended(
                LanceStorageOptions.toLanceOptions(minioCatalogProperties()), vended);

        Assertions.assertEquals("vended-ak", merged.get("aws_access_key_id"));
        Assertions.assertEquals("vended-sk", merged.get("aws_secret_access_key"));
        Assertions.assertEquals("http://127.0.0.1:9000", merged.get("aws_endpoint"));
        Assertions.assertEquals("eu-west-1", merged.get("aws_region"));
        Assertions.assertEquals("true", merged.get("aws_virtual_hosted_style_request"));
        Assertions.assertEquals("vended-token", merged.get("aws_session_token"));

        // No unprefixed duplicate may survive alongside the value it replaced.
        for (String alias : new String[] {"access_key_id", "secret_access_key", "endpoint",
                "region", "virtual_hosted_style_request", "session_token"}) {
            Assertions.assertNull(merged.get(alias), alias + " must not survive the merge");
        }
    }

    @Test
    public void testVendedPrefixedOptionsReplaceTheCatalogEntry() {
        Map<String, String> vended = new HashMap<>();
        vended.put("aws_access_key_id", "vended-ak");
        vended.put("aws_endpoint", "http://127.0.0.1:9000");

        Map<String, String> merged = LanceStorageOptions.mergeVended(
                LanceStorageOptions.toLanceOptions(minioCatalogProperties()), vended);
        Assertions.assertEquals("vended-ak", merged.get("aws_access_key_id"));
        Assertions.assertEquals("http://127.0.0.1:9000", merged.get("aws_endpoint"));
        Assertions.assertEquals("sk", merged.get("aws_secret_access_key"));
    }

    /**
     * object_store accepts five spellings of the endpoint and four of the session token. Any one
     * this class fails to recognize reintroduces the race, so the whole equivalence class has to
     * collapse - onto one value, carried by the two keys object_store actually consults.
     */
    @Test
    public void testEveryAcceptedAliasCollapsesOntoOneValue() {
        for (String alias : new String[] {"endpoint", "endpoint_url", "aws_endpoint",
                "aws_endpoint_url", "aws_endpoint_url_s3", "ENDPOINT", "AWS_Endpoint_Url"}) {
            Map<String, String> vended = new HashMap<>();
            vended.put(alias, "http://127.0.0.1:9000");

            Map<String, String> merged = LanceStorageOptions.mergeVended(
                    LanceStorageOptions.toLanceOptions(minioCatalogProperties()), vended);

            Assertions.assertEquals(
                    new TreeSet<>(Arrays.asList("aws_endpoint", "aws_endpoint_url_s3")),
                    merged.keySet().stream()
                            .filter(key -> key.toLowerCase(Locale.ROOT).contains("endpoint"))
                            .collect(Collectors.toCollection(TreeSet::new)),
                    "alias " + alias + " left a competing entry");
            Assertions.assertEquals("http://127.0.0.1:9000", merged.get("aws_endpoint"),
                    "alias " + alias + " did not win");
            Assertions.assertEquals("http://127.0.0.1:9000", merged.get("aws_endpoint_url_s3"),
                    "alias " + alias + " did not reach the key object_store prefers");
        }
    }

    /**
     * object_store resolves the endpoint as {@code s3_endpoint.or(endpoint)}, so a namespace that
     * vends both spellings means the S3-specific one. Both have to end up on the same value anyway,
     * or the FE and the BE could each read a different key and disagree.
     */
    @Test
    public void testS3SpecificEndpointWinsOverAGenericOneVendedBesideIt() {
        for (boolean s3First : new boolean[] {true, false}) {
            Map<String, String> vended = new LinkedHashMap<>();
            if (s3First) {
                vended.put("aws_endpoint_url_s3", "http://minio:9000");
                vended.put("endpoint", "https://generic.example.com");
            } else {
                vended.put("endpoint", "https://generic.example.com");
                vended.put("aws_endpoint_url_s3", "http://minio:9000");
            }

            Map<String, String> merged = LanceStorageOptions.mergeVended(
                    LanceStorageOptions.toLanceOptions(minioCatalogProperties()), vended);
            Assertions.assertEquals("http://minio:9000", merged.get("aws_endpoint"));
            Assertions.assertEquals("http://minio:9000", merged.get("aws_endpoint_url_s3"));
            Assertions.assertEquals("true", merged.get("allow_http"));
        }
    }

    /**
     * Lance's OpenDAL backend names this option {@code enable_virtual_host_style} and takes the two
     * object_store spellings as serde aliases of it, so two of the three in one map is a duplicate
     * field and the S3 operator fails to build rather than picking a winner.
     */
    @Test
    public void testOpendalVirtualHostStyleSpellingCollapsesToo() {
        for (String alias : new String[] {"virtual_hosted_style_request",
                "aws_virtual_hosted_style_request", "enable_virtual_host_style"}) {
            Map<String, String> vended = new HashMap<>();
            vended.put(alias, "true");

            Map<String, String> merged = LanceStorageOptions.mergeVended(
                    LanceStorageOptions.toLanceOptions(minioCatalogProperties()), vended);

            long entries = merged.keySet().stream()
                    .filter(key -> key.contains("virtual_host"))
                    .count();
            Assertions.assertEquals(1, entries, "alias " + alias + " left a competing entry");
            Assertions.assertEquals("true", merged.get("aws_virtual_hosted_style_request"),
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
        Assertions.assertNull(merged.get("aws_session_token"));
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
        // Every spelling has to retract it, including the canonical one the catalog itself emits:
        // an alias that resolves to no canonical name would leave the derived value standing.
        for (String alias : new String[] {"endpoint", "aws_endpoint", "aws_endpoint_url_s3"}) {
            Map<String, String> catalogOptions =
                    LanceStorageOptions.toLanceOptions(minioCatalogProperties());
            Assertions.assertEquals("true", catalogOptions.get("allow_http"));

            Map<String, String> vended = new HashMap<>();
            vended.put(alias, "https://s3.amazonaws.com");

            Map<String, String> merged = LanceStorageOptions.mergeVended(catalogOptions, vended);
            Assertions.assertEquals("https://s3.amazonaws.com", merged.get("aws_endpoint"),
                    "alias " + alias + " did not replace the endpoint");
            Assertions.assertEquals("https://s3.amazonaws.com", merged.get("aws_endpoint_url_s3"),
                    "alias " + alias + " left a stale S3-specific endpoint");
            Assertions.assertNull(merged.get("allow_http"),
                    "alias " + alias + " left allow_http standing");
        }
    }

    /**
     * The S3-specific endpoint is the one object_store would actually use, so allow_http has to be
     * derived from it rather than from the generic entry it displaces.
     */
    @Test
    public void testAllowHttpFollowsTheS3SpecificEndpoint() {
        Map<String, String> catalogProperties = minioCatalogProperties();
        catalogProperties.put("AWS_ENDPOINT", "https://s3.amazonaws.com");

        Map<String, String> vended = new HashMap<>();
        vended.put("aws_endpoint_url_s3", "http://minio:9000");

        Map<String, String> merged = LanceStorageOptions.mergeVended(
                LanceStorageOptions.toLanceOptions(catalogProperties), vended);
        Assertions.assertEquals("http://minio:9000", merged.get("aws_endpoint"));
        Assertions.assertEquals("true", merged.get("allow_http"));
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
        Assertions.assertEquals("vended-ak", merged.get("aws_access_key_id"));
        Assertions.assertNull(merged.get("bucket"));
        Assertions.assertNull(merged.get("aws_bucket_name"));
        Assertions.assertNull(merged.get("ROOT"));
    }

    /**
     * lance-c reads these options as C strings, so an embedded NUL truncates the key there while
     * this class still sees the full one - the protected-key check and the alias table would both
     * be looking at a key the BE never receives.
     */
    @Test
    public void testVendedOptionsWithEmbeddedNulAreDropped() {
        Map<String, String> vended = new HashMap<>();
        vended.put("access_key_id", "vended-ak");
        vended.put("bucket\0ignored", "attacker-bucket");
        vended.put("endpoint\0ignored", "http://attacker:9000");
        vended.put("region", "eu-west-1\0ignored");

        Map<String, String> merged = LanceStorageOptions.mergeVended(
                LanceStorageOptions.toLanceOptions(minioCatalogProperties()), vended);
        Assertions.assertEquals("vended-ak", merged.get("aws_access_key_id"));
        Assertions.assertEquals("http://minio:9000", merged.get("aws_endpoint"));
        Assertions.assertEquals("us-east-1", merged.get("aws_region"));
        for (String key : merged.keySet()) {
            Assertions.assertFalse(key.indexOf('\0') >= 0, "key " + key + " kept a NUL");
            Assertions.assertFalse(merged.get(key).indexOf('\0') >= 0, key + " kept a NUL value");
        }
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
