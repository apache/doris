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
import java.util.Map;
import java.util.TreeSet;

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
        Assertions.assertNull(options.get("aws_virtual_hosted_style_request"));
        // allow_http only makes sense for a plain-HTTP endpoint.
        Assertions.assertNull(options.get("allow_http"));
    }

    @Test
    public void testVendedOptionReplacesTheCatalogEntryUnderTheSameKey() {
        Map<String, String> vended = new HashMap<>();
        vended.put("aws_access_key_id", "vended-ak");
        vended.put("aws_endpoint", "http://127.0.0.1:9000");

        Map<String, String> merged = LanceStorageOptions.mergeVended(
                LanceStorageOptions.toLanceOptions(minioCatalogProperties()), vended);
        Assertions.assertEquals("vended-ak", merged.get("aws_access_key_id"));
        Assertions.assertEquals("http://127.0.0.1:9000", merged.get("aws_endpoint"));
        // Untouched catalog options stay.
        Assertions.assertEquals("sk", merged.get("aws_secret_access_key"));
        Assertions.assertEquals("us-east-1", merged.get("aws_region"));
    }

    /**
     * The accepted spellings differ per provider - object_store's Azure parser reads
     * {@code endpoint} but not {@code aws_endpoint}, and Lance's OSS provider requires
     * {@code endpoint} and reads {@code access_key_id}. This class cannot tell which provider a
     * dataset uses, so renaming a vended key onto the S3 spelling would destroy a working
     * configuration for every non-S3 backend. Vended keys must arrive verbatim.
     */
    @Test
    public void testVendedKeysAreNeverRenamed() {
        Map<String, String> vended = new HashMap<>();
        vended.put("endpoint", "http://azurite:10000");
        vended.put("access_key_id", "vended-ak");
        vended.put("secret_access_key", "vended-sk");
        vended.put("token", "vended-token");
        vended.put("virtual_hosted_style_request", "true");

        Map<String, String> merged = LanceStorageOptions.mergeVended(
                LanceStorageOptions.toLanceOptions(minioCatalogProperties()), vended);
        for (Map.Entry<String, String> entry : vended.entrySet()) {
            Assertions.assertEquals(entry.getValue(), merged.get(entry.getKey()),
                    entry.getKey() + " must reach Lance as the namespace wrote it");
        }
    }

    /** Options for other providers must survive too, or the catalog only ever works on S3. */
    @Test
    public void testUnknownAndNonS3VendedOptionsArePassedThrough() {
        Map<String, String> vended = new HashMap<>();
        vended.put("azure_storage_sas_token", "sas");
        vended.put("google_storage_token", "gcp-token");
        vended.put("oss_endpoint", "https://oss-cn-hangzhou.aliyuncs.com");
        // Lance reads this on its namespace-backed refresh path; Doris assigns it no meaning.
        vended.put("expires_at_millis", "1760000000000");
        // Empty is expressible as a C string, so it is carried rather than second-guessed.
        vended.put("deliberately_empty", "");

        Map<String, String> merged = LanceStorageOptions.mergeVended(Collections.emptyMap(), vended);
        Assertions.assertEquals(vended, merged);
    }

    /**
     * Options that would change which data is read are not filtered here. The namespace already
     * decides the dataset URI, so this was never a boundary, and Lance protects what it needs to.
     */
    @Test
    public void testVendedOptionsAreNotFiltered() {
        Map<String, String> vended = new HashMap<>();
        vended.put("bucket", "other-bucket");
        vended.put("root", "/elsewhere");

        Map<String, String> merged = LanceStorageOptions.mergeVended(Collections.emptyMap(), vended);
        Assertions.assertEquals("other-bucket", merged.get("bucket"));
        Assertions.assertEquals("/elsewhere", merged.get("root"));
    }

    @Test
    public void testAbsentVendedOptionsLeaveCatalogOptionsIntact() {
        Map<String, String> catalogOptions =
                LanceStorageOptions.toLanceOptions(minioCatalogProperties());
        Assertions.assertEquals(catalogOptions,
                LanceStorageOptions.mergeVended(catalogOptions, null));
        Assertions.assertEquals(catalogOptions,
                LanceStorageOptions.mergeVended(catalogOptions, new HashMap<>()));
    }

    /**
     * lance-c reads these as C strings, so a NUL truncates the option there while the FE keeps
     * reading the whole key. Dropping it would only move the divergence - an FE that drops it and
     * a BE that does not disagree the same way - so it has to fail.
     */
    @Test
    public void testOptionsThatCannotReachTheBackendAreRejected() {
        Map<String, String> withNulKey = new HashMap<>();
        withNulKey.put("bucket\0ignored", "other-bucket");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.mergeVended(Collections.emptyMap(), withNulKey));

        Map<String, String> withNulValue = new HashMap<>();
        withNulValue.put("aws_region", "us-east-1\0ignored");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.mergeVended(Collections.emptyMap(), withNulValue));

        Map<String, String> withNullValue = new HashMap<>();
        withNullValue.put("aws_region", null);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.mergeVended(Collections.emptyMap(), withNullValue));

        Map<String, String> withNullKey = new HashMap<>();
        withNullKey.put(null, "value");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceStorageOptions.mergeVended(Collections.emptyMap(), withNullKey));
    }
}
