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

package org.apache.doris.fs;

import org.apache.doris.common.Config;
import org.apache.doris.datasource.property.common.AwsCredentialsProviderMode;
import org.apache.doris.datasource.property.storage.AzureProperties;
import org.apache.doris.datasource.storage.StorageAdapter;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Golden tests for the Azure guess-routing port (legacy {@code AzureProperties.guessIsMe} +
 * {@code AzurePropertyUtils.isAzureBlobEndpoint} are the oracle until Phase D deletes them).
 * This covers the review-found drift: host extraction, case-insensitivity, dot-anchored suffix
 * matching, the dfs (ADLS Gen2) suffixes, and the Config-extensible suffix list carried into
 * the plugin through the probe-context injection.
 */
public class AzureGuessRoutingParityTest {

    private static final List<Map<String, String>> GUESS_BATTERY = Arrays.asList(
            // dfs (ADLS Gen2) endpoint — the drift that could abort journal replay
            ImmutableMap.of("s3.endpoint", "https://acct.dfs.core.windows.net"),
            // plain blob endpoint
            ImmutableMap.of("s3.endpoint", "https://acct.blob.core.windows.net"),
            // sovereign clouds, dfs variant
            ImmutableMap.of("s3.endpoint", "https://acct.dfs.core.chinacloudapi.cn"),
            // uppercase host must match (legacy lowercases the host)
            ImmutableMap.of("s3.endpoint", "HTTPS://ACCT.DFS.CORE.WINDOWS.NET"),
            // host-only spelling (no scheme)
            ImmutableMap.of("s3.endpoint", "acct.blob.core.windows.net"),
            // host:port spelling
            ImmutableMap.of("s3.endpoint", "acct.blob.core.windows.net:443"),
            // suffix in the PATH of a foreign host must NOT match (legacy extracts the host)
            ImmutableMap.of("s3.endpoint", "http://evil.com/blob.core.windows.net"),
            // suffix not dot-anchored must NOT match
            ImmutableMap.of("s3.endpoint", "https://acctblob.core.windows.netx"),
            // plain AWS endpoint must NOT match
            ImmutableMap.of("s3.endpoint", "s3.us-east-1.amazonaws.com"),
            // azure.endpoint alias is consulted
            ImmutableMap.of("azure.endpoint", "https://acct.blob.core.windows.net"),
            // AZURE_ENDPOINT was never a legacy guess alias — must NOT match through it
            ImmutableMap.of("AZURE_ENDPOINT", "https://acct.blob.core.windows.net"),
            // provider=azure shortcut
            ImmutableMap.of("provider", "AzUrE"),
            // no endpoint at all
            ImmutableMap.of("hive.metastore.uris", "thrift://127.0.0.1:9083"));

    @Test
    public void testGuessAgreesWithLegacyOracle() {
        for (Map<String, String> props : GUESS_BATTERY) {
            boolean legacy = AzureProperties.guessIsMe(props);
            boolean facade = StorageAdapter.matchesProviderGuess("AZURE", props);
            Assertions.assertEquals(legacy, facade, "props: " + props);
        }
    }

    @Test
    public void testConfigExtendedSuffixReachesThePlugin() {
        String[] saved = Config.azure_blob_host_suffixes;
        try {
            String[] extended = Arrays.copyOf(saved, saved.length + 1);
            extended[saved.length] = ".custom.mycloud.example";
            Config.azure_blob_host_suffixes = extended;
            Map<String, String> props = ImmutableMap.of("s3.endpoint", "https://acct.custom.mycloud.example");
            Assertions.assertTrue(AzureProperties.guessIsMe(props), "legacy oracle should honour Config");
            Assertions.assertTrue(StorageAdapter.matchesProviderGuess("AZURE", props),
                    "probe-context injection must carry the extended suffix into the plugin");
        } finally {
            Config.azure_blob_host_suffixes = saved;
        }
    }

    @Test
    public void testDfsEndpointRoutesToAzureViaBindPrimary() {
        // The replay-shaped scenario: raw persisted props with a dfs endpoint must route AZURE
        // through the full bind path, exactly like legacy createPrimary did.
        Map<String, String> props = ImmutableMap.of(
                "s3.endpoint", "https://acct.dfs.core.windows.net",
                "s3.access_key", "acct",
                "s3.secret_key", "c2s=",
                "s3.bucket", "container");
        StorageAdapter adapter = StorageAdapter.of(props);
        Assertions.assertEquals("AZURE", adapter.getStorageName());
    }

    @Test
    public void testAwsCredentialsProviderModeMirrorsLegacyAliases() {
        // Facade getAwsCredentialsProviderMode: same alias family and default as legacy
        // S3Properties (s3. > glue. > iceberg.rest., default DEFAULT).
        Assertions.assertEquals(AwsCredentialsProviderMode.DEFAULT,
                StorageAdapter.of(ImmutableMap.of(
                        "s3.endpoint", "s3.us-east-1.amazonaws.com",
                        "s3.access_key", "ak", "s3.secret_key", "sk"))
                        .getAwsCredentialsProviderMode());
        Assertions.assertEquals(AwsCredentialsProviderMode.INSTANCE_PROFILE,
                StorageAdapter.of(ImmutableMap.of(
                        "s3.endpoint", "s3.us-east-1.amazonaws.com",
                        "s3.credentials_provider_type", "instance_profile"))
                        .getAwsCredentialsProviderMode());
        Assertions.assertEquals(AwsCredentialsProviderMode.ENV,
                StorageAdapter.of(ImmutableMap.of(
                        "s3.endpoint", "s3.us-east-1.amazonaws.com",
                        "glue.credentials_provider_type", "env"))
                        .getAwsCredentialsProviderMode());
    }
}
