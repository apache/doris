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

package org.apache.doris.filesystem.spi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Shared Azure endpoint predicate used positively by the Azure provider and negatively by the
 * S3-compatible fallback providers (MinIO). Semantics frozen from the legacy fe-core
 * {@code AzurePropertyUtils.isAzureBlobEndpoint}: URI/host extraction, lowercase, dot-anchored
 * endsWith, builtin 8-suffix default, and the {@code _AZURE_HOST_SUFFIXES_} probe override.
 */
class AzureBlobEndpointSignalsTest {

    @Test
    void builtinDefaultsCoverBlobAndDfsAcrossSovereignClouds() {
        Map<String, String> empty = new HashMap<>();
        for (String endpoint : new String[] {
                "https://acct.blob.core.windows.net",
                "https://acct.dfs.core.windows.net",
                "https://acct.blob.core.chinacloudapi.cn",
                "https://acct.dfs.core.chinacloudapi.cn",
                "https://acct.blob.core.usgovcloudapi.net",
                "https://acct.dfs.core.usgovcloudapi.net",
                "https://acct.blob.core.cloudapi.de",
                "https://acct.dfs.core.cloudapi.de"}) {
            Assertions.assertTrue(AzureBlobEndpointSignals.isAzureBlobEndpoint(endpoint, empty),
                    endpoint);
        }
        Assertions.assertFalse(
                AzureBlobEndpointSignals.isAzureBlobEndpoint("http://127.0.0.1:9000", empty));
        // dot-anchored: a lookalike host must not match
        Assertions.assertFalse(AzureBlobEndpointSignals.isAzureBlobEndpoint(
                "https://evilblob.core.windows.net.attacker.io", empty));
    }

    @Test
    void hostExtractionStripsSchemePathAndPort() {
        Map<String, String> empty = new HashMap<>();
        Assertions.assertTrue(AzureBlobEndpointSignals.isAzureBlobEndpoint(
                "ACCT.BLOB.CORE.WINDOWS.NET:443/container/key", empty));
        Assertions.assertTrue(AzureBlobEndpointSignals.isAzureBlobEndpoint(
                "https://acct.dfs.core.usgovcloudapi.net:8443/fs/dir", empty));
    }

    @Test
    void probeOverrideReplacesDefaults() {
        Map<String, String> probe = new HashMap<>();
        probe.put(AzureBlobEndpointSignals.HOST_SUFFIXES_PROBE_KEY, "blob.private.example");
        Assertions.assertTrue(AzureBlobEndpointSignals.isAzureBlobEndpoint(
                "https://a.blob.private.example", probe));
        // override REPLACES the builtin list, exactly like Config.azure_blob_host_suffixes
        Assertions.assertFalse(AzureBlobEndpointSignals.isAzureBlobEndpoint(
                "https://acct.blob.core.windows.net", probe));
    }

    @Test
    void guessConsultsAzureEndpointAliasOrder() {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", "https://acct.dfs.core.usgovcloudapi.net");
        Assertions.assertTrue(AzureBlobEndpointSignals.guessIsAzureBlobEndpoint(props));

        // minio.endpoint is NOT an Azure alias: a map with only a private minio endpoint is
        // not Azure's, even if some unrelated value looks azure-ish.
        Map<String, String> minioOnly = new HashMap<>();
        minioOnly.put("minio.endpoint", "https://acct.blob.core.windows.net");
        Assertions.assertFalse(AzureBlobEndpointSignals.guessIsAzureBlobEndpoint(minioOnly));

        Assertions.assertFalse(AzureBlobEndpointSignals.guessIsAzureBlobEndpoint(new HashMap<>()));
    }
}
