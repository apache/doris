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

package org.apache.doris.filesystem.azure;

import org.apache.doris.foundation.property.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class AzureAccountHostTest {

    @ParameterizedTest
    @CsvSource({
            "account.dfs.core.windows.net, account, core.windows.net, true",
            "account.blob.core.windows.net, account, core.windows.net, false",
            "https://account.dfs.core.chinacloudapi.cn, account, core.chinacloudapi.cn, true",
            "account.dfs.core.usgovcloudapi.net, account, core.usgovcloudapi.net, true",
            "account.blob.core.usgovcloudapi.net, account, core.usgovcloudapi.net, false",
            "account.dfs.core.cloudapi.de, account, core.cloudapi.de, true",
            "account.blob.core.cloudapi.de, account, core.cloudapi.de, false"
    })
    void parse_extractsAccountAndCloudSuffix(String input, String account, String suffix, boolean dfs) {
        AzureAccountHost host = AzureAccountHost.parse(input);

        Assertions.assertEquals(account, host.accountName());
        Assertions.assertEquals(suffix, host.cloudSuffix());
        Assertions.assertEquals(dfs, host.isDfsHost());
        Assertions.assertEquals("account.dfs." + suffix, host.dfsHost());
        Assertions.assertEquals("account.blob." + suffix, host.blobHost());
        Assertions.assertEquals("https://account.blob." + suffix, host.blobEndpoint());
    }

    @Test
    void fromAccountName_defaultsToPublicCloud() {
        AzureAccountHost host = AzureAccountHost.fromAccountName("account");

        Assertions.assertEquals("account.dfs.core.windows.net", host.dfsHost());
        Assertions.assertEquals("https://account.blob.core.windows.net", host.blobEndpoint());
    }

    @Test
    void parse_preservesUnknownEndpointSuffix() {
        AzureAccountHost host = AzureAccountHost.parse("account.blob.example.test");

        Assertions.assertEquals("account", host.accountName());
        Assertions.assertEquals("example.test", host.cloudSuffix());
        Assertions.assertEquals("https://account.blob.example.test", host.blobEndpoint());
    }

    @Test
    void blobEndpoint_preservesTransportAndEncodedPathWhenConvertingDfsHost() {
        AzureAccountHost host = AzureAccountHost.parse(
                "http://account.dfs.core.chinacloudapi.cn:10000/proxy%2Fpath//a+b");

        Assertions.assertEquals("account.blob.core.chinacloudapi.cn", host.blobHost());
        Assertions.assertEquals("http://account.blob.core.chinacloudapi.cn:10000/proxy%2Fpath//a+b",
                host.blobEndpoint());
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://storage.example.test:8443/proxy%2Fpath",
            "http://127.0.0.1:10000/devstoreaccount1", "http://localhost:10000"})
    void parse_preservesCustomEndpointWithoutInventingServiceLabels(String endpoint) {
        AzureAccountHost host = AzureAccountHost.parse(endpoint);

        Assertions.assertEquals(endpoint, host.blobEndpoint());
        Assertions.assertEquals(host.blobHost(), host.dfsHost());
        Assertions.assertEquals("", host.cloudSuffix());
    }

    @Test
    void parse_rejectsEmptyHost() {
        Assertions.assertThrows(StoragePropertiesException.class, () -> AzureAccountHost.parse(" "));
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://proxy.dfs.internal", "http://proxy.dfs.internal:10000/base%2Fpath",
            "https://account.dfs.core.windows.net.proxy.test:8443", "https://onelake.dfs.fabric.microsoft.com"})
    void blobEndpoint_preservesNonAzureDfsEndpoints(String endpoint) {
        Assertions.assertEquals(endpoint, AzureAccountHost.parse(endpoint).blobEndpoint());
    }

    @ParameterizedTest
    @ValueSource(strings = {"core.windows.net", "core.chinacloudapi.cn", "core.usgovcloudapi.net", "core.cloudapi.de"})
    void blobEndpoint_convertsOfficialDfsHostsWithPorts(String suffix) {
        String endpoint = "https://account.dfs." + suffix + ":8443/base%2Fpath";
        Assertions.assertEquals("https://account.blob." + suffix + ":8443/base%2Fpath",
                AzureAccountHost.parse(endpoint).blobEndpoint());
    }
}
