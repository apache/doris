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

package org.apache.doris.datasource.storage;

import org.apache.doris.filesystem.properties.AzureBackendPropertyKeys;
import org.apache.doris.thrift.TAzureClientConfig;
import org.apache.doris.thrift.TAzureClientSecret;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class AzureThriftAdapterTest {

    private static Map<String, String> baseProps(String authType) {
        Map<String, String> props = new HashMap<>();
        props.put("AWS_ENDPOINT", "https://acct.blob.core.windows.net");
        props.put("provider", "azure");
        props.put(AzureBackendPropertyKeys.AUTH_TYPE, authType);
        props.put(AzureBackendPropertyKeys.ENDPOINT, "https://acct.blob.core.windows.net");
        props.put(AzureBackendPropertyKeys.ACCOUNT_NAME, "acct");
        return props;
    }

    @Test
    void sharedKeyBecomesSharedKeyCredential() {
        Map<String, String> props = baseProps(AzureBackendPropertyKeys.AUTH_SHARED_KEY);
        props.put(AzureBackendPropertyKeys.ACCOUNT_KEY, "key");
        props.put(AzureBackendPropertyKeys.CONTAINER, "c1");

        TAzureClientConfig config = AzureThriftAdapter.fromBackendProperties(props).orElseThrow();

        Assertions.assertEquals("https://acct.blob.core.windows.net", config.getEndpoint());
        Assertions.assertEquals("acct", config.getAccountName());
        Assertions.assertEquals("c1", config.getContainer());
        Assertions.assertTrue(config.getCredential().isSetSharedKey());
        Assertions.assertEquals("key", config.getCredential().getSharedKey().getAccountKey());
    }

    @Test
    void sasBecomesSasCredentialWithoutContainer() {
        Map<String, String> props = baseProps(AzureBackendPropertyKeys.AUTH_SAS);
        props.put(AzureBackendPropertyKeys.SAS_TOKEN, "sv=2024&sig=abc");

        TAzureClientConfig config = AzureThriftAdapter.fromBackendProperties(props).orElseThrow();

        Assertions.assertTrue(config.getCredential().isSetSas());
        Assertions.assertEquals("sv=2024&sig=abc", config.getCredential().getSas().getToken());
        // BE takes the container from each file location when it is not configured.
        Assertions.assertFalse(config.isSetContainer());
    }

    @Test
    void oauth2BecomesClientSecretCredential() {
        Map<String, String> props = baseProps(AzureBackendPropertyKeys.AUTH_OAUTH2);
        props.remove(AzureBackendPropertyKeys.ACCOUNT_NAME);
        props.put(AzureBackendPropertyKeys.TENANT_ID, "tenant");
        props.put(AzureBackendPropertyKeys.CLIENT_ID, "client");
        props.put(AzureBackendPropertyKeys.CLIENT_SECRET, "secret");
        props.put(AzureBackendPropertyKeys.AUTHORITY_HOST, "https://login.microsoftonline.com/");

        TAzureClientConfig config = AzureThriftAdapter.fromBackendProperties(props).orElseThrow();

        Assertions.assertFalse(config.isSetAccountName());
        Assertions.assertTrue(config.getCredential().isSetClientSecret());
        TAzureClientSecret secret = config.getCredential().getClientSecret();
        Assertions.assertEquals("tenant", secret.getTenantId());
        Assertions.assertEquals("client", secret.getClientId());
        Assertions.assertEquals("secret", secret.getClientSecret());
        Assertions.assertEquals("https://login.microsoftonline.com/", secret.getAuthorityHost());
    }

    @Test
    void mapWithoutNativeAzureKeysYieldsNothing() {
        Map<String, String> s3Props = new HashMap<>();
        s3Props.put("AWS_ENDPOINT", "s3.us-east-1.amazonaws.com");
        s3Props.put("AWS_ACCESS_KEY", "ak");
        s3Props.put("AWS_SECRET_KEY", "sk");

        Optional<TAzureClientConfig> config = AzureThriftAdapter.fromBackendProperties(s3Props);

        Assertions.assertFalse(config.isPresent());
    }

    @Test
    void unknownAuthTypeIsAProviderMismatch() {
        Assertions.assertThrows(IllegalStateException.class,
                () -> AzureThriftAdapter.fromBackendProperties(baseProps("ManagedIdentity")));
    }
}
