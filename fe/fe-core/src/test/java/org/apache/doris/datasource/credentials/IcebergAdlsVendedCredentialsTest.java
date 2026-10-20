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


package org.apache.doris.datasource.credentials;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class IcebergAdlsVendedCredentialsTest {

    @Test
    void singleAccountBecomesSasProperties() {
        Map<String, String> vended = new HashMap<>();
        vended.put("adls.sas-token.acct.dfs.core.windows.net", "sv=2024&sig=abc");
        vended.put("adls.sas-token-expires-at-ms.acct.dfs.core.windows.net", "1792400400000");

        Map<String, String> props = IcebergAdlsVendedCredentials.toAzureProperties(vended);

        Assertions.assertEquals("SAS", props.get("azure.auth_type"));
        Assertions.assertEquals("acct", props.get("azure.account_name"));
        // BE uses the Blob endpoint of the account the dfs host names.
        Assertions.assertEquals("https://acct.blob.core.windows.net", props.get("azure.endpoint"));
        Assertions.assertEquals("sv=2024&sig=abc", props.get("azure.sas_token"));
        Assertions.assertEquals("1792400400000", props.get("azure.sas_token_expires_at_ms"));
    }

    @Test
    void sovereignCloudKeepsItsSuffixAndExpiryIsOptional() {
        Map<String, String> props = IcebergAdlsVendedCredentials.toAzureProperties(
                Map.of("adls.sas-token.acct.dfs.core.chinacloudapi.cn", "sig=abc"));

        Assertions.assertEquals("https://acct.blob.core.chinacloudapi.cn", props.get("azure.endpoint"));
        Assertions.assertFalse(props.containsKey("azure.sas_token_expires_at_ms"));
    }

    @Test
    void dfsAndBlobHostsOfOneAccountShareTheToken() {
        Map<String, String> vended = new HashMap<>();
        vended.put("adls.sas-token.acct.dfs.core.windows.net", "sig=abc");
        vended.put("adls.sas-token.acct.blob.core.windows.net", "sig=abc");
        vended.put("adls.sas-token-expires-at-ms.acct.dfs.core.windows.net", "2000");
        vended.put("adls.sas-token-expires-at-ms.acct.blob.core.windows.net", "1000");

        Map<String, String> props = IcebergAdlsVendedCredentials.toAzureProperties(vended);

        Assertions.assertEquals("acct", props.get("azure.account_name"));
        // The earlier expiry applies.
        Assertions.assertEquals("1000", props.get("azure.sas_token_expires_at_ms"));
    }

    @Test
    void credentialsWithoutAzureSasYieldNothing() {
        Map<String, String> vended = new HashMap<>();
        vended.put("s3.access-key-id", "ak");
        vended.put("adls.refresh-credentials-endpoint", "https://example.com/credentials");
        vended.put("adls.sas-token.acct.dfs.core.windows.net", "");

        Assertions.assertTrue(IcebergAdlsVendedCredentials.toAzureProperties(vended).isEmpty());
    }

    @Test
    void moreThanOneAccountIsRejected() {
        Map<String, String> vended = new HashMap<>();
        vended.put("adls.sas-token.acct1.dfs.core.windows.net", "sig=a");
        vended.put("adls.sas-token.acct2.dfs.core.windows.net", "sig=b");

        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergAdlsVendedCredentials.toAzureProperties(vended));
        Assertions.assertTrue(e.getMessage().contains("[acct1, acct2]"), e.getMessage());
    }

    @Test
    void differentTokensForOneAccountAreRejected() {
        Map<String, String> vended = new HashMap<>();
        vended.put("adls.sas-token.acct.dfs.core.windows.net", "sig=a");
        vended.put("adls.sas-token.acct.blob.core.windows.net", "sig=b");

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergAdlsVendedCredentials.toAzureProperties(vended));
    }
}
