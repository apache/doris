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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AzureResourceTest {
    private static final Logger LOG = LogManager.getLogger(AzureResourceTest.class);

    @Test
    public void testPingAzure() {
        try {
            String azureAccoutName = System.getenv("AZURE_ACCOUNT_NAME");
            String azureAccoutKey = System.getenv("AZURE_ACCOUNT_KEY");
            String azureContainerName = System.getenv("AZURE_CONTAINER_NAME");

            Assumptions.assumeTrue(!Strings.isNullOrEmpty(azureAccoutName), "AZURE_ACCOUNT_NAME isNullOrEmpty.");
            Assumptions.assumeTrue(!Strings.isNullOrEmpty(azureAccoutKey), "AZURE_ACCOUNT_KEY isNullOrEmpty.");
            Assumptions.assumeTrue(!Strings.isNullOrEmpty(azureContainerName), "AZURE_CONTAINER_NAME isNullOrEmpty.");

            Map<String, String> properties = new HashMap<>();
            properties.put("s3.endpoint", "endpoint");
            properties.put("s3.region", "region");
            properties.put("s3.access_key", azureAccoutName);
            properties.put("s3.secret_key", azureAccoutKey);
            AzureResource.pingAzure(azureContainerName, "fe_ut_prefix", properties);
        } catch (DdlException e) {
            LOG.info("testPingAzure exception:", e);
            Assertions.assertTrue(false, e.getMessage());
        }
    }

    @Test
    public void testEndpointSchemeHandling() throws DdlException {
        // Test 1: endpoint without scheme should get https:// prefix
        AzureResource resource1 = new AzureResource("test1");
        Map<String, String> props1 = new HashMap<>();
        props1.put("s3.endpoint", "myaccount.blob.core.windows.net");
        props1.put("s3.region", "eu-west-1");
        props1.put("s3.access_key", "myaccount");
        props1.put("s3.secret_key", "mysecret");
        props1.put("s3.bucket", "mybucket");
        props1.put("s3.root.path", "mypath");
        props1.put("provider", "AZURE");
        props1.put("s3_validity_check", "false");
        resource1.setProperties(ImmutableMap.copyOf(props1));
        Map<String, String> result1 = resource1.getCopiedProperties();
        Assertions.assertEquals("https://myaccount.blob.core.windows.net",
                result1.get("s3.endpoint"),
                "Endpoint without scheme should get https:// prefix");

        // Test 2: endpoint with https:// should remain unchanged
        AzureResource resource2 = new AzureResource("test2");
        Map<String, String> props2 = new HashMap<>();
        props2.put("s3.endpoint", "https://myaccount.blob.core.windows.net");
        props2.put("s3.region", "eu-west-1");
        props2.put("s3.access_key", "myaccount");
        props2.put("s3.secret_key", "mysecret");
        props2.put("s3.bucket", "mybucket");
        props2.put("s3.root.path", "mypath");
        props2.put("provider", "AZURE");
        props2.put("s3_validity_check", "false");
        resource2.setProperties(ImmutableMap.copyOf(props2));
        Map<String, String> result2 = resource2.getCopiedProperties();
        Assertions.assertEquals("https://myaccount.blob.core.windows.net",
                result2.get("s3.endpoint"),
                "Endpoint with https:// should remain unchanged");

        // Test 3: endpoint with http:// should remain unchanged
        AzureResource resource3 = new AzureResource("test3");
        Map<String, String> props3 = new HashMap<>();
        props3.put("s3.endpoint", "http://myaccount.blob.core.windows.net");
        props3.put("s3.region", "eu-west-1");
        props3.put("s3.access_key", "myaccount");
        props3.put("s3.secret_key", "mysecret");
        props3.put("s3.bucket", "mybucket");
        props3.put("s3.root.path", "mypath");
        props3.put("provider", "AZURE");
        props3.put("s3_validity_check", "false");
        resource3.setProperties(ImmutableMap.copyOf(props3));
        Map<String, String> result3 = resource3.getCopiedProperties();
        Assertions.assertEquals("http://myaccount.blob.core.windows.net",
                result3.get("s3.endpoint"),
                "Endpoint with http:// should remain unchanged");
    }
}
