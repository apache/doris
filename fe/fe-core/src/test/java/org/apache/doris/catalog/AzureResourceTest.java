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
}
