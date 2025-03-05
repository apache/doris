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

package org.apache.doris.datasource.property.constants;

import org.apache.doris.common.Config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AzurePropertiesTest {

    @Test
    public static void testFormatAzureEndpointGlobal() {
        Config.force_azure_blob_global_endpoint = true;
        String endpoint = AzureProperties.formatAzureEndpoint("ANY-ENDPOINT", "ak");
        Assertions.assertEquals("https://ak.blob.core.windows.net", endpoint);
    }

    @Test
    public static void testFormatAzureEndpoint() {
        Config.force_azure_blob_global_endpoint = false;
        String endpoint = AzureProperties.formatAzureEndpoint("ak.blob.core.chinacloudapi.cn", "ANY-ACCOUNT");
        Assertions.assertEquals("https://ak.blob.core.chinacloudapi.cn", endpoint);
    }

    @Test
    public static void testFormatAzureEndpointHTTPS() {
        Config.force_azure_blob_global_endpoint = false;
        String endpoint = AzureProperties.formatAzureEndpoint("https://ak.blob.core.chinacloudapi.cn", "ANY-ACCOUNT");
        Assertions.assertEquals("https://ak.blob.core.chinacloudapi.cn", endpoint);
    }

    @Test
    public static void testFormatAzureEndpointHTTP() {
        Config.force_azure_blob_global_endpoint = false;
        String endpoint = AzureProperties.formatAzureEndpoint("http://ak.blob.core.chinacloudapi.cn", "ANY-ACCOUNT");
        Assertions.assertEquals("http://ak.blob.core.chinacloudapi.cn", endpoint);
    }

}
